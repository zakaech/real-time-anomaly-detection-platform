"""Decides when an anomaly episode starts, and which one.

Episodes arrive as a Poisson process per machine, plus any deterministic
scenarios declared in the configuration. The two coexist on purpose: the random
process builds an evaluation set with realistic irregularity, while scenarios
reproduce one specific case on demand -- for a demonstration, or to pin a
regression.

Every draw comes from the ``anomaly`` stream, which is independent of the
``noise`` stream by construction (``generation/rng.py``). Enabling anomalies
therefore cannot shift the noise sequence, so the noise carries no trace of the
injection for a model to latch onto.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta

from numpy.random import Generator
from telemetry_core.enums import AnomalyType

from event_simulator.anomalies.episode import Episode, format_episode_id
from event_simulator.config.fleet import AnomaliesConfig, AnomalySpec
from event_simulator.generation.rng import RngRegistry

__all__ = ["EpisodeScheduler"]

_SECONDS_PER_HOUR = 3600.0


@dataclass
class _MachineEpisodeState:
    active: Episode | None = None
    last_ended_at: datetime | None = None
    sequence: int = 0
    fired_scenarios: set[int] = field(default_factory=set)


class EpisodeScheduler:
    """Owns the episode lifecycle for the whole fleet."""

    def __init__(
        self,
        *,
        config: AnomaliesConfig,
        rng: RngRegistry,
        run_start: datetime,
        tick_interval: timedelta,
    ) -> None:
        self._config = config
        self._rng = rng
        self._run_start = run_start
        self._tick_seconds = tick_interval.total_seconds()
        self._states: dict[str, _MachineEpisodeState] = {}

        # Probability that an episode starts on a given tick, from the configured
        # hourly rate. Expressed per tick rather than per hour so the rate stays
        # invariant when the sampling interval changes.
        self._start_probability = config.episodes_per_hour * self._tick_seconds / _SECONDS_PER_HOUR

    def _state_for(self, machine_id: str) -> _MachineEpisodeState:
        state = self._states.get(machine_id)
        if state is None:
            state = _MachineEpisodeState()
            self._states[machine_id] = state
        return state

    def episode_at(self, machine_id: str, at: datetime) -> Episode | None:
        """Return the episode active on ``machine_id`` at ``at``, starting one if due.

        Called once per machine per tick. Retires a finished episode before
        considering a new one, so two episodes can never overlap on the same
        machine -- overlapping ground truth would make an evaluation ambiguous.
        """
        state = self._state_for(machine_id)

        if state.active is not None:
            if state.active.is_active(at):
                return state.active
            state.last_ended_at = state.active.ends_at
            state.active = None

        scenario_episode = self._scenario_episode(machine_id, at, state)
        if scenario_episode is not None:
            state.active = scenario_episode
            return scenario_episode

        if not self._config.enabled or self._start_probability <= 0.0:
            return None

        if not self._cooldown_elapsed(state, at):
            return None

        generator = self._rng.stream("anomaly", machine_id)
        if float(generator.random()) >= self._start_probability:
            return None

        state.active = self._draw_episode(machine_id, at, state, generator)
        return state.active

    def _cooldown_elapsed(self, state: _MachineEpisodeState, at: datetime) -> bool:
        """Keep a quiet gap after an episode.

        Back-to-back episodes would blur into one long anomaly, and the
        evaluation would count a single degradation as several detections.
        """
        if state.last_ended_at is None:
            return True
        elapsed = (at - state.last_ended_at).total_seconds()
        return elapsed >= self._config.min_gap_seconds

    def _scenario_episode(
        self, machine_id: str, at: datetime, state: _MachineEpisodeState
    ) -> Episode | None:
        for index, scenario in enumerate(self._config.scenarios):
            if scenario.machine_id != machine_id or index in state.fired_scenarios:
                continue
            starts_at = self._run_start + timedelta(seconds=scenario.start_offset_seconds)
            if at < starts_at:
                continue

            spec = self._config.spec_for(scenario.type)
            intensity = (
                scenario.intensity
                if scenario.intensity is not None
                else spec.intensity.sample_between(0.5)
            )
            state.fired_scenarios.add(index)
            state.sequence += 1
            return Episode(
                episode_id=format_episode_id(machine_id, at, state.sequence),
                machine_id=machine_id,
                anomaly_type=scenario.type,
                family=spec.family,
                sensor=spec.sensor,
                started_at=at,
                ends_at=at + timedelta(seconds=scenario.duration_seconds),
                intensity=intensity,
            )
        return None

    def _draw_episode(
        self,
        machine_id: str,
        at: datetime,
        state: _MachineEpisodeState,
        generator: Generator,
    ) -> Episode:
        spec = self._weighted_choice(generator)
        duration = spec.duration_seconds.sample_between(float(generator.random()))
        intensity = spec.intensity.sample_between(float(generator.random()))

        # Guard against a configuration whose duration is shorter than one tick:
        # such an episode would exist in the ground truth without ever producing
        # an anomalous sample, which would look like a missed detection.
        duration = max(duration, self._tick_seconds)

        state.sequence += 1
        return Episode(
            episode_id=format_episode_id(machine_id, at, state.sequence),
            machine_id=machine_id,
            anomaly_type=spec.type,
            family=spec.family,
            sensor=spec.sensor,
            started_at=at,
            ends_at=at + timedelta(seconds=duration),
            intensity=intensity,
        )

    def _weighted_choice(self, generator: Generator) -> AnomalySpec:
        total = sum(spec.weight for spec in self._config.types)
        target = float(generator.random()) * total
        cumulative = 0.0
        for spec in self._config.types:
            cumulative += spec.weight
            if target < cumulative:
                return spec
        return self._config.types[-1]  # pragma: no cover - float guard only

    def active_types(self) -> dict[str, AnomalyType]:
        """Currently active episode type per machine. Diagnostics only."""
        return {
            machine_id: state.active.anomaly_type
            for machine_id, state in self._states.items()
            if state.active is not None
        }
