"""One machine: hidden state, operating lifecycle, and anomaly application.

Order of operations in a tick, and why it is that order:

1. **Lifecycle** advances first, because motion gates every sensor.
2. **Load and baseline wear** advance next -- the machine's own slow evolution,
   independent of any anomaly.
3. **State-level injection** is applied to a *copy*, never persisted. An episode
   is a transient occurrence: each one must be independent for evaluation, and
   permanent damage would mean the first bearing-wear episode silently changed
   the machine for the rest of the run. Lasting degradation is what
   ``wear.rate_per_hour`` models.
4. **Observation** turns the state into five coupled measurements.
5. **Thermal state is persisted from the pre-injection reading**, so a
   temperature spike leaves no trail. A wear episode does leave one, through the
   physics, which is correct: friction genuinely heats a machine.
6. **Reading-level injection** last, because a failed sensor reports something
   other than what the machine is doing.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from telemetry_core.enums import MachineState

from event_simulator.anomalies.episode import Episode
from event_simulator.anomalies.injectors import InjectionContext, injector_for
from event_simulator.config.fleet import ResolvedMachine
from event_simulator.generation.physics import (
    clamp_to_physical_limits,
    next_load,
    observe,
    steady_state_temperature,
)
from event_simulator.generation.rng import RngRegistry
from event_simulator.generation.state import GeneratedSample, HiddenState, SampleGroundTruth

__all__ = ["MachineSimulator"]

_SECONDS_PER_HOUR = 3600.0


class MachineSimulator:
    """Stateful simulation of a single machine."""

    def __init__(
        self,
        *,
        machine: ResolvedMachine,
        rng: RngRegistry,
        start: datetime,
        tick_interval: timedelta,
    ) -> None:
        self._machine = machine
        self._profile = machine.profile
        self._tick_seconds = tick_interval.total_seconds()
        self._noise = rng.stream("noise", machine.machine_id)
        self._lifecycle_rng = rng.stream("lifecycle", machine.machine_id)
        self._episode_memory: dict[str, dict[str, float]] = {}
        self._clamped_readings = 0

        lifecycle = self._profile.simulation.lifecycle
        self._state = HiddenState(
            load=self._profile.simulation.load.mean,
            wear=self._profile.simulation.wear.initial,
            # Start at thermal equilibrium: an arbitrary initial temperature
            # would produce a warm-up transient at the start of every run that
            # looks like an anomaly and is not one.
            temperature_c=steady_state_temperature(self._profile, start),
            machine_state=MachineState.RUNNING,
            # Random residual so the fleet does not change state in unison,
            # which would produce a fleet-wide artefact every 15 minutes.
            state_seconds_remaining=float(
                self._lifecycle_rng.uniform(0.0, lifecycle.running_seconds)
            ),
        )

    @property
    def machine_id(self) -> str:
        return self._machine.machine_id

    @property
    def clamped_readings(self) -> int:
        """How many readings hit a contract bound. Non-zero means misconfiguration."""
        return self._clamped_readings

    @property
    def hidden_state(self) -> HiddenState:
        """Exposed for tests and diagnostics only; never published."""
        return self._state

    def step(self, at: datetime, episode: Episode | None) -> GeneratedSample:
        """Advance one tick and produce the sample measured at ``at``."""
        self._advance_lifecycle()
        self._advance_load_and_wear(at)

        observation_state = self._state
        context: InjectionContext | None = None
        if episode is not None:
            memory = self._episode_memory.setdefault(episode.episode_id, {})
            context = InjectionContext(
                episode=episode, progress=episode.progress(at), memory=memory
            )
            observation_state = injector_for(episode.family).affect_state(
                observation_state, context
            )

        readings = observe(observation_state, self._profile, at, self._noise)

        # Persist the thermal state before reading-level injection, so a
        # measurement excursion does not become part of the machine's history.
        settled_temperature = readings.temperature_c
        if settled_temperature is not None:
            self._state = self._state.advanced(temperature_c=settled_temperature)

        if episode is not None and context is not None:
            readings = injector_for(episode.family).affect_readings(readings, context)

        readings, clamped = clamp_to_physical_limits(readings)
        self._clamped_readings += clamped

        self._state = self._state.advanced(ticks=self._state.ticks + 1)

        return GeneratedSample(
            machine_id=self._machine.machine_id,
            line_id=self._machine.line_id,
            event_time=at,
            machine_state=self._state.machine_state,
            readings=readings,
            firmware_version=self._machine.firmware_version,
            ground_truth=(
                SampleGroundTruth.normal()
                if episode is None
                else SampleGroundTruth(
                    is_anomaly=True,
                    anomaly_type=episode.anomaly_type,
                    episode_id=episode.episode_id,
                    episode_started_at=episode.started_at,
                )
            ),
        )

    def _advance_load_and_wear(self, at: datetime) -> None:
        wear_config = self._profile.simulation.wear
        # Wear only accumulates while the machine actually turns. A stopped
        # machine that keeps ageing would make maintenance windows look like
        # degradation.
        turning = self._state.machine_state in (MachineState.RUNNING, MachineState.STARTING)
        wear_increment = (
            wear_config.rate_per_hour * self._tick_seconds / _SECONDS_PER_HOUR if turning else 0.0
        )
        self._state = self._state.advanced(
            load=next_load(self._state, at, self._profile, self._noise),
            wear=min(wear_config.max, self._state.wear + wear_increment),
        )

    def _advance_lifecycle(self) -> None:
        remaining = self._state.state_seconds_remaining - self._tick_seconds
        if remaining > 0.0:
            self._state = self._state.advanced(state_seconds_remaining=remaining)
            return

        lifecycle = self._profile.simulation.lifecycle
        current = self._state.machine_state

        if current is MachineState.RUNNING:
            goes_idle = float(self._lifecycle_rng.random()) < lifecycle.idle_probability
            next_state = MachineState.IDLE if goes_idle else MachineState.MAINTENANCE
            mean = lifecycle.idle_seconds if goes_idle else lifecycle.maintenance_seconds
        elif current in (MachineState.IDLE, MachineState.MAINTENANCE, MachineState.STOPPED):
            next_state = MachineState.STARTING
            mean = lifecycle.starting_seconds
        else:  # STARTING or UNKNOWN
            next_state = MachineState.RUNNING
            mean = lifecycle.running_seconds

        # Exponential residence times: state changes are memoryless, so the
        # fleet does not settle into a visible period that a windowed feature
        # would pick up as structure.
        duration = max(self._tick_seconds, float(self._lifecycle_rng.exponential(mean)))

        # Maintenance restores the machine. Without this, baseline wear would
        # grow without bound and the fleet would drift out of its nominal ranges
        # over a long replay -- an artefact, not a finding.
        restored_wear = (
            self._profile.simulation.wear.initial
            if current is MachineState.MAINTENANCE
            else self._state.wear
        )

        self._state = self._state.advanced(
            machine_state=next_state,
            state_seconds_remaining=duration,
            wear=restored_wear,
        )
