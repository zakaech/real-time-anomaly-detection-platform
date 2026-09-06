"""Drives the whole fleet, one tick at a time.

The runner owns the loop and nothing else: it advances the clock, asks the
scheduler whether each machine is in an episode, and asks each machine for its
sample. Publication, delay and encoding live elsewhere, so the generator can be
exercised end to end in tests without a broker.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator
from dataclasses import dataclass
from datetime import datetime, timedelta

from event_simulator.anomalies.scheduler import EpisodeScheduler
from event_simulator.config.fleet import FleetConfig
from event_simulator.generation.clock import Clock
from event_simulator.generation.machine import MachineSimulator
from event_simulator.generation.rng import RngRegistry
from event_simulator.generation.state import GeneratedSample

__all__ = ["FleetRunner", "RunLimits", "TickBatch"]


@dataclass(frozen=True, slots=True)
class RunLimits:
    """When to stop. ``None`` on both means run until interrupted."""

    max_events: int | None = None
    duration: timedelta | None = None


@dataclass(frozen=True, slots=True)
class TickBatch:
    """Every machine's sample for one tick, in fleet declaration order."""

    tick: int
    event_time: datetime
    samples: tuple[GeneratedSample, ...]


class FleetRunner:
    """Produces successive :class:`TickBatch` values for the configured fleet."""

    def __init__(self, *, fleet: FleetConfig, clock: Clock, rng: RngRegistry) -> None:
        self._fleet = fleet
        self._clock = clock
        self._rng = rng

        start = clock.event_time(0)
        self._machines = [
            MachineSimulator(
                machine=machine, rng=rng, start=start, tick_interval=clock.tick_interval
            )
            for machine in fleet.resolved_machines()
        ]
        self._scheduler = EpisodeScheduler(
            config=fleet.anomalies,
            rng=rng,
            run_start=start,
            tick_interval=clock.tick_interval,
        )

    @property
    def scheduler(self) -> EpisodeScheduler:
        return self._scheduler

    @property
    def machine_count(self) -> int:
        return len(self._machines)

    @property
    def clamped_readings(self) -> int:
        """Total readings that hit a contract bound across the fleet."""
        return sum(machine.clamped_readings for machine in self._machines)

    def run(
        self,
        limits: RunLimits | None = None,
        should_stop: Callable[[], bool] | None = None,
    ) -> Iterator[TickBatch]:
        """Yield one batch per tick until a limit is reached.

        Pacing happens here, before the batch is produced, so a slow consumer
        cannot make the generator drift: the clock computes each deadline from
        the run origin rather than by accumulating per-tick sleeps.
        """
        limits = limits or RunLimits()
        emitted = 0
        tick = 0
        start = self._clock.event_time(0)

        while True:
            # Checked between ticks, never inside one: stopping mid-tick would
            # publish a partial fleet snapshot for that instant.
            if should_stop is not None and should_stop():
                return

            event_time = self._clock.event_time(tick)

            if limits.duration is not None and event_time - start >= limits.duration:
                return

            self._clock.pace(tick)

            samples: list[GeneratedSample] = []
            for machine in self._machines:
                if limits.max_events is not None and emitted >= limits.max_events:
                    break
                episode = self._scheduler.episode_at(machine.machine_id, event_time)
                samples.append(machine.step(event_time, episode))
                emitted += 1

            if samples:
                yield TickBatch(tick=tick, event_time=event_time, samples=tuple(samples))

            if limits.max_events is not None and emitted >= limits.max_events:
                return
            tick += 1
