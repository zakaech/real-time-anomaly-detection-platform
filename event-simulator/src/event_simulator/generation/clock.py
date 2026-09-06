"""Simulated time, and the pacing that separates real-time from replay.

Two distinct notions live here, and confusing them is the classic mistake:

* **event time** -- when a sample was measured. It goes into ``event_time`` and
  is what Phase 3 will window on.
* **wall-clock time** -- when the process actually publishes. It goes into
  ``ingest_time``.

Real-time and replay are **not two code paths**. They are the same clock with
different parameters: a start instant and a speed factor. Real-time starts now
and runs at speed 1; replay starts in the past and runs as fast as it can. One
engine, two configurations, so there is no second behaviour to keep consistent
with the first.

``sleep`` and ``wall_clock`` are injected, which is what makes pacing testable
without a test that actually waits.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from datetime import datetime, timedelta
from typing import Protocol

from telemetry_core.timeutil import ensure_utc, utc_now

__all__ = ["Clock", "SimulationClock"]


class Clock(Protocol):
    """The time source a generator run depends on."""

    @property
    def tick_interval(self) -> timedelta: ...

    def event_time(self, tick: int) -> datetime:
        """Simulated instant of ``tick``."""
        ...

    def pace(self, tick: int) -> None:
        """Block until ``tick`` may be published, if the mode requires it."""
        ...

    def wall_now(self) -> datetime:
        """Current wall-clock instant, used for ``ingest_time``."""
        ...


class SimulationClock:
    """Event time from a fixed origin, pacing from a speed factor.

    Args:
        start: Simulated instant of tick 0. ``utc_now()`` for real time, a past
            instant for replay.
        tick_interval: Simulated time between two samples of a machine.
        speed_factor: Simulated seconds produced per real second. ``1.0`` is real
            time; ``60.0`` runs an hour a minute; ``None`` means unbounded --
            produce as fast as the process can, which is what a bulk replay
            wants.
        sleep: Injected for tests.
        wall_clock: Injected for tests.
    """

    __slots__ = (
        "_sleep",
        "_speed_factor",
        "_start",
        "_tick_interval",
        "_wall_clock",
        "_wall_start",
    )

    def __init__(
        self,
        *,
        start: datetime,
        tick_interval: timedelta,
        speed_factor: float | None,
        sleep: Callable[[float], None] = time.sleep,
        wall_clock: Callable[[], datetime] = utc_now,
    ) -> None:
        if tick_interval <= timedelta(0):
            raise ValueError(f"tick_interval must be positive, got {tick_interval}")
        if speed_factor is not None and speed_factor <= 0:
            raise ValueError(f"speed_factor must be positive or None, got {speed_factor}")

        self._start = ensure_utc(start)
        self._tick_interval = tick_interval
        self._speed_factor = speed_factor
        self._sleep = sleep
        self._wall_clock = wall_clock
        self._wall_start = wall_clock()

    @classmethod
    def realtime(
        cls,
        *,
        tick_interval: timedelta,
        sleep: Callable[[float], None] = time.sleep,
        wall_clock: Callable[[], datetime] = utc_now,
    ) -> SimulationClock:
        """Event time tracks the wall clock."""
        return cls(
            start=wall_clock(),
            tick_interval=tick_interval,
            speed_factor=1.0,
            sleep=sleep,
            wall_clock=wall_clock,
        )

    @classmethod
    def replay(
        cls,
        *,
        start: datetime,
        tick_interval: timedelta,
        speed_factor: float | None = None,
        sleep: Callable[[float], None] = time.sleep,
        wall_clock: Callable[[], datetime] = utc_now,
    ) -> SimulationClock:
        """Event time starts in the past and advances faster than the wall clock.

        ``ingest_time`` remains the real instant of publication, so replayed
        samples carry a large and perfectly honest source lag: that is what a
        backfill genuinely looks like.
        """
        return cls(
            start=start,
            tick_interval=tick_interval,
            speed_factor=speed_factor,
            sleep=sleep,
            wall_clock=wall_clock,
        )

    @property
    def tick_interval(self) -> timedelta:
        return self._tick_interval

    @property
    def start(self) -> datetime:
        return self._start

    @property
    def speed_factor(self) -> float | None:
        return self._speed_factor

    def event_time(self, tick: int) -> datetime:
        if tick < 0:
            raise ValueError(f"tick must not be negative, got {tick}")
        return self._start + self._tick_interval * tick

    def pace(self, tick: int) -> None:
        """Sleep until this tick is due.

        Pacing is computed from the run origin rather than by sleeping a fixed
        amount per tick: accumulating per-tick sleeps lets the error from every
        slow iteration pile up, and the run drifts further behind real time the
        longer it lasts.
        """
        if self._speed_factor is None:
            return
        simulated_elapsed = self._tick_interval.total_seconds() * tick
        due = self._wall_start + timedelta(seconds=simulated_elapsed / self._speed_factor)
        remaining = (due - self._wall_clock()).total_seconds()
        if remaining > 0:
            self._sleep(remaining)

    def wall_now(self) -> datetime:
        return ensure_utc(self._wall_clock())
