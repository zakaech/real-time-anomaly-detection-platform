"""Event time, pacing, and the difference between real-time and replay."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from conftest import RUN_START, FakeWallClock
from event_simulator.generation.clock import SimulationClock


class TestEventTime:
    def test_event_time_advances_by_the_tick_interval(self) -> None:
        clock = SimulationClock(
            start=RUN_START,
            tick_interval=timedelta(seconds=1),
            speed_factor=None,
            sleep=lambda _s: None,
            wall_clock=lambda: RUN_START,
        )
        assert clock.event_time(0) == RUN_START
        assert clock.event_time(60) == RUN_START + timedelta(seconds=60)

    def test_naive_start_is_rejected(self) -> None:
        with pytest.raises(Exception, match="naive"):
            SimulationClock(
                start=datetime(2026, 3, 2, 8, 0, 0),  # noqa: DTZ001 - deliberate
                tick_interval=timedelta(seconds=1),
                speed_factor=None,
                sleep=lambda _s: None,
                wall_clock=lambda: RUN_START,
            )

    def test_non_positive_interval_is_rejected(self) -> None:
        with pytest.raises(ValueError, match="tick_interval"):
            SimulationClock(
                start=RUN_START,
                tick_interval=timedelta(0),
                speed_factor=None,
                sleep=lambda _s: None,
                wall_clock=lambda: RUN_START,
            )

    def test_negative_tick_is_rejected(self) -> None:
        clock = SimulationClock(
            start=RUN_START,
            tick_interval=timedelta(seconds=1),
            speed_factor=None,
            sleep=lambda _s: None,
            wall_clock=lambda: RUN_START,
        )
        with pytest.raises(ValueError, match="tick must not be negative"):
            clock.event_time(-1)


class TestRealtimeMode:
    def test_event_time_starts_at_the_wall_clock(self) -> None:
        wall = FakeWallClock(RUN_START)
        clock = SimulationClock.realtime(
            tick_interval=timedelta(seconds=1), sleep=wall.sleep, wall_clock=wall
        )
        assert clock.event_time(0) == RUN_START
        assert clock.speed_factor == 1.0

    def test_it_sleeps_one_interval_per_tick(self) -> None:
        wall = FakeWallClock(RUN_START)
        clock = SimulationClock.realtime(
            tick_interval=timedelta(seconds=1), sleep=wall.sleep, wall_clock=wall
        )
        for tick in range(4):
            clock.pace(tick)
        # Tick 0 is already due, so three sleeps of one second follow.
        assert wall.slept == [1.0, 1.0, 1.0]

    def test_pacing_does_not_drift_after_a_slow_tick(self) -> None:
        """Deadlines are computed from the run origin, not accumulated.

        Sleeping a fixed amount per tick would let the delay of every slow
        iteration pile up, and the run would fall further behind the longer it
        lasted.
        """
        wall = FakeWallClock(RUN_START)
        clock = SimulationClock.realtime(
            tick_interval=timedelta(seconds=1), sleep=wall.sleep, wall_clock=wall
        )
        clock.pace(0)
        # Simulate a tick that took 2.5 s of real work.
        wall.now = wall.now + timedelta(seconds=2.5)
        clock.pace(1)  # already late: no sleep
        assert wall.slept == []

        clock.pace(4)  # due at +4 s, wall is at +2.5 s
        assert wall.slept == [pytest.approx(1.5)]

    def test_speed_factor_two_halves_the_wait(self) -> None:
        wall = FakeWallClock(RUN_START)
        clock = SimulationClock(
            start=RUN_START,
            tick_interval=timedelta(seconds=1),
            speed_factor=2.0,
            sleep=wall.sleep,
            wall_clock=wall,
        )
        clock.pace(1)
        assert wall.slept == [pytest.approx(0.5)]


class TestReplayMode:
    def test_event_time_is_historical(self) -> None:
        start = RUN_START - timedelta(days=7)
        wall = FakeWallClock(RUN_START)
        clock = SimulationClock.replay(
            start=start,
            tick_interval=timedelta(seconds=1),
            speed_factor=None,
            sleep=wall.sleep,
            wall_clock=wall,
        )
        assert clock.event_time(0) == start
        assert clock.event_time(0) < clock.wall_now()

    def test_unbounded_speed_never_sleeps(self) -> None:
        wall = FakeWallClock(RUN_START)
        clock = SimulationClock.replay(
            start=RUN_START - timedelta(hours=1),
            tick_interval=timedelta(seconds=1),
            speed_factor=None,
            sleep=wall.sleep,
            wall_clock=wall,
        )
        for tick in range(1000):
            clock.pace(tick)
        assert wall.slept == []

    def test_bounded_speed_still_paces(self) -> None:
        wall = FakeWallClock(RUN_START)
        clock = SimulationClock.replay(
            start=RUN_START - timedelta(hours=1),
            tick_interval=timedelta(seconds=1),
            speed_factor=60.0,
            sleep=wall.sleep,
            wall_clock=wall,
        )
        clock.pace(60)
        assert wall.slept == [pytest.approx(1.0)]

    def test_zero_or_negative_speed_is_rejected(self) -> None:
        with pytest.raises(ValueError, match="speed_factor"):
            SimulationClock(
                start=RUN_START,
                tick_interval=timedelta(seconds=1),
                speed_factor=0.0,
                sleep=lambda _s: None,
                wall_clock=lambda: RUN_START,
            )

    def test_wall_now_is_utc_aware(self) -> None:
        wall = FakeWallClock(RUN_START)
        clock = SimulationClock.replay(
            start=RUN_START - timedelta(days=1),
            tick_interval=timedelta(seconds=1),
            sleep=wall.sleep,
            wall_clock=wall,
        )
        assert clock.wall_now().tzinfo is UTC
