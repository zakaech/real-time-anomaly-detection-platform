"""Shared fixtures.

No test in this package needs a broker or waits for wall-clock time: the clock
and the producer are both injected. A test suite that sleeps is a test suite
nobody runs.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from telemetry_core.config import KafkaTopics

from event_simulator.config.fleet import FleetConfig, load_fleet
from event_simulator.generation.clock import SimulationClock

# tests -> event-simulator -> repository root
_REPO_ROOT = Path(__file__).resolve().parents[2]
_SIMULATOR_ROOT = Path(__file__).resolve().parents[1]

RUN_START = datetime(2026, 3, 2, 8, 0, 0, tzinfo=UTC)


@pytest.fixture(scope="session")
def repo_root() -> Path:
    return _REPO_ROOT


@pytest.fixture(scope="session")
def schema_dir(repo_root: Path) -> Path:
    directory = repo_root / "contracts" / "json-schema"
    if not directory.is_dir():
        pytest.fail(f"contract schemas not found at {directory}")
    return directory


@pytest.fixture(scope="session")
def fleet_path() -> Path:
    path = _SIMULATOR_ROOT / "config" / "fleet.yaml"
    if not path.is_file():
        pytest.fail(f"fleet configuration not found at {path}")
    return path


@pytest.fixture
def fleet(fleet_path: Path) -> FleetConfig:
    """The real committed fleet, so tests exercise the shipped configuration."""
    return load_fleet(fleet_path)


@pytest.fixture
def topics() -> KafkaTopics:
    return KafkaTopics()


def load_json(path: Path) -> dict[str, Any]:
    with path.open(encoding="utf-8") as handle:
        payload: dict[str, Any] = json.load(handle)
    return payload


class FakeWallClock:
    """A wall clock the test advances explicitly."""

    def __init__(self, start: datetime) -> None:
        self.now = start
        self.slept: list[float] = []

    def __call__(self) -> datetime:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.slept.append(seconds)
        self.now = self.now + timedelta(seconds=seconds)


def make_clock(
    *,
    start: datetime = RUN_START,
    tick_seconds: float = 1.0,
    speed_factor: float | None = None,
    wall_clock: FakeWallClock | None = None,
) -> SimulationClock:
    """Build a clock that never touches real time.

    ``speed_factor=None`` by default: tests generate as fast as the interpreter
    allows while still producing the event times a real-time run would.
    """
    fake = wall_clock or FakeWallClock(start)
    return SimulationClock(
        start=start,
        tick_interval=timedelta(seconds=tick_seconds),
        speed_factor=speed_factor,
        sleep=fake.sleep,
        wall_clock=fake,
    )


class FakeProducer:
    """Records what would have been sent to Kafka.

    ``on_delivery`` fires synchronously, which is not what librdkafka does but is
    what a test needs: the delivered counter is meaningful immediately instead of
    only after a flush.
    """

    def __init__(self, *, buffer_failures: int = 0) -> None:
        self.messages: list[tuple[str, bytes, bytes]] = []
        self.poll_calls = 0
        self.flush_calls = 0
        self._buffer_failures = buffer_failures

    def produce(
        self,
        topic: str,
        *,
        key: bytes,
        value: bytes,
        on_delivery: Callable[[Any, Any], None],
    ) -> None:
        if self._buffer_failures > 0:
            self._buffer_failures -= 1
            raise BufferError("Local: Queue full")
        self.messages.append((topic, key, value))
        on_delivery(None, None)

    def poll(self, timeout: float) -> int:
        self.poll_calls += 1
        return 0

    def flush(self, timeout: float) -> int:
        self.flush_calls += 1
        return 0

    def __len__(self) -> int:
        return 0

    def payloads_for(self, topic: str) -> list[dict[str, Any]]:
        return [
            json.loads(value.decode("utf-8"))
            for message_topic, _key, value in self.messages
            if message_topic == topic
        ]

    def keys_for(self, topic: str) -> list[str]:
        return [
            key.decode("utf-8")
            for message_topic, key, _value in self.messages
            if message_topic == topic
        ]
