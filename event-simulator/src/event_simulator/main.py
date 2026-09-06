"""Entry point.

Command-line flags override environment settings, which override defaults. The
ordering matters in practice: the container carries the environment, and an
operator overrides one value for one run without editing anything.
"""

from __future__ import annotations

import argparse
import signal
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import FrameType

from telemetry_core.config import KafkaSettings, KafkaTopics, LoggingSettings, load_settings
from telemetry_core.errors import ConfigurationError
from telemetry_core.logging import bind_trace_id, configure_logging, get_logger
from telemetry_core.timeutil import format_instant, utc_now

from event_simulator import __version__
from event_simulator.config.fleet import load_fleet
from event_simulator.config.settings import SimulatorSettings
from event_simulator.generation.clock import SimulationClock
from event_simulator.generation.fleet_runner import RunLimits
from event_simulator.generation.rng import RngRegistry, resolve_seed
from event_simulator.publishing.pipeline import SimulationPipeline
from event_simulator.publishing.producer import (
    PublishingAbortedError,
    TelemetryPublisher,
    build_kafka_producer,
)

__all__ = ["main"]

_LOGGER = get_logger("event_simulator.main")
_SERVICE = "event-simulator"

_interrupted = False


def _handle_signal(signum: int, _frame: FrameType | None) -> None:
    """Ask the run to stop at the next tick.

    A flag rather than an immediate exit: stopping mid-tick would leave messages
    in the producer's queue with no flush, and those are messages the broker
    never acknowledged.
    """
    global _interrupted
    _interrupted = True
    _LOGGER.info("shutdown_requested", signal=signum)


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="event-simulator",
        description="Generate labelled industrial telemetry into Kafka.",
    )
    parser.add_argument("--mode", choices=("realtime", "replay"))
    parser.add_argument("--fleet", type=Path, help="Path to fleet.yaml")
    parser.add_argument("--seed", type=int, help="Master seed for reproducible runs")
    parser.add_argument(
        "--speed", type=float, help="Simulated seconds per real second (replay only)"
    )
    parser.add_argument("--duration-seconds", type=float)
    parser.add_argument("--max-events", type=int)
    parser.add_argument(
        "--replay-start",
        help="ISO-8601 event time of tick 0 in replay mode (default: now minus duration)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Generate and validate without connecting to Kafka",
    )
    return parser.parse_args(argv)


def _build_settings(args: argparse.Namespace) -> SimulatorSettings:
    overrides: dict[str, object] = {}
    if args.mode is not None:
        overrides["mode"] = args.mode
    if args.fleet is not None:
        overrides["fleet_path"] = args.fleet
    if args.seed is not None:
        overrides["seed"] = args.seed
    if args.speed is not None:
        overrides["speed_factor"] = args.speed
    if args.duration_seconds is not None:
        overrides["duration_seconds"] = args.duration_seconds
    if args.max_events is not None:
        overrides["max_events"] = args.max_events
    if args.replay_start is not None:
        overrides["replay_start"] = datetime.fromisoformat(args.replay_start)

    try:
        return SimulatorSettings(**overrides)  # type: ignore[arg-type]
    except ValueError as exc:
        raise ConfigurationError(f"invalid simulator settings: {exc}") from exc


def _build_clock(settings: SimulatorSettings) -> SimulationClock:
    interval = timedelta(seconds=settings.tick_interval_seconds)
    if settings.mode == "realtime":
        return SimulationClock.realtime(tick_interval=interval)

    start = settings.replay_start
    if start is None:
        # A replay that ends at the present instant is what a backfill wants;
        # starting "now" and running into the future would not be a replay.
        span = settings.duration_seconds or 0.0
        start = utc_now() - timedelta(seconds=span)
    if start.tzinfo is None:
        start = start.replace(tzinfo=UTC)

    return SimulationClock.replay(
        start=start, tick_interval=interval, speed_factor=settings.speed_factor
    )


class _NullProducer:
    """Discards messages. Used by --dry-run to validate without a broker."""

    def __init__(self) -> None:
        self._count = 0

    def produce(self, topic: str, *, key: bytes, value: bytes, on_delivery: object) -> None:
        self._count += 1

    def poll(self, timeout: float) -> int:
        return 0

    def flush(self, timeout: float) -> int:
        return 0

    def __len__(self) -> int:
        return 0


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)

    logging_settings = load_settings(LoggingSettings)
    configure_logging(service=_SERVICE, version=__version__, level=logging_settings.log_level)
    bind_trace_id()

    try:
        settings = _build_settings(args)
        fleet = load_fleet(settings.fleet_path)
        topics = load_settings(KafkaTopics)
    except ConfigurationError as exc:
        _LOGGER.error("configuration_invalid", error=str(exc))
        return 2

    seed = resolve_seed(settings.seed)
    rng = RngRegistry(seed)
    clock = _build_clock(settings)

    if args.dry_run:
        producer: object = _NullProducer()
    else:
        try:
            kafka_settings = load_settings(KafkaSettings)
        except ConfigurationError as exc:
            _LOGGER.error("configuration_invalid", error=str(exc))
            return 2
        producer = build_kafka_producer(kafka_settings, client_id=settings.client_id)

    publisher = TelemetryPublisher(producer=producer, topics=topics)  # type: ignore[arg-type]
    pipeline = SimulationPipeline(fleet=fleet, clock=clock, rng=rng, publisher=publisher)

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    _LOGGER.info(
        "simulation_starting",
        mode=settings.mode,
        # Logged so any run, including one with a drawn seed, can be reproduced.
        seed=seed,
        machines=len(fleet.machines),
        tick_interval_seconds=settings.tick_interval_seconds,
        speed_factor=settings.effective_speed_factor,
        start_event_time=format_instant(clock.event_time(0)),
        emit_normal_labels=fleet.labels.emit_normal_labels,
        dry_run=args.dry_run,
        topic_telemetry=topics.telemetry_raw,
        topic_labels=topics.telemetry_labels,
    )

    limits = RunLimits(
        max_events=settings.max_events,
        duration=(
            timedelta(seconds=settings.duration_seconds)
            if settings.duration_seconds is not None
            else None
        ),
    )

    exit_code = 0
    try:
        result = pipeline.run(limits, should_stop=lambda: _interrupted)
    except PublishingAbortedError as exc:
        _LOGGER.error("simulation_aborted", error=str(exc))
        result = pipeline.result
        exit_code = 1
    finally:
        outstanding = publisher.flush(settings.flush_timeout_seconds)
        if outstanding:
            # Handed to the client but never acknowledged by the broker. Saying
            # the run succeeded here would be a lie the exit code has to tell.
            _LOGGER.error("flush_incomplete", outstanding=outstanding)
            exit_code = 1

    _LOGGER.info(
        "simulation_finished",
        seed=seed,
        exit_code=exit_code,
        **result.as_dict(),
        **publisher.stats.as_dict(),
    )
    return exit_code


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
