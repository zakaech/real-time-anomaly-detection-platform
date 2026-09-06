"""Consume a topic, decode it with the real contracts, and report what is there.

More than a pretty printer, and deliberately so. ``kafka-console-consumer``
already shows bytes; what it cannot do is prove that those bytes decode into the
contract, or measure the shape of the stream. This tool does both, against the
Kafka already running -- it stands up no second infrastructure.

The measurement that matters for what comes next is the **lateness
distribution**, ``ingest_time - event_time``. docs/04-streaming-semantics.md
section 2.2 states that the watermark must be set from the observed distribution
rather than guessed; this is the tool that observes it. Every number it prints
comes from messages actually read.
"""

from __future__ import annotations

import argparse
import sys
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from telemetry_core.codec import decode
from telemetry_core.config import KafkaSettings, KafkaTopics, LoggingSettings, load_settings
from telemetry_core.errors import ConfigurationError, SchemaValidationError
from telemetry_core.logging import configure_logging, get_logger
from telemetry_core.schemas import TelemetryLabel, TelemetryRaw
from telemetry_core.timeutil import to_epoch_millis

from event_simulator import __version__

__all__ = ["main"]

_LOGGER = get_logger("event_simulator.tools.inspect")
_SERVICE = "telemetry-inspect"


def _percentile(sorted_values: list[float], fraction: float) -> float:
    """Nearest-rank percentile of an already sorted list."""
    if not sorted_values:
        return 0.0
    index = min(len(sorted_values) - 1, max(0, round(fraction * (len(sorted_values) - 1))))
    return sorted_values[index]


@dataclass
class RawTopicReport:
    """Everything observed on ``telemetry.raw``."""

    messages: int = 0
    decode_failures: int = 0
    key_mismatches: int = 0
    missing_keys: int = 0
    per_machine: Counter[str] = field(default_factory=Counter)
    per_partition: Counter[int] = field(default_factory=Counter)
    machine_partitions: dict[str, set[int]] = field(default_factory=dict)
    out_of_order: int = 0
    lateness_ms: list[float] = field(default_factory=list)
    null_readings: int = 0
    states: Counter[str] = field(default_factory=Counter)
    first_event_time: datetime | None = None
    last_event_time: datetime | None = None
    _last_seen: dict[str, datetime] = field(default_factory=dict)

    def observe(self, message: TelemetryRaw, key: str | None, partition: int) -> None:
        self.messages += 1
        self.per_machine[message.machine_id] += 1
        self.per_partition[partition] += 1
        self.states[message.machine_state.value] += 1
        self.machine_partitions.setdefault(message.machine_id, set()).add(partition)

        if key is None:
            self.missing_keys += 1
        elif key != message.machine_id:
            self.key_mismatches += 1

        self.lateness_ms.append(
            float(to_epoch_millis(message.ingest_time) - to_epoch_millis(message.event_time))
        )

        previous = self._last_seen.get(message.machine_id)
        if previous is not None and message.event_time < previous:
            # Consumed after a sample with a later event time, within the same
            # partition. This is exactly what a watermark has to absorb.
            self.out_of_order += 1
        self._last_seen[message.machine_id] = max(
            previous or message.event_time, message.event_time
        )

        self.null_readings += message.readings.null_count

        if self.first_event_time is None or message.event_time < self.first_event_time:
            self.first_event_time = message.event_time
        if self.last_event_time is None or message.event_time > self.last_event_time:
            self.last_event_time = message.event_time

    def render(self) -> str:
        lateness = sorted(self.lateness_ms)
        lines = [
            "telemetry.raw",
            f"  messages decoded      : {self.messages}",
            f"  decode failures       : {self.decode_failures}",
            f"  distinct machines     : {len(self.per_machine)}",
            f"  key == machine_id     : {self.messages - self.key_mismatches - self.missing_keys}"
            f"/{self.messages}  (mismatches={self.key_mismatches}, missing={self.missing_keys})",
            f"  machines on >1 part.  : "
            f"{sum(1 for parts in self.machine_partitions.values() if len(parts) > 1)}",
            f"  partitions used       : {sorted(self.per_partition)}",
            f"  null sensor readings  : {self.null_readings}",
            f"  out-of-order arrivals : {self.out_of_order}",
            f"  event_time range      : {self.first_event_time} -> {self.last_event_time}",
            "  lateness ingest-event (ms):",
            f"      p50={_percentile(lateness, 0.50):.0f}  "
            f"p95={_percentile(lateness, 0.95):.0f}  "
            f"p99={_percentile(lateness, 0.99):.0f}  "
            f"max={max(lateness) if lateness else 0:.0f}",
            "  machine_state counts  : "
            + ", ".join(f"{state}={count}" for state, count in sorted(self.states.items())),
            "  per-machine / partition:",
        ]
        for machine_id in sorted(self.per_machine):
            partitions = sorted(self.machine_partitions[machine_id])
            lines.append(
                f"      {machine_id}: {self.per_machine[machine_id]:>5} messages, "
                f"partition(s) {partitions}"
            )
        return "\n".join(lines)


@dataclass
class LabelTopicReport:
    """Everything observed on ``telemetry.labels``."""

    messages: int = 0
    decode_failures: int = 0
    anomalous: int = 0
    normal: int = 0
    per_type: Counter[str] = field(default_factory=Counter)
    per_machine: Counter[str] = field(default_factory=Counter)
    per_source: Counter[str] = field(default_factory=Counter)
    episodes: set[str] = field(default_factory=set)
    label_lag_ms: list[float] = field(default_factory=list)

    def observe(self, message: TelemetryLabel) -> None:
        self.messages += 1
        self.per_machine[message.machine_id] += 1
        self.per_source[message.source.value] += 1
        if message.is_anomaly:
            self.anomalous += 1
        else:
            self.normal += 1
        if message.anomaly_type is not None:
            self.per_type[message.anomaly_type.value] += 1
        if message.episode_id is not None:
            self.episodes.add(message.episode_id)
        self.label_lag_ms.append(
            float(to_epoch_millis(message.emitted_at) - to_epoch_millis(message.event_time))
        )

    def render(self) -> str:
        lag = sorted(self.label_lag_ms)
        lines = [
            "telemetry.labels",
            f"  messages decoded      : {self.messages}",
            f"  decode failures       : {self.decode_failures}",
            f"  anomalous / normal    : {self.anomalous} / {self.normal}",
            f"  distinct episodes     : {len(self.episodes)}",
            f"  distinct machines     : {len(self.per_machine)}",
            "  sources               : "
            + ", ".join(f"{name}={count}" for name, count in sorted(self.per_source.items())),
            "  anomaly types         : "
            + ", ".join(f"{name}={count}" for name, count in sorted(self.per_type.items())),
            f"  label lag emitted-event (ms): p50={_percentile(lag, 0.50):.0f} "
            f"max={max(lag) if lag else 0:.0f}",
        ]
        return "\n".join(lines)


def _build_consumer(settings: KafkaSettings, *, group_id: str, from_beginning: bool) -> Any:
    from confluent_kafka import Consumer

    return Consumer(
        {
            "bootstrap.servers": settings.bootstrap_servers,
            "security.protocol": settings.security_protocol,
            "group.id": group_id,
            # An inspection must not move the offsets of a real consumer group,
            # and must be repeatable.
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest" if from_beginning else "latest",
        }
    )


def _consume(
    consumer: Any, topic: str, *, max_messages: int, timeout_seconds: float
) -> list[tuple[bytes | None, bytes, int]]:
    """Read up to ``max_messages``, stopping after ``timeout_seconds`` of silence."""
    consumer.subscribe([topic])
    collected: list[tuple[bytes | None, bytes, int]] = []
    idle_polls = 0
    max_idle_polls = max(1, int(timeout_seconds))

    while len(collected) < max_messages and idle_polls < max_idle_polls:
        message = consumer.poll(1.0)
        if message is None:
            idle_polls += 1
            continue
        if message.error() is not None:
            _LOGGER.warning("consumer_error", error=str(message.error()))
            idle_polls += 1
            continue
        idle_polls = 0
        collected.append((message.key(), message.value(), message.partition()))
    return collected


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="telemetry-inspect",
        description="Consume and decode a platform topic, then report on it.",
    )
    parser.add_argument("--topic", help="Topic to read (default: telemetry.raw)")
    parser.add_argument(
        "--kind",
        choices=("raw", "label", "auto"),
        default="auto",
        help="How to decode messages; 'auto' infers from the topic name",
    )
    parser.add_argument("--max-messages", type=int, default=500)
    parser.add_argument("--timeout-seconds", type=float, default=10.0)
    parser.add_argument("--group-id", default="telemetry-inspect")
    parser.add_argument(
        "--from-latest",
        action="store_true",
        help="Read only new messages instead of replaying from the beginning",
    )
    parser.add_argument("--show", type=int, default=3, help="How many messages to print in full")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)

    logging_settings = load_settings(LoggingSettings)
    configure_logging(service=_SERVICE, version=__version__, level=logging_settings.log_level)

    try:
        kafka_settings = load_settings(KafkaSettings)
        topics = load_settings(KafkaTopics)
    except ConfigurationError as exc:
        _LOGGER.error("configuration_invalid", error=str(exc))
        return 2

    topic = args.topic or topics.telemetry_raw
    kind = args.kind
    if kind == "auto":
        kind = "label" if topic == topics.telemetry_labels else "raw"

    consumer = _build_consumer(
        kafka_settings, group_id=args.group_id, from_beginning=not args.from_latest
    )
    try:
        records = _consume(
            consumer,
            topic,
            max_messages=args.max_messages,
            timeout_seconds=args.timeout_seconds,
        )
    finally:
        consumer.close()

    if not records:
        print(f"no message read from {topic}")
        return 1

    raw_report = RawTopicReport()
    label_report = LabelTopicReport()

    for index, (key_bytes, value, partition) in enumerate(records):
        key = key_bytes.decode("utf-8") if key_bytes is not None else None
        try:
            if kind == "raw":
                message_raw = decode(TelemetryRaw, value)
                raw_report.observe(message_raw, key, partition)
                if index < args.show:
                    print(f"--- {topic} partition={partition} key={key}")
                    print(f"    {value.decode('utf-8')}")
            else:
                message_label = decode(TelemetryLabel, value)
                label_report.observe(message_label)
                if index < args.show:
                    print(f"--- {topic} partition={partition} key={key}")
                    print(f"    {value.decode('utf-8')}")
        except SchemaValidationError as exc:
            if kind == "raw":
                raw_report.decode_failures += 1
            else:
                label_report.decode_failures += 1
            _LOGGER.error("decode_failed", topic=topic, partition=partition, error=str(exc))

    print()
    print(raw_report.render() if kind == "raw" else label_report.render())

    failures = raw_report.decode_failures if kind == "raw" else label_report.decode_failures
    mismatches = raw_report.key_mismatches + raw_report.missing_keys if kind == "raw" else 0
    return 1 if failures or mismatches else 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
