"""Read the output topics back and report what is actually in them.

Every number this prints is counted from messages that exist in Kafka. It is the
tool used to produce the figures in docs/10, so that no measurement in the
documentation comes from anywhere else.

Two of the numbers exist because Phase 3 decided something and promised to
measure the consequence rather than assume it:

* the **update ratio** -- how many times a single window is published, given
  that ``telemetry.scored`` is written in update mode (decision D-36). This is
  the cost of choosing low latency over one-shot emission, and the plan said to
  measure it before optimising anything;
* the **processing delay** -- ``scored_at - window_end``, the contract's own
  latency field, so the reported latency is the one the platform publishes
  rather than a second definition invented here. It is reported twice: over all
  emissions, where it is negative because update mode publishes a window while
  it still fills, and over **scored** emissions only, which is the delay before
  a window is actually judged. Reporting only the first would understate the
  latency; reporting only the second would hide what update mode buys.
"""

from __future__ import annotations

import argparse
import json
import statistics
import sys
from collections import Counter
from typing import Any

from telemetry_core.codec import decode
from telemetry_core.config import KafkaSettings, KafkaTopics, load_settings
from telemetry_core.dlq import DlqEnvelope
from telemetry_core.errors import SchemaValidationError
from telemetry_core.schemas import Alert, ScoredEvent

__all__ = ["main"]


def _consume(bootstrap: str, topic: str, limit: int, timeout: float) -> list[bytes]:
    """Read a topic from the beginning, up to the end offsets captured now."""
    from confluent_kafka import OFFSET_BEGINNING, Consumer, TopicPartition

    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": "stream-inspect",
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        }
    )
    try:
        metadata = consumer.list_topics(topic, timeout=30.0)
        partitions = sorted(metadata.topics[topic].partitions)
        ends = {
            partition: consumer.get_watermark_offsets(
                TopicPartition(topic, partition), timeout=30.0, cached=False
            )[1]
            for partition in partitions
        }
        remaining = {p: end for p, end in ends.items() if end > 0}
        if not remaining:
            return []
        consumer.assign([TopicPartition(topic, p, OFFSET_BEGINNING) for p in remaining])

        payloads: list[bytes] = []
        idle = 0
        while remaining and idle < 20 and len(payloads) < limit:
            message = consumer.poll(timeout)
            if message is None:
                idle += 1
                continue
            if message.error() is not None:
                idle += 1
                continue
            idle = 0
            payloads.append(message.value())
            if message.offset() >= remaining.get(message.partition(), 0) - 1:
                remaining.pop(message.partition(), None)
        return payloads
    finally:
        consumer.close()


def _report_scored(payloads: list[bytes]) -> dict[str, Any]:
    windows: Counter[tuple[str, str]] = Counter()
    delays: list[int] = []
    scored_delays: list[int] = []
    scored = 0
    anomalous = 0
    skips: Counter[str] = Counter()
    machines: Counter[str] = Counter()
    failures = 0

    for payload in payloads:
        try:
            event = decode(ScoredEvent, payload)
        except SchemaValidationError:
            failures += 1
            continue
        windows[(event.machine_id, event.window_start.isoformat())] += 1
        delays.append(event.processing_delay_ms)
        machines[event.machine_id] += 1
        if event.is_scored:
            scored += 1
            scored_delays.append(event.processing_delay_ms)
            if event.is_anomaly:
                anomalous += 1
        elif event.skip_reason is not None:
            skips[event.skip_reason.value] += 1

    distinct = len(windows)
    return {
        "messages": len(payloads),
        "decode_failures": failures,
        "distinct_windows": distinct,
        "update_ratio": (len(payloads) / distinct) if distinct else 0.0,
        "max_emissions_for_one_window": max(windows.values()) if windows else 0,
        "scored_windows": scored,
        "anomalous_windows": anomalous,
        "skipped_by_reason": dict(skips),
        "machines": len(machines),
        # Across every emission, scored or not. Dominated by windows still
        # filling, so it describes publication earliness, not decision latency.
        "processing_delay_ms": {
            "min": min(delays) if delays else None,
            "median": int(statistics.median(delays)) if delays else None,
            "max": max(delays) if delays else None,
        },
        # The one that answers "how long until a window is judged". Only mature
        # windows are scored (D-37), so these are necessarily at or after
        # window_end and the figure is a real latency rather than an earliness.
        "scored_delay_ms": {
            "min": min(scored_delays) if scored_delays else None,
            "median": int(statistics.median(scored_delays)) if scored_delays else None,
            "max": max(scored_delays) if scored_delays else None,
        },
    }


def _report_alerts(payloads: list[bytes]) -> dict[str, Any]:
    identifiers: Counter[str] = Counter()
    severities: Counter[str] = Counter()
    machines: Counter[str] = Counter()
    consecutive: list[int] = []
    failures = 0

    for payload in payloads:
        try:
            alert = decode(Alert, payload)
        except SchemaValidationError:
            failures += 1
            continue
        identifiers[alert.alert_id] += 1
        severities[alert.severity.value] += 1
        machines[alert.machine_id] += 1
        consecutive.append(alert.consecutive_windows)

    duplicates = sum(count - 1 for count in identifiers.values() if count > 1)
    return {
        "messages": len(payloads),
        "decode_failures": failures,
        "distinct_alert_ids": len(identifiers),
        # At-least-once delivery makes republished identifiers possible; the
        # downstream upsert absorbs them. Counting them is how we know how many.
        "duplicate_deliveries": duplicates,
        "by_severity": dict(severities),
        "machines": len(machines),
        "consecutive_windows_at_open": {
            "min": min(consecutive) if consecutive else None,
            "max": max(consecutive) if consecutive else None,
        },
    }


def _report_dlq(payloads: list[bytes]) -> dict[str, Any]:
    reasons: Counter[str] = Counter()
    replayable = 0
    failures = 0
    for payload in payloads:
        try:
            envelope = DlqEnvelope.from_dict(json.loads(payload.decode("utf-8")))
        except (SchemaValidationError, ValueError, UnicodeDecodeError):
            failures += 1
            continue
        reasons[envelope.dlq_reason.value] += 1
        # The original bytes must come back, or the queue is not replayable.
        if envelope.decoded_payload():
            replayable += 1
    return {
        "messages": len(payloads),
        "decode_failures": failures,
        "by_reason": dict(reasons),
        "with_recoverable_payload": replayable,
    }


def _report_late(payloads: list[bytes]) -> dict[str, Any]:
    lateness: list[float] = []
    for payload in payloads:
        record = json.loads(payload.decode("utf-8"))
        value = record.get("lateness_seconds")
        if value is not None:
            lateness.append(float(value))
    return {
        "messages": len(payloads),
        "lateness_seconds": {
            "min": min(lateness) if lateness else None,
            "median": statistics.median(lateness) if lateness else None,
            "max": max(lateness) if lateness else None,
        },
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="inspect-stream", description="Report what the output topics contain."
    )
    parser.add_argument(
        "--topics",
        default="scored,alerts,dlq,late",
        help="Comma-separated subset of scored,alerts,dlq,late.",
    )
    parser.add_argument("--limit", type=int, default=200_000)
    parser.add_argument("--timeout", type=float, default=2.0)
    args = parser.parse_args(argv)

    kafka = load_settings(KafkaSettings)
    topics = load_settings(KafkaTopics)
    wanted = {name.strip() for name in args.topics.split(",") if name.strip()}

    plan: list[tuple[str, str, Any]] = [
        ("scored", topics.telemetry_scored, _report_scored),
        ("alerts", topics.alerts, _report_alerts),
        ("dlq", topics.telemetry_dlq, _report_dlq),
        ("late", topics.telemetry_late, _report_late),
    ]

    report: dict[str, Any] = {}
    for name, topic, reporter in plan:
        if name not in wanted:
            continue
        payloads = _consume(kafka.bootstrap_servers, topic, args.limit, args.timeout)
        report[name] = {"topic": topic, **reporter(payloads)}

    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
