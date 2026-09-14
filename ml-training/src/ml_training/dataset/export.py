"""Kafka to Parquet: a bounded, reproducible snapshot of the history.

Training reads a file, not a topic. Three reasons, and the first is the one that
matters:

* **Reproducibility.** The snapshot has a SHA-256 that goes into the model's
  metadata, so an artefact names the exact bytes it was trained on. A job that
  consumed the topic directly could only say "whatever was in Kafka that day".
* Training can be re-run many times without the broker.
* Retention cannot silently change the dataset under a long experiment.

The read is **bounded**: end offsets are captured before consuming, and the run
stops on reaching them. Two exports of an unchanged topic therefore produce
identical files.

Duplicates are removed on ``event_id``. The pipeline is at-least-once by design
(docs/04 section 4), so a replayed batch can deliver the same sample twice; left
in place it would corrupt ``sample_count`` and every mean computed from it.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
from telemetry_core.codec import decode
from telemetry_core.config import KafkaSettings, KafkaTopics
from telemetry_core.errors import SchemaValidationError
from telemetry_core.logging import get_logger
from telemetry_core.schemas import SENSOR_NAMES, TelemetryLabel, TelemetryRaw
from telemetry_core.timeutil import format_instant

__all__ = ["ExportManifest", "export_dataset", "load_manifest", "sha256_of"]

_LOGGER = get_logger(__name__)

SAMPLES_FILE = "samples.parquet"
LABELS_FILE = "labels.parquet"
MANIFEST_FILE = "manifest.json"


@dataclass(frozen=True)
class ExportManifest:
    """Provenance of a snapshot. Everything here is observed, nothing assumed."""

    exported_at: str
    telemetry_topic: str
    labels_topic: str
    telemetry_end_offsets: dict[str, int]
    labels_end_offsets: dict[str, int]
    telemetry_rows: int
    telemetry_duplicates_removed: int
    telemetry_decode_failures: int
    labels_rows: int
    labels_decode_failures: int
    event_time_min: str | None
    event_time_max: str | None
    machines: int
    samples_sha256: str
    labels_sha256: str
    #: Operator-supplied: the simulator seed that produced this history. Kafka
    #: carries no such field -- and should not, since it is not telemetry -- so
    #: this is declared provenance rather than measured provenance.
    source_seed: int | None = None
    source_note: str | None = None

    def to_json(self) -> str:
        return json.dumps(asdict(self), indent=2, sort_keys=True)


def sha256_of(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _build_consumer(settings: KafkaSettings, group_id: str) -> Any:
    from confluent_kafka import Consumer

    return Consumer(
        {
            "bootstrap.servers": settings.bootstrap_servers,
            "security.protocol": settings.security_protocol,
            "group.id": group_id,
            # An export must never move a real consumer group's offsets, and it
            # must be repeatable.
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        }
    )


def _drain_topic(consumer: Any, topic: str, handler: Callable[[bytes], None]) -> dict[str, int]:
    """Read a topic from the beginning up to the end offsets captured now.

    Each payload is handed to ``handler`` and released immediately rather than
    accumulated. Holding a few million raw messages and then a few million row
    dictionaries costs several gigabytes, and the run would fail on a longer
    history -- which is exactly when an export matters most.
    """
    from confluent_kafka import OFFSET_BEGINNING, TopicPartition

    metadata = consumer.list_topics(topic, timeout=30.0)
    if topic not in metadata.topics or metadata.topics[topic].error is not None:
        raise RuntimeError(f"topic {topic!r} is not available")

    partition_ids = sorted(metadata.topics[topic].partitions)
    end_offsets: dict[int, int] = {}
    for partition in partition_ids:
        _low, high = consumer.get_watermark_offsets(
            TopicPartition(topic, partition), timeout=30.0, cached=False
        )
        end_offsets[partition] = high

    remaining = {p: offset for p, offset in end_offsets.items() if offset > 0}
    if not remaining:
        return {str(p): o for p, o in end_offsets.items()}

    consumer.assign([TopicPartition(topic, partition, OFFSET_BEGINNING) for partition in remaining])

    idle_polls = 0
    while remaining and idle_polls < 20:
        message = consumer.poll(1.0)
        if message is None:
            idle_polls += 1
            continue
        if message.error() is not None:
            _LOGGER.warning("consumer_error", topic=topic, error=str(message.error()))
            idle_polls += 1
            continue
        idle_polls = 0
        handler(message.value())
        partition = message.partition()
        if message.offset() >= remaining.get(partition, 0) - 1:
            remaining.pop(partition, None)

    consumer.unassign()
    return {str(p): o for p, o in end_offsets.items()}


class _ChunkedFrameBuilder:
    """Accumulates decoded rows and packs them into DataFrames in batches.

    A list of a few million dictionaries is the expensive shape; a DataFrame of
    the same rows is an order of magnitude smaller because pandas packs the
    numeric columns into typed arrays. Flushing every ``chunk_size`` rows keeps
    the peak at one batch plus the packed chunks, instead of the whole history in
    its most wasteful representation.
    """

    def __init__(self, chunk_size: int = 250_000) -> None:
        self._chunk_size = chunk_size
        self._rows: list[dict[str, Any]] = []
        self._chunks: list[pd.DataFrame] = []
        self.failures = 0

    def append(self, row: dict[str, Any]) -> None:
        self._rows.append(row)
        if len(self._rows) >= self._chunk_size:
            self._flush()

    def _flush(self) -> None:
        if self._rows:
            self._chunks.append(pd.DataFrame(self._rows))
            self._rows = []

    def build(self) -> pd.DataFrame:
        self._flush()
        if not self._chunks:
            return pd.DataFrame()
        frame = pd.concat(self._chunks, ignore_index=True)
        self._chunks = []
        return frame


def _telemetry_handler(builder: _ChunkedFrameBuilder) -> Callable[[bytes], None]:
    def handle(payload: bytes) -> None:
        try:
            message = decode(TelemetryRaw, payload)
        except SchemaValidationError as exc:
            builder.failures += 1
            _LOGGER.error("telemetry_decode_failed", error=str(exc))
            return
        readings = message.readings.as_mapping()
        builder.append(
            {
                "event_id": message.event_id,
                "machine_id": message.machine_id,
                "line_id": message.line_id,
                "event_time": message.event_time,
                "ingest_time": message.ingest_time,
                "machine_state": message.machine_state.value,
                **{name: readings[name] for name in SENSOR_NAMES},
            }
        )

    return handle


def _label_handler(builder: _ChunkedFrameBuilder) -> Callable[[bytes], None]:
    def handle(payload: bytes) -> None:
        try:
            message = decode(TelemetryLabel, payload)
        except SchemaValidationError as exc:
            builder.failures += 1
            _LOGGER.error("label_decode_failed", error=str(exc))
            return
        builder.append(
            {
                "event_id": message.event_id,
                "machine_id": message.machine_id,
                "event_time": message.event_time,
                "is_anomaly": message.is_anomaly,
                "anomaly_type": (
                    message.anomaly_type.value if message.anomaly_type is not None else None
                ),
                "episode_id": message.episode_id,
                "episode_started_at": message.episode_started_at,
                "emitted_at": message.emitted_at,
            }
        )

    return handle


def export_dataset(
    *,
    kafka_settings: KafkaSettings,
    topics: KafkaTopics,
    destination: Path,
    group_id: str = "ml-training-export",
    source_seed: int | None = None,
    source_note: str | None = None,
) -> ExportManifest:
    """Snapshot both topics into ``destination`` and describe what was captured."""
    destination.mkdir(parents=True, exist_ok=True)

    telemetry_builder = _ChunkedFrameBuilder()
    label_builder = _ChunkedFrameBuilder()

    consumer = _build_consumer(kafka_settings, group_id)
    try:
        telemetry_offsets = _drain_topic(
            consumer, topics.telemetry_raw, _telemetry_handler(telemetry_builder)
        )
        label_offsets = _drain_topic(
            consumer, topics.telemetry_labels, _label_handler(label_builder)
        )
    finally:
        consumer.close()

    samples = telemetry_builder.build()
    labels = label_builder.build()
    telemetry_failures = telemetry_builder.failures
    label_failures = label_builder.failures

    if samples.empty:
        raise RuntimeError(
            f"no telemetry read from {topics.telemetry_raw}: run the simulator first"
        )

    # At-least-once delivery means the same sample can arrive twice. Removing
    # duplicates here rather than downstream keeps sample_count exact, which
    # every windowed mean depends on.
    before = len(samples)
    samples = samples.drop_duplicates(subset=["event_id"], keep="first")
    duplicates_removed = before - len(samples)

    samples = samples.sort_values(["machine_id", "event_time"]).reset_index(drop=True)
    if not labels.empty:
        labels = labels.drop_duplicates(subset=["event_id", "machine_id", "event_time"])
        labels = labels.sort_values(["machine_id", "event_time"]).reset_index(drop=True)

    samples_path = destination / SAMPLES_FILE
    labels_path = destination / LABELS_FILE
    samples.to_parquet(samples_path, index=False)
    labels.to_parquet(labels_path, index=False)

    event_min: datetime | None = samples["event_time"].min()
    event_max: datetime | None = samples["event_time"].max()

    manifest = ExportManifest(
        exported_at=format_instant(pd.Timestamp.utcnow().to_pydatetime()),
        telemetry_topic=topics.telemetry_raw,
        labels_topic=topics.telemetry_labels,
        telemetry_end_offsets=telemetry_offsets,
        labels_end_offsets=label_offsets,
        telemetry_rows=len(samples),
        telemetry_duplicates_removed=int(duplicates_removed),
        telemetry_decode_failures=telemetry_failures,
        labels_rows=len(labels),
        labels_decode_failures=label_failures,
        event_time_min=format_instant(event_min) if event_min is not None else None,
        event_time_max=format_instant(event_max) if event_max is not None else None,
        machines=int(samples["machine_id"].nunique()),
        samples_sha256=sha256_of(samples_path),
        labels_sha256=sha256_of(labels_path),
        source_seed=source_seed,
        source_note=source_note,
    )
    (destination / MANIFEST_FILE).write_text(manifest.to_json(), encoding="utf-8")

    _LOGGER.info(
        "dataset_exported",
        destination=str(destination),
        telemetry_rows=manifest.telemetry_rows,
        labels_rows=manifest.labels_rows,
        duplicates_removed=manifest.telemetry_duplicates_removed,
        machines=manifest.machines,
    )
    return manifest


def load_manifest(destination: Path) -> ExportManifest:
    payload = json.loads((destination / MANIFEST_FILE).read_text(encoding="utf-8"))
    return ExportManifest(**payload)
