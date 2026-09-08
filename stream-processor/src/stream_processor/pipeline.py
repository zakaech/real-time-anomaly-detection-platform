"""The three streaming queries, assembled.

Three rather than the two the plan sketched, and the reason is a shape
mismatch rather than a preference: a query that ends in a windowed aggregation
can only emit window-shaped rows, so it cannot also emit the per-message rows
that go to the dead letter and late topics. Splitting validation out gives each
query a single concern and a single checkpoint:

    validation   telemetry.raw   -> telemetry.dlq + telemetry.late   (stateless)
    scoring      telemetry.raw   -> telemetry.scored                 (windowed state)
    alerting     telemetry.scored-> alerts                           (per-machine state)

The cost is reading ``telemetry.raw`` twice. At this volume that is cheaper than
the alternative, which would be an intermediate "clean telemetry" topic nobody
asked for.

Note that no ``foreachBatch`` is needed anywhere: Spark's Kafka sink honours a
per-row ``topic`` column, so the validation query routes to two topics from a
single append-mode sink.
"""

from __future__ import annotations

import base64
from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from telemetry_core.config import KafkaSettings, KafkaTopics
from telemetry_core.enums import DlqReason  # noqa: F401  (documented vocabulary)

from stream_processor.alerting import (
    ALERT_OUTPUT_SCHEMA,
    ALERT_STATE_SCHEMA,
    ALERT_TIMEOUT,
    make_alert_state_handler,
)
from stream_processor.config.settings import StreamSettings
from stream_processor.features_spark import (
    aggregate_windows,
    derive_features,
    prepare_signals,
)
from stream_processor.model import ModelSpec
from stream_processor.schema import CORRUPT_RECORD_COLUMN, SCORED_EVENT_SCHEMA
from stream_processor.scoring import FEATURE_ARRAY_COLUMN, features_array, make_scored_encoder
from stream_processor.source import parse_telemetry, read_raw_stream, split_late, split_valid

__all__ = [
    "build_alerting_query",
    "build_scoring_stream",
    "build_validation_stream",
    "kafka_writer",
]


def kafka_writer(
    frame: DataFrame, kafka: KafkaSettings, checkpoint: str, *, output_mode: str
) -> Any:
    """Configure the Kafka sink shared by every query.

    ``acks=all`` because a message acknowledged by one replica is a message that
    can vanish. Note what is **not** set: Spark's Kafka sink opens no
    transaction, so it cannot tie the write to the offset commit. That is a
    limit of the connector, not a missing option, and it is why the platform is
    at-least-once with idempotent identifiers rather than exactly-once.
    """
    return (
        frame.writeStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka.bootstrap_servers)
        .option("kafka.security.protocol", kafka.security_protocol)
        .option("kafka.acks", "all")
        .option("kafka.compression.type", "lz4")
        .option("checkpointLocation", checkpoint)
        .outputMode(output_mode)
    )


def build_validation_stream(
    spark: Any,
    kafka: KafkaSettings,
    topics: KafkaTopics,
    settings: StreamSettings,
    *,
    processor_version: str,
) -> DataFrame:
    """Rejected and late messages, routed to their topics by a per-row column."""
    parsed = parse_telemetry(read_raw_stream(spark, kafka, topics, settings))
    accepted, rejected = split_valid(parsed)
    _on_time, too_late = split_late(accepted, settings)

    dlq = rejected.select(
        F.col("machine_id").alias("key"),
        F.to_json(
            F.struct(
                F.lit("1.0").alias("schema_version"),
                F.col("dlq_reason"),
                F.col("dlq_detail"),
                F.col("topic").alias("source_topic"),
                F.col("partition").alias("source_partition"),
                F.col("offset").alias("source_offset"),
                F.date_format(F.current_timestamp(), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").alias(
                    "failed_at"
                ),
                F.lit(processor_version).alias("processor_version"),
                # The ORIGINAL bytes, untouched. Replay after a fix is only
                # possible because nothing was normalised on the way in.
                F.base64(F.col("raw_payload")).alias("raw_payload"),
                F.base64(F.col("raw_key")).alias("raw_key"),
            )
        ).alias("value"),
        F.lit(topics.telemetry_dlq).alias("topic"),
    )

    late = too_late.select(
        F.col("machine_id").alias("key"),
        F.to_json(
            F.struct(
                F.col("machine_id"),
                F.col("event_id"),
                F.date_format(F.col("event_time"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").alias(
                    "event_time"
                ),
                F.col("lateness_seconds"),
                F.lit(settings.watermark_seconds).alias("watermark_seconds"),
                F.lit(processor_version).alias("processor_version"),
                F.base64(F.col("raw_payload")).alias("raw_payload"),
            )
        ).alias("value"),
        F.lit(topics.telemetry_late).alias("topic"),
    )

    return dlq.unionByName(late)


def build_scoring_stream(
    spark: Any,
    kafka: KafkaSettings,
    topics: KafkaTopics,
    settings: StreamSettings,
    spec: ModelSpec,
) -> DataFrame:
    """Windowed features, scored, encoded as ``ScoredEvent`` payloads."""
    parsed = parse_telemetry(read_raw_stream(spark, kafka, topics, settings))
    accepted, _rejected = split_valid(parsed)
    on_time, _too_late = split_late(accepted, settings)

    aggregated = aggregate_windows(
        prepare_signals(on_time),
        window_duration=settings.window_duration,
        slide_duration=settings.slide_duration,
        watermark_delay=settings.watermark_delay,
    )
    features = derive_features(aggregated).withColumn(FEATURE_ARRAY_COLUMN, features_array())

    encoder = make_scored_encoder(spec, maturity_tolerance_steps=settings.maturity_tolerance_steps)
    return features.select(
        F.col("machine_id").alias("key"),
        encoder(
            F.col("machine_id"),
            F.col("line_id"),
            F.col("window_start"),
            F.col("window_end"),
            F.col("sample_count"),
            F.col("running_ratio"),
            F.col("machine_state"),
            F.col("event_time_first"),
            F.col("event_time_last"),
            F.col(FEATURE_ARRAY_COLUMN),
        ).alias("value"),
        F.lit(topics.telemetry_scored).alias("topic"),
    )


def build_alerting_query(
    spark: Any, kafka: KafkaSettings, topics: KafkaTopics, settings: StreamSettings
) -> DataFrame:
    """Anomalous windows grouped into alert episodes, one alert per episode."""
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka.bootstrap_servers)
        .option("kafka.security.protocol", kafka.security_protocol)
        .option("subscribe", topics.telemetry_scored)
        .option("startingOffsets", settings.starting_offsets)
        .option("failOnDataLoss", "true")
        .load()
    )

    decoded = raw.select(
        F.from_json(
            F.col("value").cast("string"),
            SCORED_EVENT_SCHEMA,
            {"mode": "PERMISSIVE", "columnNameOfCorruptRecord": CORRUPT_RECORD_COLUMN},
        ).alias("event")
    ).select("event.*")

    anomalous = (
        decoded.filter(
            F.col(CORRUPT_RECORD_COLUMN).isNull()
            & (F.col("is_scored") == F.lit(True))
            & (F.col("is_anomaly") == F.lit(True))
            & F.col("machine_id").isNotNull()
        )
        .select(
            F.col("machine_id"),
            F.col("line_id"),
            F.to_timestamp(F.col("window_start"), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX").alias(
                "window_start"
            ),
            F.to_timestamp(F.col("window_end"), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX").alias("window_end"),
            F.col("anomaly_score"),
            F.col("score_threshold"),
            F.col("model.name").alias("model_name"),
            F.col("model.version").alias("model_version"),
            F.col("top_contributors"),
        )
        # The timeout is on event time, so the watermark has to advance with the
        # data rather than with our clock -- otherwise an accelerated replay
        # would close episodes the machine never ended.
        .withWatermark("window_end", settings.watermark_delay)
    )

    handler = make_alert_state_handler(
        consecutive_to_open=settings.consecutive_windows_to_open,
        gap_seconds=settings.alert_gap_seconds,
    )
    alerts = anomalous.groupBy("machine_id").applyInPandasWithState(
        handler,
        outputStructType=ALERT_OUTPUT_SCHEMA,
        stateStructType=ALERT_STATE_SCHEMA,
        outputMode="Update",
        timeoutConf=ALERT_TIMEOUT,
    )

    routed: DataFrame = alerts.select(
        F.col("machine_id").alias("key"),
        F.col("payload").alias("value"),
        F.lit(topics.alerts).alias("topic"),
    )
    return routed


def decode_base64(value: str) -> bytes:
    """Helper mirroring the DLQ encoding, used by the inspection tool."""
    return base64.b64decode(value)
