"""Reading ``telemetry.raw`` and sorting it into valid, rejected and late.

The parse is deliberately in two stages. ``from_json`` in PERMISSIVE mode parks
an unparsable payload in a corrupt-record column instead of failing the batch;
a second pass then checks the fields the contract marks required. Both produce a
*reason*, because "something was invalid" is not a dead letter an operator can
act on.

Lateness is evaluated **before** the aggregation. Spark drops late rows silently
inside a windowed aggregation -- the counter exists in the progress report but
the row itself is gone -- and Structured Streaming has no side output. Branching
first is how the information is kept.
"""

from __future__ import annotations

from typing import Any

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from telemetry_core.config import KafkaSettings, KafkaTopics
from telemetry_core.enums import DlqReason
from telemetry_core.features import MIN_RUNNING_RATIO  # noqa: F401  (documented gate)

from stream_processor.config.settings import StreamSettings
from stream_processor.schema import (
    CORRUPT_RECORD_COLUMN,
    EVENT_TIME_FORMAT,
    TELEMETRY_RAW_SCHEMA,
)

__all__ = [
    "KAFKA_METADATA_COLUMNS",
    "epoch_millis",
    "parse_telemetry",
    "read_raw_stream",
    "split_late",
    "split_valid",
]

#: Kept from the Kafka source so a rejected message can be located again.
KAFKA_METADATA_COLUMNS = ("topic", "partition", "offset")


def epoch_millis(column: Column) -> Column:
    """Milliseconds since the epoch, exactly.

    Not ``cast("double") * 1000``: a double carries about 15 significant digits
    and an epoch in milliseconds needs 13, so rounding could turn ``.412`` into
    ``.411`` and move a sample across a window boundary. Seconds plus the
    millisecond field is exact integer arithmetic.
    """
    return column.cast("long") * F.lit(1000) + F.date_format(column, "SSS").cast("long")


def read_raw_stream(
    spark: Any, kafka: KafkaSettings, topics: KafkaTopics, settings: StreamSettings
) -> DataFrame:
    """Open the Kafka source for ``telemetry.raw``."""
    reader = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka.bootstrap_servers)
        .option("kafka.security.protocol", kafka.security_protocol)
        .option("subscribe", topics.telemetry_raw)
        .option("startingOffsets", settings.starting_offsets)
        # A silent gap is worse than a stop for a detection system: if the
        # offsets we need have been aged out by retention, fail loudly.
        .option("failOnDataLoss", "true")
    )
    if settings.max_offsets_per_trigger is not None:
        # Rate limiting -- what backpressure actually is in Structured
        # Streaming. spark.streaming.backpressure.enabled is a DStream setting
        # and does nothing here.
        reader = reader.option("maxOffsetsPerTrigger", str(settings.max_offsets_per_trigger))
    stream: DataFrame = reader.load()
    return stream


def parse_telemetry(raw: DataFrame) -> DataFrame:
    """Decode the payload and attach a rejection reason where one applies."""
    decoded = raw.select(
        F.col("topic"),
        F.col("partition"),
        F.col("offset"),
        F.col("key").alias("raw_key"),
        F.col("value").alias("raw_payload"),
        F.from_json(
            F.col("value").cast("string"),
            TELEMETRY_RAW_SCHEMA,
            {"mode": "PERMISSIVE", "columnNameOfCorruptRecord": CORRUPT_RECORD_COLUMN},
        ).alias("message"),
    )

    flattened = decoded.select(
        "topic",
        "partition",
        "offset",
        "raw_key",
        "raw_payload",
        F.col(f"message.{CORRUPT_RECORD_COLUMN}").alias(CORRUPT_RECORD_COLUMN),
        F.col("message.schema_version").alias("schema_version"),
        F.col("message.event_id").alias("event_id"),
        F.col("message.machine_id").alias("machine_id"),
        F.col("message.line_id").alias("line_id"),
        # Parsed explicitly rather than by from_json: a malformed instant must
        # be distinguishable from an absent one, and only an explicit conversion
        # makes that visible.
        F.to_timestamp(F.col("message.event_time"), EVENT_TIME_FORMAT).alias("event_time"),
        F.col("message.event_time").alias("event_time_raw"),
        F.to_timestamp(F.col("message.ingest_time"), EVENT_TIME_FORMAT).alias("ingest_time"),
        F.col("message.machine_state").alias("machine_state"),
        F.col("message.readings.*"),
    )

    unsupported_major = F.col("schema_version").isNotNull() & (
        ~F.col("schema_version").startswith("1.")
    )
    missing_required = (
        F.col("schema_version").isNull()
        | F.col("event_id").isNull()
        | F.col("machine_id").isNull()
        | F.col("line_id").isNull()
        | F.col("event_time").isNull()
        | F.col("machine_state").isNull()
    )

    return flattened.withColumn(
        "dlq_reason",
        F.when(F.col(CORRUPT_RECORD_COLUMN).isNotNull(), F.lit(DlqReason.MALFORMED_JSON.value))
        .when(unsupported_major, F.lit(DlqReason.UNSUPPORTED_SCHEMA_VERSION.value))
        .when(missing_required, F.lit(DlqReason.SCHEMA_VALIDATION_FAILED.value))
        .otherwise(F.lit(None).cast("string")),
    ).withColumn(
        "dlq_detail",
        F.when(
            F.col(CORRUPT_RECORD_COLUMN).isNotNull(),
            F.concat(
                F.lit("payload is not valid JSON for the v1 telemetry contract: "),
                F.substring(F.col(CORRUPT_RECORD_COLUMN), 1, 200),
            ),
        )
        .when(
            unsupported_major,
            F.concat(
                F.lit("unsupported schema_version "),
                F.col("schema_version"),
                F.lit("; this consumer speaks 1.x"),
            ),
        )
        .when(
            missing_required,
            F.concat_ws(
                ", ",
                F.array_compact(
                    F.array(
                        F.when(F.col("schema_version").isNull(), F.lit("schema_version")),
                        F.when(F.col("event_id").isNull(), F.lit("event_id")),
                        F.when(F.col("machine_id").isNull(), F.lit("machine_id")),
                        F.when(F.col("line_id").isNull(), F.lit("line_id")),
                        F.when(
                            F.col("event_time").isNull(),
                            F.concat(
                                F.lit("event_time (raw="),
                                F.coalesce(F.col("event_time_raw"), F.lit("null")),
                                F.lit(")"),
                            ),
                        ),
                        F.when(F.col("machine_state").isNull(), F.lit("machine_state")),
                    )
                ),
            ),
        )
        .otherwise(F.lit(None).cast("string")),
    )


def split_valid(parsed: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Separate messages the contract accepts from those it rejects."""
    rejected = parsed.filter(F.col("dlq_reason").isNotNull())
    accepted = parsed.filter(F.col("dlq_reason").isNull()).drop(
        CORRUPT_RECORD_COLUMN, "dlq_reason", "dlq_detail", "event_time_raw"
    )
    return accepted, rejected


def split_late(accepted: DataFrame, settings: StreamSettings) -> tuple[DataFrame, DataFrame]:
    """Separate rows the watermark will still accept from those it will not.

    Lateness is measured as ``ingest_time - event_time``: how long the producer
    held a sample before publishing it. That is the physical cause the platform
    models -- a gateway buffering through a network outage -- and unlike a
    comparison against our own clock it does not depend on when the job happens
    to run.

    The distinction matters, and getting it wrong is easy. Comparing
    ``now() - event_time`` looks equivalent and is not: during an accelerated
    replay every sample is hours old by that measure, so the whole backfill
    would be routed to the late topic and the aggregation would see nothing.

    Even so this remains an approximation of Spark's watermark, which tracks
    ``max(event_time)`` rather than any clock. The two agree in steady state and
    diverge while catching up -- acceptable for observability, which is all the
    late topic is, and the reason nothing else depends on it.

    Set ``late_detection_enabled`` to false for a backfill: producer lag is
    meaningless when the producer is replaying history.
    """
    lateness = F.unix_timestamp(F.col("ingest_time")) - F.unix_timestamp(F.col("event_time"))
    annotated = accepted.withColumn("lateness_seconds", lateness)

    if not settings.late_detection_enabled:
        empty = annotated.filter(F.lit(False))
        return annotated.drop("lateness_seconds"), empty

    threshold = F.lit(settings.watermark_seconds)
    on_time = annotated.filter(
        F.col("lateness_seconds").isNull() | (F.col("lateness_seconds") <= threshold)
    )
    too_late = annotated.filter(
        F.col("lateness_seconds").isNotNull() & (F.col("lateness_seconds") > threshold)
    )
    return on_time.drop("lateness_seconds"), too_late
