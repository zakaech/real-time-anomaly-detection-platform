"""Explicit Spark schemas, derived from the contract rather than inferred.

``inferSchema`` is never used on a stream. It would sample the data, guess, and
change its mind between restarts. An explicit ``StructType`` gives the two
properties the evolution policy depends on (docs/02 section 2): a field added
upstream is ignored, and a field missing upstream becomes ``null``. Neither
breaks the job.

Timestamps are read as **strings** and converted explicitly afterwards. Letting
``from_json`` parse them into ``TimestampType`` would turn a malformed instant
into a silent ``null`` indistinguishable from an absent field; converting
explicitly makes the failure visible and routable to the dead letter topic.
"""

from __future__ import annotations

from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)
from telemetry_core.schemas import SENSOR_NAMES

__all__ = [
    "CORRUPT_RECORD_COLUMN",
    "EVENT_TIME_FORMAT",
    "SCORED_EVENT_SCHEMA",
    "TELEMETRY_RAW_SCHEMA",
    "readings_schema",
]

#: Where ``from_json`` in PERMISSIVE mode parks an unparsable payload. The
#: column has to exist in the schema for the option to have any effect.
CORRUPT_RECORD_COLUMN = "_corrupt_record"

#: The contract's instant format: ISO-8601 UTC, milliseconds, ``Z`` suffix.
#: Pinned here so a parse failure is a contract violation rather than a guess.
EVENT_TIME_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"


def readings_schema() -> StructType:
    """The five measured quantities, all nullable.

    ``null`` means the sensor did not report; it never means zero. Declaring
    them nullable is what lets that distinction survive into the features.
    """
    return StructType([StructField(name, DoubleType(), nullable=True) for name in SENSOR_NAMES])


TELEMETRY_RAW_SCHEMA = StructType(
    [
        StructField("schema_version", StringType(), nullable=True),
        StructField("event_id", StringType(), nullable=True),
        StructField("machine_id", StringType(), nullable=True),
        StructField("line_id", StringType(), nullable=True),
        StructField("event_time", StringType(), nullable=True),
        StructField("ingest_time", StringType(), nullable=True),
        StructField("machine_state", StringType(), nullable=True),
        StructField("firmware_version", StringType(), nullable=True),
        StructField("readings", readings_schema(), nullable=True),
        StructField(CORRUPT_RECORD_COLUMN, StringType(), nullable=True),
    ]
)

#: Only what the alerting query needs from ``telemetry.scored``. Fields the
#: scoring query publishes but alerting does not read are deliberately absent:
#: declaring them would couple the two queries more tightly than the contract
#: requires.
SCORED_EVENT_SCHEMA = StructType(
    [
        StructField("schema_version", StringType(), nullable=True),
        StructField("machine_id", StringType(), nullable=True),
        StructField("line_id", StringType(), nullable=True),
        StructField("window_start", StringType(), nullable=True),
        StructField("window_end", StringType(), nullable=True),
        StructField("scored_at", StringType(), nullable=True),
        StructField("sample_count", LongType(), nullable=True),
        StructField("is_scored", BooleanType(), nullable=True),
        StructField("machine_state", StringType(), nullable=True),
        StructField("anomaly_score", DoubleType(), nullable=True),
        StructField("score_threshold", DoubleType(), nullable=True),
        StructField("is_anomaly", BooleanType(), nullable=True),
        StructField(
            "top_contributors",
            ArrayType(
                StructType(
                    [
                        StructField("feature", StringType(), nullable=True),
                        StructField("z_score", DoubleType(), nullable=True),
                    ]
                )
            ),
            nullable=True,
        ),
        StructField(
            "model",
            StructType(
                [
                    StructField("name", StringType(), nullable=True),
                    StructField("version", StringType(), nullable=True),
                    StructField("trained_at", StringType(), nullable=True),
                    StructField("artifact_sha256", StringType(), nullable=True),
                ]
            ),
            nullable=True,
        ),
        StructField(CORRUPT_RECORD_COLUMN, StringType(), nullable=True),
    ]
)
