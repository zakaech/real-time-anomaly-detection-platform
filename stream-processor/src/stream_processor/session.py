"""SparkSession construction.

Every configuration here is a decision with a reason, not a default copied from
a tutorial.
"""

from __future__ import annotations

from typing import Any

from pyspark.sql import SparkSession

from stream_processor.config.settings import StreamSettings

__all__ = ["KAFKA_PACKAGE", "build_session"]

#: Resolved into the image at build time so a run never depends on Maven being
#: reachable.
KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3"


def build_session(settings: StreamSettings, *, extra: dict[str, str] | None = None) -> Any:
    """Build the session shared by every query in the process."""
    builder = (
        SparkSession.builder.appName(settings.app_name)
        .master(settings.master)
        .config("spark.jars.packages", KAFKA_PACKAGE)
        # Arrow is what makes the scoring vectorised; without it every row would
        # cross the JVM/Python boundary on its own.
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.sql.execution.arrow.maxRecordsPerBatch", "10000")
        # RocksDB keeps window state off the JVM heap (decision D-10). The
        # default provider holds it in the heap, where growth becomes GC
        # pressure and then an OOM with no intermediate warning.
        .config(
            "spark.sql.streaming.stateStore.providerClass",
            "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider",
        )
        # Small fleet, small cluster: the default 200 shuffle partitions would
        # create 200 mostly-empty tasks per batch.
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
    )
    for key, value in (extra or {}).items():
        builder = builder.config(key, value)

    session = builder.getOrCreate()
    session.sparkContext.setLogLevel("WARN")
    return session
