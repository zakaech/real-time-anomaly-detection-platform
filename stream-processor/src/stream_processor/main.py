"""Entry point: start the three queries and keep them running.

The artefact is verified **here, on the driver, before any query starts**. A
mismatch must stop the job at launch rather than at the first batch: by then
Kafka offsets have moved and a partial run has to be reasoned about.
"""

from __future__ import annotations

import argparse
import signal
import sys
import time
from types import FrameType
from typing import Any

from telemetry_core.config import KafkaSettings, KafkaTopics, LoggingSettings, load_settings
from telemetry_core.errors import ConfigurationError
from telemetry_core.features import FEATURE_NAMES, FEATURE_SET_VERSION
from telemetry_core.logging import configure_logging, get_logger

from stream_processor import __version__
from stream_processor.config.settings import StreamSettings
from stream_processor.model import ModelSpec, load_model
from stream_processor.monitoring import ProgressCollector
from stream_processor.pipeline import (
    build_alerting_query,
    build_scoring_stream,
    build_validation_stream,
    kafka_writer,
)
from stream_processor.session import build_session

__all__ = ["main", "run"]

_LOGGER = get_logger("stream_processor.main")
_SERVICE = "stream-processor"

_STOP = False

#: How long awaitAnyTermination blocks before we poll progress again.
#: SECONDS, not milliseconds: PySpark's awaitAnyTermination takes seconds,
#: and passing a millisecond value silently waits for hours instead.
_POLL_INTERVAL_SECONDS = 5.0


def _request_stop(_signum: int, _frame: FrameType | None) -> None:
    global _STOP
    _STOP = True


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="stream-processor", description="Score telemetry windows against the detector."
    )
    parser.add_argument("--run-seconds", type=float, help="Stop after this long (bounded runs).")
    parser.add_argument(
        "--queries",
        default="validation,scoring,alerting",
        help="Comma-separated subset of queries to start.",
    )
    return parser.parse_args(argv)


def run(settings: StreamSettings, *, queries: set[str], run_seconds: float | None) -> int:
    """Start the requested queries and block until stopped."""
    try:
        kafka = load_settings(KafkaSettings)
        topics = load_settings(KafkaTopics)
    except ConfigurationError as exc:
        _LOGGER.error("configuration_invalid", error=str(exc))
        return 2

    spec = ModelSpec(directory=str(settings.artifact_dir), verify=settings.verify_artifact)

    # Fail before a single offset moves.
    loaded = load_model(spec)
    _LOGGER.info(
        "artifact_verified",
        model=loaded.model_name,
        version=loaded.model_version,
        threshold=loaded.threshold,
        sha256=loaded.metadata.get("artifact_sha256"),
        sklearn_version=loaded.metadata.get("sklearn_version"),
        feature_set_version=FEATURE_SET_VERSION,
        features=len(FEATURE_NAMES),
    )

    spark = build_session(settings)
    collector = ProgressCollector()

    started: list[Any] = []
    if "validation" in queries:
        stream = build_validation_stream(
            spark, kafka, topics, settings, processor_version=__version__
        )
        started.append(
            kafka_writer(
                stream, kafka, str(settings.checkpoint_root / "validation"), output_mode="append"
            )
            .queryName("validation")
            .trigger(processingTime=settings.trigger_interval)
            .start()
        )
    if "scoring" in queries:
        stream = build_scoring_stream(spark, kafka, topics, settings, spec)
        started.append(
            kafka_writer(stream, kafka, str(settings.scoring_checkpoint), output_mode="update")
            .queryName("scoring")
            .trigger(processingTime=settings.trigger_interval)
            .start()
        )
    if "alerting" in queries:
        stream = build_alerting_query(spark, kafka, topics, settings)
        started.append(
            kafka_writer(stream, kafka, str(settings.alerting_checkpoint), output_mode="update")
            .queryName("alerting")
            .trigger(processingTime=settings.trigger_interval)
            .start()
        )

    _LOGGER.info(
        "queries_started",
        queries=[query.name for query in started],
        watermark_seconds=settings.watermark_seconds,
        window_seconds=settings.window_seconds,
        slide_seconds=settings.slide_seconds,
    )

    deadline = None if run_seconds is None else time.monotonic() + run_seconds
    exit_code = 0
    try:
        while not _STOP:
            # awaitAnyTermination blocks inside the JVM. Polling isActive over
            # Py4J every second instead floods the gateway: with three queries
            # that starved the driver badly enough that no batch ever ran.
            spark.streams.awaitAnyTermination(_POLL_INTERVAL_SECONDS)
            collector.poll(started)

            failed = [query for query in started if not query.isActive]
            if failed:
                for query in failed:
                    exception = query.exception()
                    if exception is not None:
                        _LOGGER.error("query_failed", query=query.name, error=str(exception))
                        exit_code = 1
                break
            if deadline is not None and time.monotonic() >= deadline:
                _LOGGER.info("run_deadline_reached", run_seconds=run_seconds)
                break
    finally:
        for query in started:
            if query.isActive:
                query.stop()
        for query in started:
            query.awaitTermination(60)
        collector.poll(started)
        _LOGGER.info(
            "queries_stopped", batches_observed=collector.batches, totals=collector.totals()
        )
        spark.stop()

    return exit_code


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    logging_settings = load_settings(LoggingSettings)
    configure_logging(service=_SERVICE, version=__version__, level=logging_settings.log_level)

    signal.signal(signal.SIGTERM, _request_stop)
    signal.signal(signal.SIGINT, _request_stop)

    settings = StreamSettings()
    queries = {name.strip() for name in args.queries.split(",") if name.strip()}
    return run(settings, queries=queries, run_seconds=args.run_seconds or settings.run_seconds)


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
