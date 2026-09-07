"""Measure what the admission rule actually rejects, one condition at a time.

The Phase 3 plan added ``last machine_state == RUNNING`` beside the existing
``running_ratio >= 0.9``, and required its effect to be **measured** rather than
asserted to be an improvement. The scored topic cannot answer that on its own:
both conditions collapse into a single ``MACHINE_NOT_RUNNING`` skip reason, and
``running_ratio`` is not a published feature, so the overlap between them is not
observable downstream.

So this reads ``telemetry.raw`` as a **batch** source and applies the same
windowing the streaming job uses -- the same ``parse_telemetry``,
``prepare_signals`` and ``aggregate_windows``, not a parallel reimplementation --
then counts each condition separately and, crucially, their overlap. The number
that matters is the **marginal** one: windows the state gate rejects that the
ratio would have admitted. That is the gate's entire contribution.

Batch, not streaming, on purpose: the question is about the whole history at
once, and a batch read has no watermark and therefore no eviction to reason
about. ``withWatermark`` is inert in batch mode, which is what makes the
aggregation reusable unchanged.
"""

from __future__ import annotations

import argparse
import json
import sys
from typing import Any

from pyspark.sql import functions as F
from telemetry_core.config import KafkaSettings, KafkaTopics, load_settings
from telemetry_core.features import MIN_RUNNING_RATIO, MIN_SAMPLES_FOR_SCORING

from stream_processor.config.settings import StreamSettings
from stream_processor.features_spark import aggregate_windows, prepare_signals
from stream_processor.session import build_session
from stream_processor.source import parse_telemetry, split_valid

__all__ = ["main"]

_RUNNING = "RUNNING"


def _read_batch(spark: Any, kafka: KafkaSettings, topic: str) -> Any:
    """The whole topic, once. ``failOnDataLoss`` is irrelevant to a bounded read."""
    return (
        spark.read.format("kafka")
        .option("kafka.bootstrap.servers", kafka.bootstrap_servers)
        .option("kafka.security.protocol", kafka.security_protocol)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("endingOffsets", "latest")
        .load()
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="measure-admission",
        description="Count what each admission condition rejects, and what the state gate adds.",
    )
    parser.parse_args(argv)

    kafka = load_settings(KafkaSettings)
    topics = load_settings(KafkaTopics)
    settings = StreamSettings()

    spark = build_session(settings, extra={"spark.sql.shuffle.partitions": "8"})
    try:
        accepted, _rejected = split_valid(
            parse_telemetry(_read_batch(spark, kafka, topics.telemetry_raw))
        )
        windows = aggregate_windows(
            prepare_signals(accepted),
            window_duration=settings.window_duration,
            slide_duration=settings.slide_duration,
            watermark_delay=settings.watermark_delay,
        )

        enough = F.col("__sample_count") >= F.lit(MIN_SAMPLES_FOR_SCORING)
        ratio_ok = F.col("running_ratio") >= F.lit(MIN_RUNNING_RATIO)
        state_ok = F.col("machine_state") == F.lit(_RUNNING)

        def count(condition: Any) -> Any:
            return F.sum(F.when(condition, F.lit(1)).otherwise(F.lit(0)))

        row = windows.agg(
            F.count(F.lit(1)).alias("windows_total"),
            count(~enough).alias("rejected_by_sample_count"),
            # Among windows with enough samples, so the three conditions are
            # compared on the same population instead of on nested ones.
            count(enough & ~ratio_ok).alias("rejected_by_running_ratio"),
            count(enough & ~state_ok).alias("rejected_by_state_gate"),
            count(enough & ~ratio_ok & ~state_ok).alias("rejected_by_both"),
            # The answer to the question the plan asked.
            count(enough & ratio_ok & ~state_ok).alias("rejected_by_state_gate_alone"),
            count(enough & ratio_ok & state_ok).alias("admitted"),
            count(enough & ratio_ok).alias("admitted_without_the_state_gate"),
        ).collect()[0]

        counts = {key: int(row[key]) for key in row.asDict()}
        admitted_without = counts["admitted_without_the_state_gate"]
        report: dict[str, Any] = dict(counts)
        report["state_gate_marginal_rejection_ratio"] = (
            round(counts["rejected_by_state_gate_alone"] / admitted_without, 6)
            if admitted_without
            else None
        )
        report["thresholds"] = {
            "min_samples_for_scoring": MIN_SAMPLES_FOR_SCORING,
            "min_running_ratio": MIN_RUNNING_RATIO,
            "window_seconds": settings.window_seconds,
            "slide_seconds": settings.slide_seconds,
        }
        print(json.dumps(report, indent=2, sort_keys=True))
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
