"""The Spark translation must reproduce the shared reference.

This closes the triangle the platform depends on:

    telemetry_core.compute_features        (the reference, semantics of record)
        |                    |
        | ml_training tests  | this file
        v                    v
    ml_training pandas <-> stream_processor Spark

**Bit-exact equality is neither achievable nor required.** ``math.fsum`` in the
reference, pandas' pairwise summation and Spark's distributed aggregation add in
different orders. The tolerance below is tight enough that a real semantic
divergence -- ddof=0 against ddof=1, a positional "last", a single-pass
correlation -- fails it by many orders of magnitude.
"""

from __future__ import annotations

import math
from typing import Any

import numpy as np
import pandas as pd
import pytest
from telemetry_core.features import FEATURE_NAMES, WindowSample, compute_features
from telemetry_core.schemas import SensorReadings

from conftest import make_samples
from stream_processor.features_spark import (
    aggregate_windows,
    derive_features,
    prepare_signals,
)

RELATIVE_TOLERANCE = 1e-9
ABSOLUTE_TOLERANCE = 1e-9
WINDOW_SECONDS = 60


_EPOCH = pd.Timestamp("1970-01-01", tz="UTC")


def _epoch_seconds(values: pd.Series) -> pd.Series:
    """Whole epoch seconds, without assuming a datetime resolution.

    Arrow hands Spark timestamps back as ``datetime64[us]``, while a frame built
    in pandas is ``datetime64[ns]``. Casting either to int64 and dividing by a
    hard-coded 1e9 is right for one and wrong by a factor of a thousand for the
    other -- and the failure is a silent join mismatch, not an error.
    """
    return ((pd.to_datetime(values, utc=True) - _EPOCH).dt.total_seconds()).astype("int64")


def _optional(value: object) -> float | None:
    if value is None:
        return None
    number = float(value)  # type: ignore[arg-type]
    return None if math.isnan(number) else number


def _reference(samples: pd.DataFrame, window_seconds: int) -> pd.DataFrame:
    """Reference features, computed window by window in pure Python."""
    seconds = _epoch_seconds(samples["event_time"])
    frame = samples.assign(window_start=(seconds // window_seconds) * window_seconds)

    rows: list[dict[str, Any]] = []
    for (machine_id, window_start), group in frame.groupby(
        ["machine_id", "window_start"], sort=True
    ):
        window_samples = [
            WindowSample(
                event_time=row.event_time,  # type: ignore[arg-type]
                readings=SensorReadings(
                    temperature_c=_optional(row.temperature_c),
                    vibration_mm_s=_optional(row.vibration_mm_s),
                    pressure_bar=_optional(row.pressure_bar),
                    power_kw=_optional(row.power_kw),
                    rotation_rpm=_optional(row.rotation_rpm),
                ),
            )
            for row in group.itertuples(index=False)
        ]
        rows.append(
            {
                "machine_id": machine_id,
                "window_start": int(window_start),  # type: ignore[call-overload]
                **compute_features(window_samples),
            }
        )
    return pd.DataFrame(rows)


def _spark_features(spark: Any, samples: pd.DataFrame, window_seconds: int) -> pd.DataFrame:
    """Run the streaming translation in batch mode.

    Batch rather than streaming on purpose: the aggregation expressions are the
    thing under test, and a streaming source would add watermark behaviour that
    belongs to a different test.
    """
    frame = spark.createDataFrame(samples)
    aggregated = aggregate_windows(
        prepare_signals(frame),
        window_duration=f"{window_seconds} seconds",
        slide_duration=f"{window_seconds} seconds",
        watermark_delay="0 seconds",
    )
    derived = derive_features(aggregated).toPandas()
    derived["window_start"] = _epoch_seconds(derived["window_start"])
    return derived


def _disagreements(reference: pd.DataFrame, actual: pd.DataFrame) -> list[str]:
    merged = reference.merge(
        actual, on=["machine_id", "window_start"], suffixes=("_ref", "_spark"), how="inner"
    )
    assert len(merged) == len(reference), (
        f"different windows: reference={len(reference)} spark={len(actual)} matched={len(merged)}"
    )

    problems: list[str] = []
    for name in FEATURE_NAMES:
        for index in range(len(merged)):
            left = merged[f"{name}_ref"].iloc[index]
            right = merged[f"{name}_spark"].iloc[index]
            left_missing = left is None or (isinstance(left, float) and math.isnan(left))
            right_missing = right is None or (isinstance(right, float) and math.isnan(right))

            where = f"{merged['machine_id'].iloc[index]}/{merged['window_start'].iloc[index]}"
            if left_missing != right_missing:
                problems.append(
                    f"{name} @ {where}: reference="
                    f"{'None' if left_missing else left}, spark="
                    f"{'null' if right_missing else right}"
                )
                continue
            if left_missing:
                continue
            if not math.isclose(
                float(left), float(right), rel_tol=RELATIVE_TOLERANCE, abs_tol=ABSOLUTE_TOLERANCE
            ):
                problems.append(f"{name} @ {where}: reference={left!r}, spark={right!r}")
    return problems


class TestSparkMatchesTheReference:
    def test_on_a_full_history(self, spark: Any) -> None:
        samples = make_samples(seconds=300, seed=11)
        problems = _disagreements(
            _reference(samples, WINDOW_SECONDS), _spark_features(spark, samples, WINDOW_SECONDS)
        )
        assert not problems, "\n".join(problems[:20])

    def test_with_heavy_missing_data(self, spark: Any) -> None:
        """Per-signal exclusion and pairwise deletion must agree exactly."""
        samples = make_samples(seconds=180, seed=3, null_probability=0.25)
        problems = _disagreements(
            _reference(samples, WINDOW_SECONDS), _spark_features(spark, samples, WINDOW_SECONDS)
        )
        assert not problems, "\n".join(problems[:20])

    def test_with_a_frozen_sensor(self, spark: Any) -> None:
        """Zero variance is where stddev, z-score and correlation go undefined."""
        samples = make_samples(seconds=240, seed=5, null_probability=0.0)
        actual = _spark_features(spark, samples, WINDOW_SECONDS)

        frozen = actual.loc[actual["vibration_mm_s_stddev"] == 0.0]
        assert not frozen.empty, "the fixture no longer produces a frozen sensor"
        assert frozen["vibration_mm_s_zscore_last"].isna().all()
        assert frozen["corr_vibration_mm_s__rotation_rpm"].isna().all()

        problems = _disagreements(_reference(samples, WINDOW_SECONDS), actual)
        assert not problems, "\n".join(problems[:20])

    def test_column_order_matches_the_specification(self, spark: Any) -> None:
        """A matrix has no column names: order is the contract."""
        samples = make_samples(seconds=120, seed=1)
        actual = _spark_features(spark, samples, WINDOW_SECONDS)
        assert list(actual.columns[-len(FEATURE_NAMES) :]) == list(FEATURE_NAMES)


class TestDeterministicLastValue:
    def test_last_follows_event_time_not_arrival(self, spark: Any) -> None:
        """The reason the specification says min_by/max_by.

        Spark's first()/last() depend on row order after a shuffle, so a job
        built on them would disagree with the reference intermittently.
        """
        samples = make_samples(seconds=180, seed=4, null_probability=0.0)
        shuffled = samples.sample(frac=1.0, random_state=99).reset_index(drop=True)

        ordered = _spark_features(spark, samples, WINDOW_SECONDS)
        scrambled = _spark_features(spark, shuffled, WINDOW_SECONDS)

        key = ["machine_id", "window_start"]
        merged = ordered.merge(scrambled, on=key, suffixes=("_a", "_b"))
        assert len(merged) == len(ordered)

        # The same tolerance as the conformance comparison, and for the same
        # reason: shuffling the input changes the order Spark sums in, so the
        # last bits move. What must not move is which sample counts as "last",
        # and a positional first()/last() would change that by whole units.
        for name in FEATURE_NAMES:
            left = merged[f"{name}_a"].to_numpy(dtype="float64")
            right = merged[f"{name}_b"].to_numpy(dtype="float64")
            assert np.allclose(
                left, right, rtol=RELATIVE_TOLERANCE, atol=ABSOLUTE_TOLERANCE, equal_nan=True
            ), name

    def test_masked_timestamp_ignores_null_readings(self, spark: Any) -> None:
        """The property the smoke probe established, pinned as a test.

        Values [10, null, 30, null] at 0/10/20/30 s must give first=10, last=30
        and an elapsed time of 20 s, so the slope is 1.0 per second.
        """
        rows = [
            {
                "event_id": f"e{index}",
                "machine_id": "M-001",
                "line_id": "LINE-A",
                "event_time": pd.Timestamp("2026-04-01T06:00:00Z") + pd.Timedelta(seconds=offset),
                "machine_state": "RUNNING",
                "temperature_c": value,
                "vibration_mm_s": 1.0,
                "pressure_bar": 5.0,
                "power_kw": 10.0,
                "rotation_rpm": 1000.0,
            }
            for index, (offset, value) in enumerate([(0, 10.0), (10, None), (20, 30.0), (30, None)])
        ]
        samples = pd.DataFrame(rows)
        actual = _spark_features(spark, samples, WINDOW_SECONDS)
        assert actual["temperature_c_slope_per_second"].iloc[0] == pytest.approx(1.0)
        assert actual["temperature_c_deviation_from_mean"].iloc[0] == pytest.approx(10.0)
        assert actual["temperature_c_mean"].iloc[0] == pytest.approx(20.0)
