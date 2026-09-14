"""Vectorised pandas translation of the shared feature specification.

This is one of the two engine translations (the other is Spark). It is driven
by ``telemetry_core.features`` -- the aggregations, their order and their exact
semantics all come from there -- and ``tests/test_feature_conformance.py``
asserts that it reproduces the reference implementation on random windows.

Three semantics are implemented deliberately rather than by convenience, because
they are the ones engines get subtly different:

* ``std`` is pandas' default ddof=1, matching Spark's ``stddev_samp`` and the
  reference. Using ``numpy.std`` here would silently divide by ``n``.
* "first" and "last" mean the value at the smallest and greatest **event time**,
  obtained by sorting and using pandas' NaN-skipping ``first``/``last``. Never
  positional, because Spark's ``first()``/``last()`` are non-deterministic after
  a shuffle and the specification is stated in terms of ``min_by``/``max_by``.
* Correlations are computed in **two passes** -- group means, then centred
  products -- rather than with the single-pass computational formula. The
  single-pass form loses precision when values are large relative to their
  variance, which is exactly our case (rpm near 1500, variance near 10), and the
  conformance test would fail for a reason that has nothing to do with the
  specification.

Bit-exact equality with the reference is not achievable and not required:
``math.fsum`` and pandas' pairwise summation add in different orders. The
conformance test uses a tolerance and states why.
"""

from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd
from telemetry_core.features import (
    CORRELATION_PAIRS,
    DERIVED_SIGNALS,
    FEATURE_NAMES,
    FEATURE_SPECS,
    AggregationKind,
)
from telemetry_core.schemas import SENSOR_NAMES

from ml_training.dataset.windows import assign_windows

__all__ = ["ALL_SIGNALS", "add_derived_signals", "build_window_features"]

#: Base sensors plus the per-sample derived ratios, in the order the
#: specification uses them.
ALL_SIGNALS: tuple[str, ...] = (*SENSOR_NAMES, *DERIVED_SIGNALS)

_RUNNING = "RUNNING"
_MIN_ROTATION_RPM = 1.0


def add_derived_signals(samples: pd.DataFrame) -> pd.DataFrame:
    """Compute ``power_per_rpm`` and ``vibration_per_rpm`` per sample.

    Per sample, then aggregated -- never the ratio of the aggregates. The mean
    of a ratio is not the ratio of the means, and it is the per-instant ratio
    that carries the physical meaning: rising power at constant speed is
    friction.

    ``None`` below one revolution per minute: dividing by a near-stopped motor
    would manufacture an enormous ratio that a model reads as a dramatic
    anomaly, when the machine is merely idle.
    """
    frame = samples.copy()
    rpm = frame["rotation_rpm"]
    turning = rpm.notna() & (rpm >= _MIN_ROTATION_RPM)

    frame["power_per_rpm"] = np.where(
        turning & frame["power_kw"].notna(), frame["power_kw"] / rpm, np.nan
    )
    frame["vibration_per_rpm"] = np.where(
        turning & frame["vibration_mm_s"].notna(), frame["vibration_mm_s"] / rpm, np.nan
    )
    return frame


def _aggregate_signals(grouped: Any) -> pd.DataFrame:
    """One pass of named aggregations covering every signal."""
    specification: dict[str, tuple[str, str]] = {}
    for signal in ALL_SIGNALS:
        specification[f"{signal}__mean"] = (signal, "mean")
        specification[f"{signal}__std"] = (signal, "std")
        specification[f"{signal}__min"] = (signal, "min")
        specification[f"{signal}__max"] = (signal, "max")
        # first/last skip NaN in pandas, and the frame is sorted by event time,
        # so these are the values at the smallest and greatest event time among
        # the non-null samples -- min_by and max_by.
        specification[f"{signal}__first"] = (signal, "first")
        specification[f"{signal}__last"] = (signal, "last")
        specification[f"{signal}__t_first"] = (f"__t_{signal}", "min")
        specification[f"{signal}__t_last"] = (f"__t_{signal}", "max")
    aggregated: pd.DataFrame = grouped.agg(**specification)
    return aggregated


def _correlation(
    frame: pd.DataFrame, keys: pd.Series, left: str, right: str, index: pd.Index
) -> pd.Series:
    """Pearson correlation per group, pairwise-complete, computed in two passes."""
    both_present = frame[left].notna() & frame[right].notna()
    left_values = frame[left].where(both_present)
    right_values = frame[right].where(both_present)

    counts = left_values.groupby(keys).count()
    mean_left = left_values.groupby(keys).mean()
    mean_right = right_values.groupby(keys).mean()

    centred_left = left_values - keys.map(mean_left)
    centred_right = right_values - keys.map(mean_right)

    covariance = (centred_left * centred_right).groupby(keys).sum()
    variance_left = (centred_left**2).groupby(keys).sum()
    variance_right = (centred_right**2).groupby(keys).sum()

    denominator = np.sqrt(variance_left * variance_right)
    # Undefined -- not zero -- when either signal is flat or too few pairs
    # survive. A frozen sensor lands here, and the null is the detection signal.
    defined = (counts >= 2) & (variance_left > 0.0) & (variance_right > 0.0)
    result = pd.Series(np.where(defined, covariance / denominator, np.nan), index=counts.index)
    return result.clip(-1.0, 1.0).reindex(index)


def _apply_specs(aggregated: pd.DataFrame) -> pd.DataFrame:
    """Turn the raw aggregates into the specified features."""
    features = pd.DataFrame(index=aggregated.index)

    for signal in ALL_SIGNALS:
        mean = aggregated[f"{signal}__mean"]
        std = aggregated[f"{signal}__std"]
        last = aggregated[f"{signal}__last"]
        first = aggregated[f"{signal}__first"]
        elapsed = (aggregated[f"{signal}__t_last"] - aggregated[f"{signal}__t_first"]) / 1000.0

        computed: dict[AggregationKind, pd.Series] = {
            AggregationKind.MEAN: mean,
            AggregationKind.STDDEV: std,
            AggregationKind.MIN: aggregated[f"{signal}__min"],
            AggregationKind.MAX: aggregated[f"{signal}__max"],
            AggregationKind.RANGE: aggregated[f"{signal}__max"] - aggregated[f"{signal}__min"],
            AggregationKind.SLOPE: pd.Series(
                np.where(elapsed > 0.0, (last - first) / elapsed.replace(0.0, np.nan), np.nan),
                index=aggregated.index,
            ),
            AggregationKind.DEVIATION_FROM_MEAN: last - mean,
            AggregationKind.ZSCORE_LAST: pd.Series(
                np.where(std > 0.0, (last - mean) / std.replace(0.0, np.nan), np.nan),
                index=aggregated.index,
            ),
        }
        for spec in FEATURE_SPECS:
            if spec.signal == signal:
                features[spec.name] = computed[spec.aggregation]

    return features


def build_window_features(
    samples: pd.DataFrame, *, window_seconds: int, slide_seconds: int
) -> pd.DataFrame:
    """Build the window-level feature frame for a set of samples.

    Processed one machine at a time. The sliding expansion multiplies rows by
    the overlap factor, so chunking bounds memory -- and it mirrors how Spark
    will partition the same work by ``machine_id``.

    Returns:
        A frame indexed by ``(machine_id, window_start)`` carrying exactly
        :data:`telemetry_core.features.FEATURE_NAMES`, plus ``window_end``,
        ``running_ratio`` and ``machine_state`` for the admission gate. Feature
        columns keep ``NaN`` where the specification says the value is
        undefined: imputation is a modelling decision made later, by a pipeline
        that knows the reference distribution.
    """
    if samples.empty:
        raise ValueError("cannot build features from an empty sample frame")

    outputs: list[pd.DataFrame] = []
    for machine_id, machine_samples in samples.groupby("machine_id", sort=True):
        ordered = machine_samples.sort_values("event_time").reset_index(drop=True)
        enriched = add_derived_signals(ordered)

        event_millis = enriched["event_time"].astype("int64") // 1_000_000
        for signal in ALL_SIGNALS:
            # A per-signal masked time column: the timestamp of a sample only
            # counts for a signal that actually reported at that instant.
            enriched[f"__t_{signal}"] = event_millis.where(enriched[signal].notna())
        enriched["__null_count"] = enriched[list(SENSOR_NAMES)].isna().sum(axis=1)
        enriched["__running"] = (enriched["machine_state"] == _RUNNING).astype("float64")

        expanded = assign_windows(
            enriched, window_seconds=window_seconds, slide_seconds=slide_seconds
        )
        # Re-sorting after the expansion is what makes first/last mean min_by
        # and max_by inside each window.
        expanded = expanded.sort_values(["window_start", "event_time"])
        keys = expanded["window_start"]
        grouped = expanded.groupby("window_start", sort=True)

        aggregated = _aggregate_signals(grouped)
        features = _apply_specs(aggregated)

        sample_count = grouped.size().rename("sample_count")
        features["sample_count"] = sample_count.astype("float64")
        null_count = grouped["__null_count"].sum()
        features["null_ratio"] = np.where(
            sample_count > 0, null_count / (sample_count * len(SENSOR_NAMES)), np.nan
        )

        for pair in CORRELATION_PAIRS:
            features[pair.name] = _correlation(
                expanded, keys, pair.left, pair.right, features.index
            )

        features["running_ratio"] = grouped["__running"].mean()
        features["machine_state"] = grouped["machine_state"].last()
        features["machine_id"] = machine_id
        outputs.append(features.reset_index())

    result = pd.concat(outputs, ignore_index=True)
    result["window_end"] = result["window_start"] + window_seconds
    result["window_start_time"] = pd.to_datetime(result["window_start"], unit="s", utc=True)
    result["window_end_time"] = pd.to_datetime(result["window_end"], unit="s", utc=True)

    ordered_columns = [
        "machine_id",
        "window_start",
        "window_end",
        "window_start_time",
        "window_end_time",
        "running_ratio",
        "machine_state",
        *FEATURE_NAMES,
    ]
    return (
        result[ordered_columns].sort_values(["machine_id", "window_start"]).reset_index(drop=True)
    )
