"""Feature specification and reference implementation.

This module is the countermeasure to training/serving skew, and it is worth
being precise about how -- because the obvious framing is wrong.

Spark computes aggregations with Spark SQL over distributed columns; the
training pipeline computes them with pandas or NumPy over a local frame. Those
two cannot literally be the same code. So the shared artefact is not one
implementation, it is:

1. :data:`FEATURE_SPECS` -- a declarative specification (name, source signal,
   aggregation, canonical order) with no engine dependency at all;
2. :func:`compute_features` -- a reference implementation that fixes the exact
   semantics of every aggregation;
3. a conformance test, added with the Spark job in step 4, asserting that the
   Spark translation of the same specification reproduces the reference output
   on identical input.

Skew is therefore prevented by a shared specification plus a proof of
equivalence, not by a shared function that cannot exist.

Two semantic decisions are pinned here because they silently differ between
engines and would otherwise produce a model scoring on a distribution it never
saw:

* **Standard deviation uses ddof=1** (sample standard deviation). ``numpy.std``
  defaults to ddof=0, ``pandas.Series.std`` to ddof=1, and Spark's ``stddev`` is
  ``stddev_samp``, i.e. ddof=1. Left unpinned, training and serving would divide
  by different denominators.
* **Ratios are computed per sample, then aggregated.** The mean of a ratio is
  not the ratio of the means. ``power_per_rpm`` must be averaged over
  per-instant ratios, which is what carries the physical meaning: rising power
  at constant speed is a friction signature.
"""

from __future__ import annotations

import math
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum

from telemetry_core.errors import FeatureComputationError
from telemetry_core.schemas import SENSOR_NAMES, SensorReadings
from telemetry_core.timeutil import to_epoch_millis

__all__ = [
    "DERIVED_SIGNALS",
    "FEATURE_NAMES",
    "FEATURE_SET_VERSION",
    "FEATURE_SPECS",
    "MIN_SAMPLES_FOR_SCORING",
    "AggregationKind",
    "FeatureSpec",
    "WindowSample",
    "compute_features",
    "derived_signal_value",
]

#: Bumped whenever the specification changes. Recorded in the model artefact:
#: a model trained on one feature set must never score with another.
FEATURE_SET_VERSION = "1.0.0"

#: Minimum samples in a 60 s window before it is worth scoring. Below this the
#: aggregates are dominated by noise and generate false positives. The value is
#: a starting point to be revised against measurement, not a measured result.
MIN_SAMPLES_FOR_SCORING = 30


class AggregationKind(StrEnum):
    """Aggregation applied to a signal over one window.

    Each member names an exact semantic, not an approximate one; the Spark
    translation in step 4 maps these to specific Spark SQL functions.
    """

    MEAN = "mean"
    #: Sample standard deviation, ddof=1. Matches Spark's stddev_samp.
    STDDEV = "stddev"
    MIN = "min"
    MAX = "max"
    #: max - min. Kept even though it is derivable from min and max: Isolation
    #: Forest splits on single axes and cannot compute a difference between two
    #: features, so the spread is only visible if it is given its own axis.
    RANGE = "range"
    #: (last - first) / elapsed_seconds. A slow drift is invisible to mean and
    #: to standard deviation, and it is exactly what bearing wear looks like.
    SLOPE = "slope_per_second"


@dataclass(frozen=True, slots=True)
class FeatureSpec:
    """One feature: which signal, which aggregation, under which name."""

    signal: str
    aggregation: AggregationKind

    @property
    def name(self) -> str:
        return f"{self.signal}_{self.aggregation.value}"


#: Signals derived per sample before aggregation.
#:
#: ``power_per_rpm`` is the specific consumption: power rising at constant speed
#: means mechanical friction is increasing. Neither power nor speed alone leaves
#: its nominal range while this happens, which is precisely the class of anomaly
#: a per-sensor threshold cannot see -- and the reason a multivariate model earns
#: its place here (docs/07-ml-methodology.md section 2.2).
DERIVED_SIGNALS: tuple[str, ...] = ("power_per_rpm", "vibration_per_rpm")

_BASE_AGGREGATIONS: tuple[AggregationKind, ...] = (
    AggregationKind.MEAN,
    AggregationKind.STDDEV,
    AggregationKind.MIN,
    AggregationKind.MAX,
    AggregationKind.RANGE,
    AggregationKind.SLOPE,
)

_DERIVED_AGGREGATIONS: tuple[AggregationKind, ...] = (
    AggregationKind.MEAN,
    AggregationKind.STDDEV,
)


def _build_specs() -> tuple[FeatureSpec, ...]:
    specs: list[FeatureSpec] = []
    for signal in SENSOR_NAMES:
        specs.extend(FeatureSpec(signal, aggregation) for aggregation in _BASE_AGGREGATIONS)
    for signal in DERIVED_SIGNALS:
        specs.extend(FeatureSpec(signal, aggregation) for aggregation in _DERIVED_AGGREGATIONS)
    return tuple(specs)


#: Canonical, ordered feature specification: 5 sensors x 6 aggregations,
#: plus 2 derived signals x 2 aggregations, plus 2 data-quality features.
FEATURE_SPECS: tuple[FeatureSpec, ...] = _build_specs()

#: Data-quality features. ``sample_count`` exposes a degraded collection to the
#: model, and ``null_ratio`` exposes failing sensors. A gap in the data is itself
#: information about the machine's health, not merely a nuisance.
_QUALITY_FEATURE_NAMES: tuple[str, ...] = ("sample_count", "null_ratio")

#: Canonical feature order. A NumPy matrix has no column names: two permuted
#: features produce plausible, wrong scores. This order is written into the model
#: artefact and verified when the scoring job loads it (docs/07 section 2.3).
FEATURE_NAMES: tuple[str, ...] = tuple(spec.name for spec in FEATURE_SPECS) + _QUALITY_FEATURE_NAMES


@dataclass(frozen=True, slots=True)
class WindowSample:
    """One sample inside a window: an instant and its readings."""

    event_time: datetime
    readings: SensorReadings


def derived_signal_value(signal: str, readings: SensorReadings) -> float | None:
    """Compute a derived signal for a single sample.

    Returns ``None`` when an operand is missing or when the denominator is too
    close to zero to be meaningful. A stopped motor has ``rotation_rpm`` near
    zero, and dividing by it would manufacture an enormous ratio that the model
    would read as a dramatic anomaly -- when in fact the machine is simply idle.
    """
    denominator = readings.rotation_rpm
    if denominator is None or denominator < 1.0:
        return None

    numerator: float | None
    if signal == "power_per_rpm":
        numerator = readings.power_kw
    elif signal == "vibration_per_rpm":
        numerator = readings.vibration_mm_s
    else:  # pragma: no cover - guarded by DERIVED_SIGNALS
        raise FeatureComputationError(f"unknown derived signal: {signal!r}")

    if numerator is None:
        return None
    return numerator / denominator


def _mean(values: Sequence[float]) -> float:
    return math.fsum(values) / len(values)


def _stddev_samp(values: Sequence[float]) -> float | None:
    """Sample standard deviation (ddof=1).

    ``None`` for fewer than two points: with one observation the sample standard
    deviation is undefined. Returning 0.0 would tell the model "perfectly
    stable", which is the opposite of what one point means.
    """
    count = len(values)
    if count < 2:
        return None
    mean = _mean(values)
    variance = math.fsum((value - mean) ** 2 for value in values) / (count - 1)
    return math.sqrt(variance)


def _slope_per_second(points: Sequence[tuple[int, float]]) -> float | None:
    """Drift as ``(last - first) / elapsed_seconds``.

    A two-point estimate rather than a least-squares fit: it is what a windowed
    Spark aggregation can express with ``first()`` and ``last()`` without an
    extra pass, and the conformance test in step 4 depends on both engines
    computing the same thing. A regression slope would be more robust to noise
    and is a candidate for a later feature-set version -- with its own version
    bump, since changing it invalidates every model trained before.
    """
    if len(points) < 2:
        return None
    first_ms, first_value = points[0]
    last_ms, last_value = points[-1]
    elapsed_seconds = (last_ms - first_ms) / 1000.0
    if elapsed_seconds <= 0:
        return None
    return (last_value - first_value) / elapsed_seconds


def _aggregate(
    kind: AggregationKind, values: Sequence[float], points: Sequence[tuple[int, float]]
) -> float | None:
    if not values:
        return None
    if kind is AggregationKind.MEAN:
        return _mean(values)
    if kind is AggregationKind.STDDEV:
        return _stddev_samp(values)
    if kind is AggregationKind.MIN:
        return min(values)
    if kind is AggregationKind.MAX:
        return max(values)
    if kind is AggregationKind.RANGE:
        return max(values) - min(values)
    if kind is AggregationKind.SLOPE:
        return _slope_per_second(points)
    raise FeatureComputationError(f"unhandled aggregation: {kind}")  # pragma: no cover


def compute_features(samples: Sequence[WindowSample]) -> dict[str, float | None]:
    """Compute the canonical feature vector for one window of one machine.

    Samples are sorted by event time before aggregation, so the result does not
    depend on arrival order. That matters: Kafka guarantees order per partition,
    but a window can still receive out-of-order samples within the watermark,
    and slope must not flip sign because of it.

    Missing readings are excluded per signal rather than dropping the whole
    sample: one failed sensor must not blind the other four.

    Args:
        samples: Samples belonging to the window. May be empty.

    Returns:
        A mapping with exactly :data:`FEATURE_NAMES` as keys, in that order.
        ``None`` marks a feature that could not be computed, and is never
        replaced by a placeholder number here -- imputation is a modelling
        decision that belongs to the training pipeline, which knows what the
        reference distribution looks like.
    """
    ordered = sorted(samples, key=lambda sample: sample.event_time)

    # Per signal: the values used for aggregation, and (timestamp, value) pairs
    # used for slope. Built in one pass so both stay consistent.
    values_by_signal: dict[str, list[float]] = {name: [] for name in SENSOR_NAMES}
    points_by_signal: dict[str, list[tuple[int, float]]] = {name: [] for name in SENSOR_NAMES}
    for name in DERIVED_SIGNALS:
        values_by_signal[name] = []
        points_by_signal[name] = []

    null_slots = 0
    for sample in ordered:
        timestamp_ms = to_epoch_millis(sample.event_time)
        readings = sample.readings.as_mapping()
        for signal in SENSOR_NAMES:
            value = readings[signal]
            if value is None:
                null_slots += 1
                continue
            values_by_signal[signal].append(value)
            points_by_signal[signal].append((timestamp_ms, value))
        for signal in DERIVED_SIGNALS:
            derived = derived_signal_value(signal, sample.readings)
            if derived is None:
                continue
            values_by_signal[signal].append(derived)
            points_by_signal[signal].append((timestamp_ms, derived))

    features: dict[str, float | None] = {}
    for spec in FEATURE_SPECS:
        features[spec.name] = _aggregate(
            spec.aggregation, values_by_signal[spec.signal], points_by_signal[spec.signal]
        )

    sample_count = len(ordered)
    features["sample_count"] = float(sample_count)
    # Share of expected sensor readings that were missing across the window.
    total_slots = sample_count * len(SENSOR_NAMES)
    features["null_ratio"] = (null_slots / total_slots) if total_slots else None

    return features
