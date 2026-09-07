"""Feature specification and reference implementation.

This module is the countermeasure to training/serving skew, and it is worth
being precise about how -- because the obvious framing is wrong.

Spark computes aggregations with Spark SQL over distributed columns; the
training pipeline computes them with pandas over a local frame. Those two cannot
literally be the same code. So the shared artefact is not one implementation, it
is:

1. :data:`FEATURE_SPECS` and :data:`CORRELATION_PAIRS` -- a declarative
   specification with no engine dependency at all;
2. :func:`compute_features` -- a reference implementation that fixes the exact
   semantics of every aggregation;
3. conformance tests asserting that each engine's translation reproduces the
   reference output on identical input. The pandas translation is checked in
   ``ml-training``; the Spark one will be checked the same way.

Skew is therefore prevented by a shared specification plus a proof of
equivalence, not by a shared function that cannot exist.

Semantics that engines disagree on silently, pinned here
--------------------------------------------------------

* **Standard deviation uses ddof=1** (sample standard deviation). ``numpy.std``
  defaults to ddof=0, ``pandas.Series.std`` to ddof=1, and Spark's ``stddev`` is
  ``stddev_samp``, i.e. ddof=1. Left unpinned, training and serving would divide
  by different denominators.

* **"First" and "last" mean min_by/max_by over event time, never Spark's
  ``first()``/``last()``.** Those two are explicitly non-deterministic: their
  result depends on row order, which is not guaranteed after a shuffle. A
  conformance test built on them would fail intermittently, which is the worst
  possible failure mode. The specification is therefore expressed as
  ``min_by(value, event_time)`` and ``max_by(value, event_time)``, deterministic
  provided ``(machine_id, event_time)`` is unique -- a property the producer
  guarantees and the test suite asserts.

* **Ratios are computed per sample, then aggregated.** The mean of a ratio is
  not the ratio of the means. ``power_per_rpm`` must be averaged over per-instant
  ratios, which is what carries the physical meaning: rising power at constant
  speed is a friction signature.

* **Correlations use pairwise deletion** and are undefined -- ``None``, never
  zero -- when either signal has no variance inside the window. A frozen sensor
  therefore produces a null correlation, which is itself the signal.
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
    "CORRELATION_PAIRS",
    "DERIVED_SIGNALS",
    "FEATURE_NAMES",
    "FEATURE_SET_VERSION",
    "FEATURE_SPECS",
    "MIN_RUNNING_RATIO",
    "MIN_SAMPLES_FOR_SCORING",
    "QUALITY_FEATURE_NAMES",
    "AggregationKind",
    "CorrelationSpec",
    "FeatureSpec",
    "WindowSample",
    "compute_features",
    "derived_signal_value",
    "pearson_correlation",
]

#: Bumped whenever the specification changes, and recorded in the model
#: artefact. A model trained on one feature set must never score with another:
#: the columns would not mean what the model learned, and nothing would fail
#: loudly. Version 2.0.0 added deviation-from-mean, the within-window z-score,
#: cross-sensor correlations and a slope on the derived signals, and restated
#: the slope in terms of min_by/max_by.
FEATURE_SET_VERSION = "2.0.0"

#: Minimum samples in a 60 s window before it is worth scoring. Below this the
#: aggregates are dominated by noise and generate false positives. A starting
#: point to be revised against measurement, not a measured result.
MIN_SAMPLES_FOR_SCORING = 30

#: Minimum share of RUNNING samples for a window to be scored at all. High
#: vibration during a start-up is normal; scoring those windows would produce
#: false positives that say nothing about machine health.
MIN_RUNNING_RATIO = 0.9


class AggregationKind(StrEnum):
    """Aggregation applied to one signal over one window.

    Each member names an exact semantic, not an approximate one; every engine
    translation maps these to specific functions.
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
    #: (max_by(v, t) - min_by(v, t)) / elapsed_seconds. A slow drift is invisible
    #: to the mean and to the standard deviation, and it is exactly what bearing
    #: wear looks like.
    SLOPE = "slope_per_second"
    #: max_by(v, t) - avg(v): how far the latest reading sits from the window's
    #: own moving average. Catches a departure that has not yet moved the mean.
    DEVIATION_FROM_MEAN = "deviation_from_mean"
    #: The same departure, in units of the window's own dispersion. It gets its
    #: own axis rather than being left derivable, because an axis-aligned split
    #: cannot divide one feature by another.
    ZSCORE_LAST = "zscore_last"


@dataclass(frozen=True, slots=True)
class FeatureSpec:
    """One feature: which signal, which aggregation, under which name."""

    signal: str
    aggregation: AggregationKind

    @property
    def name(self) -> str:
        return f"{self.signal}_{self.aggregation.value}"


@dataclass(frozen=True, slots=True)
class CorrelationSpec:
    """A Pearson correlation between two sensors, inside one window."""

    left: str
    right: str
    rationale: str

    @property
    def name(self) -> str:
        return f"corr_{self.left}__{self.right}"


#: Signals derived per sample before aggregation.
#:
#: ``power_per_rpm`` is the specific consumption: power rising at constant speed
#: means mechanical friction is increasing. Neither power nor speed alone leaves
#: its nominal range while this happens, which is precisely the class of anomaly
#: a per-sensor threshold cannot see -- and the reason a multivariate model earns
#: its place here (docs/07-ml-methodology.md section 2.2).
DERIVED_SIGNALS: tuple[str, ...] = ("power_per_rpm", "vibration_per_rpm")

#: Cross-sensor correlations, chosen for physical meaning rather than taken
#: exhaustively. All ten pairs of five sensors would add dimensional noise with
#: no hypothesis behind it; each pair below breaks under a specific failure.
CORRELATION_PAIRS: tuple[CorrelationSpec, ...] = (
    CorrelationSpec(
        "power_kw", "rotation_rpm", "core electromechanical coupling; breaks on a motor stall"
    ),
    CorrelationSpec(
        "vibration_mm_s", "rotation_rpm", "rotating imbalance; breaks when vibration decouples"
    ),
    CorrelationSpec(
        "temperature_c", "power_kw", "thermal coupling; breaks on a temperature sensor fault"
    ),
    CorrelationSpec(
        "pressure_bar", "rotation_rpm", "hydraulic coupling; breaks on a leak or a stuck sensor"
    ),
)

_BASE_AGGREGATIONS: tuple[AggregationKind, ...] = (
    AggregationKind.MEAN,
    AggregationKind.STDDEV,
    AggregationKind.MIN,
    AggregationKind.MAX,
    AggregationKind.RANGE,
    AggregationKind.SLOPE,
    AggregationKind.DEVIATION_FROM_MEAN,
    AggregationKind.ZSCORE_LAST,
)

#: Derived signals get fewer aggregations: the min and max of a ratio are
#: dominated by the noisiest single sample and carry little beyond the mean and
#: the spread. The slope is kept because a rising specific consumption is the
#: wear signature itself.
_DERIVED_AGGREGATIONS: tuple[AggregationKind, ...] = (
    AggregationKind.MEAN,
    AggregationKind.STDDEV,
    AggregationKind.SLOPE,
)


def _build_specs() -> tuple[FeatureSpec, ...]:
    specs: list[FeatureSpec] = []
    for signal in SENSOR_NAMES:
        specs.extend(FeatureSpec(signal, aggregation) for aggregation in _BASE_AGGREGATIONS)
    for signal in DERIVED_SIGNALS:
        specs.extend(FeatureSpec(signal, aggregation) for aggregation in _DERIVED_AGGREGATIONS)
    return tuple(specs)


#: Canonical, ordered specification: 5 sensors x 8 aggregations, plus 2 derived
#: signals x 3 aggregations.
FEATURE_SPECS: tuple[FeatureSpec, ...] = _build_specs()

#: Data-quality features. ``sample_count`` exposes a degraded collection to the
#: model and ``null_ratio`` exposes failing sensors. A gap in the data is itself
#: information about the machine's health, not merely a nuisance.
QUALITY_FEATURE_NAMES: tuple[str, ...] = ("sample_count", "null_ratio")

#: Canonical feature order. A NumPy matrix has no column names: two permuted
#: features produce plausible, wrong scores. This order is written into the model
#: artefact and verified when a scoring job loads it (docs/07 section 2.3).
FEATURE_NAMES: tuple[str, ...] = (
    tuple(spec.name for spec in FEATURE_SPECS)
    + tuple(pair.name for pair in CORRELATION_PAIRS)
    + QUALITY_FEATURE_NAMES
)


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
    """Drift as ``(value at max t - value at min t) / elapsed_seconds``.

    A two-point estimate rather than a least-squares fit: it is what a windowed
    aggregation expresses with ``min_by``/``max_by`` in a single pass, and the
    conformance tests depend on every engine computing the same thing. A
    regression slope would be more robust to noise and is a candidate for a
    later feature-set version -- with its own version bump, since changing it
    invalidates every model trained before.
    """
    if len(points) < 2:
        return None
    first_ms, first_value = points[0]
    last_ms, last_value = points[-1]
    elapsed_seconds = (last_ms - first_ms) / 1000.0
    if elapsed_seconds <= 0:
        return None
    return (last_value - first_value) / elapsed_seconds


def pearson_correlation(xs: Sequence[float], ys: Sequence[float]) -> float | None:
    """Pearson correlation, matching Spark's ``corr`` and pandas' ``corr``.

    ``None`` rather than zero when either signal has no variance inside the
    window: the coefficient is genuinely undefined there, and zero would claim
    "measured, and uncorrelated". A frozen sensor lands in exactly this case, so
    the null is the detection signal.

    The ``n - 1`` of the sample covariance cancels against the denominators, so
    the ddof convention does not matter here -- unlike for the standard
    deviation.
    """
    count = len(xs)
    if count < 2:
        return None

    mean_x = _mean(xs)
    mean_y = _mean(ys)
    covariance = math.fsum((x - mean_x) * (y - mean_y) for x, y in zip(xs, ys, strict=True))
    variance_x = math.fsum((x - mean_x) ** 2 for x in xs)
    variance_y = math.fsum((y - mean_y) ** 2 for y in ys)

    if variance_x <= 0.0 or variance_y <= 0.0:
        return None

    coefficient = covariance / math.sqrt(variance_x * variance_y)
    # Floating-point error can push a perfect correlation just outside [-1, 1].
    return max(-1.0, min(1.0, coefficient))


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
    if kind is AggregationKind.DEVIATION_FROM_MEAN:
        # "Latest" is the value at the greatest event time, i.e. max_by.
        return points[-1][1] - _mean(values)
    if kind is AggregationKind.ZSCORE_LAST:
        dispersion = _stddev_samp(values)
        if dispersion is None or dispersion <= 0.0:
            # Undefined, not zero: a flat signal has no scale to measure against.
            return None
        return (points[-1][1] - _mean(values)) / dispersion
    raise FeatureComputationError(f"unhandled aggregation: {kind}")  # pragma: no cover


def compute_features(samples: Sequence[WindowSample]) -> dict[str, float | None]:
    """Compute the canonical feature vector for one window of one machine.

    Samples are sorted by event time before aggregation, so the result does not
    depend on arrival order. That matters: Kafka guarantees order per partition,
    but a window can still receive out-of-order samples within the watermark, and
    a slope must not flip sign because of it. Sorting is what makes this
    reference agree with an engine using ``min_by``/``max_by``.

    Missing readings are excluded per signal rather than dropping the whole
    sample: one failed sensor must not blind the other four. Correlations use
    pairwise deletion for the same reason.

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

    all_signals = (*SENSOR_NAMES, *DERIVED_SIGNALS)
    values_by_signal: dict[str, list[float]] = {name: [] for name in all_signals}
    points_by_signal: dict[str, list[tuple[int, float]]] = {name: [] for name in all_signals}
    correlation_values: dict[str, list[tuple[float, float]]] = {
        pair.name: [] for pair in CORRELATION_PAIRS
    }

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
        for pair in CORRELATION_PAIRS:
            left = readings[pair.left]
            right = readings[pair.right]
            if left is None or right is None:
                continue
            correlation_values[pair.name].append((left, right))

    features: dict[str, float | None] = {}
    for spec in FEATURE_SPECS:
        features[spec.name] = _aggregate(
            spec.aggregation, values_by_signal[spec.signal], points_by_signal[spec.signal]
        )

    for pair in CORRELATION_PAIRS:
        paired = correlation_values[pair.name]
        features[pair.name] = pearson_correlation(
            [left for left, _ in paired], [right for _, right in paired]
        )

    sample_count = len(ordered)
    features["sample_count"] = float(sample_count)
    total_slots = sample_count * len(SENSOR_NAMES)
    features["null_ratio"] = (null_slots / total_slots) if total_slots else None

    return features
