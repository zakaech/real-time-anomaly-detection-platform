"""Feature semantics, pinned by hand-computed values.

Expected values are computed by hand in the comments rather than by calling
NumPy or pandas. A test that derives its expectation from the same library the
code uses cannot detect the exact class of bug this module exists to prevent: a
``ddof`` mismatch, or a non-deterministic "last" between two engines.
"""

from __future__ import annotations

import math
from datetime import UTC, datetime, timedelta

import pytest

from telemetry_core.features import (
    CORRELATION_PAIRS,
    DERIVED_SIGNALS,
    FEATURE_NAMES,
    FEATURE_SET_VERSION,
    FEATURE_SPECS,
    MIN_RUNNING_RATIO,
    MIN_SAMPLES_FOR_SCORING,
    QUALITY_FEATURE_NAMES,
    AggregationKind,
    WindowSample,
    compute_features,
    derived_signal_value,
    pearson_correlation,
)
from telemetry_core.schemas import SENSOR_NAMES, SensorReadings

_T0 = datetime(2026, 9, 6, 14, 22, 10, tzinfo=UTC)


def _sample(
    offset_seconds: int,
    *,
    temperature_c: float | None = 10.0,
    vibration_mm_s: float | None = 1.0,
    pressure_bar: float | None = 5.0,
    power_kw: float | None = 10.0,
    rotation_rpm: float | None = 1000.0,
) -> WindowSample:
    return WindowSample(
        event_time=_T0 + timedelta(seconds=offset_seconds),
        readings=SensorReadings(
            temperature_c=temperature_c,
            vibration_mm_s=vibration_mm_s,
            pressure_bar=pressure_bar,
            power_kw=power_kw,
            rotation_rpm=rotation_rpm,
        ),
    )


# Three samples, ten seconds apart, with a linear temperature ramp 10/20/30 and
# a linear power ramp 10/20/30 at a constant 1000 rpm.
_RAMP_WINDOW = [
    _sample(0, temperature_c=10.0, power_kw=10.0, vibration_mm_s=1.0),
    _sample(10, temperature_c=20.0, power_kw=20.0, vibration_mm_s=2.0),
    _sample(20, temperature_c=30.0, power_kw=30.0, vibration_mm_s=3.0),
]


class TestFeatureSpecification:
    def test_version_is_declared(self) -> None:
        """Recorded in the artefact so a v1 model cannot score v2 features."""
        assert FEATURE_SET_VERSION == "2.0.0"

    def test_feature_count_is_stable(self) -> None:
        # 5 sensors x 8 aggregations + 2 derived x 3 + 4 correlations + 2 quality
        assert len(FEATURE_SPECS) == len(SENSOR_NAMES) * 8 + len(DERIVED_SIGNALS) * 3
        assert len(CORRELATION_PAIRS) == 4
        assert len(FEATURE_NAMES) == 52

    def test_feature_order_is_canonical(self) -> None:
        """A NumPy matrix has no column names: order is the contract.

        Two permuted features produce plausible and wrong scores, silently.
        """
        assert FEATURE_NAMES[:8] == (
            "temperature_c_mean",
            "temperature_c_stddev",
            "temperature_c_min",
            "temperature_c_max",
            "temperature_c_range",
            "temperature_c_slope_per_second",
            "temperature_c_deviation_from_mean",
            "temperature_c_zscore_last",
        )
        assert FEATURE_NAMES[-6:] == (
            "corr_power_kw__rotation_rpm",
            "corr_vibration_mm_s__rotation_rpm",
            "corr_temperature_c__power_kw",
            "corr_pressure_bar__rotation_rpm",
            "sample_count",
            "null_ratio",
        )

    def test_feature_names_are_unique(self) -> None:
        assert len(set(FEATURE_NAMES)) == len(FEATURE_NAMES)

    def test_quality_features_come_last(self) -> None:
        assert FEATURE_NAMES[-len(QUALITY_FEATURE_NAMES) :] == QUALITY_FEATURE_NAMES

    def test_every_spec_name_is_present_in_the_vector(self) -> None:
        computed = compute_features(_RAMP_WINDOW)
        assert tuple(computed) == FEATURE_NAMES

    def test_scoring_gates_are_declared(self) -> None:
        """Shared with the streaming job so both apply the same admission rule."""
        assert MIN_SAMPLES_FOR_SCORING == 30
        assert MIN_RUNNING_RATIO == 0.9

    def test_correlation_pairs_carry_a_rationale(self) -> None:
        """Four physically motivated pairs, not ten mechanical ones."""
        for pair in CORRELATION_PAIRS:
            assert pair.left in SENSOR_NAMES
            assert pair.right in SENSOR_NAMES
            assert pair.rationale


class TestAggregationSemantics:
    def test_mean_min_max_range(self) -> None:
        features = compute_features(_RAMP_WINDOW)
        assert features["temperature_c_mean"] == pytest.approx(20.0)
        assert features["temperature_c_min"] == pytest.approx(10.0)
        assert features["temperature_c_max"] == pytest.approx(30.0)
        assert features["temperature_c_range"] == pytest.approx(20.0)

    def test_stddev_uses_ddof_1(self) -> None:
        """Sample standard deviation, matching Spark's stddev_samp.

        For 10, 20, 30: mean 20, squared deviations 100 + 0 + 100 = 200.
        ddof=1 divides by n-1 = 2  -> sqrt(100) = 10
        ddof=0 would divide by n   -> sqrt(66.67) = 8.165
        The two differ by 22 percent on three points; a model trained on one and
        served the other scores a distribution it never saw.
        """
        features = compute_features(_RAMP_WINDOW)
        assert features["temperature_c_stddev"] == pytest.approx(10.0)
        assert features["temperature_c_stddev"] != pytest.approx(math.sqrt(200.0 / 3.0))

    def test_stddev_is_none_for_a_single_sample(self) -> None:
        """Undefined, not zero. Zero would claim perfect stability."""
        features = compute_features([_sample(0)])
        assert features["temperature_c_stddev"] is None

    def test_slope_is_per_second(self) -> None:
        """(30 - 10) / 20 seconds = 1.0 per second."""
        features = compute_features(_RAMP_WINDOW)
        assert features["temperature_c_slope_per_second"] == pytest.approx(1.0)

    def test_slope_is_zero_for_a_flat_signal(self) -> None:
        features = compute_features(_RAMP_WINDOW)
        assert features["rotation_rpm_slope_per_second"] == pytest.approx(0.0)

    def test_slope_is_none_for_a_single_sample(self) -> None:
        features = compute_features([_sample(0)])
        assert features["temperature_c_slope_per_second"] is None

    def test_deviation_from_mean_uses_the_latest_value(self) -> None:
        """last - mean = 30 - 20 = 10."""
        features = compute_features(_RAMP_WINDOW)
        assert features["temperature_c_deviation_from_mean"] == pytest.approx(10.0)

    def test_zscore_last_normalises_the_deviation(self) -> None:
        """(30 - 20) / 10 = 1.0, with the ddof=1 standard deviation."""
        features = compute_features(_RAMP_WINDOW)
        assert features["temperature_c_zscore_last"] == pytest.approx(1.0)

    def test_zscore_is_none_for_a_flat_signal(self) -> None:
        """A frozen sensor has no scale to measure a departure against.

        Returning zero would claim "measured, and perfectly typical", which is
        the opposite of what a stuck sensor means.
        """
        features = compute_features(_RAMP_WINDOW)
        assert features["rotation_rpm_stddev"] == pytest.approx(0.0)
        assert features["rotation_rpm_zscore_last"] is None

    def test_empty_window_yields_all_none_except_counts(self) -> None:
        features = compute_features([])
        assert features["sample_count"] == 0.0
        assert features["null_ratio"] is None
        assert features["temperature_c_mean"] is None
        assert features["corr_power_kw__rotation_rpm"] is None


class TestDeterministicLastValue:
    def test_last_is_defined_by_event_time_not_by_arrival(self) -> None:
        """The reason the specification says min_by/max_by.

        Spark's first()/last() depend on row order after a shuffle, so a
        conformance test built on them would fail intermittently. Sorting by
        event time is what makes every engine agree.
        """
        shuffled = [_RAMP_WINDOW[1], _RAMP_WINDOW[2], _RAMP_WINDOW[0]]
        assert compute_features(shuffled) == compute_features(_RAMP_WINDOW)

    def test_deviation_and_slope_follow_event_time(self) -> None:
        reversed_order = list(reversed(_RAMP_WINDOW))
        features = compute_features(reversed_order)
        assert features["temperature_c_deviation_from_mean"] == pytest.approx(10.0)
        assert features["temperature_c_slope_per_second"] == pytest.approx(1.0)


class TestCorrelations:
    def test_perfect_positive_correlation(self) -> None:
        """Power and temperature both ramp linearly here, so r = 1."""
        features = compute_features(_RAMP_WINDOW)
        assert features["corr_temperature_c__power_kw"] == pytest.approx(1.0)

    def test_perfect_negative_correlation(self) -> None:
        window = [
            _sample(0, power_kw=10.0, rotation_rpm=1300.0),
            _sample(10, power_kw=20.0, rotation_rpm=1200.0),
            _sample(20, power_kw=30.0, rotation_rpm=1100.0),
        ]
        features = compute_features(window)
        assert features["corr_power_kw__rotation_rpm"] == pytest.approx(-1.0)

    def test_correlation_is_none_when_one_signal_is_flat(self) -> None:
        """Undefined, not zero -- and the null is itself the stuck-sensor signal."""
        features = compute_features(_RAMP_WINDOW)
        assert features["rotation_rpm_stddev"] == pytest.approx(0.0)
        assert features["corr_power_kw__rotation_rpm"] is None

    def test_correlation_uses_pairwise_deletion(self) -> None:
        """A missing value drops that pair only, not the whole window."""
        window = [
            _sample(0, power_kw=10.0, rotation_rpm=1000.0),
            _sample(10, power_kw=None, rotation_rpm=1100.0),
            _sample(20, power_kw=30.0, rotation_rpm=1200.0),
            _sample(30, power_kw=40.0, rotation_rpm=1300.0),
        ]
        features = compute_features(window)
        # Remaining pairs (10, 1000), (30, 1200), (40, 1300) are collinear.
        assert features["corr_power_kw__rotation_rpm"] == pytest.approx(1.0)

    def test_correlation_needs_two_pairs(self) -> None:
        assert pearson_correlation([1.0], [2.0]) is None

    def test_correlation_is_bounded(self) -> None:
        """Floating-point error must not push a perfect correlation past 1."""
        xs = [float(i) for i in range(100)]
        ys = [3.0 * x + 7.0 for x in xs]
        result = pearson_correlation(xs, ys)
        assert result is not None
        assert -1.0 <= result <= 1.0

    def test_correlation_matches_the_textbook_value(self) -> None:
        """Hand-computed rather than taken from NumPy.

        x = [1, 2, 3, 4], y = [2, 4, 5, 9]: mean_x = 2.5, mean_y = 5.
        cov = (-1.5)(-3) + (-0.5)(-1) + (0.5)(0) + (1.5)(4) = 4.5 + 0.5 + 0 + 6 = 11
        var_x = 2.25 + 0.25 + 0.25 + 2.25 = 5
        var_y = 9 + 1 + 0 + 16 = 26
        r = 11 / sqrt(130) = 0.96476...
        """
        result = pearson_correlation([1.0, 2.0, 3.0, 4.0], [2.0, 4.0, 5.0, 9.0])
        assert result == pytest.approx(11.0 / math.sqrt(130.0))


class TestDerivedSignals:
    def test_ratio_is_computed_per_sample_then_aggregated(self) -> None:
        features = compute_features(_RAMP_WINDOW)
        assert features["power_per_rpm_mean"] == pytest.approx(0.02)

    def test_ratio_of_means_would_give_a_different_answer(self) -> None:
        """With varying rpm the two definitions genuinely diverge.

        Samples: 10 kW at 1000 rpm -> 0.010, 30 kW at 3000 rpm -> 0.010,
        30 kW at 1000 rpm -> 0.030.
        Mean of ratios     = (0.010 + 0.010 + 0.030) / 3 = 0.016667
        Ratio of the means = (10 + 30 + 30) / (1000 + 3000 + 1000) = 0.014
        """
        window = [
            _sample(0, power_kw=10.0, rotation_rpm=1000.0),
            _sample(10, power_kw=30.0, rotation_rpm=3000.0),
            _sample(20, power_kw=30.0, rotation_rpm=1000.0),
        ]
        features = compute_features(window)
        assert features["power_per_rpm_mean"] == pytest.approx(0.0166667, abs=1e-6)
        ratio_of_means = (10.0 + 30.0 + 30.0) / (1000.0 + 3000.0 + 1000.0)
        assert features["power_per_rpm_mean"] != pytest.approx(ratio_of_means, abs=1e-6)

    def test_derived_signals_have_a_slope(self) -> None:
        """A rising specific consumption is the wear signature itself."""
        window = [
            _sample(0, power_kw=10.0, rotation_rpm=1000.0),
            _sample(10, power_kw=20.0, rotation_rpm=1000.0),
        ]
        # 0.010 -> 0.020 over 10 s = 0.001 per second
        features = compute_features(window)
        assert features["power_per_rpm_slope_per_second"] == pytest.approx(0.001)

    def test_ratio_is_none_when_the_machine_is_barely_turning(self) -> None:
        readings = SensorReadings(
            temperature_c=20.0,
            vibration_mm_s=0.1,
            pressure_bar=1.0,
            power_kw=5.0,
            rotation_rpm=0.4,
        )
        assert derived_signal_value("power_per_rpm", readings) is None

    def test_ratio_is_none_when_the_numerator_is_missing(self) -> None:
        readings = SensorReadings(
            temperature_c=20.0,
            vibration_mm_s=1.0,
            pressure_bar=1.0,
            power_kw=None,
            rotation_rpm=1500.0,
        )
        assert derived_signal_value("power_per_rpm", readings) is None


class TestMissingData:
    def test_a_failed_sensor_does_not_blind_the_others(self) -> None:
        window = [
            _sample(0, temperature_c=10.0),
            _sample(10, temperature_c=None),
            _sample(20, temperature_c=30.0),
        ]
        features = compute_features(window)
        assert features["temperature_c_mean"] == pytest.approx(20.0)
        assert features["pressure_bar_mean"] == pytest.approx(5.0)

    def test_null_ratio_counts_missing_readings_across_the_window(self) -> None:
        """One missing reading out of 3 samples x 5 sensors = 15 slots."""
        window = [_sample(0), _sample(10, temperature_c=None), _sample(20)]
        features = compute_features(window)
        assert features["null_ratio"] == pytest.approx(1.0 / 15.0)

    def test_a_fully_missing_sensor_yields_none_everywhere(self) -> None:
        window = [_sample(offset, pressure_bar=None) for offset in (0, 10, 20)]
        features = compute_features(window)
        assert features["pressure_bar_mean"] is None
        assert features["pressure_bar_zscore_last"] is None
        assert features["corr_pressure_bar__rotation_rpm"] is None

    def test_sample_count_is_exposed_as_a_feature(self) -> None:
        features = compute_features(_RAMP_WINDOW)
        assert features["sample_count"] == pytest.approx(3.0)


class TestDeterminism:
    def test_result_is_independent_of_arrival_order(self) -> None:
        shuffled = [_RAMP_WINDOW[2], _RAMP_WINDOW[0], _RAMP_WINDOW[1]]
        assert compute_features(shuffled) == compute_features(_RAMP_WINDOW)

    def test_repeated_computation_is_identical(self) -> None:
        assert compute_features(_RAMP_WINDOW) == compute_features(_RAMP_WINDOW)


def test_every_aggregation_kind_is_exercised_by_the_specification() -> None:
    """Guards against adding an AggregationKind that nothing uses."""
    used = {spec.aggregation for spec in FEATURE_SPECS}
    assert used == set(AggregationKind)
