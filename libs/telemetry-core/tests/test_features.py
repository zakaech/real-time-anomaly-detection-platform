"""Feature semantics, pinned by hand-computed values.

Expected values are computed by hand in the comments rather than by calling
NumPy or pandas. A test that derives its expectation from the same library the
code uses cannot detect the exact class of bug this module exists to prevent:
a ``ddof`` mismatch between Spark and the training pipeline.
"""

from __future__ import annotations

import math
from datetime import UTC, datetime, timedelta

import pytest

from telemetry_core.features import (
    DERIVED_SIGNALS,
    FEATURE_NAMES,
    FEATURE_SET_VERSION,
    FEATURE_SPECS,
    AggregationKind,
    WindowSample,
    compute_features,
    derived_signal_value,
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


# A window of three samples, ten seconds apart, with a linear temperature ramp
# 10 -> 20 -> 30 and a linear power ramp 10 -> 20 -> 30 at constant 1000 rpm.
_RAMP_WINDOW = [
    _sample(0, temperature_c=10.0, power_kw=10.0, vibration_mm_s=1.0),
    _sample(10, temperature_c=20.0, power_kw=20.0, vibration_mm_s=2.0),
    _sample(20, temperature_c=30.0, power_kw=30.0, vibration_mm_s=3.0),
]


class TestFeatureSpecification:
    def test_feature_count_is_stable(self) -> None:
        # 5 sensors x 6 aggregations + 2 derived signals x 2 + 2 quality features
        assert len(FEATURE_SPECS) == len(SENSOR_NAMES) * 6 + len(DERIVED_SIGNALS) * 2
        assert len(FEATURE_NAMES) == 36

    def test_feature_order_is_canonical(self) -> None:
        """A NumPy matrix has no column names: order is the contract.

        Two permuted features produce plausible and wrong scores, silently.
        """
        assert FEATURE_NAMES[:6] == (
            "temperature_c_mean",
            "temperature_c_stddev",
            "temperature_c_min",
            "temperature_c_max",
            "temperature_c_range",
            "temperature_c_slope_per_second",
        )
        assert FEATURE_NAMES[-6:] == (
            "power_per_rpm_mean",
            "power_per_rpm_stddev",
            "vibration_per_rpm_mean",
            "vibration_per_rpm_stddev",
            "sample_count",
            "null_ratio",
        )

    def test_feature_names_are_unique(self) -> None:
        assert len(set(FEATURE_NAMES)) == len(FEATURE_NAMES)

    def test_feature_set_version_is_declared(self) -> None:
        """Recorded in the model artefact: a model must not score another set."""
        assert FEATURE_SET_VERSION == "1.0.0"

    def test_every_spec_name_is_present_in_the_vector(self) -> None:
        computed = compute_features(_RAMP_WINDOW)
        assert set(computed) == set(FEATURE_NAMES)
        assert tuple(computed) == FEATURE_NAMES


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

    def test_empty_window_yields_all_none_except_counts(self) -> None:
        features = compute_features([])
        assert features["sample_count"] == 0.0
        assert features["null_ratio"] is None
        assert features["temperature_c_mean"] is None


class TestDerivedSignals:
    def test_ratio_is_computed_per_sample_then_aggregated(self) -> None:
        """The mean of a ratio is not the ratio of the means.

        Here power ramps 10/20/30 at a constant 1000 rpm, so per-sample ratios
        are 0.01, 0.02, 0.03 and their mean is 0.02. The ratio of means happens
        to coincide at constant rpm -- which is why the next test varies it.
        """
        features = compute_features(_RAMP_WINDOW)
        assert features["power_per_rpm_mean"] == pytest.approx(0.02)

    def test_ratio_of_means_would_give_a_different_answer(self) -> None:
        """With varying rpm the two definitions genuinely diverge.

        Samples: 10 kW at 1000 rpm -> 0.010, 30 kW at 3000 rpm -> 0.010.
        Mean of ratios = 0.010. Ratio of means = 20 / 2000 = 0.010 as well, so
        a third point breaks the symmetry: 30 kW at 1000 rpm -> 0.030.
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

    def test_ratio_is_none_when_the_machine_is_barely_turning(self) -> None:
        """A stopped motor would otherwise produce an enormous fake anomaly."""
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
        # Temperature aggregates over the two surviving points...
        assert features["temperature_c_mean"] == pytest.approx(20.0)
        # ...while pressure is untouched.
        assert features["pressure_bar_mean"] == pytest.approx(5.0)

    def test_null_ratio_counts_missing_readings_across_the_window(self) -> None:
        """One missing reading out of 3 samples x 5 sensors = 15 slots."""
        window = [
            _sample(0),
            _sample(10, temperature_c=None),
            _sample(20),
        ]
        features = compute_features(window)
        assert features["null_ratio"] == pytest.approx(1.0 / 15.0)

    def test_sample_count_is_exposed_as_a_feature(self) -> None:
        """Degraded collection is information about the machine, not a nuisance."""
        features = compute_features(_RAMP_WINDOW)
        assert features["sample_count"] == pytest.approx(3.0)


class TestDeterminism:
    def test_result_is_independent_of_arrival_order(self) -> None:
        """Within the watermark, samples can arrive out of order.

        Slope must not flip sign because of it.
        """
        shuffled = [_RAMP_WINDOW[2], _RAMP_WINDOW[0], _RAMP_WINDOW[1]]
        assert compute_features(shuffled) == compute_features(_RAMP_WINDOW)

    def test_repeated_computation_is_identical(self) -> None:
        assert compute_features(_RAMP_WINDOW) == compute_features(_RAMP_WINDOW)


def test_every_aggregation_kind_is_exercised_by_the_specification() -> None:
    """Guards against adding an AggregationKind that nothing uses."""
    used = {spec.aggregation for spec in FEATURE_SPECS}
    assert used == set(AggregationKind)
