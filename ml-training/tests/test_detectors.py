"""Detector behaviour: score orientation, the baseline, and the collinearity trap."""

from __future__ import annotations

import numpy as np
import numpy.typing as npt
import pytest
from telemetry_core.features import FEATURE_NAMES
from telemetry_core.schemas import SENSOR_NAMES

from ml_training.models.detectors import (
    RAW_SENSOR_MEAN_FEATURES,
    EllipticEnvelopeDetector,
    IsolationForestDetector,
    OneClassSvmDetector,
    ThreeSigmaDetector,
    column_indices,
    range_feature_indices,
)


def _blob_with_outliers(seed: int = 0) -> tuple[npt.NDArray[np.float64], npt.NDArray[np.float64]]:
    """A tight Gaussian cluster plus a handful of obvious outliers."""
    rng = np.random.default_rng(seed)
    normal = rng.normal(0.0, 1.0, size=(400, 6))
    outliers = rng.normal(12.0, 0.5, size=(10, 6))
    return normal, outliers


class TestScoreOrientation:
    """Every detector must agree that higher means more anomalous.

    scikit-learn uses the opposite sign, so each wrapper negates it. Getting
    that backwards yields a model that is confidently inverted, and nothing in a
    "does it run" check would notice.
    """

    @pytest.mark.parametrize(
        "detector",
        [
            IsolationForestDetector(n_estimators=60, max_samples=128, random_state=0),
            OneClassSvmDetector(nu=0.05),
            EllipticEnvelopeDetector(random_state=0),
            ThreeSigmaDetector(),
        ],
        ids=["isolation_forest", "one_class_svm", "elliptic_envelope", "three_sigma"],
    )
    def test_outliers_score_higher_than_normal_points(self, detector: object) -> None:
        normal, outliers = _blob_with_outliers()
        detector.fit(normal)  # type: ignore[attr-defined]
        normal_scores = detector.score_samples(normal)  # type: ignore[attr-defined]
        outlier_scores = detector.score_samples(outliers)  # type: ignore[attr-defined]
        assert float(np.median(outlier_scores)) > float(np.median(normal_scores))

    def test_scores_are_finite(self) -> None:
        normal, outliers = _blob_with_outliers()
        detector = IsolationForestDetector(n_estimators=60, max_samples=128, random_state=0)
        detector.fit(normal)
        assert np.isfinite(detector.score_samples(outliers)).all()


class TestThreeSigmaBaseline:
    def test_it_defaults_to_the_raw_sensor_means(self) -> None:
        """What a plant has without any feature engineering at all."""
        assert RAW_SENSOR_MEAN_FEATURES == tuple(f"{name}_mean" for name in SENSOR_NAMES)
        assert len(column_indices(RAW_SENSOR_MEAN_FEATURES)) == len(SENSOR_NAMES)

    def test_it_is_blind_to_a_change_outside_its_columns(self) -> None:
        """The discriminating case, stated as a test.

        A frozen sensor keeps a perfectly normal mean, so a rule that only looks
        at means cannot see it -- while the standard-deviation feature drops to
        zero. This is why the comparison is worth running rather than assuming.
        """
        rng = np.random.default_rng(3)
        columns = len(FEATURE_NAMES)
        normal = rng.normal(0.0, 1.0, size=(500, columns))

        mean_indices = column_indices(RAW_SENSOR_MEAN_FEATURES)
        stddev_index = FEATURE_NAMES.index("vibration_mm_s_stddev")

        # An event that moves only the dispersion feature, never a mean.
        anomalous = rng.normal(0.0, 1.0, size=(20, columns))
        anomalous[:, stddev_index] = -9.0

        baseline = ThreeSigmaDetector(column_subset=mean_indices)
        baseline.fit(normal)
        informed = ThreeSigmaDetector()
        informed.fit(normal)

        baseline_gap = float(np.median(baseline.score_samples(anomalous))) - float(
            np.median(baseline.score_samples(normal))
        )
        informed_gap = float(np.median(informed.score_samples(anomalous))) - float(
            np.median(informed.score_samples(normal))
        )
        assert informed_gap > baseline_gap
        assert baseline_gap < 1.0

    def test_a_constant_column_does_not_divide_by_zero(self) -> None:
        values = np.column_stack([np.random.default_rng(0).normal(size=200), np.full(200, 4.0)])
        detector = ThreeSigmaDetector()
        detector.fit(values)
        assert np.isfinite(detector.score_samples(values)).all()

    def test_it_uses_ddof_one(self) -> None:
        """Consistent with every other standard deviation in the platform."""
        values = np.array([[10.0], [20.0], [30.0]])
        detector = ThreeSigmaDetector()
        detector.fit(values)
        assert float(detector.scale_[0]) == pytest.approx(10.0)


class TestCollinearity:
    def test_range_features_are_identified(self) -> None:
        indices = range_feature_indices()
        assert indices
        assert all(FEATURE_NAMES[index].endswith("_range") for index in indices)
        assert len(indices) == len(SENSOR_NAMES)

    def test_an_exact_linear_dependency_makes_the_covariance_singular(self) -> None:
        """Why EllipticEnvelope drops the range columns.

        ``range = max - min`` is an exact linear combination, so the covariance
        is rank-deficient. scikit-learn does **not** raise on it -- it warns and
        proceeds, so the model loads, scores, and rests on a matrix at the edge
        of float64 precision. Excluding the range columns is a precaution, not a
        workaround for a crash.
        """
        rng = np.random.default_rng(5)
        minimum = rng.normal(size=(300, 1))
        maximum = minimum + rng.uniform(0.5, 1.5, size=(300, 1))
        collinear = np.hstack([minimum, maximum, maximum - minimum])

        detector = EllipticEnvelopeDetector(random_state=0)
        detector.fit(collinear)

        # The dependent column is dropped, so what is actually fitted is sound.
        assert detector.dropped_dependent_ == 1
        diagnostics = detector.diagnostics()
        assert diagnostics["covariance_rank"] == diagnostics["covariance_columns"]
        assert diagnostics["covariance_condition"] < 1e6
        assert np.isfinite(detector.score_samples(collinear)).all()

        # Left to itself, scikit-learn accepts the singular matrix; that is what
        # the guard exists for.
        from sklearn.covariance import EllipticEnvelope

        unguarded = EllipticEnvelope(contamination=0.05, random_state=0).fit(collinear)
        assert np.linalg.matrix_rank(unguarded.covariance_) < collinear.shape[1]

    def test_a_zero_variance_column_is_dropped(self) -> None:
        """A constant column is a degenerate case of linear dependence."""
        rng = np.random.default_rng(7)
        values = np.column_stack([rng.normal(size=(300, 3)), np.full(300, 4.0)])
        detector = EllipticEnvelopeDetector(random_state=0)
        detector.fit(values)

        assert detector.dropped_dependent_ == 1
        diagnostics = detector.diagnostics()
        assert diagnostics["covariance_rank"] == diagnostics["covariance_columns"]
        assert diagnostics["covariance_condition"] < 1e6
        assert np.isfinite(detector.score_samples(values)).all()


class TestOneClassSvmCost:
    def test_support_vector_count_is_exposed(self) -> None:
        """Inference cost is proportional to it, so it belongs in the report."""
        normal, _ = _blob_with_outliers()
        detector = OneClassSvmDetector(nu=0.05)
        detector.fit(normal)
        assert detector.n_support_vectors_ > 0

    def test_support_vectors_grow_with_the_training_set(self) -> None:
        rng = np.random.default_rng(11)
        small = rng.normal(size=(200, 4))
        large = rng.normal(size=(800, 4))

        small_detector = OneClassSvmDetector(nu=0.05)
        small_detector.fit(small)
        large_detector = OneClassSvmDetector(nu=0.05)
        large_detector.fit(large)

        assert large_detector.n_support_vectors_ > small_detector.n_support_vectors_
