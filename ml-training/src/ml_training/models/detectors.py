"""The candidate detectors, behind one interface.

**Strategy pattern.** Every detector is a scikit-learn estimator exposing
``score_samples`` with one convention: **higher means more anomalous**. The
scikit-learn library uses the opposite sign, so each wrapper negates it. Getting
that backwards produces a model that is confidently wrong in a way no test of
"does it run" would catch, which is why the orientation is asserted in the test
suite rather than trusted.

Sharing the interface means every model goes through exactly the same
preprocessing -- per-machine normalisation, imputation, scaling -- so a
difference in results is a difference between models, not between pipelines. In
particular the baseline is **not** handicapped: giving it worse preprocessing
than the challenger would make the comparison meaningless.

One structural constraint is worth stating. The feature set contains an exact
linear dependency: ``range = max - min``. Tree-based and kernel methods are
indifferent to it, but a covariance-based method inherits a rank-deficient
matrix. scikit-learn does not raise on that: it warns and carries on with a
condition number at the edge of float64 precision, and the model then loads
and scores as if nothing were wrong. :class:`EllipticEnvelopeDetector`
therefore drops the ``*_range`` columns, and the exclusion is recorded in the
artefact.
"""

from __future__ import annotations

from typing import Any, Protocol

import numpy as np
import numpy.typing as npt
from sklearn.base import BaseEstimator
from sklearn.covariance import EllipticEnvelope
from sklearn.ensemble import IsolationForest
from sklearn.svm import OneClassSVM
from telemetry_core.features import FEATURE_NAMES
from telemetry_core.schemas import SENSOR_NAMES

__all__ = [
    "RAW_SENSOR_MEAN_FEATURES",
    "AnomalyDetector",
    "EllipticEnvelopeDetector",
    "IsolationForestDetector",
    "OneClassSvmDetector",
    "ThreeSigmaDetector",
    "column_indices",
    "range_feature_indices",
]

#: The five window means of the raw sensors. This is what a plant has without
#: any feature engineering at all, and it is the subset the mandated 3-sigma
#: baseline uses -- so the comparison measures the value of the whole approach,
#: not of one estimator against another on identical inputs.
RAW_SENSOR_MEAN_FEATURES: tuple[str, ...] = tuple(f"{name}_mean" for name in SENSOR_NAMES)


def column_indices(names: tuple[str, ...] | list[str]) -> list[int]:
    """Positions of ``names`` within the canonical feature order."""
    lookup = {name: index for index, name in enumerate(FEATURE_NAMES)}
    missing = [name for name in names if name not in lookup]
    if missing:
        raise KeyError(f"unknown features: {missing}")
    return [lookup[name] for name in names]


def range_feature_indices() -> list[int]:
    """Positions of every ``*_range`` feature, which are exactly collinear."""
    return [index for index, name in enumerate(FEATURE_NAMES) if name.endswith("_range")]


class AnomalyDetector(Protocol):
    """Anything the training pipeline can fit and score."""

    def fit(self, X: npt.NDArray[np.float64], y: Any = None) -> Any: ...

    def score_samples(self, X: npt.NDArray[np.float64]) -> npt.NDArray[np.float64]: ...


class _SubsettingDetector(BaseEstimator):  # type: ignore[misc]
    """Base for detectors that may look at only part of the feature matrix."""

    def __init__(self, column_subset: list[int] | None = None) -> None:
        self.column_subset = column_subset

    def _select(self, X: npt.NDArray[np.float64]) -> npt.NDArray[np.float64]:
        if self.column_subset is None:
            return X
        return X[:, self.column_subset]


class IsolationForestDetector(_SubsettingDetector):
    """Isolation Forest.

    Hypothesis: an anomaly is isolated by fewer random axis-aligned cuts than a
    normal point, because it sits in a sparse region.

    Strong here because it assumes no distribution, costs ``O(t log n)`` per
    inference, and serialises to a few hundred kilobytes with no training data
    inside -- which is what makes it shippable to every Spark executor. Weak on
    oblique boundaries, since its cuts are axis-aligned; that is precisely why
    the ratio and correlation features exist, to give it axes that already point
    along the physics.
    """

    def __init__(
        self,
        *,
        n_estimators: int = 300,
        max_samples: int | float = 256,
        max_features: float = 1.0,
        random_state: int = 0,
        column_subset: list[int] | None = None,
    ) -> None:
        super().__init__(column_subset=column_subset)
        self.n_estimators = n_estimators
        self.max_samples = max_samples
        self.max_features = max_features
        self.random_state = random_state

    def fit(self, X: npt.NDArray[np.float64], y: Any = None) -> IsolationForestDetector:
        self.model_ = IsolationForest(
            n_estimators=self.n_estimators,
            max_samples=self.max_samples,
            max_features=self.max_features,
            # The proportion of anomalies is a policy decision, not a model
            # parameter: the model produces a score, the threshold produces a
            # decision. Fixing contamination here would make it impossible to
            # change the operating point without retraining.
            contamination="auto",
            random_state=self.random_state,
            n_jobs=-1,
        )
        self.model_.fit(self._select(X))
        return self

    def score_samples(self, X: npt.NDArray[np.float64]) -> npt.NDArray[np.float64]:
        # scikit-learn returns higher = more normal; we invert so that every
        # detector in this module agrees on the direction.
        return -np.asarray(self.model_.score_samples(self._select(X)), dtype="float64")


class OneClassSvmDetector(_SubsettingDetector):
    """One-Class SVM with an RBF kernel.

    Hypothesis: normal points occupy a compact region once mapped into a
    reproducing-kernel space, and a maximum-margin boundary separates them from
    the origin.

    Its boundary is genuinely non-linear, which Isolation Forest's axis-aligned
    cuts are not. The cost is the problem: training is between ``O(n^2)`` and
    ``O(n^3)``, and inference is proportional to the number of support vectors,
    which itself grows with the training set. For a job that must score every
    window of every machine continuously, that is a scaling property worth
    measuring rather than assuming -- which is what the training run does.
    """

    def __init__(
        self,
        *,
        nu: float = 0.05,
        gamma: str | float = "scale",
        kernel: str = "rbf",
        column_subset: list[int] | None = None,
    ) -> None:
        super().__init__(column_subset=column_subset)
        self.nu = nu
        self.gamma = gamma
        self.kernel = kernel

    def fit(self, X: npt.NDArray[np.float64], y: Any = None) -> OneClassSvmDetector:
        self.model_ = OneClassSVM(nu=self.nu, gamma=self.gamma, kernel=self.kernel)
        self.model_.fit(self._select(X))
        self.n_support_vectors_ = int(self.model_.support_vectors_.shape[0])
        return self

    def score_samples(self, X: npt.NDArray[np.float64]) -> npt.NDArray[np.float64]:
        return -np.asarray(self.model_.score_samples(self._select(X)), dtype="float64")


class EllipticEnvelopeDetector(_SubsettingDetector):
    """Robust Gaussian envelope (minimum covariance determinant).

    Hypothesis: the normal data is roughly elliptical, so a robust covariance
    gives a Mahalanobis distance that ranks outliers.

    Included as a fourth, exploratory model because docs/07 named it "the
    reference to beat": it captures linear correlations that a per-sensor rule
    cannot, at a fraction of the cost of a kernel method. It is the only
    candidate that must not take the full feature set: ``range = max - min``
    makes the covariance rank-deficient, and scikit-learn warns rather than
    refusing, so the range columns are excluded here instead.
    """

    def __init__(
        self,
        *,
        support_fraction: float | None = None,
        random_state: int = 0,
        column_subset: list[int] | None = None,
        min_variance: float = 1e-12,
    ) -> None:
        super().__init__(column_subset=column_subset)
        self.support_fraction = support_fraction
        self.random_state = random_state
        self.min_variance = min_variance

    def fit(self, X: npt.NDArray[np.float64], y: Any = None) -> EllipticEnvelopeDetector:
        selected = self._select(X)

        # Excluding the range columns upstream is not enough, and measurement is
        # what showed it: on the real dataset the covariance was still rank 45 of
        # 47, with a condition number around 1e47. A guard against zero-variance
        # columns alone missed it, because the remaining dependencies were linear
        # combinations rather than constants.
        #
        # So the columns are chosen properly: a rank-revealing QR picks a maximal
        # linearly independent subset, which subsumes the zero-variance case (a
        # null column is dependent on nothing and drops out on its own). The
        # result is a covariance that is full rank by construction rather than by
        # hope -- and scikit-learn would not have told us otherwise, since it
        # warns on a singular covariance and carries on scoring.
        self.kept_columns_ = self._independent_columns(selected)
        self.dropped_dependent_ = int(selected.shape[1] - len(self.kept_columns_))

        self.model_ = EllipticEnvelope(
            support_fraction=self.support_fraction,
            contamination=0.05,
            random_state=self.random_state,
        )
        self.model_.fit(selected[:, self.kept_columns_])

        # Recorded rather than hoped for: scikit-learn warns on a rank-deficient
        # covariance and carries on, so the only way to know the estimate is
        # sound is to measure it and put the number in the report.
        covariance = self.model_.covariance_
        self.covariance_columns_ = int(covariance.shape[0])
        self.covariance_rank_ = int(np.linalg.matrix_rank(covariance))
        self.covariance_condition_ = float(np.linalg.cond(covariance))
        return self

    @staticmethod
    def _independent_columns(values: npt.NDArray[np.float64], tolerance: float = 1e-8) -> list[int]:
        """Indices of a maximal linearly independent subset of the columns.

        Rank-revealing QR on the centred matrix: the first ``rank`` pivots are a
        set of columns that spans the same space without redundancy.
        """
        from scipy.linalg import qr

        centred = np.nan_to_num(values - np.nanmean(values, axis=0), nan=0.0)
        rank = int(np.linalg.matrix_rank(centred, tol=tolerance))
        _q, _r, pivots = qr(centred, mode="economic", pivoting=True)
        return sorted(int(index) for index in pivots[:rank])

    def diagnostics(self) -> dict[str, float | int]:
        """Measured health of the fitted covariance, for the report."""
        return {
            "covariance_columns": self.covariance_columns_,
            "covariance_rank": self.covariance_rank_,
            "covariance_condition": self.covariance_condition_,
            "dropped_dependent_columns": self.dropped_dependent_,
        }

    def score_samples(self, X: npt.NDArray[np.float64]) -> npt.NDArray[np.float64]:
        selected = self._select(X)[:, self.kept_columns_]
        return -np.asarray(self.model_.score_samples(selected), dtype="float64")


class ThreeSigmaDetector(_SubsettingDetector):
    """The mandatory trivial baseline: per-sensor distance from the mean.

    Hypothesis: each sensor is roughly Gaussian and independent, so anything
    beyond a few standard deviations is abnormal.

    The score is ``max_j |x_j - mu_j| / sigma_j`` over the selected columns,
    which keeps it continuous and therefore comparable with the others once
    calibrated -- a hard yes/no rule could not produce a precision-recall curve.

    By default it sees only the five raw sensor means, because that is the rule a
    plant already has. If a model with the full feature set cannot beat it, the
    feature engineering earns nothing here.
    """

    def __init__(self, *, column_subset: list[int] | None = None) -> None:
        super().__init__(column_subset=column_subset)

    def fit(self, X: npt.NDArray[np.float64], y: Any = None) -> ThreeSigmaDetector:
        selected = self._select(X)
        self.center_ = np.nanmean(selected, axis=0)
        # ddof=1, consistent with every other standard deviation in the platform.
        self.scale_ = np.nanstd(selected, axis=0, ddof=1)
        self.scale_ = np.where(np.isfinite(self.scale_) & (self.scale_ > 1e-12), self.scale_, 1.0)
        return self

    def score_samples(self, X: npt.NDArray[np.float64]) -> npt.NDArray[np.float64]:
        selected = self._select(X)
        deviations = np.abs((selected - self.center_) / self.scale_)
        return np.asarray(np.nanmax(deviations, axis=1), dtype="float64")
