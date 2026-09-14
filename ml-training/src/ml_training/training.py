"""Orchestration: splits, pipelines, calibration, threshold, evaluation.

The order of operations here is the anti-leakage strategy, so it is worth
reading as such:

1. **Split the samples by time, then window each split separately.** Windowing
   first and splitting after would let a window straddle the boundary and carry
   training data into validation.
2. **Fit everything on train only** -- normaliser, imputer, scaler, detector and
   the calibration grid.
3. **Choose the threshold on validation**, never on test.
4. **Touch test once**, with the threshold already frozen.

Training data is *not* filtered by the labels (decision D-27). In production
nobody knows which windows are clean, so filtering them would give the model
information it will never have and make every metric optimistic. The
label-filtered variant is measured separately, as a secondary experiment, to
quantify what the contamination costs.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import numpy.typing as npt
import pandas as pd
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from telemetry_core.features import (
    FEATURE_NAMES,
    MIN_RUNNING_RATIO,
    MIN_SAMPLES_FOR_SCORING,
)
from telemetry_core.logging import get_logger

from ml_training.calibration import EcdfCalibrator
from ml_training.config.settings import TrainingSettings
from ml_training.dataset.labels import align_window_labels, build_episode_table
from ml_training.evaluation import (
    episode_outcomes,
    false_positive_profile,
    missed_episode_profile,
    operating_points,
    precision_recall_points,
    select_threshold_for_budget,
    window_metrics,
)
from ml_training.features.pandas_builder import build_window_features
from ml_training.models.detectors import (
    RAW_SENSOR_MEAN_FEATURES,
    EllipticEnvelopeDetector,
    IsolationForestDetector,
    OneClassSvmDetector,
    ThreeSigmaDetector,
    column_indices,
    range_feature_indices,
)
from ml_training.models.normalizer import MACHINE_COLUMN, PerMachineNormalizer

__all__ = [
    "MAX_COVARIANCE_CONDITION",
    "Split",
    "SplitSet",
    "build_pipeline",
    "prepare_splits",
    "run_training",
    "selection_exclusion",
]

_LOGGER = get_logger(__name__)

#: Threshold sweep. Dense in the tail, because that is where any usable
#: operating point lives: the interesting question is 0.5 % versus 0.1 % of
#: windows, not 20 % versus 10 %.
THRESHOLD_QUANTILES: tuple[float, ...] = (
    0.90,
    0.95,
    0.97,
    0.98,
    0.99,
    0.993,
    0.995,
    0.997,
    0.998,
    0.999,
    0.9995,
    0.9999,
)


#: Above this, a fitted covariance carries no usable information: float64 has
#: about 16 significant digits, so a condition number beyond 1e12 means the
#: inverse -- which is what a Mahalanobis distance needs -- is dominated by
#: rounding error.
MAX_COVARIANCE_CONDITION = 1e12


def selection_exclusion(payload: dict[str, Any]) -> str | None:
    """Why a fitted candidate must not be selected, or ``None`` if it may be.

    Performance is not the only criterion. A model whose own diagnostics show a
    degenerate numerical basis has to be disqualified regardless of its recall:
    scikit-learn does not refuse a singular covariance, it warns and scores, so
    an unusable estimate can otherwise look like the best candidate. Selecting
    it because its recall was highest would be picking the model whose numbers
    are least trustworthy.
    """
    if payload.get("failed"):
        return "the model could not be fitted"
    if payload.get("role") == "secondary":
        return "secondary variant, reported for comparison only"

    diagnostics = payload.get("covariance_diagnostics")
    if diagnostics:
        if diagnostics["covariance_rank"] < diagnostics["covariance_columns"]:
            return (
                f"rank-deficient covariance ({diagnostics['covariance_rank']} of "
                f"{diagnostics['covariance_columns']} columns): the Mahalanobis "
                "distance rests on a matrix that cannot be inverted reliably"
            )
        if diagnostics["covariance_condition"] > MAX_COVARIANCE_CONDITION:
            return (
                f"covariance condition number {diagnostics['covariance_condition']:.3e} "
                f"exceeds {MAX_COVARIANCE_CONDITION:.0e}: the inverse is dominated by "
                "rounding error"
            )
    return None


@dataclass
class Split:
    """One temporal slice: features on one side, labels on the other."""

    name: str
    features: pd.DataFrame
    labels: pd.DataFrame
    episodes: pd.DataFrame

    @property
    def X(self) -> pd.DataFrame:
        """The model's input. Carries the machine and the features, never a label."""
        return self.features[[MACHINE_COLUMN, *FEATURE_NAMES]]

    @property
    def y(self) -> npt.NDArray[np.bool_]:
        return self.labels["is_anomaly"].to_numpy(dtype=bool)

    def scored_frame(self, scores: npt.NDArray[np.float64]) -> pd.DataFrame:
        """Join features, labels and scores for evaluation only."""
        frame = self.features[
            [
                MACHINE_COLUMN,
                "window_start",
                "window_start_time",
                "window_end_time",
                "machine_state",
            ]
        ].copy()
        frame["score"] = scores
        frame["is_anomaly"] = self.labels["is_anomaly"].to_numpy()
        frame["anomaly_fraction"] = self.labels["anomaly_fraction"].to_numpy()
        frame["episode_id"] = self.labels["episode_id"].to_numpy()
        return frame

    def summary(self) -> dict[str, Any]:
        hours = float(
            (self.features["window_start"].max() - self.features["window_start"].min()) / 3600.0
        )
        return {
            "windows": len(self.features),
            "machines": int(self.features[MACHINE_COLUMN].nunique()),
            "hours": hours,
            "anomalous_windows": int(self.labels["is_anomaly"].sum()),
            "prevalence": float(self.labels["is_anomaly"].mean()),
            "episodes": int(self.labels["episode_id"].nunique()),
            "first_window": str(self.features["window_start_time"].min()),
            "last_window": str(self.features["window_start_time"].max()),
        }


@dataclass
class SplitSet:
    train: Split
    validation: Split
    test: Split

    def summary(self) -> dict[str, Any]:
        return {
            "train": self.train.summary(),
            "validation": self.validation.summary(),
            "test": self.test.summary(),
        }


def _scorable(features: pd.DataFrame) -> pd.DataFrame:
    """Apply the same admission gate the streaming job will apply.

    Windows that the pipeline would refuse to score must not appear in training
    either, or the model learns a distribution it will never be shown.
    """
    admitted = (features["sample_count"] >= MIN_SAMPLES_FOR_SCORING) & (
        features["running_ratio"] >= MIN_RUNNING_RATIO
    )
    return features.loc[admitted].reset_index(drop=True)


def _slice(
    frame: pd.DataFrame, column: str, start: pd.Timestamp, end: pd.Timestamp
) -> pd.DataFrame:
    return frame.loc[(frame[column] >= start) & (frame[column] < end)].reset_index(drop=True)


def prepare_splits(
    samples: pd.DataFrame, labels: pd.DataFrame, settings: TrainingSettings
) -> SplitSet:
    """Cut by time, window each slice separately, then attach the labels."""
    origin = samples["event_time"].min()
    train_end = origin + pd.Timedelta(hours=settings.train_end_hours)
    validation_end = origin + pd.Timedelta(hours=settings.validation_end_hours)
    dataset_end = samples["event_time"].max() + pd.Timedelta(seconds=1)

    boundaries = {
        "train": (origin, train_end, settings.window_seconds),
        "validation": (train_end, validation_end, settings.slide_seconds),
        "test": (validation_end, dataset_end, settings.slide_seconds),
    }

    built: dict[str, Split] = {}
    for name, (start, end, slide) in boundaries.items():
        sample_slice = _slice(samples, "event_time", start, end)
        if sample_slice.empty:
            raise ValueError(f"split {name!r} is empty; check the split boundaries")
        label_slice = _slice(labels, "event_time", start, end) if not labels.empty else labels

        features = build_window_features(
            sample_slice, window_seconds=settings.window_seconds, slide_seconds=slide
        )
        features = _scorable(features)
        window_labels = align_window_labels(
            label_slice,
            features,
            window_seconds=settings.window_seconds,
            slide_seconds=slide,
            min_anomaly_fraction=settings.min_anomaly_fraction,
        )
        built[name] = Split(
            name=name,
            features=features,
            labels=window_labels,
            episodes=build_episode_table(label_slice),
        )
        _LOGGER.info("split_prepared", split=name, **built[name].summary())

    return SplitSet(train=built["train"], validation=built["validation"], test=built["test"])


def build_pipeline(detector: Any, *, seed: int) -> Pipeline:
    """The inference path, identical for every candidate.

    Same preprocessing for all models on purpose: a difference in results must
    be a difference between models, not between pipelines. In particular the
    baseline is not handicapped -- giving it worse inputs would make beating it
    meaningless.

    ``add_indicator`` matters more than it looks. A dropped-out sensor produces
    null features; the indicator turns that absence into an explicit column, so
    "this sensor stopped reporting" becomes something the detector can use
    rather than something median imputation erases.
    """
    return Pipeline(
        [
            ("per_machine", PerMachineNormalizer(feature_names=list(FEATURE_NAMES))),
            ("impute", SimpleImputer(strategy="median", add_indicator=True)),
            ("scale", StandardScaler()),
            ("detector", detector),
        ]
    )


def _candidate_detectors(settings: TrainingSettings) -> dict[str, dict[str, Any]]:
    """The mandated three, plus two exploratory variants, each clearly labelled."""
    return {
        "isolation_forest": {
            "detector": IsolationForestDetector(
                n_estimators=300, max_samples=256, random_state=settings.seed
            ),
            "role": "mandatory",
            "features_used": "all",
        },
        "one_class_svm": {
            "detector": OneClassSvmDetector(nu=0.05, gamma="scale"),
            "role": "mandatory",
            "features_used": "all",
            "max_training_rows": settings.ocsvm_max_training_rows,
        },
        "three_sigma": {
            "detector": ThreeSigmaDetector(column_subset=column_indices(RAW_SENSOR_MEAN_FEATURES)),
            "role": "mandatory baseline",
            "features_used": "raw sensor means only",
        },
        "three_sigma_all_features": {
            "detector": ThreeSigmaDetector(),
            "role": "secondary",
            "features_used": "all",
        },
        "elliptic_envelope": {
            "detector": EllipticEnvelopeDetector(random_state=settings.seed),
            "role": "exploratory",
            "features_used": "all except *_range (exactly collinear, singular covariance)",
        },
    }


def _detector_column_subset(name: str, detector: Any) -> None:
    """Exclude the collinear range columns from the covariance-based model."""
    if name == "elliptic_envelope":
        excluded = set(range_feature_indices())
        detector.column_subset = [i for i in range(len(FEATURE_NAMES)) if i not in excluded]


def _fit_one(
    name: str,
    configuration: dict[str, Any],
    splits: SplitSet,
    settings: TrainingSettings,
    rng: np.random.Generator,
) -> dict[str, Any]:
    """Fit, calibrate, choose a threshold on validation, evaluate once on test."""
    detector = configuration["detector"]
    _detector_column_subset(name, detector)
    pipeline = build_pipeline(detector, seed=settings.seed)

    training_frame = splits.train.X
    subsampled = False
    limit = configuration.get("max_training_rows")
    if limit is not None and len(training_frame) > limit:
        # Deterministic subsample: One-Class SVM trains in O(n^2)-O(n^3), and
        # the cost is measured at several sizes rather than asserted.
        chosen = np.sort(rng.choice(len(training_frame), size=limit, replace=False))
        training_frame = training_frame.iloc[chosen]
        subsampled = True

    started = time.perf_counter()
    pipeline.fit(training_frame)
    fit_seconds = time.perf_counter() - started

    calibrator = EcdfCalibrator.fit(
        np.asarray(pipeline.score_samples(splits.train.X), dtype="float64")
    )

    def calibrated(split: Split) -> npt.NDArray[np.float64]:
        return calibrator.transform(np.asarray(pipeline.score_samples(split.X), dtype="float64"))

    validation_scores = calibrated(splits.validation)
    validation_frame = splits.validation.scored_frame(validation_scores)
    points = operating_points(
        validation_frame,
        splits.validation.episodes,
        quantiles=list(THRESHOLD_QUANTILES),
        slide_seconds=settings.slide_seconds,
    )
    chosen_point = select_threshold_for_budget(
        points, budget_per_machine_hour=settings.alert_budget_per_machine_hour
    )
    best_f1_point = max(points, key=lambda point: point.window_f1)

    test_scores = calibrated(splits.test)
    test_frame = splits.test.scored_frame(test_scores)
    test_metrics = window_metrics(splits.test.y, test_scores, chosen_point.threshold)
    scorable_test_episodes = splits.test.episodes.loc[
        splits.test.episodes["episode_id"].isin(set(test_frame["episode_id"].dropna()))
    ]
    outcomes = episode_outcomes(
        test_frame, scorable_test_episodes, threshold=chosen_point.threshold
    )
    detected = [outcome for outcome in outcomes if outcome.detected]
    latencies = [
        outcome.detection_latency_seconds
        for outcome in detected
        if outcome.detection_latency_seconds is not None
    ]

    return {
        "role": configuration["role"],
        "features_used": configuration["features_used"],
        "hyperparameters": {
            key: value for key, value in detector.get_params().items() if key != "column_subset"
        },
        "training_rows": len(training_frame),
        "training_subsampled": subsampled,
        "fit_seconds": float(fit_seconds),
        "support_vectors": getattr(detector, "n_support_vectors_", None),
        "covariance_diagnostics": (
            detector.diagnostics() if hasattr(detector, "diagnostics") else None
        ),
        "operating_points": [point.as_dict() for point in points],
        "selected_operating_point": chosen_point.as_dict(),
        "best_f1_operating_point": best_f1_point.as_dict(),
        "test": {
            "window_metrics": test_metrics.as_dict(),
            "precision_recall_curve": precision_recall_points(splits.test.y, test_scores),
            "episodes_scorable": len(outcomes),
            "episodes_total": len(splits.test.episodes),
            "episodes_detected": len(detected),
            "episode_recall": float(len(detected) / len(outcomes)) if outcomes else 0.0,
            "median_detection_latency_seconds": (
                float(np.median(latencies)) if latencies else None
            ),
            "false_positives": false_positive_profile(test_frame, threshold=chosen_point.threshold),
            "missed_episodes": missed_episode_profile(outcomes),
        },
        "_pipeline": pipeline,
        "_calibrator": calibrator,
        "_threshold": chosen_point.threshold,
        "_test_frame": test_frame,
    }


def measure_ocsvm_scaling(
    splits: SplitSet, settings: TrainingSettings, rng: np.random.Generator
) -> list[dict[str, Any]]:
    """Measure how One-Class SVM training scales, instead of claiming it.

    "Quadratic" is a statement about an algorithm; what matters for a streaming
    system is the wall-clock cost at the sizes we would actually use, and how
    many support vectors inference then has to carry.
    """
    available = len(splits.train.X)
    sizes = [size for size in (1000, 2000, 4000, 8000) if size <= available]
    results: list[dict[str, Any]] = []
    for size in sizes:
        chosen = np.sort(rng.choice(available, size=size, replace=False))
        frame = splits.train.X.iloc[chosen]
        pipeline = build_pipeline(OneClassSvmDetector(nu=0.05, gamma="scale"), seed=settings.seed)
        started = time.perf_counter()
        pipeline.fit(frame)
        elapsed = time.perf_counter() - started
        detector = pipeline.named_steps["detector"]
        results.append(
            {
                "training_rows": int(size),
                "fit_seconds": float(elapsed),
                "support_vectors": int(detector.n_support_vectors_),
            }
        )
        _LOGGER.info("ocsvm_scaling_measured", rows=size, seconds=elapsed)
    return results


def measure_clean_training_variant(
    splits: SplitSet, settings: TrainingSettings, *, detector_name: str
) -> dict[str, Any]:
    """Secondary experiment: what does training on label-filtered data buy?

    Not the main protocol (decision D-27): filtering by the labels uses
    information production does not have, so any gain it shows is unavailable in
    practice. Measuring it puts a number on that gap instead of leaving it as an
    assertion.

    It uses **the same detector as the selected model**, and the same subsampling
    rule. Comparing a filtered Isolation Forest against a contaminated One-Class
    SVM would measure the difference between two estimators and report it as the
    cost of contamination.
    """
    configuration = _candidate_detectors(settings)[detector_name]
    detector = configuration["detector"]
    _detector_column_subset(detector_name, detector)

    clean_mask = ~splits.train.labels["is_anomaly"].to_numpy(dtype=bool)
    clean_frame = splits.train.X.loc[clean_mask]

    subsampled = False
    limit = configuration.get("max_training_rows")
    if limit is not None and len(clean_frame) > limit:
        rng = np.random.default_rng(settings.seed)
        chosen = np.sort(rng.choice(len(clean_frame), size=limit, replace=False))
        clean_frame = clean_frame.iloc[chosen]
        subsampled = True

    pipeline = build_pipeline(detector, seed=settings.seed)
    pipeline.fit(clean_frame)
    calibrator = EcdfCalibrator.fit(
        np.asarray(pipeline.score_samples(clean_frame), dtype="float64")
    )

    validation_scores = calibrator.transform(
        np.asarray(pipeline.score_samples(splits.validation.X), dtype="float64")
    )
    validation_frame = splits.validation.scored_frame(validation_scores)
    points = operating_points(
        validation_frame,
        splits.validation.episodes,
        quantiles=list(THRESHOLD_QUANTILES),
        slide_seconds=settings.slide_seconds,
    )
    chosen_point = select_threshold_for_budget(
        points, budget_per_machine_hour=settings.alert_budget_per_machine_hour
    )

    test_scores = calibrator.transform(
        np.asarray(pipeline.score_samples(splits.test.X), dtype="float64")
    )
    metrics = window_metrics(splits.test.y, test_scores, chosen_point.threshold)
    test_frame = splits.test.scored_frame(test_scores)
    scorable = splits.test.episodes.loc[
        splits.test.episodes["episode_id"].isin(set(test_frame["episode_id"].dropna()))
    ]
    outcomes = episode_outcomes(test_frame, scorable, threshold=chosen_point.threshold)
    detected = sum(1 for outcome in outcomes if outcome.detected)

    return {
        "detector": detector_name,
        "training_rows": len(clean_frame),
        "training_subsampled": subsampled,
        "removed_by_label_filter": int(splits.train.labels["is_anomaly"].sum()),
        "selected_operating_point": chosen_point.as_dict(),
        "test_window_metrics": metrics.as_dict(),
        "test_episode_recall": float(detected / len(outcomes)) if outcomes else 0.0,
    }


def run_training(
    samples: pd.DataFrame,
    labels: pd.DataFrame,
    settings: TrainingSettings,
) -> tuple[SplitSet, dict[str, Any]]:
    """Fit every candidate and return the raw, measured results."""
    splits = prepare_splits(samples, labels, settings)
    rng = np.random.default_rng(settings.seed)

    results: dict[str, Any] = {}
    for name, configuration in _candidate_detectors(settings).items():
        _LOGGER.info("model_training_started", model=name)
        try:
            results[name] = _fit_one(name, configuration, splits, settings, rng)
            _LOGGER.info(
                "model_trained",
                model=name,
                fit_seconds=results[name]["fit_seconds"],
                test_f1=results[name]["test"]["window_metrics"]["f1"],
            )
        except Exception as exc:
            # A model that cannot be fitted is reported as such rather than
            # silently dropped from the comparison.
            _LOGGER.error("model_training_failed", model=name, error=str(exc))
            results[name] = {
                "role": configuration["role"],
                "failed": True,
                "error": f"{type(exc).__name__}: {exc}",
            }

    return splits, results


def reference_profile(features: pd.DataFrame) -> dict[str, Any]:
    """Per-feature summary of the training distribution, for drift tracking.

    Frozen with the artefact so drift is always measured against the same
    origin. Comparing against yesterday instead would make a slow drift
    invisible: every day looks like the one before.
    """
    described = features[list(FEATURE_NAMES)].describe(
        percentiles=[0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99]
    )
    return {
        "rows": len(features),
        "features": {
            name: {
                statistic: (None if pd.isna(value) else float(value))
                for statistic, value in described[name].items()
            }
            for name in FEATURE_NAMES
        },
    }


def artifact_directory(base: Path, model_name: str, version: str) -> Path:
    return base / model_name / version
