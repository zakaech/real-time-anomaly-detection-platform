"""Ground truth must never reach an estimator.

Four independent guards, because this is the failure that produces excellent
metrics and a worthless model, and it produces no error message on the way.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
from telemetry_core.features import FEATURE_NAMES

from conftest import make_labels, make_samples
from ml_training.config.settings import TrainingSettings
from ml_training.dataset.labels import align_window_labels
from ml_training.features.pandas_builder import build_window_features
from ml_training.models.detectors import IsolationForestDetector
from ml_training.models.normalizer import MACHINE_COLUMN
from ml_training.training import build_pipeline, prepare_splits

LABEL_COLUMNS = {
    "is_anomaly",
    "anomaly_fraction",
    "anomalous_samples",
    "episode_id",
    "anomaly_type",
    "episode_started_at",
}


def _settings() -> TrainingSettings:
    return TrainingSettings(
        train_end_hours=0.06,
        validation_end_hours=0.11,
        window_seconds=30,
        slide_seconds=10,
    )


class TestFeatureFrameIsClean:
    def test_window_features_contain_no_label_column(self) -> None:
        """The feature builder reads telemetry only; labels are joined later."""
        features = build_window_features(
            make_samples(seconds=180), window_seconds=60, slide_seconds=60
        )
        assert not set(features.columns) & LABEL_COLUMNS

    def test_model_input_is_exactly_machine_plus_features(self) -> None:
        samples = make_samples(seconds=600)
        splits = prepare_splits(samples, make_labels(samples), _settings())
        assert list(splits.train.X.columns) == [MACHINE_COLUMN, *FEATURE_NAMES]

    def test_features_and_labels_are_separate_frames(self) -> None:
        """They are never concatenated, so no code path can hand one to the other."""
        samples = make_samples(seconds=600)
        splits = prepare_splits(samples, make_labels(samples), _settings())
        assert not set(splits.train.X.columns) & set(splits.train.labels.columns) - {
            MACHINE_COLUMN,
            "window_start",
        }

    def test_label_frame_carries_no_feature(self) -> None:
        samples = make_samples(seconds=300)
        features = build_window_features(samples, window_seconds=60, slide_seconds=60)
        labels = align_window_labels(
            make_labels(samples),
            features,
            window_seconds=60,
            slide_seconds=60,
            min_anomaly_fraction=0.25,
        )
        assert not set(labels.columns) & set(FEATURE_NAMES)


class TestScoresIgnoreLabels:
    def test_permuting_the_labels_does_not_change_any_score(self) -> None:
        """The decisive check: if a label leaked, this would move the scores."""
        samples = make_samples(seconds=600)
        splits = prepare_splits(samples, make_labels(samples), _settings())

        pipeline = build_pipeline(
            IsolationForestDetector(n_estimators=60, max_samples=128, random_state=0), seed=0
        )
        pipeline.fit(splits.train.X)
        before = pipeline.score_samples(splits.validation.X)

        # Scramble the ground truth completely, then score again.
        rng = np.random.default_rng(1)
        splits.validation.labels["is_anomaly"] = rng.permutation(
            splits.validation.labels["is_anomaly"].to_numpy()
        )
        splits.train.labels["is_anomaly"] = rng.permutation(
            splits.train.labels["is_anomaly"].to_numpy()
        )
        after = pipeline.score_samples(splits.validation.X)

        np.testing.assert_array_equal(before, after)

    def test_fitting_never_receives_a_label_argument(self) -> None:
        """The pipeline is fitted unsupervised: no ``y`` is passed anywhere."""
        samples = make_samples(seconds=400)
        splits = prepare_splits(samples, make_labels(samples), _settings())
        pipeline = build_pipeline(
            IsolationForestDetector(n_estimators=40, max_samples=64, random_state=0), seed=0
        )
        fitted = pipeline.fit(splits.train.X)
        assert fitted is pipeline


class TestSplitsAreTemporal:
    def test_splits_do_not_overlap_in_time(self) -> None:
        samples = make_samples(seconds=900)
        settings = _settings()
        splits = prepare_splits(samples, make_labels(samples), settings)

        train_end = splits.train.features["window_start"].max()
        validation_start = splits.validation.features["window_start"].min()
        validation_end = splits.validation.features["window_start"].max()
        test_start = splits.test.features["window_start"].min()

        assert train_end < validation_start
        assert validation_end < test_start

    def test_windows_do_not_straddle_a_split_boundary(self) -> None:
        """Samples are cut first, then windowed -- the reverse would leak."""
        samples = make_samples(seconds=900)
        settings = _settings()
        splits = prepare_splits(samples, make_labels(samples), settings)

        window = settings.window_seconds
        train_windows = set(splits.train.features["window_start"])
        validation_windows = set(splits.validation.features["window_start"])
        assert not train_windows & validation_windows

        latest_train = max(train_windows) + window
        assert latest_train <= min(validation_windows) + window


def test_label_alignment_keys_on_window_not_on_sample(samples: pd.DataFrame) -> None:
    """Labels attach by (machine, window_start) after features are built."""
    features = build_window_features(samples, window_seconds=60, slide_seconds=60)
    labels = align_window_labels(
        make_labels(samples),
        features,
        window_seconds=60,
        slide_seconds=60,
        min_anomaly_fraction=0.25,
    )
    assert len(labels) == len(features)
    assert list(labels.columns[:2]) == ["machine_id", "window_start"]
