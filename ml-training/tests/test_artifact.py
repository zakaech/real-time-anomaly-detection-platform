"""Serialisation: round-trip fidelity, and refusing what does not match.

The scoring-equality test is the important one. A pipeline that reloads and then
scores slightly differently is the worst outcome available: it does not raise,
it just produces different alerts than the ones the report measured.
"""

from __future__ import annotations

import json
import platform
from datetime import UTC, datetime
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
import sklearn
from telemetry_core.features import FEATURE_NAMES, FEATURE_SET_VERSION

from conftest import make_labels, make_samples
from ml_training.artifact import (
    METADATA_FILE,
    ArtifactMismatchError,
    CalibratedAnomalyModel,
    ModelMetadata,
    load_artifact,
    save_artifact,
)
from ml_training.calibration import EcdfCalibrator
from ml_training.config.settings import TrainingSettings
from ml_training.models.detectors import IsolationForestDetector
from ml_training.training import build_pipeline, prepare_splits, reference_profile


def _settings() -> TrainingSettings:
    return TrainingSettings(
        train_end_hours=0.06, validation_end_hours=0.11, window_seconds=30, slide_seconds=10
    )


@pytest.fixture
def fitted(tmp_path: Path) -> tuple[Path, CalibratedAnomalyModel, pd.DataFrame]:
    samples = make_samples(seconds=900)
    splits = prepare_splits(samples, make_labels(samples), _settings())

    pipeline = build_pipeline(
        IsolationForestDetector(n_estimators=60, max_samples=128, random_state=0), seed=0
    )
    pipeline.fit(splits.train.X)
    calibrator = EcdfCalibrator.fit(np.asarray(pipeline.score_samples(splits.train.X)))
    model = CalibratedAnomalyModel(
        pipeline=pipeline,
        calibrator=calibrator,
        threshold=0.995,
        model_name="isolation_forest",
        model_version="2.0.0",
    )
    metadata = ModelMetadata(
        model_name="isolation_forest",
        model_version="2.0.0",
        trained_at=datetime.now(tz=UTC).isoformat(),
        feature_set_version=FEATURE_SET_VERSION,
        feature_names=list(FEATURE_NAMES),
        hyperparameters={"n_estimators": 60},
        threshold=0.995,
        threshold_quantile=0.995,
        threshold_rule="test fixture",
        training_rows=len(splits.train.X),
        training_seed=0,
        python_version=platform.python_version(),
        sklearn_version=sklearn.__version__,
        numpy_version=np.__version__,
        pandas_version=pd.__version__,
        dataset={"directory": "fixture"},
        metrics={},
    )
    directory = tmp_path / "isolation_forest" / "2.0.0"
    save_artifact(
        directory,
        model=model,
        metadata=metadata,
        machine_profiles=pipeline.named_steps["per_machine"].to_profile_dict(),
        reference_profile=reference_profile(splits.train.features),
    )
    return directory, model, splits.validation.X


class TestRoundTrip:
    def test_every_file_is_written(
        self, fitted: tuple[Path, CalibratedAnomalyModel, pd.DataFrame]
    ) -> None:
        directory, _model, _frame = fitted
        for name in (
            "model.joblib",
            "metadata.json",
            "calibration.json",
            "machine_profiles.json",
            "reference_profile.json",
        ):
            assert (directory / name).is_file(), name

    def test_scores_are_identical_after_reloading(
        self, fitted: tuple[Path, CalibratedAnomalyModel, pd.DataFrame]
    ) -> None:
        """Exact equality, not approximate.

        A reloaded model that scores "almost" the same would raise no error and
        produce different alerts than the ones the report measured.
        """
        directory, model, frame = fitted
        before = model.scores(frame)
        reloaded, _metadata = load_artifact(directory)
        np.testing.assert_array_equal(before, reloaded.scores(frame))

    def test_raw_scores_survive_too(
        self, fitted: tuple[Path, CalibratedAnomalyModel, pd.DataFrame]
    ) -> None:
        directory, model, frame = fitted
        reloaded, _metadata = load_artifact(directory)
        np.testing.assert_array_equal(model.raw_scores(frame), reloaded.raw_scores(frame))

    def test_threshold_and_identity_survive(
        self, fitted: tuple[Path, CalibratedAnomalyModel, pd.DataFrame]
    ) -> None:
        directory, model, _frame = fitted
        reloaded, metadata = load_artifact(directory)
        assert reloaded.threshold == model.threshold
        assert reloaded.model_name == model.model_name
        assert metadata["feature_set_version"] == FEATURE_SET_VERSION

    def test_decision_is_the_threshold_applied_to_the_score(
        self, fitted: tuple[Path, CalibratedAnomalyModel, pd.DataFrame]
    ) -> None:
        _directory, model, frame = fitted
        np.testing.assert_array_equal(
            model.is_anomaly(frame), model.scores(frame) >= model.threshold
        )


class TestMetadata:
    def test_it_records_what_is_needed_to_reproduce(
        self, fitted: tuple[Path, CalibratedAnomalyModel, pd.DataFrame]
    ) -> None:
        directory, _model, _frame = fitted
        metadata = json.loads((directory / METADATA_FILE).read_text(encoding="utf-8"))
        for key in (
            "model_version",
            "trained_at",
            "sklearn_version",
            "numpy_version",
            "python_version",
            "hyperparameters",
            "feature_names",
            "feature_set_version",
            "threshold",
            "training_seed",
            "artifact_sha256",
        ):
            assert key in metadata, key

    def test_the_digest_is_recorded(
        self, fitted: tuple[Path, CalibratedAnomalyModel, pd.DataFrame]
    ) -> None:
        directory, _model, _frame = fitted
        metadata = json.loads((directory / METADATA_FILE).read_text(encoding="utf-8"))
        assert len(metadata["artifact_sha256"]) == 64

    def test_feature_order_is_recorded_not_just_the_set(
        self, fitted: tuple[Path, CalibratedAnomalyModel, pd.DataFrame]
    ) -> None:
        directory, _model, _frame = fitted
        metadata = json.loads((directory / METADATA_FILE).read_text(encoding="utf-8"))
        assert metadata["feature_names"] == list(FEATURE_NAMES)


class TestLoadingRefusesMismatches:
    def _patch(self, directory: Path, **changes: object) -> None:
        path = directory / METADATA_FILE
        payload = json.loads(path.read_text(encoding="utf-8"))
        payload.update(changes)
        path.write_text(json.dumps(payload), encoding="utf-8")

    def test_a_corrupted_model_file_is_refused(
        self, fitted: tuple[Path, CalibratedAnomalyModel, pd.DataFrame]
    ) -> None:
        directory, _model, _frame = fitted
        self._patch(directory, artifact_sha256="0" * 64)
        with pytest.raises(ArtifactMismatchError, match="digest mismatch"):
            load_artifact(directory)

    def test_a_different_sklearn_version_is_refused(
        self, fitted: tuple[Path, CalibratedAnomalyModel, pd.DataFrame]
    ) -> None:
        """Unpickling across versions is not guaranteed to raise on its own."""
        directory, _model, _frame = fitted
        self._patch(directory, sklearn_version="0.0.1")
        with pytest.raises(ArtifactMismatchError, match="scikit-learn"):
            load_artifact(directory)

    def test_a_stale_feature_set_version_is_refused(
        self, fitted: tuple[Path, CalibratedAnomalyModel, pd.DataFrame]
    ) -> None:
        """A v1 artefact must not be scored with v2 features."""
        directory, _model, _frame = fitted
        self._patch(directory, feature_set_version="1.0.0")
        with pytest.raises(ArtifactMismatchError, match="feature set"):
            load_artifact(directory)

    def test_a_permuted_feature_order_is_refused(
        self, fitted: tuple[Path, CalibratedAnomalyModel, pd.DataFrame]
    ) -> None:
        directory, _model, _frame = fitted
        permuted = list(FEATURE_NAMES)
        permuted[0], permuted[1] = permuted[1], permuted[0]
        self._patch(directory, feature_names=permuted)
        with pytest.raises(ArtifactMismatchError, match="feature order"):
            load_artifact(directory)

    def test_verification_can_be_skipped_deliberately(
        self, fitted: tuple[Path, CalibratedAnomalyModel, pd.DataFrame]
    ) -> None:
        directory, _model, _frame = fitted
        self._patch(directory, sklearn_version="0.0.1")
        model, _metadata = load_artifact(directory, verify=False)
        assert model.model_name == "isolation_forest"


class TestInputValidation:
    def test_a_missing_feature_is_reported(
        self, fitted: tuple[Path, CalibratedAnomalyModel, pd.DataFrame]
    ) -> None:
        _directory, model, frame = fitted
        with pytest.raises(ArtifactMismatchError, match="missing columns"):
            model.scores(frame.drop(columns=[FEATURE_NAMES[3]]))

    def test_a_missing_machine_column_is_reported(
        self, fitted: tuple[Path, CalibratedAnomalyModel, pd.DataFrame]
    ) -> None:
        _directory, model, frame = fitted
        with pytest.raises(ArtifactMismatchError, match="missing columns"):
            model.scores(frame.drop(columns=["machine_id"]))

    def test_column_order_in_the_input_does_not_matter(
        self, fitted: tuple[Path, CalibratedAnomalyModel, pd.DataFrame]
    ) -> None:
        """The model reorders by name; only the recorded order is authoritative."""
        _directory, model, frame = fitted
        shuffled = frame[list(reversed(frame.columns))]
        np.testing.assert_array_equal(model.scores(frame), model.scores(shuffled))
