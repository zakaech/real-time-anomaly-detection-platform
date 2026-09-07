"""The deliverable: one object that scores a window, and the metadata to trust it.

``model.joblib`` holds a :class:`CalibratedAnomalyModel` wrapping the whole
inference path -- per-machine normalisation, imputation, scaling, detector,
calibration and threshold. The streaming job calls one method and reimplements
no machine learning, which is the requirement that shaped the design: anything
left outside the artefact would have to be rebuilt in Spark and could drift.

Loading verifies rather than trusts. A mismatch in the SHA-256, the
scikit-learn version, the feature-set version or the column order **fails at
load**. Every one of those has the same failure mode if unchecked: the model
loads, scores, and is quietly wrong. A pickle written by one scikit-learn
version and read by another is not guaranteed to raise -- it can produce
different predictions -- which is precisely why the version is compared instead
of hoped for.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

import joblib
import numpy as np
import numpy.typing as npt
import pandas as pd
import sklearn
from telemetry_core.features import FEATURE_NAMES, FEATURE_SET_VERSION
from telemetry_core.logging import get_logger

from ml_training.calibration import EcdfCalibrator
from ml_training.models.normalizer import MACHINE_COLUMN

__all__ = [
    "ArtifactMismatchError",
    "CalibratedAnomalyModel",
    "ModelMetadata",
    "load_artifact",
    "save_artifact",
]

_LOGGER = get_logger(__name__)

MODEL_FILE = "model.joblib"
METADATA_FILE = "metadata.json"
CALIBRATION_FILE = "calibration.json"
PROFILES_FILE = "machine_profiles.json"
REFERENCE_FILE = "reference_profile.json"


class ArtifactMismatchError(RuntimeError):
    """The artefact does not match the environment trying to load it."""


class CalibratedAnomalyModel:
    """A fitted pipeline, its calibration and its operating threshold."""

    def __init__(
        self,
        *,
        pipeline: Any,
        calibrator: EcdfCalibrator,
        threshold: float,
        model_name: str,
        model_version: str,
        feature_names: tuple[str, ...] = FEATURE_NAMES,
        feature_set_version: str = FEATURE_SET_VERSION,
    ) -> None:
        self.pipeline = pipeline
        self.calibrator = calibrator
        self.threshold = float(threshold)
        self.model_name = model_name
        self.model_version = model_version
        self.feature_names = tuple(feature_names)
        self.feature_set_version = feature_set_version

    def _prepare(self, frame: pd.DataFrame) -> pd.DataFrame:
        """Check the input carries exactly what the model was fitted on.

        Order matters as much as presence: a matrix has no column names, so two
        permuted features produce plausible and wrong scores.
        """
        required = [MACHINE_COLUMN, *self.feature_names]
        missing = [column for column in required if column not in frame.columns]
        if missing:
            raise ArtifactMismatchError(f"input is missing columns: {missing}")
        return frame[required]

    def raw_scores(self, frame: pd.DataFrame) -> npt.NDArray[np.float64]:
        """Detector output, on its own scale. Higher means more anomalous."""
        return np.asarray(self.pipeline.score_samples(self._prepare(frame)), dtype="float64")

    def scores(self, frame: pd.DataFrame) -> npt.NDArray[np.float64]:
        """Calibrated score in ``[0, 1]``: the rank in the reference distribution."""
        return self.calibrator.transform(self.raw_scores(frame))

    def is_anomaly(self, frame: pd.DataFrame) -> npt.NDArray[np.bool_]:
        return self.scores(frame) >= self.threshold


@dataclass
class ModelMetadata:
    """Everything needed to reproduce, audit and safely load the artefact."""

    model_name: str
    model_version: str
    trained_at: str
    feature_set_version: str
    feature_names: list[str]
    hyperparameters: dict[str, Any]
    threshold: float
    threshold_quantile: float
    threshold_rule: str
    training_rows: int
    training_seed: int
    python_version: str
    sklearn_version: str
    numpy_version: str
    pandas_version: str
    dataset: dict[str, Any]
    metrics: dict[str, Any]
    notes: dict[str, Any] = field(default_factory=dict)
    artifact_sha256: str | None = None

    def to_json(self) -> str:
        return json.dumps(asdict(self), indent=2, sort_keys=True, default=str)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def save_artifact(
    directory: Path,
    *,
    model: CalibratedAnomalyModel,
    metadata: ModelMetadata,
    machine_profiles: dict[str, Any],
    reference_profile: dict[str, Any],
) -> ModelMetadata:
    """Write the artefact directory and return the metadata with its digest.

    The digest is computed after the model file is written and then stored
    beside it, so a corrupted or substituted binary is detected at load rather
    than at the first surprising score.
    """
    directory.mkdir(parents=True, exist_ok=True)
    model_path = directory / MODEL_FILE
    joblib.dump(model, model_path)

    metadata.artifact_sha256 = _sha256(model_path)
    (directory / METADATA_FILE).write_text(metadata.to_json(), encoding="utf-8")
    (directory / CALIBRATION_FILE).write_text(
        json.dumps(model.calibrator.to_dict(), indent=2), encoding="utf-8"
    )
    (directory / PROFILES_FILE).write_text(
        json.dumps(machine_profiles, indent=2, sort_keys=True), encoding="utf-8"
    )
    (directory / REFERENCE_FILE).write_text(
        json.dumps(reference_profile, indent=2, sort_keys=True), encoding="utf-8"
    )

    _LOGGER.info(
        "artifact_written",
        directory=str(directory),
        model=metadata.model_name,
        version=metadata.model_version,
        sha256=metadata.artifact_sha256,
    )
    return metadata


def load_artifact(
    directory: Path, *, verify: bool = True
) -> tuple[CalibratedAnomalyModel, dict[str, Any]]:
    """Load an artefact, refusing anything that does not match this environment.

    Raises:
        ArtifactMismatchError: on a digest, library-version, feature-set or
            column-order mismatch. Every one of those would otherwise produce a
            model that loads and scores incorrectly without raising.
    """
    model_path = directory / MODEL_FILE
    metadata: dict[str, Any] = json.loads((directory / METADATA_FILE).read_text(encoding="utf-8"))

    if verify:
        expected_digest = metadata.get("artifact_sha256")
        actual_digest = _sha256(model_path)
        if expected_digest and expected_digest != actual_digest:
            raise ArtifactMismatchError(
                f"{MODEL_FILE} digest mismatch: metadata says {expected_digest}, "
                f"file is {actual_digest}"
            )

        recorded_sklearn = metadata.get("sklearn_version")
        if recorded_sklearn and recorded_sklearn != sklearn.__version__:
            raise ArtifactMismatchError(
                f"artefact was trained with scikit-learn {recorded_sklearn}, this "
                f"environment has {sklearn.__version__}. Unpickling across versions is "
                "not guaranteed to raise and can silently change predictions."
            )

        recorded_feature_set = metadata.get("feature_set_version")
        if recorded_feature_set != FEATURE_SET_VERSION:
            raise ArtifactMismatchError(
                f"artefact expects feature set {recorded_feature_set}, this environment "
                f"produces {FEATURE_SET_VERSION}"
            )

        recorded_features = list(metadata.get("feature_names", []))
        if recorded_features != list(FEATURE_NAMES):
            raise ArtifactMismatchError(
                "feature order differs from the artefact; a matrix has no column names, "
                "so scoring would be plausible and wrong"
            )

    model: CalibratedAnomalyModel = joblib.load(model_path)
    return model, metadata
