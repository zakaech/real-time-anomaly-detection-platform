"""The artefact is refused unless it matches this environment exactly.

The streaming job loads a pickle trained offline and scores production traffic
with it.
Every mismatch this file exercises has the same failure mode: the artefact loads,
scores, and is quietly wrong. Nothing raises on its own, so the checks have to be
explicit and they have to block -- a job that refuses to start is an incident, a
job that scores with the wrong model is an incident nobody notices.

The four rejections run without a trained model on purpose: ``load_artifact``
validates the metadata *before* unpickling, so a placeholder file is enough to
reach every check. That keeps them running in CI, where the binary is absent
because it is a build output and is never committed.
"""

from __future__ import annotations

import hashlib
import json
import shutil
from pathlib import Path
from typing import Any

import pytest
from ml_training.artifact import ArtifactMismatchError
from telemetry_core.features import FEATURE_NAMES, FEATURE_SET_VERSION

from stream_processor.model import ModelSpec, load_model, reset_cache


@pytest.fixture(autouse=True)
def _clean_cache() -> Any:
    """The loader is a process-wide singleton; leaking it between tests would
    make each one depend on the order the others ran in."""
    reset_cache()
    yield
    reset_cache()


def _plausible_metadata(digest: str) -> dict[str, Any]:
    """Metadata that passes every check, for a test to break one field of."""
    import sklearn

    return {
        "artifact_sha256": digest,
        "sklearn_version": sklearn.__version__,
        "feature_set_version": FEATURE_SET_VERSION,
        "feature_names": list(FEATURE_NAMES),
        "model_name": "one_class_svm",
        "model_version": "2.0.0",
    }


def _artifact_with(tmp_path: Path, **overrides: Any) -> Path:
    """A directory whose metadata is valid except for what the caller overrides."""
    directory = tmp_path / "artifact"
    directory.mkdir()
    model_file = directory / "model.joblib"
    model_file.write_bytes(b"not a real pickle; verification never gets far enough")
    metadata = _plausible_metadata(hashlib.sha256(model_file.read_bytes()).hexdigest())
    metadata.update(overrides)
    (directory / "metadata.json").write_text(json.dumps(metadata), encoding="utf-8")
    return directory


class TestTheJobRefusesAnIncompatibleArtefact:
    def test_a_tampered_binary_is_refused(self, tmp_path: Path) -> None:
        """The digest is recorded at training time. A file that no longer matches
        it is not the model that was evaluated, whatever the metadata claims."""
        directory = _artifact_with(tmp_path)
        (directory / "model.joblib").write_bytes(b"different bytes entirely")

        with pytest.raises(ArtifactMismatchError, match="digest mismatch"):
            load_model(ModelSpec(directory=str(directory)))

    def test_a_different_scikit_learn_is_refused(self, tmp_path: Path) -> None:
        """Unpickling an estimator across scikit-learn versions is not guaranteed
        to raise. It can succeed and predict differently, so this check cannot
        be a warning."""
        directory = _artifact_with(tmp_path, sklearn_version="0.24.2")

        with pytest.raises(ArtifactMismatchError, match="scikit-learn"):
            load_model(ModelSpec(directory=str(directory)))

    def test_a_different_feature_set_is_refused(self, tmp_path: Path) -> None:
        """The columns would not mean what the model learned."""
        directory = _artifact_with(tmp_path, feature_set_version="1.0.0")

        with pytest.raises(ArtifactMismatchError, match="feature set"):
            load_model(ModelSpec(directory=str(directory)))

    def test_a_reordered_feature_list_is_refused(self, tmp_path: Path) -> None:
        """The most dangerous of the four. A matrix carries no column names, so
        scoring a permuted one produces plausible numbers and no error at all."""
        swapped = list(FEATURE_NAMES)
        swapped[0], swapped[1] = swapped[1], swapped[0]
        directory = _artifact_with(tmp_path, feature_names=swapped)

        with pytest.raises(ArtifactMismatchError, match="feature order"):
            load_model(ModelSpec(directory=str(directory)))

    def test_a_missing_column_is_refused_even_if_the_rest_match(self, tmp_path: Path) -> None:
        directory = _artifact_with(tmp_path, feature_names=list(FEATURE_NAMES)[:-1])

        with pytest.raises(ArtifactMismatchError, match="feature order"):
            load_model(ModelSpec(directory=str(directory)))

    def test_verification_is_on_by_default(self) -> None:
        """A default that has to be opted out of, not into."""
        assert ModelSpec(directory="/anywhere").verify is True


class TestTheRealArtefact:
    """These need the trained binary and are skipped where it is absent."""

    def test_it_loads_and_verifies(self, artifact_dir: Path) -> None:
        loaded = load_model(ModelSpec(directory=str(artifact_dir)))

        assert loaded.model_name == "one_class_svm"
        assert loaded.model_version == "2.0.0"
        assert loaded.metadata["feature_set_version"] == FEATURE_SET_VERSION
        assert loaded.reference_mean.shape == (len(FEATURE_NAMES),)
        # A zero standard deviation would make every contribution infinite.
        assert (loaded.reference_std > 0).all()

    def test_it_is_loaded_once_per_process(self, artifact_dir: Path) -> None:
        """A One-Class SVM with hundreds of support vectors is unpickled once
        per worker, not once per batch. Identity is the assertion: an equal but
        distinct object would mean the cache silently does nothing."""
        first = load_model(ModelSpec(directory=str(artifact_dir)))
        second = load_model(ModelSpec(directory=str(artifact_dir)))

        assert first is second

        reset_cache()
        assert load_model(ModelSpec(directory=str(artifact_dir))) is not first

    def test_a_corrupted_copy_of_the_real_artefact_is_refused(
        self, artifact_dir: Path, tmp_path: Path
    ) -> None:
        """The same rejection as above, on the artefact actually shipped: a
        single flipped byte in the pickle must stop the job."""
        directory = tmp_path / "copy"
        shutil.copytree(artifact_dir, directory)
        binary = directory / "model.joblib"
        payload = bytearray(binary.read_bytes())
        payload[-1] ^= 0x01
        binary.write_bytes(bytes(payload))

        with pytest.raises(ArtifactMismatchError, match="digest mismatch"):
            load_model(ModelSpec(directory=str(directory)))
