"""Loading the trained artefact once per Python worker process.

A Spark executor runs several Python worker processes, and each one needs its
own copy of the model. Loading it per row would mean unpickling a One-Class SVM
with hundreds of support vectors for every window; loading it once per process
reduces that to a handful of loads for the lifetime of the job.

What is broadcast is the **path and the expected digest**, not the object.
Broadcasting the fitted pipeline would also work at this size, but the
module-level singleton is needed regardless -- so there is one mechanism instead
of two.

Every check is blocking, and they all live in ``load_artifact``: the SHA-256 of
the binary, the scikit-learn version, the feature-set version and the exact
column order. A mismatch fails the job. It has to -- unpickling across
scikit-learn versions is not guaranteed to raise, and a matrix carries no column
names, so the alternative is a model that loads, scores, and is quietly wrong.

This module deliberately re-checks none of it. An earlier revision repeated the
feature-set comparison here; since both sides read the same
``FEATURE_SET_VERSION`` constant, the branch could never be taken and only
suggested a second line of defence that did not exist.
"""

from __future__ import annotations

import json
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import numpy.typing as npt
from ml_training.artifact import REFERENCE_FILE, CalibratedAnomalyModel, load_artifact
from telemetry_core.features import FEATURE_NAMES

__all__ = ["LoadedModel", "ModelSpec", "load_model", "reset_cache"]


@dataclass(frozen=True)
class ModelSpec:
    """What an executor needs to find and trust the artefact.

    Small and picklable on purpose: this is what crosses to the workers.
    """

    directory: str
    verify: bool = True


@dataclass
class LoadedModel:
    """The fitted model plus what the scoring path needs beside it."""

    model: CalibratedAnomalyModel
    metadata: dict[str, Any]
    #: Per-feature mean and standard deviation of the training distribution,
    #: used to explain an alert: which feature departed, and by how much.
    reference_mean: npt.NDArray[np.float64]
    reference_std: npt.NDArray[np.float64]

    @property
    def threshold(self) -> float:
        return self.model.threshold

    @property
    def model_name(self) -> str:
        return self.model.model_name

    @property
    def model_version(self) -> str:
        return self.model.model_version

    def contributions(self, features: npt.NDArray[np.float64]) -> npt.NDArray[np.float64]:
        """Signed z-score of every feature against the training reference."""
        return (features - self.reference_mean) / self.reference_std


_CACHE: dict[tuple[str, bool], LoadedModel] = {}
_LOCK = threading.Lock()


def _reference_arrays(directory: Path) -> tuple[npt.NDArray[np.float64], npt.NDArray[np.float64]]:
    payload = json.loads((directory / REFERENCE_FILE).read_text(encoding="utf-8"))
    features = payload["features"]
    means = np.array([float(features[name]["mean"] or 0.0) for name in FEATURE_NAMES])
    stds = np.array([float(features[name]["std"] or 0.0) for name in FEATURE_NAMES])
    # A feature that never moved in training has no scale to measure a departure
    # against; 1.0 keeps the arithmetic finite and the contribution near zero.
    stds = np.where(np.isfinite(stds) & (stds > 1e-12), stds, 1.0)
    return means, np.asarray(stds, dtype="float64")


def load_model(spec: ModelSpec) -> LoadedModel:
    """Return the process-wide model, loading it on first use.

    Double-checked locking: several Arrow batches can hit this concurrently in
    one worker, and unpickling twice would waste the load it exists to avoid.
    """
    key = (spec.directory, spec.verify)
    cached = _CACHE.get(key)
    if cached is not None:
        return cached

    with _LOCK:
        cached = _CACHE.get(key)
        if cached is not None:
            return cached

        directory = Path(spec.directory)
        model, metadata = load_artifact(directory, verify=spec.verify)
        means, stds = _reference_arrays(directory)
        loaded = LoadedModel(
            model=model, metadata=metadata, reference_mean=means, reference_std=stds
        )
        _CACHE[key] = loaded
        return loaded


def reset_cache() -> None:
    """Drop the cached model. Tests only."""
    with _LOCK:
        _CACHE.clear()
