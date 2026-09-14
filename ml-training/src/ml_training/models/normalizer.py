"""Per-machine robust normalisation, as the first step of the pipeline.

Decision D-07: one global model, but features normalised per
machine. Without it a machine that legitimately runs hotter than its neighbours
would be permanently anomalous, and the detector would spend its budget
rediscovering the fleet's heterogeneity instead of its faults.

Median and inter-quartile range rather than mean and standard deviation: the
training data is deliberately contaminated (decision D-27), and a mean is
dragged by the very anomalies the model is meant to find later.

The transformer lives **inside** the serialised pipeline, so the streaming job
calls one method and reimplements nothing. That is why it takes a
DataFrame carrying ``machine_id`` rather than a bare matrix: the machine is part
of the input, not context the caller has to apply itself.
"""

from __future__ import annotations

from typing import Any

import numpy as np
import numpy.typing as npt
import pandas as pd
from sklearn.base import BaseEstimator, TransformerMixin

__all__ = ["MACHINE_COLUMN", "PerMachineNormalizer"]

MACHINE_COLUMN = "machine_id"

#: Below this, an inter-quartile range is treated as no dispersion at all and
#: the fleet profile takes over. A feature that never moves for one machine
#: would otherwise divide by almost zero and explode.
_MIN_SCALE = 1e-9


class PerMachineNormalizer(BaseEstimator, TransformerMixin):  # type: ignore[misc]
    """Centre and scale each feature by the machine's own median and IQR."""

    def __init__(self, feature_names: list[str] | None = None) -> None:
        self.feature_names = feature_names

    def fit(self, X: pd.DataFrame, y: Any = None) -> PerMachineNormalizer:
        if MACHINE_COLUMN not in X.columns:
            raise ValueError(
                f"{MACHINE_COLUMN!r} must be present: per-machine normalisation cannot "
                "be applied to an anonymous matrix"
            )
        names = self.feature_names or [c for c in X.columns if c != MACHINE_COLUMN]
        self.feature_names_ = list(names)
        self.feature_names_in_ = [MACHINE_COLUMN, *self.feature_names_]
        self.n_features_in_ = len(self.feature_names_in_)

        values = X[self.feature_names_]
        # Fleet-wide fallback, used for a machine never seen in training. A new
        # machine must still be scorable on its first day; it simply gets the
        # fleet's profile until it has a history of its own.
        self.fleet_center_ = values.median().to_numpy(dtype="float64")
        fleet_scale = (values.quantile(0.75) - values.quantile(0.25)).to_numpy(dtype="float64")
        self.fleet_scale_ = np.where(
            np.isfinite(fleet_scale) & (fleet_scale > _MIN_SCALE), fleet_scale, 1.0
        )

        grouped = X.groupby(MACHINE_COLUMN, sort=True)[self.feature_names_]
        centers = grouped.median()
        scales = grouped.quantile(0.75) - grouped.quantile(0.25)

        self.machine_index_ = {machine: row for row, machine in enumerate(centers.index)}
        self.centers_ = np.where(
            np.isfinite(centers.to_numpy(dtype="float64")),
            centers.to_numpy(dtype="float64"),
            self.fleet_center_,
        )
        scale_values = scales.to_numpy(dtype="float64")
        self.scales_ = np.where(
            np.isfinite(scale_values) & (scale_values > _MIN_SCALE),
            scale_values,
            self.fleet_scale_,
        )
        return self

    def transform(self, X: pd.DataFrame) -> npt.NDArray[np.float64]:
        centers, scales = self._profiles_for(X[MACHINE_COLUMN])
        values = X[self.feature_names_].to_numpy(dtype="float64")
        # NaN passes straight through: what a feature could not be computed from
        # is an imputation decision, taken by the next step which knows the
        # reference distribution.
        normalised: npt.NDArray[np.float64] = (values - centers) / scales
        return normalised

    def _profiles_for(
        self, machines: pd.Series
    ) -> tuple[npt.NDArray[np.float64], npt.NDArray[np.float64]]:
        rows = machines.map(self.machine_index_)
        known = rows.notna().to_numpy()
        indices = rows.fillna(0).to_numpy(dtype="int64")

        centers = self.centers_[indices]
        scales = self.scales_[indices]
        if not known.all():
            centers = np.where(known[:, None], centers, self.fleet_center_)
            scales = np.where(known[:, None], scales, self.fleet_scale_)
        return centers, scales

    def get_feature_names_out(self, input_features: Any = None) -> npt.NDArray[np.object_]:
        names: npt.NDArray[np.object_] = np.asarray(self.feature_names_, dtype=object)
        return names

    def unknown_machines(self, machines: pd.Series) -> list[str]:
        """Machines with no training profile. Reported, never silently defaulted."""
        return sorted(set(machines) - set(self.machine_index_))

    def to_profile_dict(self) -> dict[str, Any]:
        """Export the fitted profiles for inspection and drift tracking."""
        return {
            "feature_names": self.feature_names_,
            "fleet": {
                "center": self.fleet_center_.tolist(),
                "scale": self.fleet_scale_.tolist(),
            },
            "machines": {
                machine: {
                    "center": self.centers_[row].tolist(),
                    "scale": self.scales_[row].tolist(),
                }
                for machine, row in sorted(self.machine_index_.items())
            },
        }
