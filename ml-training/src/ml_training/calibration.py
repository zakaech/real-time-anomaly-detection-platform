"""Turning incomparable raw scores into one interpretable scale.

Each detector produces a score on its own scale: an averaged isolation depth, a
distance to a kernel boundary, a maximum z-score. Comparing them directly is
meaningless, and so is carrying a raw threshold across a retraining.

An empirical CDF fitted on the **training** scores maps any raw score to its
rank in that reference distribution. Three things follow:

1. The scale becomes interpretable. ``0.995`` means "more extreme than 99.5 % of
   the reference windows" -- a sentence an operator understands, unlike
   ``-0.0731``.
2. The threshold becomes an alert budget. At an unchanged input distribution,
   a threshold of ``0.995`` flags about 0.5 % of windows, so the operating point
   can be chosen from what an operator can absorb.
3. The alert rate becomes a drift detector, for free. The expected rate is known
   by construction, so a sustained departure from it says the input distribution
   moved -- no extra instrumentation required.

Fitted on training scores, never on validation or test: an ECDF fitted on the
data used to pick the threshold would make the budget look exact by
construction.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import numpy as np
import numpy.typing as npt

__all__ = ["QUANTILE_GRID_SIZE", "EcdfCalibrator"]

#: Resolution of the stored grid. A thousand points resolve the tail to 0.1 %,
#: which is finer than any operating point we would choose, and the whole grid
#: is a few kilobytes in the artefact.
QUANTILE_GRID_SIZE = 1001


@dataclass
class EcdfCalibrator:
    """Maps a raw anomaly score to its rank in the reference distribution."""

    grid: npt.NDArray[np.float64]
    probabilities: npt.NDArray[np.float64]

    @classmethod
    def fit(
        cls, raw_scores: npt.NDArray[np.float64], *, grid_size: int = QUANTILE_GRID_SIZE
    ) -> EcdfCalibrator:
        finite = np.asarray(raw_scores, dtype="float64")
        finite = finite[np.isfinite(finite)]
        if finite.size == 0:
            raise ValueError("cannot calibrate on an empty or non-finite score sample")
        probabilities = np.linspace(0.0, 1.0, grid_size)
        return cls(grid=np.quantile(finite, probabilities), probabilities=probabilities)

    def transform(self, raw_scores: npt.NDArray[np.float64]) -> npt.NDArray[np.float64]:
        """Map raw scores to ``[0, 1]``.

        Linear interpolation between stored quantiles, clamped at both ends: a
        score beyond anything seen in training is simply "at least as extreme as
        everything", which is the correct statement rather than an extrapolation
        into a region the reference says nothing about.
        """
        values = np.asarray(raw_scores, dtype="float64")
        calibrated = np.interp(values, self.grid, self.probabilities)
        # np.interp already clamps, but NaN passes through and must not be
        # silently read as a low score.
        return np.where(np.isfinite(values), calibrated, np.nan)

    def to_dict(self) -> dict[str, Any]:
        return {
            "grid_size": int(self.grid.size),
            "probabilities": self.probabilities.tolist(),
            "raw_score_quantiles": self.grid.tolist(),
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> EcdfCalibrator:
        return cls(
            grid=np.asarray(payload["raw_score_quantiles"], dtype="float64"),
            probabilities=np.asarray(payload["probabilities"], dtype="float64"),
        )
