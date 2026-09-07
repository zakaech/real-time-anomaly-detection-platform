"""Offline training and evaluation of the anomaly detector.

Produces one artefact carrying the whole inference path -- normalisation,
imputation, scaling, detector, calibration and threshold -- so the streaming job
loads it and reimplements no machine learning.
"""

from __future__ import annotations

__version__ = "0.1.0"

__all__ = ["__version__"]
