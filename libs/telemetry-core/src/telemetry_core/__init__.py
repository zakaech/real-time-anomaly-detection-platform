"""Shared contracts, codecs and feature specification.

This package is the only thing every Python component of the platform depends
on, and it is shipped to Spark executors, so it stays deliberately small and
carries no engine dependency.

It holds what must not be duplicated:

* the wire contracts and their codecs (:mod:`telemetry_core.schemas`,
  :mod:`telemetry_core.codec`);
* the instant format shared by every timestamp (:mod:`telemetry_core.timeutil`);
* the deterministic alert identifier that makes replay idempotent
  (:mod:`telemetry_core.ids`);
* the feature specification shared by training and scoring
  (:mod:`telemetry_core.features`);
* startup-validated configuration and structured logging.
"""

from __future__ import annotations

__version__ = "0.1.0"

__all__ = ["__version__"]
