"""Industrial telemetry simulator.

Produces a coherent multivariate sensor stream to ``telemetry.raw`` and its
ground truth to ``telemetry.labels``, using the contracts and codecs of
``telemetry-core``. The two streams are separate by construction: the telemetry
message type has no field in which a label could travel.
"""

from __future__ import annotations

__version__ = "0.1.0"

__all__ = ["__version__"]
