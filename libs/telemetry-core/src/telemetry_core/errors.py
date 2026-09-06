"""Exception hierarchy for the shared library.

One root exception lets a consumer catch everything this library can raise
without also swallowing unrelated failures. That distinction matters most in the
stream processor, where a data error must route the message to the dead letter
topic while an infrastructure error must let the batch fail and be retried
(docs/03-kafka-topology.md section 7). Catching a bare ``Exception`` there would
send perfectly valid messages to the DLQ during a broker outage.
"""

from __future__ import annotations

__all__ = [
    "ConfigurationError",
    "FeatureComputationError",
    "InvalidInstantError",
    "SchemaValidationError",
    "TelemetryCoreError",
]


class TelemetryCoreError(Exception):
    """Base class for every error raised by :mod:`telemetry_core`."""


class SchemaValidationError(TelemetryCoreError):
    """A payload does not satisfy the message contract.

    This is a *data* error: the message is malformed or violates an invariant.
    The correct response is to route it to the dead letter topic, never to retry
    it -- replaying a malformed message produces the same failure for ever.
    """

    def __init__(self, message: str, *, field: str | None = None) -> None:
        super().__init__(message)
        self.field = field


class InvalidInstantError(SchemaValidationError):
    """A timestamp is naive, malformed, or not ISO-8601 with a UTC offset."""


class ConfigurationError(TelemetryCoreError):
    """Configuration is missing or inconsistent.

    Raised at startup only. A component must refuse to start rather than run
    with a partial configuration: discovering a wrong broker address on the
    first message, in production, costs far more than failing loudly at boot.
    """


class FeatureComputationError(TelemetryCoreError):
    """A feature vector could not be computed from a window of samples."""
