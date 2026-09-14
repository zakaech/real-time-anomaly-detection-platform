"""Enumerations shared by every message contract.

Two decoding policies coexist here on purpose:

* **Tolerant** (:class:`MachineState`, :class:`AnomalyType`): decoded with an
  ``UNKNOWN`` fallback. The schema evolution rule in
  docs/02-data-contracts.md section 2 allows a producer to add an enum value in
  a minor version, *provided consumers have a default case*. Without a fallback,
  that allowance is a lie: adding a machine state would crash every consumer.

* **Strict** (:class:`Severity`, :class:`AlertStatus`): decoded exactly. These
  are produced inside the platform and drive control flow. Silently mapping an
  unknown severity to a default would hide a contract break at the worst
  possible moment -- an alert would be displayed with the wrong urgency.

Tolerance applies to what we consume from others; strictness to what we produce
ourselves.
"""

from __future__ import annotations

from enum import StrEnum
from typing import TypeVar

from telemetry_core.errors import SchemaValidationError

__all__ = [
    "AlertStatus",
    "AnomalyType",
    "DlqReason",
    "LabelSource",
    "MachineState",
    "Severity",
    "SkipReason",
]

_E = TypeVar("_E", bound="WireEnum")


class WireEnum(StrEnum):
    """Base for enums carried on the wire.

    :class:`~enum.StrEnum` rather than ``(str, Enum)``: a member then serialises
    to its own value with no special-casing in the codec, compares equal to the
    raw string, and ``str(member)`` yields the value rather than
    ``ClassName.MEMBER`` -- which is what would otherwise end up in a log line.
    """

    @classmethod
    def parse_strict(cls: type[_E], value: object, *, field: str) -> _E:
        """Decode ``value``, rejecting anything outside the declared set."""
        if not isinstance(value, str):
            raise SchemaValidationError(
                f"{field}: expected a string, got {type(value).__name__}", field=field
            )
        try:
            return cls(value)
        except ValueError as exc:
            allowed = ", ".join(member.value for member in cls)
            raise SchemaValidationError(
                f"{field}: {value!r} is not one of [{allowed}]", field=field
            ) from exc


class _TolerantWireEnum(WireEnum):
    """Base for enums that degrade to ``UNKNOWN`` instead of failing."""

    @classmethod
    def parse_tolerant(cls: type[_E], value: object, *, field: str) -> _E:
        """Decode ``value``, mapping anything unrecognised to ``UNKNOWN``.

        A ``None`` or a value added by a newer producer yields ``UNKNOWN`` so an
        additive schema change never breaks an existing consumer.
        """
        if isinstance(value, str):
            try:
                return cls(value)
            except ValueError:
                pass
        return cls("UNKNOWN")


class MachineState(_TolerantWireEnum):
    """Operational state of a machine at sampling time.

    It matters for detection, not only as metadata: high vibration during
    ``STARTING`` is normal, the same reading while ``RUNNING`` is not.
    Only ``RUNNING`` windows are scored (docs/07-ml-methodology.md section 2.2).
    """

    RUNNING = "RUNNING"
    IDLE = "IDLE"
    STARTING = "STARTING"
    MAINTENANCE = "MAINTENANCE"
    STOPPED = "STOPPED"
    UNKNOWN = "UNKNOWN"


class AnomalyType(_TolerantWireEnum):
    """Ground-truth anomaly family, used for offline evaluation only."""

    BEARING_WEAR = "BEARING_WEAR"
    OVERHEAT = "OVERHEAT"
    PRESSURE_DROP = "PRESSURE_DROP"
    POWER_SURGE = "POWER_SURGE"
    MOTOR_STALL = "MOTOR_STALL"
    SENSOR_STUCK = "SENSOR_STUCK"
    SENSOR_DROPOUT = "SENSOR_DROPOUT"
    UNKNOWN = "UNKNOWN"


class Severity(WireEnum):
    """Alert urgency, decided once by the stream processor.

    Never recomputed downstream: a second implementation of the rule is a second
    place for it to drift (docs/01-architecture.md section 2).
    """

    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"


class AlertStatus(WireEnum):
    """Lifecycle state of an alert, owned exclusively by alert-service."""

    NEW = "NEW"
    ACKNOWLEDGED = "ACKNOWLEDGED"
    RESOLVED = "RESOLVED"
    DISMISSED = "DISMISSED"


class SkipReason(WireEnum):
    """Why a window was published without being scored.

    A window that could not be scored is still emitted. Silence and "nothing is
    wrong" must never look alike, otherwise an outage of the scoring path is
    indistinguishable from a healthy plant (docs/02 section 6).
    """

    MACHINE_NOT_RUNNING = "MACHINE_NOT_RUNNING"
    INSUFFICIENT_SAMPLES = "INSUFFICIENT_SAMPLES"
    MISSING_SENSOR_DATA = "MISSING_SENSOR_DATA"
    MODEL_UNAVAILABLE = "MODEL_UNAVAILABLE"
    #: The window is still filling: its data does not yet reach its own end.
    #: Distinct from INSUFFICIENT_SAMPLES, which counts rows without asking
    #: *where* in the window they fall. A window can hold plenty of samples and
    #: still cover only its first half -- which is what update mode publishes at
    #: every trigger, and what the model was never trained on (decision D-37).
    WINDOW_NOT_MATURE = "WINDOW_NOT_MATURE"


class LabelSource(WireEnum):
    """Where a ground-truth label came from, and therefore how late it is.

    ``SIMULATOR`` is immediate, ``OPERATOR`` lags by minutes to hours, and
    ``MAINTENANCE_LOG`` by days. The lag is why labels travel on their own topic
    rather than inside the telemetry message (docs/02 section 5).
    """

    SIMULATOR = "SIMULATOR"
    OPERATOR = "OPERATOR"
    MAINTENANCE_LOG = "MAINTENANCE_LOG"


class DlqReason(WireEnum):
    """Why a message was routed to the dead letter topic.

    Data errors only. Transient infrastructure failures are retried by the
    engine and must never land here (docs/03 section 7).
    """

    MALFORMED_JSON = "MALFORMED_JSON"
    SCHEMA_VALIDATION_FAILED = "SCHEMA_VALIDATION_FAILED"
    UNSUPPORTED_SCHEMA_VERSION = "UNSUPPORTED_SCHEMA_VERSION"
    UNKNOWN_MACHINE = "UNKNOWN_MACHINE"
