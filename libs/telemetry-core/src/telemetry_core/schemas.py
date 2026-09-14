"""Message contracts as frozen dataclasses (decision D-13).

The authoritative contract lives in ``contracts/json-schema/*.json``. The classes
here *conform* to it; they do not define it. ``tests/test_schema_conformance.py``
enforces that by validating everything these classes emit against those files, so
the code cannot drift from the contract without breaking the build.

Why dataclasses and hand-written mapping rather than a validation library:

* The wire format is byte-exact. Timestamps must be ``...412Z``; a library's
  default datetime serialisation emits ``+00:00``. Making the mapping explicit
  keeps the wire shape visible in the code instead of hidden in a library's
  configuration.
* Everything in this package is shipped to Spark executors, so ``schemas`` and
  ``codec`` carry no third-party dependency at all.

Invariants that cannot be expressed in JSON Schema are enforced in
``__post_init__``. The most important is in :class:`Alert`: its identifier must
equal the deterministic derivation, which is what makes replay idempotent
(docs/04-streaming-semantics.md section 4.3).
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from telemetry_core.enums import (
    AlertStatus,
    AnomalyType,
    LabelSource,
    MachineState,
    Severity,
    SkipReason,
)
from telemetry_core.errors import SchemaValidationError
from telemetry_core.ids import derive_alert_id
from telemetry_core.timeutil import format_instant, parse_instant, to_epoch_millis

__all__ = [
    "SCHEMA_VERSION",
    "SENSOR_NAMES",
    "SUPPORTED_SCHEMA_MAJOR",
    "Alert",
    "FeatureContribution",
    "ModelRef",
    "ScoredEvent",
    "SensorReadings",
    "TelemetryLabel",
    "TelemetryRaw",
]

SCHEMA_VERSION = "1.0"
SUPPORTED_SCHEMA_MAJOR = 1

#: Canonical sensor order. Referenced by the feature specification, so a change
#: here is a contract change, not a refactor.
SENSOR_NAMES: tuple[str, ...] = (
    "temperature_c",
    "vibration_mm_s",
    "pressure_bar",
    "power_kw",
    "rotation_rpm",
)


# ---------------------------------------------------------------------------
# Decoding helpers
#
# Each raises SchemaValidationError carrying the offending field path, which is
# what the stream processor writes into the dead letter envelope. "Something was
# invalid" is not an actionable DLQ message; "readings.power_kw: expected a
# number, got str" is.
# ---------------------------------------------------------------------------


def _as_mapping(value: object, field_path: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise SchemaValidationError(
            f"{field_path}: expected an object, got {type(value).__name__}", field=field_path
        )
    return value


def _require(payload: Mapping[str, Any], key: str, field_path: str) -> Any:
    if key not in payload or payload[key] is None:
        raise SchemaValidationError(f"{field_path}: required field is missing", field=field_path)
    return payload[key]


def _req_str(payload: Mapping[str, Any], key: str, field_path: str) -> str:
    value = _require(payload, key, field_path)
    if not isinstance(value, str) or not value:
        raise SchemaValidationError(
            f"{field_path}: expected a non-empty string, got {value!r}", field=field_path
        )
    return value


def _opt_str(payload: Mapping[str, Any], key: str, field_path: str) -> str | None:
    value = payload.get(key)
    if value is None:
        return None
    if not isinstance(value, str):
        raise SchemaValidationError(
            f"{field_path}: expected a string or null, got {type(value).__name__}",
            field=field_path,
        )
    return value


def _req_number(payload: Mapping[str, Any], key: str, field_path: str) -> float:
    value = _require(payload, key, field_path)
    # bool is a subclass of int in Python; accepting it here would silently turn
    # True into 1.0 and hide a producer bug.
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise SchemaValidationError(
            f"{field_path}: expected a number, got {type(value).__name__}", field=field_path
        )
    return float(value)


def _opt_number(payload: Mapping[str, Any], key: str, field_path: str) -> float | None:
    if payload.get(key) is None:
        return None
    return _req_number(payload, key, field_path)


def _req_int(payload: Mapping[str, Any], key: str, field_path: str) -> int:
    value = _require(payload, key, field_path)
    if isinstance(value, bool) or not isinstance(value, int):
        raise SchemaValidationError(
            f"{field_path}: expected an integer, got {type(value).__name__}", field=field_path
        )
    # int() is a no-op here; it exists so the declared return type is honoured
    # rather than leaking the Any that json.loads produces.
    return int(value)


def _req_bool(payload: Mapping[str, Any], key: str, field_path: str) -> bool:
    value = _require(payload, key, field_path)
    if not isinstance(value, bool):
        raise SchemaValidationError(
            f"{field_path}: expected a boolean, got {type(value).__name__}", field=field_path
        )
    return value


def _opt_bool(payload: Mapping[str, Any], key: str, field_path: str) -> bool | None:
    if payload.get(key) is None:
        return None
    return _req_bool(payload, key, field_path)


def _req_instant(payload: Mapping[str, Any], key: str, field_path: str) -> datetime:
    value = _require(payload, key, field_path)
    if not isinstance(value, str):
        raise SchemaValidationError(
            f"{field_path}: expected an ISO-8601 string, got {type(value).__name__}",
            field=field_path,
        )
    return parse_instant(value)


def _opt_instant(payload: Mapping[str, Any], key: str, field_path: str) -> datetime | None:
    if payload.get(key) is None:
        return None
    return _req_instant(payload, key, field_path)


def _req_features(payload: Mapping[str, Any], key: str, field_path: str) -> dict[str, float | None]:
    raw = _as_mapping(payload.get(key, {}), field_path)
    decoded: dict[str, float | None] = {}
    for name, value in raw.items():
        if value is None:
            decoded[name] = None
        elif isinstance(value, bool) or not isinstance(value, (int, float)):
            raise SchemaValidationError(
                f"{field_path}.{name}: expected a number or null, got {type(value).__name__}",
                field=f"{field_path}.{name}",
            )
        else:
            decoded[name] = float(value)
    return decoded


def _check_schema_version(payload: Mapping[str, Any], field_path: str = "schema_version") -> str:
    """Accept any minor version of the supported major, reject anything else.

    Additive minor changes must be readable by an older consumer -- that is the
    BACKWARD compatibility rule from docs/02 section 2. A different major version
    is a different topic by construction, so seeing one here is a routing bug.
    """
    version = _req_str(payload, "schema_version", field_path)
    major = version.split(".", 1)[0]
    if major != str(SUPPORTED_SCHEMA_MAJOR):
        raise SchemaValidationError(
            f"{field_path}: unsupported major version {version!r}, "
            f"this consumer speaks {SUPPORTED_SCHEMA_MAJOR}.x",
            field=field_path,
        )
    return version


def _check_unit_interval(value: float | None, field_path: str) -> None:
    if value is not None and not 0.0 <= value <= 1.0:
        raise SchemaValidationError(f"{field_path}: {value} is outside [0, 1]", field=field_path)


# ---------------------------------------------------------------------------
# Value objects
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class SensorReadings:
    """One sample of the five measured quantities.

    ``None`` means the sensor did not report. It never means zero: zero is a
    valid physical value, and conflating the two manufactures anomalies that
    never happened (docs/02 section 4).
    """

    temperature_c: float | None
    vibration_mm_s: float | None
    pressure_bar: float | None
    power_kw: float | None
    rotation_rpm: float | None

    def as_mapping(self) -> dict[str, float | None]:
        """Return the readings keyed by sensor name, in canonical order."""
        return {name: getattr(self, name) for name in SENSOR_NAMES}

    @property
    def null_count(self) -> int:
        return sum(1 for value in self.as_mapping().values() if value is None)

    def to_dict(self) -> dict[str, Any]:
        return dict(self.as_mapping())

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any], field_path: str = "readings") -> SensorReadings:
        data = _as_mapping(payload, field_path)
        for name in SENSOR_NAMES:
            if name not in data:
                raise SchemaValidationError(
                    f"{field_path}.{name}: required field is missing",
                    field=f"{field_path}.{name}",
                )
        return cls(
            temperature_c=_opt_number(data, "temperature_c", f"{field_path}.temperature_c"),
            vibration_mm_s=_opt_number(data, "vibration_mm_s", f"{field_path}.vibration_mm_s"),
            pressure_bar=_opt_number(data, "pressure_bar", f"{field_path}.pressure_bar"),
            power_kw=_opt_number(data, "power_kw", f"{field_path}.power_kw"),
            rotation_rpm=_opt_number(data, "rotation_rpm", f"{field_path}.rotation_rpm"),
        )


@dataclass(frozen=True, slots=True)
class ModelRef:
    """Identity of the model that produced a score.

    Travels with every scored event and every alert. Without it, an alert cannot
    be explained months later, and two scores cannot be compared across model
    versions (docs/01 ADR-001, point 5).
    """

    name: str
    version: str
    trained_at: datetime | None = None
    artifact_sha256: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "version": self.version,
            "trained_at": format_instant(self.trained_at) if self.trained_at else None,
            "artifact_sha256": self.artifact_sha256,
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any], field_path: str = "model") -> ModelRef:
        data = _as_mapping(payload, field_path)
        return cls(
            name=_req_str(data, "name", f"{field_path}.name"),
            version=_req_str(data, "version", f"{field_path}.version"),
            trained_at=_opt_instant(data, "trained_at", f"{field_path}.trained_at"),
            artifact_sha256=_opt_str(data, "artifact_sha256", f"{field_path}.artifact_sha256"),
        )


@dataclass(frozen=True, slots=True)
class FeatureContribution:
    """How far one feature sat from the reference profile.

    Carries the explainability of an alert: an operator asked to act needs to
    know *which* signal deviated, not only that a score crossed a threshold.
    """

    feature: str
    z_score: float

    def to_dict(self) -> dict[str, Any]:
        return {"feature": self.feature, "z_score": self.z_score}

    @classmethod
    def from_dict(
        cls, payload: Mapping[str, Any], field_path: str = "top_contributors[]"
    ) -> FeatureContribution:
        data = _as_mapping(payload, field_path)
        return cls(
            feature=_req_str(data, "feature", f"{field_path}.feature"),
            z_score=_req_number(data, "z_score", f"{field_path}.z_score"),
        )


def _contributors_to_list(items: Sequence[FeatureContribution]) -> list[dict[str, Any]]:
    return [item.to_dict() for item in items]


def _contributors_from_list(
    payload: Mapping[str, Any], key: str, field_path: str
) -> tuple[FeatureContribution, ...]:
    raw = payload.get(key) or []
    if not isinstance(raw, Sequence) or isinstance(raw, (str, bytes)):
        raise SchemaValidationError(
            f"{field_path}: expected an array, got {type(raw).__name__}", field=field_path
        )
    return tuple(
        FeatureContribution.from_dict(item, f"{field_path}[{index}]")
        for index, item in enumerate(raw)
    )


# ---------------------------------------------------------------------------
# Messages
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class TelemetryRaw:
    """A raw sensor sample. Topic ``telemetry.raw``.

    Carries no ground truth. Labels travel on ``telemetry.labels`` so that
    leaking them into the feature pipeline is impossible by construction rather
    than prevented by vigilance (decision D-03).
    """

    event_id: str
    machine_id: str
    line_id: str
    event_time: datetime
    ingest_time: datetime
    machine_state: MachineState
    readings: SensorReadings
    firmware_version: str | None = None
    schema_version: str = SCHEMA_VERSION

    @property
    def partition_key(self) -> str:
        """Kafka partition key.

        ``machine_id``, so every sample of a machine lands in one partition and
        is therefore consumed in production order. Ordering between machines is
        physically meaningless -- they are independent processes (docs/03 s.2).
        """
        return self.machine_id

    @property
    def source_lag_ms(self) -> int:
        """Delay between measurement and publication, in milliseconds."""
        return to_epoch_millis(self.ingest_time) - to_epoch_millis(self.event_time)

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "event_id": self.event_id,
            "machine_id": self.machine_id,
            "line_id": self.line_id,
            "event_time": format_instant(self.event_time),
            "ingest_time": format_instant(self.ingest_time),
            "machine_state": self.machine_state.value,
            "firmware_version": self.firmware_version,
            "readings": self.readings.to_dict(),
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> TelemetryRaw:
        data = _as_mapping(payload, "$")
        return cls(
            schema_version=_check_schema_version(data),
            event_id=_req_str(data, "event_id", "event_id"),
            machine_id=_req_str(data, "machine_id", "machine_id"),
            line_id=_req_str(data, "line_id", "line_id"),
            event_time=_req_instant(data, "event_time", "event_time"),
            ingest_time=_req_instant(data, "ingest_time", "ingest_time"),
            machine_state=MachineState.parse_tolerant(
                data.get("machine_state"), field="machine_state"
            ),
            firmware_version=_opt_str(data, "firmware_version", "firmware_version"),
            readings=SensorReadings.from_dict(_require(data, "readings", "readings")),
        )


@dataclass(frozen=True, slots=True)
class ScoredEvent:
    """Result of scoring one window for one machine. Topic ``telemetry.scored``.

    ``processing_delay_ms`` is not stored: it is derived from ``scored_at`` and
    ``window_end`` at serialisation time. A derived value kept as its own field
    can disagree with the fields it derives from; computing it makes that
    impossible.
    """

    machine_id: str
    line_id: str
    window_start: datetime
    window_end: datetime
    scored_at: datetime
    sample_count: int
    is_scored: bool
    machine_state: MachineState
    model: ModelRef
    skip_reason: SkipReason | None = None
    anomaly_score: float | None = None
    raw_score: float | None = None
    score_threshold: float | None = None
    is_anomaly: bool | None = None
    consecutive_windows: int = 0
    top_contributors: tuple[FeatureContribution, ...] = ()
    features: Mapping[str, float | None] = field(default_factory=dict)
    schema_version: str = SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.window_end <= self.window_start:
            raise SchemaValidationError(
                f"window_end ({format_instant(self.window_end)}) must be after "
                f"window_start ({format_instant(self.window_start)})",
                field="window_end",
            )
        if self.sample_count < 0:
            raise SchemaValidationError("sample_count: must not be negative", field="sample_count")
        _check_unit_interval(self.anomaly_score, "anomaly_score")
        _check_unit_interval(self.score_threshold, "score_threshold")

        if self.is_scored:
            for name in ("anomaly_score", "score_threshold", "is_anomaly"):
                if getattr(self, name) is None:
                    raise SchemaValidationError(
                        f"{name}: required when is_scored is true", field=name
                    )
        elif self.skip_reason is None:
            raise SchemaValidationError(
                "skip_reason: required when is_scored is false. A window that "
                "could not be scored must say why; silence must never look like "
                "a healthy result.",
                field="skip_reason",
            )

    @property
    def partition_key(self) -> str:
        return self.machine_id

    @property
    def processing_delay_ms(self) -> int:
        """When this window was published, relative to the instant it closes.

        **Negative while the window is still open**, and that is the normal case,
        not an anomaly. In update mode a window is republished at every trigger
        that changes it, so its early emissions are published before
        ``window_end`` is reached -- measured at a median of about -18 s on a
        live run (docs/10 section 5.4), which is precisely the latency that mode
        buys over ``append``.

        So this is not end-to-end pipeline lag, and reading it as such would be
        wrong in both directions: a negative value is not time travel, and a
        large positive one during a backfill measures the age of the replayed
        history rather than any delay of ours.
        """
        return to_epoch_millis(self.scored_at) - to_epoch_millis(self.window_end)

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "machine_id": self.machine_id,
            "line_id": self.line_id,
            "window_start": format_instant(self.window_start),
            "window_end": format_instant(self.window_end),
            "scored_at": format_instant(self.scored_at),
            "sample_count": self.sample_count,
            "is_scored": self.is_scored,
            "skip_reason": self.skip_reason.value if self.skip_reason else None,
            "machine_state": self.machine_state.value,
            "anomaly_score": self.anomaly_score,
            "raw_score": self.raw_score,
            "score_threshold": self.score_threshold,
            "is_anomaly": self.is_anomaly,
            "consecutive_windows": self.consecutive_windows,
            "top_contributors": _contributors_to_list(self.top_contributors),
            "features": dict(self.features),
            "model": self.model.to_dict(),
            "processing_delay_ms": self.processing_delay_ms,
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> ScoredEvent:
        data = _as_mapping(payload, "$")
        raw_skip = data.get("skip_reason")
        return cls(
            schema_version=_check_schema_version(data),
            machine_id=_req_str(data, "machine_id", "machine_id"),
            line_id=_req_str(data, "line_id", "line_id"),
            window_start=_req_instant(data, "window_start", "window_start"),
            window_end=_req_instant(data, "window_end", "window_end"),
            scored_at=_req_instant(data, "scored_at", "scored_at"),
            sample_count=_req_int(data, "sample_count", "sample_count"),
            is_scored=_req_bool(data, "is_scored", "is_scored"),
            skip_reason=(
                None if raw_skip is None else SkipReason.parse_strict(raw_skip, field="skip_reason")
            ),
            machine_state=MachineState.parse_tolerant(
                data.get("machine_state"), field="machine_state"
            ),
            anomaly_score=_opt_number(data, "anomaly_score", "anomaly_score"),
            raw_score=_opt_number(data, "raw_score", "raw_score"),
            score_threshold=_opt_number(data, "score_threshold", "score_threshold"),
            is_anomaly=_opt_bool(data, "is_anomaly", "is_anomaly"),
            consecutive_windows=(
                0
                if data.get("consecutive_windows") is None
                else _req_int(data, "consecutive_windows", "consecutive_windows")
            ),
            top_contributors=_contributors_from_list(data, "top_contributors", "top_contributors"),
            features=_req_features(data, "features", "features"),
            model=ModelRef.from_dict(_require(data, "model", "model")),
        )


@dataclass(frozen=True, slots=True)
class Alert:
    """A qualified anomaly, destined for an operator. Topic ``alerts``.

    ``alert_id`` must equal the deterministic derivation from
    ``(machine_id, window_start, model.name, model.version)``. That equality is
    validated here rather than trusted, because it is the single mechanism that
    turns at-least-once delivery into effectively-once persistence: a replayed
    batch republishes the same identifier, and the primary key absorbs it
    (docs/04 section 4.3). A producer bug here would not fail loudly -- it would
    quietly duplicate alerts in the operator's queue.
    """

    alert_id: str
    machine_id: str
    line_id: str
    severity: Severity
    anomaly_score: float
    score_threshold: float
    detected_at: datetime
    window_start: datetime
    window_end: datetime
    published_at: datetime
    model: ModelRef
    consecutive_windows: int = 1
    top_contributors: tuple[FeatureContribution, ...] = ()
    features: Mapping[str, float | None] = field(default_factory=dict)
    status: AlertStatus = AlertStatus.NEW
    schema_version: str = SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.window_end <= self.window_start:
            raise SchemaValidationError("window_end must be after window_start", field="window_end")
        if self.detected_at != self.window_end:
            raise SchemaValidationError(
                f"detected_at ({format_instant(self.detected_at)}) must equal window_end "
                f"({format_instant(self.window_end)}): an operator must see when the machine "
                "deviated, not when our computation finished",
                field="detected_at",
            )
        _check_unit_interval(self.anomaly_score, "anomaly_score")
        _check_unit_interval(self.score_threshold, "score_threshold")
        if self.consecutive_windows < 1:
            raise SchemaValidationError(
                "consecutive_windows: must be at least 1", field="consecutive_windows"
            )
        if self.status is not AlertStatus.NEW:
            raise SchemaValidationError(
                f"status: must be NEW on the wire, got {self.status.value}. The alert "
                "lifecycle belongs exclusively to alert-service.",
                field="status",
            )

        expected = derive_alert_id(
            machine_id=self.machine_id,
            window_start=self.window_start,
            model_name=self.model.name,
            model_version=self.model.version,
        )
        if self.alert_id != expected:
            raise SchemaValidationError(
                f"alert_id: {self.alert_id!r} does not match the deterministic derivation "
                f"{expected!r}. Idempotent persistence depends on this equality.",
                field="alert_id",
            )

    @property
    def partition_key(self) -> str:
        return self.machine_id

    @property
    def detection_latency_ms(self) -> int:
        """Delay between the window closing and the alert being published."""
        return to_epoch_millis(self.published_at) - to_epoch_millis(self.detected_at)

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "alert_id": self.alert_id,
            "machine_id": self.machine_id,
            "line_id": self.line_id,
            "severity": self.severity.value,
            "status": self.status.value,
            "anomaly_score": self.anomaly_score,
            "score_threshold": self.score_threshold,
            "detected_at": format_instant(self.detected_at),
            "window_start": format_instant(self.window_start),
            "window_end": format_instant(self.window_end),
            "published_at": format_instant(self.published_at),
            "consecutive_windows": self.consecutive_windows,
            "top_contributors": _contributors_to_list(self.top_contributors),
            "features": dict(self.features),
            "model": self.model.to_dict(),
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> Alert:
        data = _as_mapping(payload, "$")
        raw_status = data.get("status")
        return cls(
            schema_version=_check_schema_version(data),
            alert_id=_req_str(data, "alert_id", "alert_id"),
            machine_id=_req_str(data, "machine_id", "machine_id"),
            line_id=_req_str(data, "line_id", "line_id"),
            severity=Severity.parse_strict(
                _require(data, "severity", "severity"), field="severity"
            ),
            status=(
                AlertStatus.NEW
                if raw_status is None
                else AlertStatus.parse_strict(raw_status, field="status")
            ),
            anomaly_score=_req_number(data, "anomaly_score", "anomaly_score"),
            score_threshold=_req_number(data, "score_threshold", "score_threshold"),
            detected_at=_req_instant(data, "detected_at", "detected_at"),
            window_start=_req_instant(data, "window_start", "window_start"),
            window_end=_req_instant(data, "window_end", "window_end"),
            published_at=_req_instant(data, "published_at", "published_at"),
            consecutive_windows=(
                1
                if data.get("consecutive_windows") is None
                else _req_int(data, "consecutive_windows", "consecutive_windows")
            ),
            top_contributors=_contributors_from_list(data, "top_contributors", "top_contributors"),
            features=_req_features(data, "features", "features"),
            model=ModelRef.from_dict(_require(data, "model", "model")),
        )


@dataclass(frozen=True, slots=True)
class TelemetryLabel:
    """Ground truth for one sample or one alert. Topic ``telemetry.labels``.

    Never consumed by the stream processor. It exists for offline evaluation and
    drift tracking only. ``emitted_at`` is always later than ``event_time``, often
    by days -- that lag is the nature of a label in production, which is why
    this is a separate stream (docs/02 section 5).
    """

    machine_id: str
    event_time: datetime
    is_anomaly: bool
    source: LabelSource
    emitted_at: datetime
    event_id: str | None = None
    alert_id: str | None = None
    anomaly_type: AnomalyType | None = None
    episode_id: str | None = None
    episode_started_at: datetime | None = None
    operator_id: str | None = None
    root_cause_code: str | None = None
    schema_version: str = SCHEMA_VERSION

    @property
    def partition_key(self) -> str:
        return self.machine_id

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "event_id": self.event_id,
            "alert_id": self.alert_id,
            "machine_id": self.machine_id,
            "event_time": format_instant(self.event_time),
            "is_anomaly": self.is_anomaly,
            "anomaly_type": self.anomaly_type.value if self.anomaly_type else None,
            "episode_id": self.episode_id,
            "episode_started_at": (
                format_instant(self.episode_started_at) if self.episode_started_at else None
            ),
            "source": self.source.value,
            "operator_id": self.operator_id,
            "root_cause_code": self.root_cause_code,
            "emitted_at": format_instant(self.emitted_at),
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> TelemetryLabel:
        data = _as_mapping(payload, "$")
        raw_type = data.get("anomaly_type")
        return cls(
            schema_version=_check_schema_version(data),
            event_id=_opt_str(data, "event_id", "event_id"),
            alert_id=_opt_str(data, "alert_id", "alert_id"),
            machine_id=_req_str(data, "machine_id", "machine_id"),
            event_time=_req_instant(data, "event_time", "event_time"),
            is_anomaly=_req_bool(data, "is_anomaly", "is_anomaly"),
            anomaly_type=(
                None
                if raw_type is None
                else AnomalyType.parse_tolerant(raw_type, field="anomaly_type")
            ),
            episode_id=_opt_str(data, "episode_id", "episode_id"),
            episode_started_at=_opt_instant(data, "episode_started_at", "episode_started_at"),
            source=LabelSource.parse_strict(_require(data, "source", "source"), field="source"),
            operator_id=_opt_str(data, "operator_id", "operator_id"),
            root_cause_code=_opt_str(data, "root_cause_code", "root_cause_code"),
            emitted_at=_req_instant(data, "emitted_at", "emitted_at"),
        )
