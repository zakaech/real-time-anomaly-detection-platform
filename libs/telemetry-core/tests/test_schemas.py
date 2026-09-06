"""Invariants enforced by the dataclasses themselves.

These are the rules JSON Schema cannot express: relationships between fields.
The most consequential is the alert identifier check -- it is the mechanism that
makes at-least-once delivery safe, and a producer that got it wrong would not
fail, it would quietly duplicate alerts.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from telemetry_core.enums import AlertStatus, MachineState, Severity, SkipReason
from telemetry_core.errors import SchemaValidationError
from telemetry_core.ids import derive_alert_id
from telemetry_core.schemas import Alert, ModelRef, ScoredEvent, SensorReadings

_WINDOW_START = datetime(2026, 9, 6, 14, 22, 10, tzinfo=UTC)
_WINDOW_END = datetime(2026, 9, 6, 14, 23, 10, tzinfo=UTC)
_PUBLISHED = datetime(2026, 9, 6, 14, 23, 41, 930000, tzinfo=UTC)
_MODEL = ModelRef(name="isolation_forest", version="1.3.0")

_VALID_ALERT_ID = derive_alert_id(
    machine_id="M-014",
    window_start=_WINDOW_START,
    model_name=_MODEL.name,
    model_version=_MODEL.version,
)


def _alert(**overrides: object) -> Alert:
    kwargs: dict[str, object] = {
        "alert_id": _VALID_ALERT_ID,
        "machine_id": "M-014",
        "line_id": "LINE-A",
        "severity": Severity.HIGH,
        "anomaly_score": 0.9962,
        "score_threshold": 0.995,
        "detected_at": _WINDOW_END,
        "window_start": _WINDOW_START,
        "window_end": _WINDOW_END,
        "published_at": _PUBLISHED,
        "model": _MODEL,
    }
    kwargs.update(overrides)
    return Alert(**kwargs)  # type: ignore[arg-type]


def _scored(**overrides: object) -> ScoredEvent:
    kwargs: dict[str, object] = {
        "machine_id": "M-014",
        "line_id": "LINE-A",
        "window_start": _WINDOW_START,
        "window_end": _WINDOW_END,
        "scored_at": datetime(2026, 9, 6, 14, 23, 41, 905000, tzinfo=UTC),
        "sample_count": 58,
        "is_scored": True,
        "machine_state": MachineState.RUNNING,
        "model": _MODEL,
        "anomaly_score": 0.9962,
        "score_threshold": 0.995,
        "is_anomaly": True,
    }
    kwargs.update(overrides)
    return ScoredEvent(**kwargs)  # type: ignore[arg-type]


class TestAlertIdentity:
    def test_valid_identifier_is_accepted(self) -> None:
        assert _alert().alert_id == _VALID_ALERT_ID

    def test_mismatched_identifier_is_rejected(self) -> None:
        """The keystone of idempotent persistence, checked rather than trusted."""
        with pytest.raises(SchemaValidationError, match="deterministic derivation"):
            _alert(alert_id="00000000-0000-5000-8000-000000000000")

    def test_identifier_must_track_the_model_version(self) -> None:
        """Changing the model without changing the id would overwrite a verdict."""
        other_model = ModelRef(name="isolation_forest", version="1.4.0")
        with pytest.raises(SchemaValidationError, match="deterministic derivation"):
            _alert(model=other_model)

    def test_identifier_must_track_the_window(self) -> None:
        shifted = datetime(2026, 9, 6, 14, 22, 20, tzinfo=UTC)
        with pytest.raises(SchemaValidationError, match="deterministic derivation"):
            _alert(window_start=shifted)


class TestAlertInvariants:
    def test_detected_at_must_equal_window_end(self) -> None:
        """An operator must see when the machine deviated, not when we finished."""
        with pytest.raises(SchemaValidationError, match="must equal window_end"):
            _alert(detected_at=_PUBLISHED)

    def test_window_must_be_ordered(self) -> None:
        with pytest.raises(SchemaValidationError, match="window_end must be after"):
            _alert(window_start=_WINDOW_END, window_end=_WINDOW_START)

    @pytest.mark.parametrize("score", [-0.1, 1.1])
    def test_score_must_be_in_the_unit_interval(self, score: float) -> None:
        with pytest.raises(SchemaValidationError, match="outside"):
            _alert(anomaly_score=score)

    def test_status_must_be_new_on_the_wire(self) -> None:
        """The lifecycle belongs exclusively to alert-service."""
        with pytest.raises(SchemaValidationError, match="must be NEW on the wire"):
            _alert(status=AlertStatus.ACKNOWLEDGED)

    def test_consecutive_windows_must_be_positive(self) -> None:
        with pytest.raises(SchemaValidationError, match="at least 1"):
            _alert(consecutive_windows=0)

    def test_detection_latency_is_derived(self) -> None:
        assert _alert().detection_latency_ms == 31930


class TestScoredEventInvariants:
    def test_scored_event_requires_its_score(self) -> None:
        with pytest.raises(SchemaValidationError, match="required when is_scored is true"):
            _scored(anomaly_score=None)

    def test_unscored_event_requires_a_reason(self) -> None:
        """Silence and a healthy result must never look alike."""
        with pytest.raises(SchemaValidationError, match="required when is_scored is false"):
            _scored(
                is_scored=False,
                anomaly_score=None,
                score_threshold=None,
                is_anomaly=None,
                skip_reason=None,
            )

    def test_unscored_event_with_a_reason_is_valid(self) -> None:
        event = _scored(
            is_scored=False,
            anomaly_score=None,
            score_threshold=None,
            is_anomaly=None,
            skip_reason=SkipReason.INSUFFICIENT_SAMPLES,
            sample_count=11,
        )
        assert event.skip_reason is SkipReason.INSUFFICIENT_SAMPLES

    def test_processing_delay_is_derived_from_its_inputs(self) -> None:
        """Not stored, so it cannot disagree with scored_at and window_end."""
        assert _scored().processing_delay_ms == 31905

    def test_negative_sample_count_is_rejected(self) -> None:
        with pytest.raises(SchemaValidationError, match="must not be negative"):
            _scored(sample_count=-1)


class TestSensorReadings:
    def test_readings_expose_canonical_order(self) -> None:
        readings = SensorReadings(72.48, 2.81, 5.12, 12.44, 1478.2)
        assert tuple(readings.as_mapping()) == (
            "temperature_c",
            "vibration_mm_s",
            "pressure_bar",
            "power_kw",
            "rotation_rpm",
        )

    def test_null_count_distinguishes_missing_from_zero(self) -> None:
        """Zero is a valid physical value; conflating the two invents anomalies."""
        with_zero = SensorReadings(0.0, 0.0, 0.0, 0.0, 0.0)
        with_nulls = SensorReadings(None, 2.81, None, 12.44, 1478.2)
        assert with_zero.null_count == 0
        assert with_nulls.null_count == 2

    def test_messages_are_frozen(self) -> None:
        readings = SensorReadings(72.48, 2.81, 5.12, 12.44, 1478.2)
        with pytest.raises((AttributeError, TypeError)):
            readings.temperature_c = 80.0  # type: ignore[misc]
