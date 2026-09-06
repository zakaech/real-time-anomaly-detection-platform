"""Codec behaviour: byte-level guarantees and decoding failures.

Decoding failures matter as much as successes here. The stream processor decides
between the dead letter topic and a retry based on the exception it catches
(docs/03 section 7), so every data error must surface as ``SchemaValidationError``
and never as a bare ``KeyError`` or ``TypeError``.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from telemetry_core.codec import (
    decode,
    decode_dict,
    encode,
    encode_dict,
    partition_key_bytes,
)
from telemetry_core.enums import MachineState, Severity
from telemetry_core.errors import SchemaValidationError
from telemetry_core.ids import derive_alert_id
from telemetry_core.schemas import (
    Alert,
    ModelRef,
    ScoredEvent,
    SensorReadings,
    TelemetryRaw,
)

_WINDOW_START = datetime(2026, 9, 6, 14, 22, 10, tzinfo=UTC)
_WINDOW_END = datetime(2026, 9, 6, 14, 23, 10, tzinfo=UTC)
_MODEL = ModelRef(name="isolation_forest", version="1.3.0")


def _raw() -> TelemetryRaw:
    return TelemetryRaw(
        event_id="5c0a3f8e-6f5b-4a0e-9d1c-8f2b7a1e4c93",
        machine_id="M-014",
        line_id="LINE-A",
        event_time=datetime(2026, 9, 6, 14, 23, 7, 412000, tzinfo=UTC),
        ingest_time=datetime(2026, 9, 6, 14, 23, 7, 610000, tzinfo=UTC),
        machine_state=MachineState.RUNNING,
        readings=SensorReadings(72.48, 2.81, 5.12, 12.44, 1478.2),
        firmware_version="2.3.1",
    )


def _alert() -> Alert:
    alert_id = derive_alert_id(
        machine_id="M-014",
        window_start=_WINDOW_START,
        model_name=_MODEL.name,
        model_version=_MODEL.version,
    )
    return Alert(
        alert_id=alert_id,
        machine_id="M-014",
        line_id="LINE-A",
        severity=Severity.HIGH,
        anomaly_score=0.9962,
        score_threshold=0.995,
        detected_at=_WINDOW_END,
        window_start=_WINDOW_START,
        window_end=_WINDOW_END,
        published_at=datetime(2026, 9, 6, 14, 23, 41, 930000, tzinfo=UTC),
        model=_MODEL,
        consecutive_windows=2,
    )


class TestEncoding:
    def test_encoding_is_compact(self) -> None:
        payload = encode(_raw())
        assert b", " not in payload
        assert b'": ' not in payload

    def test_encoding_is_deterministic(self) -> None:
        """Same input, same bytes.

        Obtained by building the dictionary in a fixed order rather than by
        sorting keys, so the wire order still matches the contract document and
        stays readable in kafka-console-consumer.
        """
        assert encode(_raw()) == encode(_raw())

    def test_field_order_follows_the_contract(self) -> None:
        payload = encode(_raw()).decode("utf-8")
        assert payload.startswith('{"schema_version":"1.0","event_id":')

    def test_timestamps_use_the_contract_format(self) -> None:
        payload = encode(_raw()).decode("utf-8")
        assert '"event_time":"2026-09-06T14:23:07.412Z"' in payload
        assert "+00:00" not in payload

    def test_non_finite_numbers_are_refused(self) -> None:
        """NaN is not valid JSON, and it means a feature computation went wrong.

        Failing here beats publishing something a strict parser rejects and a
        lenient one silently scores.
        """
        with pytest.raises(SchemaValidationError, match="not JSON-serialisable"):
            encode_dict({"value": float("nan")})
        with pytest.raises(SchemaValidationError):
            encode_dict({"value": float("inf")})

    def test_unicode_is_not_escaped(self) -> None:
        assert encode_dict({"note": "sur-régime"}) == '{"note":"sur-régime"}'.encode()


class TestRoundTrip:
    def test_raw_roundtrip(self) -> None:
        assert decode(TelemetryRaw, encode(_raw())) == _raw()

    def test_alert_roundtrip(self) -> None:
        assert decode(Alert, encode(_alert())) == _alert()

    def test_scored_roundtrip(self) -> None:
        event = ScoredEvent(
            machine_id="M-014",
            line_id="LINE-A",
            window_start=_WINDOW_START,
            window_end=_WINDOW_END,
            scored_at=datetime(2026, 9, 6, 14, 23, 41, 905000, tzinfo=UTC),
            sample_count=58,
            is_scored=True,
            machine_state=MachineState.RUNNING,
            model=_MODEL,
            anomaly_score=0.9962,
            raw_score=-0.0731,
            score_threshold=0.995,
            is_anomaly=True,
            consecutive_windows=2,
            features={"temperature_c_mean": 72.4, "vibration_mm_s_stddev": None},
        )
        assert decode(ScoredEvent, encode(event)) == event


class TestForwardCompatibility:
    def test_unknown_fields_are_ignored(self) -> None:
        """The consumer half of BACKWARD compatibility.

        A producer may add an optional field in a minor version; rejecting it
        would turn every additive change into a coordinated redeployment.
        """
        payload = _raw().to_dict()
        payload["humidity_pct"] = 41.2
        payload["readings"]["torque_nm"] = 88.0

        decoded = decode_dict(encode_dict(payload))
        message = TelemetryRaw.from_dict(decoded)
        assert message.machine_id == "M-014"

    def test_unknown_machine_state_degrades_to_unknown(self) -> None:
        """Adding an enum value must not crash existing consumers."""
        payload = _raw().to_dict()
        payload["machine_state"] = "PURGING"
        assert TelemetryRaw.from_dict(payload).machine_state is MachineState.UNKNOWN

    def test_unknown_severity_is_rejected(self) -> None:
        """Severity is produced inside the platform, so tolerance would hide a bug.

        Silently defaulting it would display an alert with the wrong urgency.
        """
        payload = _alert().to_dict()
        payload["severity"] = "APOCALYPTIC"
        with pytest.raises(SchemaValidationError, match="severity"):
            Alert.from_dict(payload)

    def test_newer_minor_schema_version_is_accepted(self) -> None:
        payload = _raw().to_dict()
        payload["schema_version"] = "1.7"
        assert TelemetryRaw.from_dict(payload).schema_version == "1.7"

    def test_different_major_schema_version_is_rejected(self) -> None:
        """A major version lives on its own topic, so seeing one here is a routing bug."""
        payload = _raw().to_dict()
        payload["schema_version"] = "2.0"
        with pytest.raises(SchemaValidationError, match="unsupported major version"):
            TelemetryRaw.from_dict(payload)


class TestDecodingFailures:
    def test_invalid_json_raises_schema_validation_error(self) -> None:
        with pytest.raises(SchemaValidationError, match="not valid JSON"):
            decode_dict(b'{"machine_id": "M-014"')

    def test_invalid_utf8_raises_schema_validation_error(self) -> None:
        with pytest.raises(SchemaValidationError, match="not valid UTF-8"):
            decode_dict(b"\xff\xfe\x00")

    def test_non_object_payload_is_rejected(self) -> None:
        with pytest.raises(SchemaValidationError, match="must be a JSON object"):
            decode_dict(b"[1, 2, 3]")

    def test_missing_required_field_names_the_field(self) -> None:
        payload = _raw().to_dict()
        del payload["machine_id"]
        with pytest.raises(SchemaValidationError) as info:
            TelemetryRaw.from_dict(payload)
        assert info.value.field == "machine_id"

    def test_wrong_type_names_the_field_path(self) -> None:
        payload = _raw().to_dict()
        payload["readings"]["power_kw"] = "twelve"
        with pytest.raises(SchemaValidationError) as info:
            TelemetryRaw.from_dict(payload)
        assert info.value.field == "readings.power_kw"

    def test_boolean_is_not_accepted_as_a_number(self) -> None:
        """bool subclasses int in Python; accepting it would hide a producer bug."""
        payload = _raw().to_dict()
        payload["readings"]["power_kw"] = True
        with pytest.raises(SchemaValidationError, match="expected a number"):
            TelemetryRaw.from_dict(payload)

    def test_missing_sensor_key_is_rejected(self) -> None:
        """An absent sensor is not the same as a sensor reporting null."""
        payload = _raw().to_dict()
        del payload["readings"]["vibration_mm_s"]
        with pytest.raises(SchemaValidationError) as info:
            TelemetryRaw.from_dict(payload)
        assert info.value.field == "readings.vibration_mm_s"

    def test_malformed_timestamp_is_rejected(self) -> None:
        payload = _raw().to_dict()
        payload["event_time"] = "2026-09-06 14:23:07"
        with pytest.raises(SchemaValidationError):
            TelemetryRaw.from_dict(payload)


class TestPartitionKey:
    def test_key_is_utf8_bytes(self) -> None:
        assert partition_key_bytes("M-014") == b"M-014"

    def test_empty_key_is_refused(self) -> None:
        """A null key round-robins, which would destroy per-machine ordering."""
        with pytest.raises(ValueError, match="must not be empty"):
            partition_key_bytes("")

    def test_messages_expose_machine_id_as_partition_key(self) -> None:
        assert _raw().partition_key == "M-014"
        assert _alert().partition_key == "M-014"
