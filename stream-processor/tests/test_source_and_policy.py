"""Parsing, dead-lettering, late routing, admission and scored-event building."""

from __future__ import annotations

import base64
import json
from datetime import timedelta
from pathlib import Path
from typing import Any

import numpy as np
import numpy.typing as npt
import pytest
from jsonschema import Draft202012Validator
from pyspark.sql import functions as F
from telemetry_core.dlq import DlqEnvelope
from telemetry_core.enums import DlqReason, MachineState, Severity, SkipReason
from telemetry_core.features import FEATURE_NAMES, MIN_SAMPLES_FOR_SCORING
from telemetry_core.schemas import ModelRef

from conftest import ORIGIN
from stream_processor.config.settings import StreamSettings
from stream_processor.policy import (
    admission_skip_reason,
    build_scored_event,
    severity_for,
    to_utc_datetime,
    top_contributors,
)
from stream_processor.source import parse_telemetry, split_late, split_valid


def _kafka_row(payload: str, *, offset: int = 0, partition: int = 0) -> dict[str, Any]:
    return {
        "topic": "telemetry.raw",
        "partition": partition,
        "offset": offset,
        "key": bytearray(b"M-014"),
        "value": bytearray(payload.encode("utf-8")),
    }


def _valid_payload(**overrides: Any) -> str:
    message = {
        "schema_version": "1.0",
        "event_id": "5c0a3f8e-6f5b-4a0e-9d1c-8f2b7a1e4c93",
        "machine_id": "M-014",
        "line_id": "LINE-A",
        "event_time": "2026-04-01T06:00:00.412Z",
        "ingest_time": "2026-04-01T06:00:00.610Z",
        "machine_state": "RUNNING",
        "firmware_version": "2.3.1",
        "readings": {
            "temperature_c": 72.48,
            "vibration_mm_s": 2.81,
            "pressure_bar": 5.12,
            "power_kw": 12.44,
            "rotation_rpm": 1478.2,
        },
    }
    message.update(overrides)
    return json.dumps(message)


def _parse(spark: Any, rows: list[dict[str, Any]]) -> Any:
    schema = "topic string, partition int, offset long, key binary, value binary"
    return parse_telemetry(spark.createDataFrame(rows, schema=schema))


class TestParsingAndDlq:
    def test_a_valid_message_is_accepted(self, spark: Any) -> None:
        parsed = _parse(spark, [_kafka_row(_valid_payload())])
        accepted, rejected = split_valid(parsed)
        assert accepted.count() == 1
        assert rejected.count() == 0

    def test_malformed_json_is_rejected_with_a_reason(self, spark: Any) -> None:
        parsed = _parse(spark, [_kafka_row('{"machine_id": "M-014"')])
        _accepted, rejected = split_valid(parsed)
        row = rejected.collect()[0]
        assert row["dlq_reason"] == DlqReason.MALFORMED_JSON.value
        assert "not valid JSON" in row["dlq_detail"]

    def test_a_missing_required_field_names_the_field(self, spark: Any) -> None:
        payload = json.loads(_valid_payload())
        del payload["machine_id"]
        parsed = _parse(spark, [_kafka_row(json.dumps(payload))])
        _accepted, rejected = split_valid(parsed)
        row = rejected.collect()[0]
        assert row["dlq_reason"] == DlqReason.SCHEMA_VALIDATION_FAILED.value
        assert "machine_id" in row["dlq_detail"]

    def test_an_unsupported_major_version_is_rejected(self, spark: Any) -> None:
        parsed = _parse(spark, [_kafka_row(_valid_payload(schema_version="2.0"))])
        _accepted, rejected = split_valid(parsed)
        row = rejected.collect()[0]
        assert row["dlq_reason"] == DlqReason.UNSUPPORTED_SCHEMA_VERSION.value

    def test_a_malformed_timestamp_is_rejected_not_silently_null(self, spark: Any) -> None:
        """Parsing the instant explicitly is what makes this visible."""
        parsed = _parse(spark, [_kafka_row(_valid_payload(event_time="2026-04-01 06:00:00"))])
        _accepted, rejected = split_valid(parsed)
        row = rejected.collect()[0]
        assert row["dlq_reason"] == DlqReason.SCHEMA_VALIDATION_FAILED.value
        assert "event_time" in row["dlq_detail"]

    def test_one_bad_message_does_not_reject_the_good_ones(self, spark: Any) -> None:
        parsed = _parse(
            spark,
            [
                _kafka_row(_valid_payload(), offset=0),
                _kafka_row("not json at all", offset=1),
                _kafka_row(_valid_payload(), offset=2),
            ],
        )
        accepted, rejected = split_valid(parsed)
        assert accepted.count() == 2
        assert rejected.count() == 1

    def test_the_original_payload_survives_for_replay(self, spark: Any) -> None:
        """A dead letter without the original bytes is a log line, not a queue."""
        broken = '{"machine_id": "M-014", oops}'
        parsed = _parse(spark, [_kafka_row(broken)])
        _accepted, rejected = split_valid(parsed)
        encoded = rejected.select(F.base64(F.col("raw_payload")).alias("b")).collect()[0]["b"]
        assert base64.b64decode(encoded).decode("utf-8") == broken


class TestDlqEnvelopeContract:
    def test_the_envelope_validates_against_its_schema(self, repo_root: Path) -> None:
        schema = json.loads(
            (repo_root / "contracts" / "json-schema" / "telemetry-dlq.v1.json").read_text(
                encoding="utf-8"
            )
        )
        Draft202012Validator.check_schema(schema)

        envelope = DlqEnvelope.wrap(
            payload=b'{"broken":',
            key=b"M-014",
            reason=DlqReason.MALFORMED_JSON,
            detail="payload is not valid JSON",
            topic="telemetry.raw",
            partition=3,
            offset=148223,
            failed_at=ORIGIN,
            processor_version="0.1.0",
        )
        errors = list(Draft202012Validator(schema).iter_errors(envelope.to_dict()))
        assert not errors, [error.message for error in errors]

    def test_it_round_trips(self) -> None:
        envelope = DlqEnvelope.wrap(
            payload=b"original bytes",
            key=None,
            reason=DlqReason.SCHEMA_VALIDATION_FAILED,
            detail="machine_id",
            topic="telemetry.raw",
            partition=0,
            offset=7,
            failed_at=ORIGIN,
            processor_version="0.1.0",
        )
        restored = DlqEnvelope.from_dict(envelope.to_dict())
        assert restored == envelope
        assert restored.decoded_payload() == b"original bytes"

    def test_an_unactionable_detail_is_refused(self) -> None:
        from telemetry_core.errors import SchemaValidationError

        with pytest.raises(SchemaValidationError, match="dlq_detail"):
            DlqEnvelope.wrap(
                payload=b"x",
                key=None,
                reason=DlqReason.MALFORMED_JSON,
                detail="",
                topic="telemetry.raw",
                partition=0,
                offset=0,
                failed_at=ORIGIN,
                processor_version="0.1.0",
            )


class TestLateRouting:
    def test_a_promptly_published_sample_stays_on_the_main_path(self, spark: Any) -> None:
        """Two hundred milliseconds of producer lag is normal, not late."""
        settings = StreamSettings(watermark_seconds=90)
        accepted, _rejected = split_valid(_parse(spark, [_kafka_row(_valid_payload())]))
        on_time, too_late = split_late(accepted, settings)
        assert on_time.count() == 1
        assert too_late.count() == 0

    def test_a_buffered_sample_is_routed_to_the_late_topic(self, spark: Any) -> None:
        """Spark drops late rows silently inside the aggregation; branching
        first is what keeps the information."""
        settings = StreamSettings(watermark_seconds=90)
        payload = _valid_payload(ingest_time="2026-04-01T06:02:30.000Z")  # 150 s later
        accepted, _rejected = split_valid(_parse(spark, [_kafka_row(payload)]))
        on_time, too_late = split_late(accepted, settings)
        assert on_time.count() == 0
        assert too_late.count() == 1
        assert too_late.collect()[0]["lateness_seconds"] == 150

    def test_detection_can_be_disabled_for_a_backfill(self, spark: Any) -> None:
        """During a replay every sample is old by construction; leaving this on
        would divert the entire history away from the aggregation."""
        settings = StreamSettings(watermark_seconds=90, late_detection_enabled=False)
        payload = _valid_payload(ingest_time="2026-04-01T09:00:00.000Z")
        accepted, _rejected = split_valid(_parse(spark, [_kafka_row(payload)]))
        on_time, too_late = split_late(accepted, settings)
        assert on_time.count() == 1
        assert too_late.count() == 0


class TestAdmission:
    def test_a_short_window_is_not_scored(self) -> None:
        reason = admission_skip_reason(
            sample_count=MIN_SAMPLES_FOR_SCORING - 1, running_ratio=1.0, machine_state="RUNNING"
        )
        assert reason is SkipReason.INSUFFICIENT_SAMPLES

    def test_a_mostly_idle_window_is_not_scored(self) -> None:
        reason = admission_skip_reason(sample_count=60, running_ratio=0.5, machine_state="RUNNING")
        assert reason is SkipReason.MACHINE_NOT_RUNNING

    def test_a_window_ending_outside_running_is_not_scored(self) -> None:
        """The Phase 2 finding: 42 of 221 false positives ended in MAINTENANCE
        while still being more than 90 % RUNNING."""
        reason = admission_skip_reason(
            sample_count=60, running_ratio=0.95, machine_state="MAINTENANCE"
        )
        assert reason is SkipReason.MACHINE_NOT_RUNNING

    def test_a_full_running_window_is_admitted(self) -> None:
        assert (
            admission_skip_reason(sample_count=60, running_ratio=1.0, machine_state="RUNNING")
            is None
        )


class TestSeverity:
    def test_bands_are_relative_to_the_headroom(self) -> None:
        threshold = 0.99
        assert severity_for(0.991, threshold) is Severity.MEDIUM
        assert severity_for(0.995, threshold) is Severity.HIGH
        assert severity_for(0.999, threshold) is Severity.CRITICAL


class TestScoredEvent:
    def _features(self) -> npt.NDArray[np.float64]:
        return np.arange(len(FEATURE_NAMES), dtype="float64")

    def test_an_unscored_window_is_still_published_with_a_reason(self) -> None:
        """Silence and "nothing is wrong" must never look alike."""
        event = build_scored_event(
            machine_id="M-014",
            line_id="LINE-A",
            window_start=ORIGIN,
            window_end=ORIGIN + timedelta(seconds=60),
            sample_count=5,
            running_ratio=1.0,
            machine_state="RUNNING",
            is_mature=True,
            features=self._features(),
            model_ref=ModelRef(name="one_class_svm", version="2.0.0"),
            raw_score=None,
            score=None,
            threshold=0.99,
            contributions=None,
        )
        assert event.is_scored is False
        assert event.skip_reason is SkipReason.INSUFFICIENT_SAMPLES
        assert event.anomaly_score is None

    def test_a_scored_window_carries_the_decision(self) -> None:
        event = build_scored_event(
            machine_id="M-014",
            line_id="LINE-A",
            window_start=ORIGIN,
            window_end=ORIGIN + timedelta(seconds=60),
            sample_count=60,
            running_ratio=1.0,
            machine_state="RUNNING",
            is_mature=True,
            features=self._features(),
            model_ref=ModelRef(name="one_class_svm", version="2.0.0"),
            raw_score=-0.07,
            score=0.996,
            threshold=0.9927653712984937,
            contributions=np.full(len(FEATURE_NAMES), 2.0),
        )
        assert event.is_scored is True
        assert event.is_anomaly is True
        assert event.machine_state is MachineState.RUNNING
        assert len(event.features) == len(FEATURE_NAMES)

    def test_processing_delay_is_derived_not_stored(self) -> None:
        event = build_scored_event(
            machine_id="M-014",
            line_id="LINE-A",
            window_start=ORIGIN,
            window_end=ORIGIN + timedelta(seconds=60),
            sample_count=60,
            running_ratio=1.0,
            machine_state="RUNNING",
            is_mature=True,
            features=self._features(),
            model_ref=ModelRef(name="one_class_svm", version="2.0.0"),
            raw_score=-0.07,
            score=0.5,
            threshold=0.99,
            contributions=None,
            scored_at=ORIGIN + timedelta(seconds=75),
        )
        assert event.processing_delay_ms == 15_000

    def test_non_finite_features_become_null(self) -> None:
        features = self._features()
        features[0] = np.nan
        event = build_scored_event(
            machine_id="M-014",
            line_id="LINE-A",
            window_start=ORIGIN,
            window_end=ORIGIN + timedelta(seconds=60),
            sample_count=60,
            running_ratio=1.0,
            machine_state="RUNNING",
            is_mature=True,
            features=features,
            model_ref=ModelRef(name="one_class_svm", version="2.0.0"),
            raw_score=-0.07,
            score=0.5,
            threshold=0.99,
            contributions=None,
        )
        assert event.features[FEATURE_NAMES[0]] is None


class TestContributors:
    def test_the_largest_departures_come_first(self) -> None:
        contributions = np.zeros(len(FEATURE_NAMES))
        contributions[5] = -8.0
        contributions[9] = 3.0
        ranked = top_contributors(contributions)
        assert [item.feature for item in ranked][:2] == [FEATURE_NAMES[5], FEATURE_NAMES[9]]

    def test_non_finite_contributions_are_dropped(self) -> None:
        contributions = np.full(len(FEATURE_NAMES), np.nan)
        contributions[2] = 4.0
        ranked = top_contributors(contributions)
        assert [item.feature for item in ranked] == [FEATURE_NAMES[2]]


def test_naive_timestamps_are_localised_at_the_boundary() -> None:
    """Arrow hands Spark timestamps over naive even in a UTC session."""
    import pandas as pd

    converted = to_utc_datetime(pd.Timestamp("2026-04-01 06:00:00"))
    assert converted.tzinfo is not None
    assert converted.utcoffset() == timedelta(0)


class TestSettingsFromTheEnvironment:
    def test_a_blank_run_duration_means_unbounded(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Compose writes ``STREAM_RUN_SECONDS=""`` for an undefined variable.

        Read as a value it is not a float, and the job died during settings
        validation before opening a single stream -- on the exact command the
        README documents.
        """
        monkeypatch.setenv("STREAM_RUN_SECONDS", "")
        monkeypatch.setenv("STREAM_MAX_OFFSETS_PER_TRIGGER", "")

        settings = StreamSettings()

        assert settings.run_seconds is None
        assert settings.max_offsets_per_trigger == 20_000

    def test_a_real_value_is_still_read(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("STREAM_RUN_SECONDS", "42.5")

        assert StreamSettings().run_seconds == 42.5
