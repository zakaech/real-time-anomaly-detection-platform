"""Separation of telemetry from ground truth, and conformance to the contracts.

The no-leak tests are the ones this phase exists to guarantee. They are written
against the produced payload rather than against the code, because a review can
miss a field but a schema comparison cannot.
"""

from __future__ import annotations

from datetime import timedelta
from pathlib import Path
from typing import Any

import pytest
from jsonschema import Draft202012Validator
from telemetry_core.codec import decode_dict, encode
from telemetry_core.enums import AnomalyType, LabelSource, MachineState
from telemetry_core.schemas import SensorReadings

from conftest import RUN_START, load_json
from event_simulator.generation.rng import RngRegistry
from event_simulator.generation.state import GeneratedSample, SampleGroundTruth
from event_simulator.publishing.projections import (
    new_event_id,
    to_telemetry_label,
    to_telemetry_raw,
)

_EVENT_ID = "5c0a3f8e-6f5b-4a0e-9d1c-8f2b7a1e4c93"
_INGEST = RUN_START + timedelta(milliseconds=180)

_READINGS = SensorReadings(
    temperature_c=72.48,
    vibration_mm_s=2.81,
    pressure_bar=5.12,
    power_kw=12.44,
    rotation_rpm=1478.2,
)


def _anomalous_sample() -> GeneratedSample:
    return GeneratedSample(
        machine_id="M-014",
        line_id="LINE-C",
        event_time=RUN_START,
        machine_state=MachineState.RUNNING,
        readings=_READINGS,
        firmware_version="2.3.1",
        ground_truth=SampleGroundTruth(
            is_anomaly=True,
            anomaly_type=AnomalyType.BEARING_WEAR,
            episode_id="ep-20260302-M014-007",
            episode_started_at=RUN_START - timedelta(seconds=120),
        ),
    )


def _normal_sample() -> GeneratedSample:
    return GeneratedSample(
        machine_id="M-004",
        line_id="LINE-A",
        event_time=RUN_START,
        machine_state=MachineState.RUNNING,
        readings=_READINGS,
        firmware_version=None,
        ground_truth=SampleGroundTruth.normal(),
    )


def _validation_errors(schema_dir: Path, name: str, payload: dict[str, Any]) -> list[str]:
    """Validate and return readable messages.

    Returns strings rather than a validator object so no jsonschema type reaches
    a signature: mypy runs with ``disallow_any_unimported``, and a partially
    typed dependency would turn this helper into ``Any``, silently weakening
    every assertion built on it.
    """
    schema = load_json(schema_dir / name)
    Draft202012Validator.check_schema(schema)
    validator = Draft202012Validator(schema)
    return [
        f"{list(error.absolute_path)}: {error.message}" for error in validator.iter_errors(payload)
    ]


class TestGroundTruthDoesNotLeak:
    def test_telemetry_payload_keys_are_exactly_the_contract(self, schema_dir: Path) -> None:
        """The strongest form of the check: compare against the schema itself.

        A future field added to the internal sample cannot slip into the message
        without this test noticing.
        """
        schema = load_json(schema_dir / "telemetry-raw.v1.json")
        allowed = set(schema["properties"])

        payload = decode_dict(
            encode(to_telemetry_raw(_anomalous_sample(), event_id=_EVENT_ID, ingest_time=_INGEST))
        )
        assert set(payload) <= allowed
        assert set(payload) >= set(schema["required"])

    def test_no_ground_truth_vocabulary_appears_in_the_payload(self) -> None:
        payload = decode_dict(
            encode(to_telemetry_raw(_anomalous_sample(), event_id=_EVENT_ID, ingest_time=_INGEST))
        )
        serialised = str(payload).lower()
        for forbidden in ("anomaly", "episode", "is_anomaly", "label", "ground_truth", "bearing"):
            assert forbidden not in serialised

    def test_no_value_reveals_the_episode(self) -> None:
        sample = _anomalous_sample()
        payload = decode_dict(
            encode(to_telemetry_raw(sample, event_id=_EVENT_ID, ingest_time=_INGEST))
        )
        values = {str(value) for value in payload.values()}
        assert sample.ground_truth.episode_id not in values
        assert "BEARING_WEAR" not in values

    def test_an_anomalous_and_a_normal_sample_are_indistinguishable_by_shape(self) -> None:
        """Only the values differ, never the structure.

        A message whose shape changed under anomaly would hand the model a
        trivially perfect feature.
        """
        anomalous = decode_dict(
            encode(to_telemetry_raw(_anomalous_sample(), event_id=_EVENT_ID, ingest_time=_INGEST))
        )
        normal = decode_dict(
            encode(to_telemetry_raw(_normal_sample(), event_id=_EVENT_ID, ingest_time=_INGEST))
        )
        assert set(anomalous) == set(normal)

    def test_the_telemetry_type_has_no_field_for_a_label(self) -> None:
        """The structural guarantee behind the tests above.

        Leakage is not prevented by remembering to strip a field; there is
        nowhere to put one.
        """
        from telemetry_core.schemas import TelemetryRaw

        fields = set(TelemetryRaw.__dataclass_fields__)
        assert not fields & {"ground_truth", "is_anomaly", "anomaly_type", "episode_id", "label"}


class TestTelemetryProjection:
    def test_it_carries_the_observable_fields(self) -> None:
        message = to_telemetry_raw(_anomalous_sample(), event_id=_EVENT_ID, ingest_time=_INGEST)
        assert message.machine_id == "M-014"
        assert message.line_id == "LINE-C"
        assert message.event_time == RUN_START
        assert message.ingest_time == _INGEST
        assert message.machine_state is MachineState.RUNNING
        assert message.readings == _READINGS
        assert message.firmware_version == "2.3.1"

    def test_partition_key_is_the_machine_id(self) -> None:
        message = to_telemetry_raw(_normal_sample(), event_id=_EVENT_ID, ingest_time=_INGEST)
        assert message.partition_key == "M-004"

    def test_it_validates_against_the_contract(self, schema_dir: Path) -> None:
        payload = decode_dict(
            encode(to_telemetry_raw(_anomalous_sample(), event_id=_EVENT_ID, ingest_time=_INGEST))
        )
        errors = _validation_errors(schema_dir, "telemetry-raw.v1.json", payload)
        assert not errors, errors


class TestLabelProjection:
    def test_normal_samples_produce_no_label_by_default(self) -> None:
        """Decision D-22: normal is the complement, not a published fact."""
        assert to_telemetry_label(_normal_sample(), event_id=_EVENT_ID, emitted_at=_INGEST) is None

    def test_normal_labels_can_be_enabled(self) -> None:
        label = to_telemetry_label(
            _normal_sample(),
            event_id=_EVENT_ID,
            emitted_at=_INGEST,
            emit_normal_labels=True,
        )
        assert label is not None
        assert label.is_anomaly is False

    def test_anomalous_samples_always_produce_a_label(self) -> None:
        label = to_telemetry_label(_anomalous_sample(), event_id=_EVENT_ID, emitted_at=_INGEST)
        assert label is not None
        assert label.is_anomaly is True
        assert label.anomaly_type is AnomalyType.BEARING_WEAR
        assert label.episode_id == "ep-20260302-M014-007"
        assert label.source is LabelSource.SIMULATOR

    def test_label_links_to_its_telemetry_event(self) -> None:
        """The join key for offline evaluation."""
        sample = _anomalous_sample()
        telemetry = to_telemetry_raw(sample, event_id=_EVENT_ID, ingest_time=_INGEST)
        label = to_telemetry_label(sample, event_id=_EVENT_ID, emitted_at=_INGEST)
        assert label is not None
        assert label.event_id == telemetry.event_id
        assert label.event_time == telemetry.event_time
        assert label.machine_id == telemetry.machine_id

    def test_emitted_at_is_later_than_event_time(self) -> None:
        """A label is always known after the fact; that lag is its nature."""
        label = to_telemetry_label(_anomalous_sample(), event_id=_EVENT_ID, emitted_at=_INGEST)
        assert label is not None
        assert label.emitted_at > label.event_time

    def test_it_validates_against_the_contract(self, schema_dir: Path) -> None:
        label = to_telemetry_label(_anomalous_sample(), event_id=_EVENT_ID, emitted_at=_INGEST)
        assert label is not None
        payload = decode_dict(encode(label))
        errors = _validation_errors(schema_dir, "telemetry-label.v1.json", payload)
        assert not errors, errors


class TestEventId:
    def test_it_matches_the_contract_pattern(self, schema_dir: Path) -> None:
        schema = load_json(schema_dir / "telemetry-raw.v1.json")
        import re

        pattern = re.compile(schema["properties"]["event_id"]["pattern"])
        generator = RngRegistry(3).stream("event_id", "M-001")
        for _ in range(20):
            assert pattern.match(new_event_id(generator))

    def test_it_is_reproducible_under_a_fixed_seed(self) -> None:
        """Random as the contract intends, yet replayable, which uuid4() is not."""
        first = new_event_id(RngRegistry(3).stream("event_id", "M-001"))
        second = new_event_id(RngRegistry(3).stream("event_id", "M-001"))
        assert first == second

    def test_successive_identifiers_differ(self) -> None:
        generator = RngRegistry(3).stream("event_id", "M-001")
        assert len({new_event_id(generator) for _ in range(50)}) == 50

    def test_different_seeds_give_different_identifiers(self) -> None:
        first = new_event_id(RngRegistry(3).stream("event_id", "M-001"))
        second = new_event_id(RngRegistry(4).stream("event_id", "M-001"))
        assert first != second


def test_readings_with_a_null_sensor_still_validate(schema_dir: Path) -> None:
    sample = GeneratedSample(
        machine_id="M-007",
        line_id="LINE-B",
        event_time=RUN_START,
        machine_state=MachineState.RUNNING,
        readings=SensorReadings(68.1, None, 4.98, 11.02, 1462.0),
        firmware_version=None,
        ground_truth=SampleGroundTruth.normal(),
    )
    payload = decode_dict(encode(to_telemetry_raw(sample, event_id=_EVENT_ID, ingest_time=_INGEST)))
    assert payload["readings"]["vibration_mm_s"] is None
    errors = _validation_errors(schema_dir, "telemetry-raw.v1.json", payload)
    assert not errors, errors


def test_projection_rejects_a_non_finite_reading() -> None:
    """A NaN means the generator produced garbage; publishing it would hide that."""
    sample = GeneratedSample(
        machine_id="M-007",
        line_id="LINE-B",
        event_time=RUN_START,
        machine_state=MachineState.RUNNING,
        readings=SensorReadings(float("nan"), 1.0, 1.0, 1.0, 1.0),
        firmware_version=None,
        ground_truth=SampleGroundTruth.normal(),
    )
    with pytest.raises(Exception, match="not JSON-serialisable"):
        encode(to_telemetry_raw(sample, event_id=_EVENT_ID, ingest_time=_INGEST))
