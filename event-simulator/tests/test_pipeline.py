"""The whole path, offline: generate, project, delay, publish.

Runs against a fake producer, so it covers the real code path without a broker
and without waiting. Every message produced by a full run is validated against
the committed JSON Schema -- which is a stronger statement than validating a
hand-written example.
"""

from __future__ import annotations

from datetime import timedelta
from itertools import pairwise
from pathlib import Path

from jsonschema import Draft202012Validator
from telemetry_core.config import KafkaTopics
from telemetry_core.timeutil import parse_instant

from conftest import RUN_START, FakeProducer, FakeWallClock, load_json, make_clock
from event_simulator.config.fleet import FleetConfig
from event_simulator.generation.fleet_runner import RunLimits
from event_simulator.generation.rng import RngRegistry
from event_simulator.publishing.pipeline import PipelineResult, SimulationPipeline
from event_simulator.publishing.producer import TelemetryPublisher


def _run(
    fleet: FleetConfig,
    topics: KafkaTopics,
    *,
    seed: int = 77,
    ticks: int = 120,
    clock: object | None = None,
) -> tuple[PipelineResult, FakeProducer]:
    producer = FakeProducer()
    pipeline = SimulationPipeline(
        fleet=fleet,
        clock=clock or make_clock(),  # type: ignore[arg-type]
        rng=RngRegistry(seed),
        publisher=TelemetryPublisher(producer=producer, topics=topics),
    )
    result = pipeline.run(RunLimits(max_events=ticks * len(fleet.machines)))
    return result, producer


class TestContractConformance:
    def test_every_telemetry_message_validates(
        self, fleet: FleetConfig, topics: KafkaTopics, schema_dir: Path
    ) -> None:
        _result, producer = _run(fleet, topics, ticks=200)
        schema = load_json(schema_dir / "telemetry-raw.v1.json")
        Draft202012Validator.check_schema(schema)
        validator = Draft202012Validator(schema)

        payloads = producer.payloads_for(topics.telemetry_raw)
        assert payloads
        for payload in payloads:
            errors = list(validator.iter_errors(payload))
            assert not errors, f"{payload}: {[error.message for error in errors]}"

    def test_every_label_message_validates(
        self, fleet: FleetConfig, topics: KafkaTopics, schema_dir: Path
    ) -> None:
        _result, producer = _run(fleet, topics, ticks=800)
        schema = load_json(schema_dir / "telemetry-label.v1.json")
        Draft202012Validator.check_schema(schema)
        validator = Draft202012Validator(schema)

        payloads = producer.payloads_for(topics.telemetry_labels)
        assert payloads, "the run produced no label at all"
        for payload in payloads:
            errors = list(validator.iter_errors(payload))
            assert not errors, f"{payload}: {[error.message for error in errors]}"


class TestStreamSeparation:
    def test_labels_are_produced_only_for_anomalous_samples(
        self, fleet: FleetConfig, topics: KafkaTopics
    ) -> None:
        result, producer = _run(fleet, topics, ticks=800)
        labels = producer.payloads_for(topics.telemetry_labels)
        assert labels
        assert all(payload["is_anomaly"] is True for payload in labels)
        assert len(labels) == result.anomalous_samples

    def test_telemetry_is_produced_for_every_published_sample(
        self, fleet: FleetConfig, topics: KafkaTopics
    ) -> None:
        result, producer = _run(fleet, topics, ticks=120)
        assert len(producer.payloads_for(topics.telemetry_raw)) == result.telemetry_published

    def test_telemetry_far_outnumbers_labels(self, fleet: FleetConfig, topics: KafkaTopics) -> None:
        """Decision D-22: labels stay proportional to the interesting signal."""
        _result, producer = _run(fleet, topics, ticks=800)
        assert len(producer.payloads_for(topics.telemetry_labels)) < 0.25 * len(
            producer.payloads_for(topics.telemetry_raw)
        )

    def test_no_telemetry_payload_carries_ground_truth(
        self, fleet: FleetConfig, topics: KafkaTopics, schema_dir: Path
    ) -> None:
        """The end-to-end no-leak assertion, on a full run rather than a fixture."""
        _result, producer = _run(fleet, topics, ticks=800)
        allowed = set(load_json(schema_dir / "telemetry-raw.v1.json")["properties"])

        for payload in producer.payloads_for(topics.telemetry_raw):
            assert set(payload) <= allowed
            serialised = str(payload).lower()
            assert "episode" not in serialised
            assert "anomaly" not in serialised

    def test_a_label_can_be_joined_back_to_its_event(
        self, fleet: FleetConfig, topics: KafkaTopics
    ) -> None:
        """Evaluation joins the two streams on event_id; the link must hold."""
        _result, producer = _run(fleet, topics, ticks=800)
        telemetry_ids = {
            payload["event_id"] for payload in producer.payloads_for(topics.telemetry_raw)
        }
        label_ids = {
            payload["event_id"] for payload in producer.payloads_for(topics.telemetry_labels)
        }
        assert label_ids
        assert label_ids <= telemetry_ids


class TestKafkaKeys:
    def test_every_message_is_keyed_by_its_machine(
        self, fleet: FleetConfig, topics: KafkaTopics
    ) -> None:
        _result, producer = _run(fleet, topics, ticks=60)
        for topic, key, value in producer.messages:
            payload = load_json_bytes(value)
            assert key.decode("utf-8") == payload["machine_id"], topic


def load_json_bytes(value: bytes) -> dict[str, object]:
    import json

    payload: dict[str, object] = json.loads(value.decode("utf-8"))
    return payload


class TestDeterminism:
    def test_two_runs_with_the_same_seed_are_byte_identical(
        self, fleet: FleetConfig, topics: KafkaTopics
    ) -> None:
        _first_result, first = _run(fleet, topics, seed=123, ticks=80)
        _second_result, second = _run(fleet, topics, seed=123, ticks=80)
        assert first.messages == second.messages

    def test_a_different_seed_produces_different_bytes(
        self, fleet: FleetConfig, topics: KafkaTopics
    ) -> None:
        _first_result, first = _run(fleet, topics, seed=123, ticks=80)
        _second_result, second = _run(fleet, topics, seed=124, ticks=80)
        assert first.messages != second.messages


class TestRunModes:
    def test_realtime_mode_paces_one_tick_per_interval(
        self, fleet: FleetConfig, topics: KafkaTopics
    ) -> None:
        wall = FakeWallClock(RUN_START)
        clock = make_clock(speed_factor=1.0, wall_clock=wall)
        _result, _producer = _run(fleet, topics, ticks=10, clock=clock)

        assert wall.slept, "realtime mode must pace the loop"
        assert all(duration <= 1.0 for duration in wall.slept)

    def test_replay_mode_never_sleeps(self, fleet: FleetConfig, topics: KafkaTopics) -> None:
        wall = FakeWallClock(RUN_START)
        clock = make_clock(start=RUN_START - timedelta(days=7), speed_factor=None, wall_clock=wall)
        _result, _producer = _run(fleet, topics, ticks=200, clock=clock)
        assert wall.slept == []

    def test_replay_event_times_are_historical(
        self, fleet: FleetConfig, topics: KafkaTopics
    ) -> None:
        """A backfill carries a large, honest source lag: the samples are old,
        the publication is now."""
        wall = FakeWallClock(RUN_START)
        replay_start = RUN_START - timedelta(days=7)
        clock = make_clock(start=replay_start, speed_factor=None, wall_clock=wall)
        _result, producer = _run(fleet, topics, ticks=60, clock=clock)

        for payload in producer.payloads_for(topics.telemetry_raw):
            event_time = parse_instant(payload["event_time"])
            ingest_time = parse_instant(payload["ingest_time"])
            assert event_time < RUN_START
            assert ingest_time > event_time
            assert (ingest_time - event_time) > timedelta(days=6)

    def test_replay_advances_by_the_tick_interval(
        self, fleet: FleetConfig, topics: KafkaTopics
    ) -> None:
        replay_start = RUN_START - timedelta(hours=2)
        clock = make_clock(start=replay_start, speed_factor=None)
        _result, producer = _run(fleet, topics, ticks=30, clock=clock)

        times = sorted(
            {
                parse_instant(payload["event_time"])
                for payload in producer.payloads_for(topics.telemetry_raw)
            }
        )
        deltas = {(later - earlier).total_seconds() for earlier, later in pairwise(times)}
        assert deltas == {1.0}


class TestLimits:
    def test_max_events_is_respected(self, fleet: FleetConfig, topics: KafkaTopics) -> None:
        producer = FakeProducer()
        pipeline = SimulationPipeline(
            fleet=fleet,
            clock=make_clock(),
            rng=RngRegistry(5),
            publisher=TelemetryPublisher(producer=producer, topics=topics),
        )
        result = pipeline.run(RunLimits(max_events=37))
        assert result.samples_generated == 37

    def test_duration_is_respected(self, fleet: FleetConfig, topics: KafkaTopics) -> None:
        producer = FakeProducer()
        pipeline = SimulationPipeline(
            fleet=fleet,
            clock=make_clock(),
            rng=RngRegistry(5),
            publisher=TelemetryPublisher(producer=producer, topics=topics),
        )
        result = pipeline.run(RunLimits(duration=timedelta(seconds=30)))
        assert result.ticks == 30
        assert result.samples_generated == 30 * len(fleet.machines)

    def test_a_stop_request_ends_the_run(self, fleet: FleetConfig, topics: KafkaTopics) -> None:
        producer = FakeProducer()
        pipeline = SimulationPipeline(
            fleet=fleet,
            clock=make_clock(),
            rng=RngRegistry(5),
            publisher=TelemetryPublisher(producer=producer, topics=topics),
        )
        ticks_seen = 0

        def should_stop() -> bool:
            nonlocal ticks_seen
            ticks_seen += 1
            return ticks_seen > 5

        result = pipeline.run(RunLimits(), should_stop=should_stop)
        assert result.ticks == 5


def test_result_reports_only_observed_values(fleet: FleetConfig, topics: KafkaTopics) -> None:
    """Nothing in the summary is estimated; every field is a count."""
    result, producer = _run(fleet, topics, ticks=100)
    summary = result.as_dict()

    assert summary["samples_generated"] == 100 * len(fleet.machines)
    assert summary["telemetry_published"] == len(producer.payloads_for(topics.telemetry_raw))
    assert summary["labels_published"] == len(producer.payloads_for(topics.telemetry_labels))
    assert summary["clamped_readings"] == 0
