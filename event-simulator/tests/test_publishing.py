"""Publication: routing, Kafka keys, and the three failure modes."""

from __future__ import annotations

from datetime import timedelta

import pytest
from telemetry_core.config import KafkaTopics
from telemetry_core.enums import AnomalyType, LabelSource, MachineState
from telemetry_core.schemas import SensorReadings, TelemetryLabel, TelemetryRaw

from conftest import RUN_START, FakeProducer
from event_simulator.publishing.producer import (
    PublishingAbortedError,
    TelemetryPublisher,
)

_READINGS = SensorReadings(72.48, 2.81, 5.12, 12.44, 1478.2)


def _telemetry(machine_id: str = "M-014") -> TelemetryRaw:
    return TelemetryRaw(
        event_id="5c0a3f8e-6f5b-4a0e-9d1c-8f2b7a1e4c93",
        machine_id=machine_id,
        line_id="LINE-C",
        event_time=RUN_START,
        ingest_time=RUN_START + timedelta(milliseconds=120),
        machine_state=MachineState.RUNNING,
        readings=_READINGS,
    )


def _label(machine_id: str = "M-014") -> TelemetryLabel:
    return TelemetryLabel(
        machine_id=machine_id,
        event_time=RUN_START,
        is_anomaly=True,
        source=LabelSource.SIMULATOR,
        emitted_at=RUN_START + timedelta(milliseconds=120),
        anomaly_type=AnomalyType.OVERHEAT,
        episode_id="ep-20260302-M014-001",
    )


class TestRouting:
    def test_telemetry_goes_to_the_telemetry_topic(self, topics: KafkaTopics) -> None:
        producer = FakeProducer()
        publisher = TelemetryPublisher(producer=producer, topics=topics)

        assert publisher.publish_telemetry(_telemetry()) is True
        assert [topic for topic, _k, _v in producer.messages] == [topics.telemetry_raw]

    def test_labels_go_to_the_label_topic(self, topics: KafkaTopics) -> None:
        """A separate topic is what makes a leak into the feature pipeline impossible."""
        producer = FakeProducer()
        publisher = TelemetryPublisher(producer=producer, topics=topics)

        assert publisher.publish_label(_label()) is True
        assert [topic for topic, _k, _v in producer.messages] == [topics.telemetry_labels]

    def test_the_two_streams_never_share_a_topic(self, topics: KafkaTopics) -> None:
        producer = FakeProducer()
        publisher = TelemetryPublisher(producer=producer, topics=topics)
        publisher.publish_telemetry(_telemetry())
        publisher.publish_label(_label())

        assert len(producer.payloads_for(topics.telemetry_raw)) == 1
        assert len(producer.payloads_for(topics.telemetry_labels)) == 1
        assert topics.telemetry_raw != topics.telemetry_labels


class TestPartitionKey:
    def test_telemetry_key_is_the_machine_id(self, topics: KafkaTopics) -> None:
        """The key is what keeps a machine's samples in one partition, in order.

        Publishing without one would round-robin them and destroy the only
        ordering that has physical meaning.
        """
        producer = FakeProducer()
        publisher = TelemetryPublisher(producer=producer, topics=topics)
        publisher.publish_telemetry(_telemetry("M-007"))

        assert producer.keys_for(topics.telemetry_raw) == ["M-007"]

    def test_label_key_is_the_machine_id(self, topics: KafkaTopics) -> None:
        producer = FakeProducer()
        publisher = TelemetryPublisher(producer=producer, topics=topics)
        publisher.publish_label(_label("M-003"))

        assert producer.keys_for(topics.telemetry_labels) == ["M-003"]

    def test_the_key_is_utf8_bytes(self, topics: KafkaTopics) -> None:
        producer = FakeProducer()
        publisher = TelemetryPublisher(producer=producer, topics=topics)
        publisher.publish_telemetry(_telemetry("M-014"))

        _topic, key, _value = producer.messages[0]
        assert key == b"M-014"

    def test_the_same_machine_always_yields_the_same_key(self, topics: KafkaTopics) -> None:
        producer = FakeProducer()
        publisher = TelemetryPublisher(producer=producer, topics=topics)
        for _ in range(5):
            publisher.publish_telemetry(_telemetry("M-002"))

        assert set(producer.keys_for(topics.telemetry_raw)) == {"M-002"}


class TestBackpressure:
    def test_a_full_queue_is_retried_rather_than_dropped(self, topics: KafkaTopics) -> None:
        """BufferError is backpressure, felt inside the producer.

        Letting it propagate would drop the event. The correct response is to
        poll so delivery callbacks drain the queue, then retry.
        """
        producer = FakeProducer(buffer_failures=3)
        publisher = TelemetryPublisher(producer=producer, topics=topics)

        assert publisher.publish_telemetry(_telemetry()) is True
        assert publisher.stats.buffer_full_events == 3
        assert publisher.stats.dropped == 0
        assert len(producer.messages) == 1

    def test_polling_happens_between_retries(self, topics: KafkaTopics) -> None:
        """Polling is what frees space; sleeping would waste the same time."""
        producer = FakeProducer(buffer_failures=2)
        publisher = TelemetryPublisher(producer=producer, topics=topics)
        publisher.publish_telemetry(_telemetry())

        assert producer.poll_calls >= 2

    def test_a_permanently_full_queue_eventually_gives_up(self, topics: KafkaTopics) -> None:
        producer = FakeProducer(buffer_failures=1_000)
        publisher = TelemetryPublisher(producer=producer, topics=topics, buffer_retries=4)

        assert publisher.publish_telemetry(_telemetry()) is False
        assert publisher.stats.dropped == 1
        assert publisher.stats.telemetry_produced == 0


class TestEncodingFailures:
    def test_a_non_finite_reading_is_counted_not_published(self, topics: KafkaTopics) -> None:
        producer = FakeProducer()
        publisher = TelemetryPublisher(producer=producer, topics=topics)

        broken = TelemetryRaw(
            event_id="5c0a3f8e-6f5b-4a0e-9d1c-8f2b7a1e4c93",
            machine_id="M-014",
            line_id="LINE-C",
            event_time=RUN_START,
            ingest_time=RUN_START,
            machine_state=MachineState.RUNNING,
            readings=SensorReadings(float("nan"), 1.0, 1.0, 1.0, 1.0),
        )
        assert publisher.publish_telemetry(broken) is False
        assert publisher.stats.encoding_failures == 1
        assert publisher.stats.dropped == 1
        assert producer.messages == []

    def test_a_single_failure_does_not_stop_the_run(self, topics: KafkaTopics) -> None:
        producer = FakeProducer()
        publisher = TelemetryPublisher(producer=producer, topics=topics)
        broken = TelemetryRaw(
            event_id="5c0a3f8e-6f5b-4a0e-9d1c-8f2b7a1e4c93",
            machine_id="M-014",
            line_id="LINE-C",
            event_time=RUN_START,
            ingest_time=RUN_START,
            machine_state=MachineState.RUNNING,
            readings=SensorReadings(float("inf"), 1.0, 1.0, 1.0, 1.0),
        )
        publisher.publish_telemetry(broken)
        assert publisher.publish_telemetry(_telemetry()) is True

    def test_a_flood_of_failures_aborts(self, topics: KafkaTopics) -> None:
        """A broken generator must not produce an empty topic while looking healthy."""
        producer = FakeProducer()
        publisher = TelemetryPublisher(producer=producer, topics=topics)
        broken = TelemetryRaw(
            event_id="5c0a3f8e-6f5b-4a0e-9d1c-8f2b7a1e4c93",
            machine_id="M-014",
            line_id="LINE-C",
            event_time=RUN_START,
            ingest_time=RUN_START,
            machine_state=MachineState.RUNNING,
            readings=SensorReadings(float("nan"), 1.0, 1.0, 1.0, 1.0),
        )
        with pytest.raises(PublishingAbortedError, match="consecutive encoding failures"):
            for _ in range(200):
                publisher.publish_telemetry(broken)


class TestStats:
    def test_delivery_is_counted_from_the_callback(self, topics: KafkaTopics) -> None:
        """Counting successful produce() calls would count messages handed to the
        client, not messages the broker acknowledged."""
        producer = FakeProducer()
        publisher = TelemetryPublisher(producer=producer, topics=topics)
        publisher.publish_telemetry(_telemetry())
        publisher.publish_label(_label())

        stats = publisher.stats
        assert stats.telemetry_produced == 1
        assert stats.labels_produced == 1
        assert stats.delivered == 2
        assert stats.delivery_failures == 0

    def test_stats_serialise_to_plain_integers(self, topics: KafkaTopics) -> None:
        publisher = TelemetryPublisher(producer=FakeProducer(), topics=topics)
        assert all(isinstance(value, int) for value in publisher.stats.as_dict().values())
