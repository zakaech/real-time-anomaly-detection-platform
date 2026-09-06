"""Kafka publication, with the failure modes handled rather than assumed away.

Three distinct things can go wrong, and conflating them is how a producer loses
data quietly:

* **Serialisation** -- the message cannot be encoded. That is a bug in our own
  generator (a ``NaN`` reaching a float field), not a transport problem.
  Retrying is pointless: the same input fails identically for ever. It is
  counted and logged, and a run that fails repeatedly aborts rather than
  spinning silently.
* **Local queue full** -- ``produce()`` raises ``BufferError`` when librdkafka's
  buffer is saturated, which happens whenever the broker is slower than the
  generator. This is backpressure, felt inside the producer. The usual reflex,
  letting the exception propagate, drops the event; the correct response is to
  ``poll()`` so delivery callbacks drain the queue, then retry.
* **Delivery** -- asynchronous, reported through a callback long after
  ``produce()`` returned. Counting only successful ``produce()`` calls is the
  classic mistake: it counts messages *handed to* the client, not messages the
  broker acknowledged.

The client is used through :class:`MessageProducer` so unit tests inject a fake
and no test needs a broker.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Protocol

from telemetry_core.codec import encode, partition_key_bytes
from telemetry_core.config import KafkaSettings, KafkaTopics
from telemetry_core.errors import SchemaValidationError
from telemetry_core.logging import get_logger
from telemetry_core.schemas import TelemetryLabel, TelemetryRaw

__all__ = [
    "MessageProducer",
    "PublishStats",
    "TelemetryPublisher",
    "build_kafka_producer",
]

_LOGGER = get_logger(__name__)

#: How many times to poll-and-retry when the local queue is full before giving
#: up on a message. Generous: a full queue is a transient condition that clears
#: as soon as the broker acknowledges outstanding batches.
_BUFFER_RETRIES = 20
_BUFFER_POLL_SECONDS = 0.5

#: Consecutive serialisation failures tolerated before aborting the run. One is
#: a bad sample; a hundred in a row is a broken generator, and continuing would
#: produce an empty topic while the process looks healthy.
_MAX_CONSECUTIVE_ENCODING_FAILURES = 50


class MessageProducer(Protocol):
    """The subset of the Kafka producer this component depends on."""

    def produce(
        self,
        topic: str,
        *,
        key: bytes,
        value: bytes,
        on_delivery: Callable[[Any, Any], None],
    ) -> None: ...

    def poll(self, timeout: float) -> int: ...

    def flush(self, timeout: float) -> int: ...

    def __len__(self) -> int: ...


@dataclass
class PublishStats:
    """Counters reported at the end of a run. Every value is observed, not estimated."""

    telemetry_produced: int = 0
    labels_produced: int = 0
    delivered: int = 0
    delivery_failures: int = 0
    encoding_failures: int = 0
    buffer_full_events: int = 0
    dropped: int = 0

    def as_dict(self) -> dict[str, int]:
        return {
            "telemetry_produced": self.telemetry_produced,
            "labels_produced": self.labels_produced,
            "delivered": self.delivered,
            "delivery_failures": self.delivery_failures,
            "encoding_failures": self.encoding_failures,
            "buffer_full_events": self.buffer_full_events,
            "dropped": self.dropped,
        }


class PublishingAbortedError(RuntimeError):
    """Raised when the generator produces messages that cannot be encoded."""


class TelemetryPublisher:
    """Publishes telemetry and labels to their respective topics."""

    def __init__(
        self,
        *,
        producer: MessageProducer,
        topics: KafkaTopics,
        buffer_retries: int = _BUFFER_RETRIES,
        buffer_poll_seconds: float = _BUFFER_POLL_SECONDS,
    ) -> None:
        self._producer = producer
        self._topics = topics
        self._buffer_retries = buffer_retries
        self._buffer_poll_seconds = buffer_poll_seconds
        self._stats = PublishStats()
        self._consecutive_encoding_failures = 0

    @property
    def stats(self) -> PublishStats:
        return self._stats

    def publish_telemetry(self, message: TelemetryRaw) -> bool:
        """Publish to ``telemetry.raw``, keyed by ``machine_id``.

        The key is what makes every sample of a machine land in one partition,
        and therefore be consumed in production order. Publishing without a key
        would round-robin the samples and destroy the only ordering that has
        physical meaning (docs/03 section 2).
        """
        published = self._publish(
            topic=self._topics.telemetry_raw, key=message.partition_key, message=message
        )
        if published:
            self._stats.telemetry_produced += 1
        return published

    def publish_label(self, message: TelemetryLabel) -> bool:
        """Publish ground truth to ``telemetry.labels``.

        A separate topic, never consumed by the stream processor. That is what
        makes a leak into the feature pipeline impossible rather than merely
        discouraged.
        """
        published = self._publish(
            topic=self._topics.telemetry_labels, key=message.partition_key, message=message
        )
        if published:
            self._stats.labels_produced += 1
        return published

    def _publish(self, *, topic: str, key: str, message: TelemetryRaw | TelemetryLabel) -> bool:
        try:
            payload = encode(message)
            key_bytes = partition_key_bytes(key)
        except (SchemaValidationError, ValueError) as exc:
            self._stats.encoding_failures += 1
            self._stats.dropped += 1
            self._consecutive_encoding_failures += 1
            _LOGGER.error(
                "message_encoding_failed",
                topic=topic,
                machine_id=key,
                error=str(exc),
                consecutive=self._consecutive_encoding_failures,
            )
            if self._consecutive_encoding_failures >= _MAX_CONSECUTIVE_ENCODING_FAILURES:
                raise PublishingAbortedError(
                    f"{self._consecutive_encoding_failures} consecutive encoding failures; "
                    "the generator is producing invalid messages"
                ) from exc
            return False

        self._consecutive_encoding_failures = 0
        return self._produce_with_backpressure(topic=topic, key=key_bytes, value=payload)

    def _produce_with_backpressure(self, *, topic: str, key: bytes, value: bytes) -> bool:
        for attempt in range(self._buffer_retries):
            try:
                self._producer.produce(topic, key=key, value=value, on_delivery=self._on_delivery)
            except BufferError:
                # The local queue is full: the broker is slower than we are.
                # Polling lets delivery callbacks run and free space. Sleeping
                # instead would waste the same time without draining anything.
                self._stats.buffer_full_events += 1
                _LOGGER.warning(
                    "producer_queue_full",
                    topic=topic,
                    attempt=attempt + 1,
                    queued=len(self._producer),
                )
                self._producer.poll(self._buffer_poll_seconds)
                continue
            else:
                # Non-blocking poll so delivery callbacks are serviced on the
                # producing thread; without it they only run at flush time and
                # the delivered counter stays at zero until the very end.
                self._producer.poll(0.0)
                return True

        self._stats.dropped += 1
        _LOGGER.error("producer_queue_full_giving_up", topic=topic, retries=self._buffer_retries)
        return False

    def _on_delivery(self, error: Any, message: Any) -> None:
        if error is not None:
            self._stats.delivery_failures += 1
            _LOGGER.error("delivery_failed", error=str(error))
            return
        self._stats.delivered += 1

    def poll(self, timeout: float = 0.0) -> int:
        return self._producer.poll(timeout)

    def flush(self, timeout: float) -> int:
        """Block until outstanding messages are delivered.

        Returns the number still undelivered. A non-zero result means messages
        were handed to the client and never acknowledged, so the caller exits
        non-zero rather than reporting success.
        """
        return self._producer.flush(timeout)


def build_kafka_producer(settings: KafkaSettings, *, client_id: str) -> MessageProducer:
    """Build the real client with the settings agreed in docs/03 section 5.

    ``enable.idempotence`` deserves its caveat: it prevents a network retry from
    duplicating a message *within a producer session*. It guarantees nothing
    across a restart -- a new session gets a new producer id, and a message sent
    twice by two sessions is two messages. End-to-end deduplication is the job of
    ``event_id`` downstream, not of this flag.
    """
    from confluent_kafka import Producer  # imported here so tests need no client

    config: dict[str, object] = {
        "bootstrap.servers": settings.bootstrap_servers,
        "security.protocol": settings.security_protocol,
        "client.id": settings.client_id or client_id,
        "acks": "all",
        "enable.idempotence": True,
        "max.in.flight.requests.per.connection": 5,
        "retries": 2_147_483_647,
        "compression.type": "lz4",
        # 20 ms of added latency buys a real batch. At one sample per machine
        # per second the alternative is a stream of tiny requests.
        "linger.ms": 20,
        "batch.size": 65_536,
    }
    producer: MessageProducer = Producer(config)
    return producer
