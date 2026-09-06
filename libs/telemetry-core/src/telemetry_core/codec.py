"""Byte-level encoding and decoding of Kafka messages.

Split of responsibility: :mod:`telemetry_core.schemas` owns the mapping between
a dataclass and a plain dictionary; this module owns everything about bytes --
UTF-8, JSON syntax, compactness, and turning any decoding failure into a
:class:`~telemetry_core.errors.SchemaValidationError` carrying enough context to
fill a dead letter envelope.

Encoding is deterministic without sorting keys: ``to_dict`` always builds its
dictionary in the same order, and Python preserves insertion order. That keeps
the on-wire field order identical to the contract document, which matters when
somebody reads a message with ``kafka-console-consumer`` at three in the morning.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any, Protocol, TypeVar, runtime_checkable

from telemetry_core.errors import SchemaValidationError

__all__ = [
    "JSON_CONTENT_TYPE",
    "decode",
    "decode_dict",
    "encode",
    "encode_dict",
    "partition_key_bytes",
]

JSON_CONTENT_TYPE = "application/json"

_ENCODING = "utf-8"
# Compact separators: no whitespace. At one sample per machine per second this
# is not about bandwidth, it is about not paying for bytes that carry nothing.
_SEPARATORS = (",", ":")


@runtime_checkable
class WireMessage(Protocol):
    """Structural type implemented by every message dataclass."""

    def to_dict(self) -> dict[str, Any]: ...


class _Decodable(Protocol):
    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> Any: ...


_T = TypeVar("_T", bound=_Decodable)


def encode_dict(payload: Mapping[str, Any]) -> bytes:
    """Serialise a plain mapping to compact UTF-8 JSON.

    ``allow_nan=False`` is deliberate: ``NaN`` and ``Infinity`` are not valid
    JSON, and a float arriving here as ``NaN`` means a feature computation
    produced garbage. Failing at the boundary beats publishing a payload that
    every strict parser downstream rejects -- or worse, that a lenient one
    accepts and silently scores.

    Raises:
        SchemaValidationError: if the payload contains a non-finite float or any
            value JSON cannot represent. The translation happens here rather
            than in :func:`encode` so that both entry points present the same
            error contract: the stream processor routes on the exception type,
            and a raw ``ValueError`` escaping would be classified as an
            infrastructure failure and retried for ever.
    """
    try:
        serialised = json.dumps(
            payload,
            separators=_SEPARATORS,
            ensure_ascii=False,
            allow_nan=False,
        )
    except (ValueError, TypeError) as exc:
        raise SchemaValidationError(f"payload is not JSON-serialisable: {exc}") from exc
    return serialised.encode(_ENCODING)


def encode(message: WireMessage) -> bytes:
    """Serialise a message dataclass to its wire representation.

    Raises:
        SchemaValidationError: if the message cannot be represented in JSON.
    """
    return encode_dict(message.to_dict())


def decode_dict(data: bytes | str) -> dict[str, Any]:
    """Parse raw bytes into a dictionary.

    Raises:
        SchemaValidationError: on invalid UTF-8, invalid JSON, or a top-level
            value that is not an object.
    """
    try:
        text = data.decode(_ENCODING) if isinstance(data, bytes) else data
    except UnicodeDecodeError as exc:
        raise SchemaValidationError(f"payload is not valid UTF-8: {exc}") from exc

    try:
        parsed = json.loads(text)
    except json.JSONDecodeError as exc:
        raise SchemaValidationError(
            f"payload is not valid JSON at line {exc.lineno} column {exc.colno}: {exc.msg}"
        ) from exc

    if not isinstance(parsed, dict):
        raise SchemaValidationError(f"payload must be a JSON object, got {type(parsed).__name__}")
    return parsed


def decode(message_type: type[_T], data: bytes | str) -> _T:
    """Parse raw bytes into a message dataclass.

    Unknown fields are ignored rather than rejected. That is the consumer half of
    the BACKWARD compatibility rule: a producer may add an optional field in a
    minor version, and existing consumers must keep working (docs/02 section 2).
    Rejecting unknown fields would turn every additive change into a coordinated
    redeployment of every consumer.
    """
    payload = decode_dict(data)
    decoded: _T = message_type.from_dict(payload)
    return decoded


def partition_key_bytes(key: str) -> bytes:
    """Encode a Kafka partition key.

    Centralised so that every producer hashes the same bytes for the same key.
    Kafka's default partitioner runs murmur2 over these bytes, so an encoding
    difference between two producers would scatter one machine's samples across
    partitions and quietly destroy per-machine ordering (docs/03 section 2).
    """
    if not key:
        raise ValueError("partition key must not be empty: a null key round-robins")
    return key.encode(_ENCODING)
