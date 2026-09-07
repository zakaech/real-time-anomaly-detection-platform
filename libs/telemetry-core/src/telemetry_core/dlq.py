"""Dead letter envelope (ADR-002).

Until now this shape existed only as prose in docs/03 section 7, so nothing
enforced it. It is a platform contract like the others and gets the same
treatment: a JSON Schema of record, a frozen dataclass that conforms to it, and
a conformance test.

A new module rather than an addition to :mod:`telemetry_core.schemas`: the
message is not telemetry, it is an operational envelope wrapping telemetry, and
keeping it separate means adding it changes nothing that already works.

**What belongs here and what does not.** Only *data* errors: a payload that is
malformed, violates the schema, or announces an unsupported major version. A
broker outage or a lost executor is an infrastructure error, handled by retry
and by the checkpoint. Sending those here would discard perfectly valid messages
during an incident -- precisely when losing them costs the most.
"""

from __future__ import annotations

import base64
import binascii
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from telemetry_core.enums import DlqReason
from telemetry_core.errors import SchemaValidationError
from telemetry_core.schemas import SCHEMA_VERSION
from telemetry_core.timeutil import format_instant, parse_instant

__all__ = ["DlqEnvelope"]


@dataclass(frozen=True, slots=True)
class DlqEnvelope:
    """A rejected message, with everything needed to diagnose and replay it."""

    dlq_reason: DlqReason
    dlq_detail: str
    source_topic: str
    source_partition: int
    source_offset: int
    failed_at: datetime
    processor_version: str
    #: The ORIGINAL bytes, base64-encoded and otherwise untouched. Replay after
    #: a fix is only possible because nothing was normalised on the way in.
    raw_payload: str
    raw_key: str | None = None
    schema_version: str = SCHEMA_VERSION

    def __post_init__(self) -> None:
        if not self.dlq_detail:
            raise SchemaValidationError(
                "dlq_detail must not be empty: an unactionable rejection is a log "
                "line, not a dead letter",
                field="dlq_detail",
            )
        if self.source_partition < 0 or self.source_offset < 0:
            raise SchemaValidationError(
                "source_partition and source_offset locate the message for replay "
                "and cannot be negative",
                field="source_offset",
            )

    @classmethod
    def wrap(
        cls,
        *,
        payload: bytes | None,
        key: bytes | None,
        reason: DlqReason,
        detail: str,
        topic: str,
        partition: int,
        offset: int,
        failed_at: datetime,
        processor_version: str,
    ) -> DlqEnvelope:
        """Build an envelope around raw Kafka bytes.

        A ``None`` payload is encoded as an empty string rather than rejected: a
        tombstone that reached the parser is itself worth recording.
        """
        return cls(
            dlq_reason=reason,
            dlq_detail=detail,
            source_topic=topic,
            source_partition=partition,
            source_offset=offset,
            failed_at=failed_at,
            processor_version=processor_version,
            raw_payload=base64.b64encode(payload or b"").decode("ascii"),
            raw_key=None if key is None else base64.b64encode(key).decode("ascii"),
        )

    def decoded_payload(self) -> bytes:
        """Return the original bytes, for replay or inspection.

        Line breaks are stripped before decoding. Spark's ``base64`` function
        emits MIME-chunked output -- 76-character lines separated by newlines --
        for payloads beyond that length, which is still valid base64 but is
        rejected by a strict decoder. Producers that do not chunk are unaffected.
        """
        compact = "".join(self.raw_payload.split())
        try:
            return base64.b64decode(compact, validate=True)
        except (binascii.Error, ValueError) as exc:
            raise SchemaValidationError(
                f"raw_payload is not valid base64: {exc}", field="raw_payload"
            ) from exc

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "dlq_reason": self.dlq_reason.value,
            "dlq_detail": self.dlq_detail,
            "source_topic": self.source_topic,
            "source_partition": self.source_partition,
            "source_offset": self.source_offset,
            "failed_at": format_instant(self.failed_at),
            "processor_version": self.processor_version,
            "raw_payload": self.raw_payload,
            "raw_key": self.raw_key,
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> DlqEnvelope:
        if not isinstance(payload, Mapping):
            raise SchemaValidationError(f"$: expected an object, got {type(payload).__name__}")

        def required_str(key: str) -> str:
            value = payload.get(key)
            if not isinstance(value, str) or not value:
                raise SchemaValidationError(
                    f"{key}: expected a non-empty string, got {value!r}", field=key
                )
            return value

        def required_int(key: str) -> int:
            value = payload.get(key)
            if isinstance(value, bool) or not isinstance(value, int):
                raise SchemaValidationError(
                    f"{key}: expected an integer, got {type(value).__name__}", field=key
                )
            return value

        raw_key = payload.get("raw_key")
        if raw_key is not None and not isinstance(raw_key, str):
            raise SchemaValidationError("raw_key: expected a string or null", field="raw_key")

        return cls(
            schema_version=required_str("schema_version"),
            dlq_reason=DlqReason.parse_strict(payload.get("dlq_reason"), field="dlq_reason"),
            dlq_detail=required_str("dlq_detail"),
            source_topic=required_str("source_topic"),
            source_partition=required_int("source_partition"),
            source_offset=required_int("source_offset"),
            failed_at=parse_instant(required_str("failed_at")),
            processor_version=required_str("processor_version"),
            raw_payload=payload.get("raw_payload") or "",
            raw_key=raw_key,
        )
