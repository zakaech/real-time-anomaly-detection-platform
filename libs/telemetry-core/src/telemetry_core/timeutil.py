"""UTC instant handling.

Every timestamp crossing a component boundary in this platform is an ISO-8601
UTC instant with millisecond precision and a ``Z`` suffix::

    2026-09-06T14:23:07.412Z

That exact shape is part of the data contract (docs/02-data-contracts.md
section 3), which is why formatting lives here rather than being left to
whatever a serialization library happens to produce. ``datetime.isoformat()``
emits ``+00:00`` and a variable number of fractional digits; both are valid
ISO-8601 and neither matches the contract.

The module follows Postel's rule at its two edges:

* :func:`parse_instant` is liberal -- it accepts ``Z`` or a numeric offset, and
  any number of fractional digits, because we do not control every producer.
* :func:`format_instant` is strict -- exactly three fractional digits and ``Z``,
  because we do control what we emit.

Naive datetimes are rejected everywhere. A datetime without a timezone in a
distributed system is not a point in time, it is a string that looks like one.
"""

from __future__ import annotations

import re
from datetime import UTC, datetime

from telemetry_core.errors import InvalidInstantError

__all__ = [
    "MILLIS_PER_SECOND",
    "ensure_utc",
    "format_instant",
    "from_epoch_millis",
    "parse_instant",
    "to_epoch_millis",
    "utc_now",
]

MILLIS_PER_SECOND = 1000

# Accepts an optional 'T' or ' ' separator, optional fractional seconds of any
# length, and either 'Z' or a +HH:MM / -HH:MM offset.
_ISO_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(\.\d+)?(Z|z|[+-]\d{2}:?\d{2})$")


def utc_now() -> datetime:
    """Return the current instant as a timezone-aware UTC datetime."""
    return datetime.now(tz=UTC)


def ensure_utc(value: datetime) -> datetime:
    """Return ``value`` normalised to UTC, rejecting naive datetimes.

    Raises:
        InvalidInstantError: if ``value`` carries no timezone information.
    """
    if value.tzinfo is None or value.tzinfo.utcoffset(value) is None:
        raise InvalidInstantError(
            f"naive datetime is not an instant: {value!r}. "
            "Attach a timezone (datetime.now(tz=UTC)) at the point of creation."
        )
    return value.astimezone(UTC)


def format_instant(value: datetime) -> str:
    """Format ``value`` as ``YYYY-MM-DDTHH:MM:SS.mmmZ``.

    Sub-millisecond precision is truncated, not rounded: rounding could move an
    event across a window boundary, which would make the same input produce a
    different windowing result depending on formatting.
    """
    normalised = ensure_utc(value)
    millis = normalised.microsecond // MILLIS_PER_SECOND
    return f"{normalised.strftime('%Y-%m-%dT%H:%M:%S')}.{millis:03d}Z"


def parse_instant(value: str) -> datetime:
    """Parse an ISO-8601 instant into a timezone-aware UTC datetime.

    Raises:
        InvalidInstantError: if the value is not a well-formed offset-carrying
            ISO-8601 timestamp. An offset-less timestamp is rejected rather than
            assumed to be UTC: assuming is how a one-hour bug reaches production.
    """
    if not isinstance(value, str):
        raise InvalidInstantError(f"expected an ISO-8601 string, got {type(value).__name__}")

    if not _ISO_PATTERN.match(value):
        raise InvalidInstantError(
            f"not a valid ISO-8601 UTC instant: {value!r}. Expected e.g. 2026-09-06T14:23:07.412Z"
        )

    normalised = value.replace("z", "+00:00").replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalised)
    except ValueError as exc:  # pragma: no cover - guarded by the regex above
        raise InvalidInstantError(f"not a valid ISO-8601 UTC instant: {value!r}") from exc

    if parsed.tzinfo is None:
        raise InvalidInstantError(f"missing UTC offset: {value!r}")

    return parsed.astimezone(UTC)


def to_epoch_millis(value: datetime) -> int:
    """Return the instant as milliseconds since the Unix epoch."""
    return int(ensure_utc(value).timestamp() * MILLIS_PER_SECOND)


def from_epoch_millis(millis: int) -> datetime:
    """Build a UTC datetime from milliseconds since the Unix epoch."""
    return datetime.fromtimestamp(millis / MILLIS_PER_SECOND, tz=UTC)
