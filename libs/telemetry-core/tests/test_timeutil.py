"""The instant format is part of the data contract, so it is tested as one."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta, timezone

import pytest

from telemetry_core.errors import InvalidInstantError
from telemetry_core.timeutil import (
    ensure_utc,
    format_instant,
    from_epoch_millis,
    parse_instant,
    to_epoch_millis,
    utc_now,
)


def test_format_uses_exactly_three_fractional_digits_and_z() -> None:
    value = datetime(2026, 9, 6, 14, 23, 7, 412000, tzinfo=UTC)
    assert format_instant(value) == "2026-09-06T14:23:07.412Z"


def test_format_pads_fractional_digits() -> None:
    """A zero-millisecond instant must still carry three digits.

    Variable-width fractions would make two encodings of the same instant differ
    byte for byte, which would break the deterministic alert identifier.
    """
    value = datetime(2026, 9, 6, 14, 23, 7, 0, tzinfo=UTC)
    assert format_instant(value) == "2026-09-06T14:23:07.000Z"

    value = datetime(2026, 9, 6, 14, 23, 7, 5000, tzinfo=UTC)
    assert format_instant(value) == "2026-09-06T14:23:07.005Z"


def test_format_truncates_rather_than_rounds() -> None:
    """Rounding could move an event across a window boundary."""
    value = datetime(2026, 9, 6, 14, 23, 7, 412999, tzinfo=UTC)
    assert format_instant(value) == "2026-09-06T14:23:07.412Z"


def test_format_normalises_other_timezones_to_utc() -> None:
    paris = timezone(timedelta(hours=2))
    value = datetime(2026, 9, 6, 16, 23, 7, 412000, tzinfo=paris)
    assert format_instant(value) == "2026-09-06T14:23:07.412Z"


def test_isoformat_would_not_satisfy_the_contract() -> None:
    """Documents precisely why this module exists rather than using isoformat()."""
    value = datetime(2026, 9, 6, 14, 23, 7, 412000, tzinfo=UTC)
    assert value.isoformat() == "2026-09-06T14:23:07.412000+00:00"
    assert format_instant(value) != value.isoformat()


def test_parse_accepts_z_suffix() -> None:
    parsed = parse_instant("2026-09-06T14:23:07.412Z")
    assert parsed == datetime(2026, 9, 6, 14, 23, 7, 412000, tzinfo=UTC)


def test_parse_accepts_numeric_offset_and_normalises() -> None:
    """Liberal on input: we do not control every producer."""
    parsed = parse_instant("2026-09-06T16:23:07.412+02:00")
    assert parsed == datetime(2026, 9, 6, 14, 23, 7, 412000, tzinfo=UTC)
    assert parsed.tzinfo is UTC


def test_parse_accepts_missing_fractional_seconds() -> None:
    assert parse_instant("2026-09-06T14:23:07Z") == datetime(2026, 9, 6, 14, 23, 7, tzinfo=UTC)


def test_parse_accepts_microsecond_precision() -> None:
    parsed = parse_instant("2026-09-06T14:23:07.412999Z")
    assert parsed.microsecond == 412999


@pytest.mark.parametrize(
    "value",
    [
        "2026-09-06T14:23:07.412",  # no offset: would require assuming UTC
        "2026-09-06 14:23:07",  # no offset
        "06/09/2026 14:23:07Z",  # not ISO-8601
        "not a timestamp",
        "",
    ],
)
def test_parse_rejects_malformed_values(value: str) -> None:
    with pytest.raises(InvalidInstantError):
        parse_instant(value)


def test_parse_rejects_offsetless_timestamp_rather_than_assuming_utc() -> None:
    """Assuming a timezone is how a one-hour bug reaches production."""
    with pytest.raises(InvalidInstantError, match="ISO-8601"):
        parse_instant("2026-09-06T14:23:07.412")


def test_naive_datetime_is_rejected_everywhere() -> None:
    naive = datetime(2026, 9, 6, 14, 23, 7)  # noqa: DTZ001 - deliberate
    with pytest.raises(InvalidInstantError, match="naive"):
        ensure_utc(naive)
    with pytest.raises(InvalidInstantError):
        format_instant(naive)
    with pytest.raises(InvalidInstantError):
        to_epoch_millis(naive)


def test_roundtrip_at_millisecond_resolution() -> None:
    value = datetime(2026, 9, 6, 14, 23, 7, 412000, tzinfo=UTC)
    assert parse_instant(format_instant(value)) == value


def test_epoch_millis_roundtrip() -> None:
    value = datetime(2026, 9, 6, 14, 23, 7, 412000, tzinfo=UTC)
    assert from_epoch_millis(to_epoch_millis(value)) == value


def test_epoch_millis_is_an_integer_count() -> None:
    epoch = datetime(1970, 1, 1, tzinfo=UTC)
    assert to_epoch_millis(epoch) == 0
    assert to_epoch_millis(epoch + timedelta(milliseconds=1500)) == 1500


def test_utc_now_is_timezone_aware() -> None:
    now = utc_now()
    assert now.tzinfo is not None
    assert now.utcoffset() == timedelta(0)
