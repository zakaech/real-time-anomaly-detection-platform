"""Structured logging emits one JSON object per line, with the shared envelope."""

from __future__ import annotations

import json
from typing import Any

import pytest

from telemetry_core.logging import (
    TRACE_ID_KEY,
    bind,
    bind_trace_id,
    clear_context,
    configure_logging,
    get_logger,
    new_trace_id,
)
from telemetry_core.timeutil import parse_instant


@pytest.fixture(autouse=True)
def configured() -> Any:
    configure_logging(service="test-service", version="9.9.9", level="INFO")
    clear_context()
    yield
    clear_context()


def _emit_and_capture(capsys: pytest.CaptureFixture[str], **fields: Any) -> dict[str, Any]:
    get_logger("test").info("alert_published", **fields)
    captured = capsys.readouterr().out.strip().splitlines()
    assert len(captured) == 1, f"expected exactly one line, got {captured}"
    payload: dict[str, Any] = json.loads(captured[0])
    return payload


def test_output_is_a_single_json_object_per_line(capsys: pytest.CaptureFixture[str]) -> None:
    payload = _emit_and_capture(capsys, machine_id="M-014")
    assert payload["event"] == "alert_published"


def test_envelope_carries_service_and_version(capsys: pytest.CaptureFixture[str]) -> None:
    """Without the version, correlating a behaviour change with a deployment is guesswork."""
    payload = _emit_and_capture(capsys)
    assert payload["service"] == "test-service"
    assert payload["version"] == "9.9.9"
    assert payload["level"] == "info"


def test_timestamp_uses_the_project_instant_format(capsys: pytest.CaptureFixture[str]) -> None:
    """Log timestamps and message timestamps must be comparable without normalisation."""
    payload = _emit_and_capture(capsys)
    assert payload["timestamp"].endswith("Z")
    parsed = parse_instant(payload["timestamp"])
    assert parsed.tzinfo is not None


def test_structured_fields_are_top_level(capsys: pytest.CaptureFixture[str]) -> None:
    """A machine_id field is queryable; an interpolated sentence is not."""
    payload = _emit_and_capture(capsys, machine_id="M-014", anomaly_score=0.9962)
    assert payload["machine_id"] == "M-014"
    assert payload["anomaly_score"] == 0.9962


def test_trace_id_is_bound_to_the_context(capsys: pytest.CaptureFixture[str]) -> None:
    """Bound once per message, then present on every later line for free."""
    trace_id = bind_trace_id("abc123def4567890")
    payload = _emit_and_capture(capsys)
    assert payload[TRACE_ID_KEY] == trace_id


def test_trace_id_is_generated_when_absent() -> None:
    trace_id = bind_trace_id()
    assert len(trace_id) == 16
    int(trace_id, 16)


def test_new_trace_ids_differ() -> None:
    assert new_trace_id() != new_trace_id()


def test_clearing_context_drops_the_trace_id(capsys: pytest.CaptureFixture[str]) -> None:
    """A stale trace_id is worse than none: it attributes one message's logs to another."""
    bind_trace_id("abc123def4567890")
    clear_context()
    payload = _emit_and_capture(capsys)
    assert TRACE_ID_KEY not in payload


def test_arbitrary_context_fields_are_bound(capsys: pytest.CaptureFixture[str]) -> None:
    bind(machine_id="M-007", line_id="LINE-B")
    payload = _emit_and_capture(capsys)
    assert payload["machine_id"] == "M-007"
    assert payload["line_id"] == "LINE-B"


def test_level_filtering_is_applied(capsys: pytest.CaptureFixture[str]) -> None:
    configure_logging(service="test-service", version="9.9.9", level="WARNING")
    get_logger("test").info("suppressed")
    assert capsys.readouterr().out.strip() == ""

    get_logger("test").warning("emitted")
    assert json.loads(capsys.readouterr().out.strip())["event"] == "emitted"
