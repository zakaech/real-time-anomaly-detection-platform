"""Golden test locking the deterministic alert identifier.

This is the most important test in the package, and the reason deserves stating
plainly: if the derivation changes, **nothing breaks loudly**. No exception, no
failing request. Replayed Spark batches simply stop colliding with the rows
already in PostgreSQL, and duplicate alerts begin accumulating in the operator's
queue after the next deployment. By the time anyone notices, the cause is
several releases behind.

The only defence is a test that pins the exact output for a fixed input. If a
future change to the namespace, the field order, the separator or the timestamp
format is deliberate, this test must be updated in the same commit -- and that
commit is then the record of a decision that invalidates deduplication against
every alert already stored.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta, timezone

import pytest

from telemetry_core.errors import InvalidInstantError
from telemetry_core.ids import PROJECT_NAMESPACE, derive_alert_id

# Fixed reference case. Do not "fix" these values to make a change pass.
_MACHINE_ID = "M-014"
_WINDOW_START = datetime(2026, 9, 6, 14, 22, 10, tzinfo=UTC)
_MODEL_NAME = "isolation_forest"
_MODEL_VERSION = "1.3.0"

_EXPECTED_ALERT_ID = "2c891855-4e2f-51f1-87e0-9103b5091c7c"


def test_namespace_is_frozen() -> None:
    """The namespace must never be regenerated: it seeds every identifier."""
    assert str(PROJECT_NAMESPACE) == "89a66a48-8acb-427e-900e-8e9c63fe7e3e"


def test_alert_id_matches_golden_value() -> None:
    assert (
        derive_alert_id(
            machine_id=_MACHINE_ID,
            window_start=_WINDOW_START,
            model_name=_MODEL_NAME,
            model_version=_MODEL_VERSION,
        )
        == _EXPECTED_ALERT_ID
    )


def test_alert_id_is_stable_across_calls() -> None:
    """Reproducible within a process, unlike ``hash()`` which is salted."""
    first = derive_alert_id(
        machine_id=_MACHINE_ID,
        window_start=_WINDOW_START,
        model_name=_MODEL_NAME,
        model_version=_MODEL_VERSION,
    )
    second = derive_alert_id(
        machine_id=_MACHINE_ID,
        window_start=_WINDOW_START,
        model_name=_MODEL_NAME,
        model_version=_MODEL_VERSION,
    )
    assert first == second


def test_equivalent_instants_produce_the_same_id() -> None:
    """The same instant expressed in another timezone must not change the id.

    A replay that reconstructs timestamps in local time would otherwise produce
    fresh identifiers and defeat deduplication.
    """
    paris = timezone(timedelta(hours=2))
    same_instant_elsewhere = _WINDOW_START.astimezone(paris)
    assert same_instant_elsewhere != _WINDOW_START.replace(tzinfo=paris)

    assert (
        derive_alert_id(
            machine_id=_MACHINE_ID,
            window_start=same_instant_elsewhere,
            model_name=_MODEL_NAME,
            model_version=_MODEL_VERSION,
        )
        == _EXPECTED_ALERT_ID
    )


def test_sub_millisecond_precision_is_truncated_consistently() -> None:
    """Microseconds below the millisecond are truncated, not rounded.

    Two samples of the same window boundary must not land on different ids
    because of a microsecond of jitter.
    """
    jittered = _WINDOW_START.replace(microsecond=999)
    assert (
        derive_alert_id(
            machine_id=_MACHINE_ID,
            window_start=jittered,
            model_name=_MODEL_NAME,
            model_version=_MODEL_VERSION,
        )
        == _EXPECTED_ALERT_ID
    )


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("machine_id", "M-015"),
        ("model_name", "elliptic_envelope"),
        ("model_version", "1.3.1"),
    ],
)
def test_each_component_changes_the_id(field: str, value: str) -> None:
    """Every part of the key participates, so none can be silently dropped."""
    kwargs: dict[str, object] = {
        "machine_id": _MACHINE_ID,
        "window_start": _WINDOW_START,
        "model_name": _MODEL_NAME,
        "model_version": _MODEL_VERSION,
    }
    kwargs[field] = value
    assert derive_alert_id(**kwargs) != _EXPECTED_ALERT_ID  # type: ignore[arg-type]


def test_model_version_is_part_of_the_identity() -> None:
    """Rescoring a window with a new model is a distinct verdict.

    It therefore gets its own alert instead of overwriting the previous one
    (docs/04-streaming-semantics.md section 4.4).
    """
    upgraded = derive_alert_id(
        machine_id=_MACHINE_ID,
        window_start=_WINDOW_START,
        model_name=_MODEL_NAME,
        model_version="1.4.0",
    )
    assert upgraded != _EXPECTED_ALERT_ID


def test_different_windows_produce_different_ids() -> None:
    shifted = derive_alert_id(
        machine_id=_MACHINE_ID,
        window_start=_WINDOW_START + timedelta(seconds=10),
        model_name=_MODEL_NAME,
        model_version=_MODEL_VERSION,
    )
    assert shifted != _EXPECTED_ALERT_ID


def test_naive_window_start_is_rejected() -> None:
    with pytest.raises(InvalidInstantError):
        derive_alert_id(
            machine_id=_MACHINE_ID,
            window_start=datetime(2026, 9, 6, 14, 22, 10),  # noqa: DTZ001 - deliberate
            model_name=_MODEL_NAME,
            model_version=_MODEL_VERSION,
        )


def test_empty_component_is_rejected() -> None:
    with pytest.raises(ValueError, match="must not be empty"):
        derive_alert_id(
            machine_id="",
            window_start=_WINDOW_START,
            model_name=_MODEL_NAME,
            model_version=_MODEL_VERSION,
        )


def test_separator_in_component_is_rejected() -> None:
    """Guards against ambiguity: ("A|B", "C") and ("A", "B|C") must not collide."""
    with pytest.raises(ValueError, match="ambiguous"):
        derive_alert_id(
            machine_id="M-014|extra",
            window_start=_WINDOW_START,
            model_name=_MODEL_NAME,
            model_version=_MODEL_VERSION,
        )


def test_identifier_is_a_uuid_version_5() -> None:
    """The wire contract constrains the version nibble, so it must hold."""
    parts = _EXPECTED_ALERT_ID.split("-")
    assert parts[2][0] == "5"
    assert parts[3][0] in "89ab"
