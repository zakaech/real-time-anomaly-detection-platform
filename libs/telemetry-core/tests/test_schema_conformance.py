"""Conformance between the code and the contract of record.

``contracts/json-schema/*.json`` is authoritative; the dataclasses conform to it.
This module enforces that in both directions, so the two cannot drift:

* every committed example validates against its schema (the contract is
  self-consistent);
* every example decodes into a dataclass, and what that dataclass re-encodes
  validates against the same schema (the code round-trips through the contract).

The second direction is the one that catches real regressions. A renamed field
or a changed timestamp format would still produce valid-looking JSON; only
re-validating the *output* proves the platform still speaks its own contract.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
from jsonschema import Draft202012Validator

from conftest import load_json
from telemetry_core.codec import decode_dict, encode
from telemetry_core.schemas import Alert, ScoredEvent, TelemetryLabel, TelemetryRaw

# example file -> (schema file, dataclass)
_CASES: dict[str, tuple[str, type[Any]]] = {
    "telemetry-raw.v1.example.json": ("telemetry-raw.v1.json", TelemetryRaw),
    "telemetry-raw.v1.sensor-failure.example.json": ("telemetry-raw.v1.json", TelemetryRaw),
    "telemetry-scored.v1.example.json": ("telemetry-scored.v1.json", ScoredEvent),
    "telemetry-scored.v1.skipped.example.json": ("telemetry-scored.v1.json", ScoredEvent),
    "alert.v1.example.json": ("alert.v1.json", Alert),
    "telemetry-label.v1.example.json": ("telemetry-label.v1.json", TelemetryLabel),
}


def _validation_errors(schema_dir: Path, schema_name: str, payload: dict[str, Any]) -> list[str]:
    """Validate ``payload`` against a contract schema, returning readable messages.

    Returns strings rather than a validator object so no jsonschema type appears
    in a signature. mypy runs with ``disallow_any_unimported``, and a partially
    typed dependency would otherwise turn this helper into ``Any`` -- silently
    weakening every assertion built on top of it.
    """
    schema = load_json(schema_dir / schema_name)
    Draft202012Validator.check_schema(schema)
    validator = Draft202012Validator(schema)
    return [
        f"{list(error.absolute_path)}: {error.message}"
        for error in sorted(validator.iter_errors(payload), key=str)
    ]


def test_every_schema_is_itself_valid(schema_dir: Path) -> None:
    """A malformed schema silently validates everything, which is worse than none."""
    schemas = sorted(schema_dir.glob("*.json"))
    assert schemas, "no contract schema found"
    for path in schemas:
        Draft202012Validator.check_schema(load_json(path))


def test_every_example_is_covered(example_dir: Path) -> None:
    """Guard against an example being added without a conformance case."""
    on_disk = {path.name for path in example_dir.glob("*.json")}
    assert on_disk == set(_CASES), (
        "contracts/examples and the conformance table have diverged: "
        f"only on disk={sorted(on_disk - set(_CASES))}, "
        f"only in table={sorted(set(_CASES) - on_disk)}"
    )


@pytest.mark.parametrize("example_name", sorted(_CASES))
def test_example_validates_against_schema(
    example_name: str, example_dir: Path, schema_dir: Path
) -> None:
    schema_name, _ = _CASES[example_name]
    payload = load_json(example_dir / example_name)
    errors = _validation_errors(schema_dir, schema_name, payload)
    assert not errors, "\n".join(errors)


@pytest.mark.parametrize("example_name", sorted(_CASES))
def test_roundtrip_output_validates_against_schema(
    example_name: str, example_dir: Path, schema_dir: Path
) -> None:
    """Decode the example, re-encode it, and validate the result.

    This is what stops the code from drifting away from the contract without
    anyone noticing.
    """
    schema_name, message_type = _CASES[example_name]
    original = load_json(example_dir / example_name)

    reencoded = decode_dict(encode(message_type.from_dict(original)))

    errors = _validation_errors(schema_dir, schema_name, reencoded)
    assert not errors, "\n".join(errors)


@pytest.mark.parametrize("example_name", sorted(_CASES))
def test_roundtrip_preserves_declared_fields(example_name: str, example_dir: Path) -> None:
    """Every field present in the example survives a decode/encode cycle.

    ``processing_delay_ms`` is excluded because it is derived at serialisation
    time rather than stored; the round-trip recomputes it, and a separate test
    checks that the recomputed value is correct.
    """
    _, message_type = _CASES[example_name]
    original = load_json(example_dir / example_name)

    reencoded = decode_dict(encode(message_type.from_dict(original)))

    for key, value in original.items():
        assert key in reencoded, f"field {key} was dropped by the round-trip"
        if key == "processing_delay_ms":
            continue
        assert reencoded[key] == value, f"field {key} changed: {value!r} -> {reencoded[key]!r}"


def test_scored_example_delay_is_derived_not_copied(example_dir: Path) -> None:
    """The published latency is recomputed from its inputs, so it cannot lie."""
    payload = load_json(example_dir / "telemetry-scored.v1.example.json")
    event = ScoredEvent.from_dict(payload)
    assert event.processing_delay_ms == payload["processing_delay_ms"]


def test_alert_example_id_matches_the_derivation(example_dir: Path) -> None:
    """The committed example is a real derived identifier, not a placeholder.

    A hand-written identifier in a fixture would make the fixture useless as a
    check on the idempotency mechanism.
    """
    payload = load_json(example_dir / "alert.v1.example.json")
    alert = Alert.from_dict(payload)
    assert alert.alert_id == payload["alert_id"]
