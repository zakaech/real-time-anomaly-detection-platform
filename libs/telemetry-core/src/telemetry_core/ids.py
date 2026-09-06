"""Deterministic identifier derivation.

This module is the keystone of the delivery semantics. The platform is
at-least-once end to end -- Spark's Kafka sink is not transactional, so a
replayed micro-batch republishes its output (docs/04-streaming-semantics.md
section 4.2). What makes that harmless is that a replay produces *the same
alert identifier*, so the PostgreSQL primary key absorbs the duplicate.

Consequences, which is why this lives in its own module with a golden test:

* The derivation must be stable across processes, machines, Python versions and
  releases. UUIDv5 is SHA-1 over a namespace plus a name, so it is by
  construction reproducible -- unlike ``hash()``, which is randomised per process
  by default.
* Changing the namespace, the field order, the separator, or the timestamp
  format silently breaks idempotence. Nothing crashes; alerts simply start
  duplicating in the operator's queue after the next deployment. That failure
  mode is invisible in unit tests unless one is written specifically to lock the
  derivation, which is what ``tests/test_ids.py`` does.

UUIDv5 rather than UUIDv7: v7 is time-ordered and kinder to B-tree index
locality, but it is not deterministic. Determinism is worth more here than
insertion locality (docs/05-data-model.md section 4.1).
"""

from __future__ import annotations

import uuid
from datetime import datetime

from telemetry_core.timeutil import format_instant

__all__ = ["ALERT_ID_SEPARATOR", "PROJECT_NAMESPACE", "derive_alert_id"]

#: Fixed namespace for every derived identifier in the platform.
#: Generated once and frozen for ever: regenerating it would change every
#: identifier and break deduplication against alerts already persisted.
PROJECT_NAMESPACE = uuid.UUID("89a66a48-8acb-427e-900e-8e9c63fe7e3e")

#: Separator for the canonical name. A character that cannot occur in any of the
#: parts, so that ("M-01", "4|...") and ("M-014", "...") cannot collide.
ALERT_ID_SEPARATOR = "|"


def derive_alert_id(
    *,
    machine_id: str,
    window_start: datetime,
    model_name: str,
    model_version: str,
) -> str:
    """Derive the deterministic alert identifier.

    The canonical name is::

        machine_id | window_start | model_name | model_version

    with ``window_start`` rendered by :func:`telemetry_core.timeutil.format_instant`
    so that two equal instants expressed in different timezones or with different
    sub-millisecond precision produce the same identifier.

    The model version is part of the key on purpose: rescoring the same window
    with a new model is a *different verdict*, so it deserves its own alert
    rather than overwriting the previous one (docs/04 section 4.4).

    Args:
        machine_id: Identifier of the machine, e.g. ``"M-014"``.
        window_start: Inclusive start of the scored window, timezone-aware.
        model_name: Model family, e.g. ``"isolation_forest"``.
        model_version: Semantic version of the artefact, e.g. ``"1.3.0"``.

    Returns:
        The canonical string form of a UUIDv5.

    Raises:
        telemetry_core.errors.InvalidInstantError: if ``window_start`` is naive.
        ValueError: if any part is empty, which would make the key ambiguous.
    """
    parts = {
        "machine_id": machine_id,
        "model_name": model_name,
        "model_version": model_version,
    }
    for label, value in parts.items():
        if not value:
            raise ValueError(f"{label} must not be empty: it is part of the alert identity")
        if ALERT_ID_SEPARATOR in value:
            raise ValueError(
                f"{label} must not contain {ALERT_ID_SEPARATOR!r}: it would make the "
                "canonical name ambiguous and two distinct alerts could collide"
            )

    canonical_name = ALERT_ID_SEPARATOR.join(
        (machine_id, format_instant(window_start), model_name, model_version)
    )
    return str(uuid.uuid5(PROJECT_NAMESPACE, canonical_name))
