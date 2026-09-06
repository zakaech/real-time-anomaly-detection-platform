"""The two projections that split a simulated sample from its ground truth.

This is the enforcement point for decision D-03. Two separate functions, two
separate topics, and -- more importantly -- a target type that has nowhere to put
a label: ``TelemetryRaw`` simply has no field for one. Leakage is prevented by
the type system rather than by remembering to strip a field, which is the kind
of thing that survives review and fails in production.

Fields are listed one by one rather than spread from a dictionary. An unpacked
mapping would happily carry an extra key into the message the day someone adds
one to the internal sample.
"""

from __future__ import annotations

import uuid
from datetime import datetime

from numpy.random import Generator
from telemetry_core.enums import LabelSource
from telemetry_core.schemas import TelemetryLabel, TelemetryRaw

from event_simulator.generation.state import GeneratedSample

__all__ = ["new_event_id", "to_telemetry_label", "to_telemetry_raw"]


def new_event_id(generator: Generator) -> str:
    """Draw a random UUIDv4 from a seeded generator.

    Random, as the contract intends -- an identifier that carries no information
    -- yet reproducible under a fixed seed, which is what makes the generator
    testable. ``uuid.uuid4()`` would satisfy the first property and destroy the
    second.
    """
    return str(uuid.UUID(bytes=bytes(generator.bytes(16)), version=4))


def to_telemetry_raw(
    sample: GeneratedSample, *, event_id: str, ingest_time: datetime
) -> TelemetryRaw:
    """Project the observable part of a sample onto the telemetry contract.

    ``sample.ground_truth`` is deliberately not referenced. It could not be
    carried even if it were: the target type has no field for it.
    """
    return TelemetryRaw(
        event_id=event_id,
        machine_id=sample.machine_id,
        line_id=sample.line_id,
        event_time=sample.event_time,
        ingest_time=ingest_time,
        machine_state=sample.machine_state,
        readings=sample.readings,
        firmware_version=sample.firmware_version,
    )


def to_telemetry_label(
    sample: GeneratedSample,
    *,
    event_id: str,
    emitted_at: datetime,
    emit_normal_labels: bool = False,
) -> TelemetryLabel | None:
    """Project the ground truth, or ``None`` when there is nothing to publish.

    By default only anomalous samples are labelled (decision D-22). Normal is the
    complement: episodes carry explicit bounds, so any event stays identifiable
    without doubling the volume of the label stream.

    ``emitted_at`` is the publication instant, always later than ``event_time``.
    In production that gap is days -- a maintenance report, an operator's
    verdict. Keeping the two fields distinct here is what makes the stream a
    faithful model of a label rather than a convenient fiction.
    """
    truth = sample.ground_truth
    if not truth.is_anomaly and not emit_normal_labels:
        return None

    return TelemetryLabel(
        machine_id=sample.machine_id,
        event_time=sample.event_time,
        is_anomaly=truth.is_anomaly,
        source=LabelSource.SIMULATOR,
        emitted_at=emitted_at,
        event_id=event_id,
        anomaly_type=truth.anomaly_type,
        episode_id=truth.episode_id,
        episode_started_at=truth.episode_started_at,
    )
