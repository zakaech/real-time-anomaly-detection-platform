"""Admission, severity and message construction -- the decisions, not the plumbing.

Everything here is plain Python operating on plain values, so it is testable
without starting Spark. The user-defined functions in
:mod:`stream_processor.scoring` are thin wrappers around these.

Messages are built through the ``telemetry_core`` dataclasses rather than by
assembling JSON with Spark's ``to_json``. That costs a Python loop over the rows
of a batch, and buys three things a hand-built string cannot: the exact instant
format of the contract, the field order of the contract, and the invariants
checked in ``__post_init__`` -- including the one that matters most, that an
alert's identifier really is the deterministic derivation.

The loop is affordable because the volume is windows, not samples: fifteen
machines at one window per ten seconds is ninety rows a minute. It would stop
being affordable at a few thousand windows per batch, and that is the point at
which this would need to become a vectorised encoder.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import numpy as np
import numpy.typing as npt
import pandas as pd
from telemetry_core.enums import MachineState, Severity, SkipReason
from telemetry_core.features import (
    FEATURE_NAMES,
    MIN_RUNNING_RATIO,
    MIN_SAMPLES_FOR_SCORING,
)
from telemetry_core.schemas import FeatureContribution, ModelRef, ScoredEvent

__all__ = [
    "MAX_TOP_CONTRIBUTORS",
    "RUNNING_STATE",
    "admission_skip_reason",
    "build_scored_event",
    "severity_for",
    "to_utc_datetime",
    "top_contributors",
]

RUNNING_STATE = "RUNNING"
MAX_TOP_CONTRIBUTORS = 3


def to_utc_datetime(value: Any) -> datetime:
    """Convert a Spark/pandas timestamp to a timezone-aware UTC datetime.

    Arrow hands Spark timestamps over as **naive** ``datetime64[ns]`` even when
    the session timezone is UTC. Passing that straight into the contract would
    raise, because a naive datetime is not an instant -- so it is localised here,
    once, at the boundary.
    """
    stamp = pd.Timestamp(value)
    if stamp.tzinfo is None:
        stamp = stamp.tz_localize("UTC")
    else:
        stamp = stamp.tz_convert("UTC")
    return stamp.to_pydatetime()


def admission_skip_reason(
    *, sample_count: int, running_ratio: float, machine_state: str
) -> SkipReason | None:
    """Why this window must not be scored, or ``None`` if it may be.

    The same gate the training pipeline applied. A window the model would never
    have seen in training must not be scored in production either.

    The last-state check is an addition measured in Phase 2: 42 of 221 false
    positives fell on windows that were more than 90 % RUNNING but *ended* in
    MAINTENANCE. Whether it actually helps is measured, not asserted -- the
    result is reported in docs/10.
    """
    if sample_count < MIN_SAMPLES_FOR_SCORING:
        return SkipReason.INSUFFICIENT_SAMPLES
    if running_ratio < MIN_RUNNING_RATIO or machine_state != RUNNING_STATE:
        return SkipReason.MACHINE_NOT_RUNNING
    return None


def severity_for(score: float, threshold: float) -> Severity:
    """Map a calibrated score onto the contract's three bands.

    Bands are expressed as a share of the headroom above the threshold rather
    than as fixed scores, so changing the operating point does not silently
    change what CRITICAL means.
    """
    headroom = max(1.0 - threshold, 1e-9)
    position = (score - threshold) / headroom
    if position >= 0.7:
        return Severity.CRITICAL
    if position >= 0.4:
        return Severity.HIGH
    return Severity.MEDIUM


def top_contributors(
    contributions: npt.NDArray[np.float64], *, limit: int = MAX_TOP_CONTRIBUTORS
) -> tuple[FeatureContribution, ...]:
    """The features furthest from the training reference, largest first.

    This is what turns "score 0.995" into something an operator can act on. A
    non-finite contribution is dropped rather than ranked: it means the feature
    could not be computed, which is reported through ``null_ratio`` instead.
    """
    finite = np.isfinite(contributions)
    if not finite.any():
        return ()
    ranked = sorted(
        (index for index in range(len(contributions)) if finite[index]),
        key=lambda index: abs(float(contributions[index])),
        reverse=True,
    )[:limit]
    return tuple(
        FeatureContribution(feature=FEATURE_NAMES[index], z_score=float(contributions[index]))
        for index in ranked
    )


def build_scored_event(
    *,
    machine_id: str,
    line_id: str,
    window_start: datetime,
    window_end: datetime,
    sample_count: int,
    running_ratio: float,
    machine_state: str,
    features: npt.NDArray[np.float64],
    model_ref: ModelRef,
    raw_score: float | None,
    score: float | None,
    threshold: float | None,
    contributions: npt.NDArray[np.float64] | None,
    scored_at: datetime | None = None,
) -> ScoredEvent:
    """Assemble one scored window, admitted or not.

    A window that fails the gate is still published, with its reason. Silence
    and "nothing is wrong" must never look alike: without this, an outage of the
    scoring path would be indistinguishable from a healthy plant.
    """
    skip_reason = admission_skip_reason(
        sample_count=sample_count, running_ratio=running_ratio, machine_state=machine_state
    )
    is_scored = skip_reason is None and score is not None

    feature_map: dict[str, float | None] = {
        name: (float(value) if np.isfinite(value) else None)
        for name, value in zip(FEATURE_NAMES, features, strict=True)
    }

    contributors: tuple[FeatureContribution, ...] = ()
    if is_scored and contributions is not None:
        contributors = top_contributors(contributions)

    return ScoredEvent(
        machine_id=machine_id,
        line_id=line_id,
        window_start=window_start,
        window_end=window_end,
        scored_at=scored_at or datetime.now(tz=UTC),
        sample_count=int(sample_count),
        is_scored=is_scored,
        skip_reason=skip_reason if not is_scored else None,
        machine_state=MachineState.parse_tolerant(machine_state, field="machine_state"),
        model=model_ref,
        anomaly_score=float(score) if is_scored and score is not None else None,
        raw_score=float(raw_score) if is_scored and raw_score is not None else None,
        score_threshold=float(threshold) if is_scored and threshold is not None else None,
        is_anomaly=(
            bool(score >= threshold)
            if is_scored and score is not None and threshold is not None
            else None
        ),
        # Not tracked at scoring time: this query holds no state. The alerting
        # query counts consecutive windows and carries the count on the Alert.
        consecutive_windows=0,
        top_contributors=contributors,
        features=feature_map,
    )
