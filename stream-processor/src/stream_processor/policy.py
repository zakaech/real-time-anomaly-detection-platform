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
    "DEFAULT_MATURITY_TOLERANCE_STEPS",
    "MAX_TOP_CONTRIBUTORS",
    "RUNNING_STATE",
    "admission_skip_reason",
    "build_scored_event",
    "severity_for",
    "to_utc_datetime",
    "top_contributors",
    "window_is_mature",
]

RUNNING_STATE = "RUNNING"
MAX_TOP_CONTRIBUTORS = 3

#: How much of the machine's own sampling interval may be missing from the end
#: of a window before it stops counting as complete. Two steps tolerate one
#: dropped sample at the tail plus publication jitter; it is not a score
#: threshold and does not interact with the model's operating point.
DEFAULT_MATURITY_TOLERANCE_STEPS = 2.0


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


def window_is_mature(
    *,
    window_start_epoch: float,
    window_end_epoch: float,
    first_event_epoch: float | None,
    last_event_epoch: float | None,
    sample_count: int,
    tolerance_steps: float = DEFAULT_MATURITY_TOLERANCE_STEPS,
) -> bool:
    """Has this window received the data that belongs to it, up to its own end?

    **Maturity is not the watermark, and must not be confused with it**
    (decision D-37, ADR-004). The watermark is a single stream-wide bound on
    lateness, computed from the newest event time seen across *every* machine;
    it decides when state may be evicted. Maturity is a property of one window
    of one machine, computed from that window's own contents. Machine A's
    traffic advances the watermark; it says nothing whatever about whether
    machine B's window is full.

    The rule is event-time coverage, and it is checked at **both ends**::

        mean_step = (last - first) / (sample_count - 1)
        head_gap  = first - window_start
        tail_gap  = window_end - last
        mature    = max(head_gap, tail_gap) <= mean_step * tolerance_steps

    Checking only the tail is not enough, and that was measured rather than
    reasoned: a tail-only rule still scored every machine's opening windows,
    which are truncated at the *head* because the stream simply began part-way
    through them. All fifteen machines then alerted on the same window start
    with the same 44 samples -- one synchronised false storm at every cold
    start, which is the worst possible moment for one.

    Deriving the cadence from the window instead of configuring it is what makes
    this work across machines that sample at different rates: at 1 Hz a complete
    60 s window ends within ~1 s of its bound, at 0.2 Hz within ~5 s, and both
    are mature. A fixed threshold in seconds would admit partial windows from
    the fast machine or reject complete ones from the slow machine, and a
    threshold in samples would need the rate the job does not know.

    Two properties this buys, both of which a watermark-based rule loses:

    * it is a **pure function of the window's aggregates**, so a replay produces
      exactly the same decisions regardless of how batches happen to be cut --
      the same determinism the feature-parity work exists to protect;
    * it is **monotone in arrival**: late data can only ever complete a window's
      tail, so maturity is granted and never revoked.
    """
    if sample_count < 2 or first_event_epoch is None or last_event_epoch is None:
        # One sample spans no interval, so there is no cadence to measure and
        # nothing to conclude. Treated as immature: the sample-count gate is
        # what rejects these in practice.
        return False

    span = last_event_epoch - first_event_epoch
    if span <= 0.0:
        return False

    mean_step = span / (sample_count - 1)
    head_gap = first_event_epoch - window_start_epoch
    tail_gap = window_end_epoch - last_event_epoch
    # Negative gaps cannot happen (the window bounds its own samples), but
    # clamping keeps the comparison honest if one ever did.
    worst_gap = max(head_gap, tail_gap, 0.0)
    return worst_gap <= mean_step * tolerance_steps


def admission_skip_reason(
    *,
    sample_count: int,
    running_ratio: float,
    machine_state: str,
    is_mature: bool = True,
) -> SkipReason | None:
    """Why this window must not be scored, or ``None`` if it may be.

    The same gate the training pipeline applied. A window the model would never
    have seen in training must not be scored in production either.

    The last-state check is an addition measured in Phase 2: 42 of 221 false
    positives fell on windows that were more than 90 % RUNNING but *ended* in
    MAINTENANCE. Whether it actually helps is measured, not asserted -- the
    result is reported in docs/10.

    The maturity check is the correction of D-37, and it exists because the
    other two were not enough. ``sample_count`` counts rows without asking where
    in the window they fall, so a half-filled window published mid-flight passes
    it easily -- and in update mode that is the *usual* state of a window. It
    was measured flagging anomalies at essentially 100 %, against 1.0 % for
    complete ones, because the model was trained on complete windows and a
    partial one is simply out of distribution.
    """
    if sample_count < MIN_SAMPLES_FOR_SCORING:
        return SkipReason.INSUFFICIENT_SAMPLES
    if running_ratio < MIN_RUNNING_RATIO or machine_state != RUNNING_STATE:
        return SkipReason.MACHINE_NOT_RUNNING
    if not is_mature:
        return SkipReason.WINDOW_NOT_MATURE
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
    is_mature: bool,
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
        sample_count=sample_count,
        running_ratio=running_ratio,
        machine_state=machine_state,
        is_mature=is_mature,
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
