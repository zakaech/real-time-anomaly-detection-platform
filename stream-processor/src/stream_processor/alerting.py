"""Turning a run of anomalous windows into one alert.

A continuous degradation produces an anomalous window every ten seconds. Sending
one alert per window would send eighty for a fifteen-minute fault, which is not
a detection system, it is a way of getting alerts ignored.

The state machine, keyed by machine:

    2 consecutive anomalous windows  -> open an alert, emit once
    further anomalous windows        -> counted, nothing emitted
    120 s without an anomalous window -> close, state removed
    a new run after closing          -> a new alert

**Update mode makes deduplication mandatory.** ``telemetry.scored`` is written in
update mode, so the same window is re-emitted several times as it fills and as
late samples arrive. Without a check the state would count one window as several
and open an alert from a single genuine one. The logical identity of a window is
``(machine_id, window_start)``, and the state tracks the highest window start it
has already counted.

**Event-time timeout, not processing-time.** The gap is a statement about the
machine's timeline, not about ours. With a processing-time timeout an accelerated
replay would close episodes that, in the data, never ended.

Delivery stays **at-least-once**. The identifier is derived deterministically
from the opening window, so a replay republishes the same one and the idempotent
upsert downstream absorbs it. That is not exactly-once and is not claimed to be.
"""

from __future__ import annotations

import math
from collections.abc import Iterator
from datetime import datetime
from typing import Any

import pandas as pd
from pyspark.sql.streaming.state import GroupState, GroupStateTimeout
from pyspark.sql.types import (
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)
from telemetry_core.codec import encode
from telemetry_core.errors import SchemaValidationError
from telemetry_core.ids import derive_alert_id
from telemetry_core.schemas import Alert, FeatureContribution, ModelRef
from telemetry_core.timeutil import parse_instant, utc_now

from stream_processor.policy import severity_for, to_utc_datetime

__all__ = [
    "ALERT_OUTPUT_SCHEMA",
    "ALERT_STATE_SCHEMA",
    "ALERT_TIMEOUT",
    "make_alert_state_handler",
]

#: What the alerting query emits: a Kafka key and an encoded Alert.
ALERT_OUTPUT_SCHEMA = StructType(
    [
        StructField("machine_id", StringType(), nullable=False),
        StructField("payload", StringType(), nullable=False),
    ]
)

#: Deliberately flat and small. State schema changes break a checkpoint, so
#: every field here has to earn its place.
ALERT_STATE_SCHEMA = StructType(
    [
        StructField("is_open", IntegerType(), nullable=False),
        StructField("consecutive", IntegerType(), nullable=False),
        StructField("last_window_start", LongType(), nullable=False),
    ]
)

ALERT_TIMEOUT = GroupStateTimeout.EventTimeTimeout

_NO_WINDOW = -1


def _as_epoch_seconds(value: Any) -> int:
    return int(to_utc_datetime(value).timestamp())


def _contributors(raw: Any) -> tuple[FeatureContribution, ...]:
    if raw is None:
        return ()
    contributions: list[FeatureContribution] = []
    for item in raw:
        if item is None:
            continue
        feature = item["feature"] if isinstance(item, dict) else item.feature
        z_score = item["z_score"] if isinstance(item, dict) else item.z_score
        if feature is None or z_score is None:
            continue
        contributions.append(FeatureContribution(feature=str(feature), z_score=float(z_score)))
    return tuple(contributions)


def _optional_text(value: Any) -> str | None:
    """A nullable string column, as pandas hands it over.

    Arrow renders SQL NULL as ``None`` but a missing column as ``NaN``, and
    ``str(nan)`` is the string ``"nan"`` -- which would sail straight through the
    contract's 64-hex pattern check and fail there instead of here.
    """
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return None
    text = str(value)
    return text or None


def _optional_instant(value: Any) -> datetime | None:
    """Parse an optional ISO instant, tolerating an absent artefact timestamp."""
    text = _optional_text(value)
    if text is None:
        return None
    try:
        return parse_instant(text)
    except SchemaValidationError:
        # A malformed timestamp must not cost us the alert: the field is
        # provenance, and the alert is the operational signal.
        return None


def make_alert_state_handler(*, consecutive_to_open: int, gap_seconds: int) -> Any:
    """Build the state function for ``applyInPandasWithState``."""

    def handle(
        key: tuple[Any, ...], frames: Iterator[pd.DataFrame], state: GroupState
    ) -> Iterator[pd.DataFrame]:
        machine_id = str(key[0])

        if state.hasTimedOut:
            # The machine has been quiet for longer than the gap: the episode is
            # over. Removing the state is the whole closing action -- the alert
            # contract has no CLOSED status, since the lifecycle belongs to
            # alert-service.
            state.remove()
            return

        is_open, consecutive, last_window_start = state.get if state.exists else (0, 0, _NO_WINDOW)

        rows: list[dict[str, Any]] = []
        for frame in frames:
            if len(frame):
                rows.extend(
                    {str(key): value for key, value in record.items()}
                    for record in frame.to_dict("records")
                )
        if not rows:
            return

        # One entry per logical window, keeping the strongest update. This is
        # what makes repeated emissions of the same window count once.
        by_window: dict[int, dict[str, Any]] = {}
        for row in rows:
            window_start = _as_epoch_seconds(row["window_start"])
            existing = by_window.get(window_start)
            score = float(row["anomaly_score"])
            if existing is None or score > float(existing["anomaly_score"]):
                by_window[window_start] = row

        emitted: list[dict[str, str]] = []
        latest_seen = last_window_start

        for window_start in sorted(by_window):
            if window_start <= last_window_start:
                # Already counted: a later update of a window we have seen.
                continue

            if last_window_start != _NO_WINDOW and (window_start - last_window_start) > gap_seconds:
                # The run was interrupted for longer than the gap: this is a new
                # episode, not a continuation of the previous one.
                is_open, consecutive = 0, 0

            consecutive += 1
            last_window_start = window_start
            latest_seen = max(latest_seen, window_start)

            if is_open or consecutive < consecutive_to_open:
                continue

            row = by_window[window_start]
            model_ref = ModelRef(
                name=str(row["model_name"]),
                version=str(row["model_version"]),
                trained_at=_optional_instant(row.get("model_trained_at")),
                artifact_sha256=_optional_text(row.get("model_artifact_sha256")),
            )
            opening_start = to_utc_datetime(row["window_start"])
            opening_end = to_utc_datetime(row["window_end"])
            score = float(row["anomaly_score"])
            threshold = float(row["score_threshold"])

            alert = Alert(
                alert_id=derive_alert_id(
                    machine_id=machine_id,
                    window_start=opening_start,
                    model_name=model_ref.name,
                    model_version=model_ref.version,
                ),
                machine_id=machine_id,
                line_id=str(row["line_id"]),
                severity=severity_for(score, threshold),
                anomaly_score=score,
                score_threshold=threshold,
                # The contract requires detected_at == window_end: an operator
                # must see when the machine deviated, not when we decided.
                detected_at=opening_end,
                window_start=opening_start,
                window_end=opening_end,
                published_at=utc_now(),
                consecutive_windows=consecutive,
                top_contributors=_contributors(row.get("top_contributors")),
                model=model_ref,
            )
            emitted.append({"machine_id": machine_id, "payload": encode(alert).decode("utf-8")})
            is_open = 1

        state.update((int(is_open), int(consecutive), int(last_window_start)))
        if latest_seen != _NO_WINDOW:
            # Spark refuses an event-time timeout that is already behind the
            # watermark, and the refusal kills the query. It happens whenever a
            # batch carries windows older than the watermark -- routinely after a
            # restart, where the watermark is restored from the checkpoint while
            # the replayed batch contains data from before it. Clamping to just
            # past the watermark is correct, not an approximation: a requested
            # timeout already behind it means the silence gap has itself elapsed
            # in event time, so expiring at the next opportunity is the right
            # outcome.
            requested = (latest_seen + gap_seconds) * 1000
            state.setTimeoutTimestamp(max(requested, int(state.getCurrentWatermarkMs()) + 1))

        if emitted:
            yield pd.DataFrame(emitted)

    return handle
