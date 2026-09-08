"""The anti-alert-storm state machine, tested as a pure function.

``applyInPandasWithState`` hands the handler a key, an iterator of frames and a
state object. All three are simple enough to fake, so the logic is tested
without starting a streaming query -- which keeps these tests in milliseconds
and makes the failures readable.

The cases the brief asks for are all here: repeated updates of one window must
count once, a continuous run must yield one alert, and two runs separated by
more than the gap must yield two.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from typing import Any

import pandas as pd
import pytest

from stream_processor.alerting import make_alert_state_handler
from stream_processor.monitoring import summarise_progress

ORIGIN = datetime(2026, 4, 1, 6, 0, 0, tzinfo=UTC)
GAP_SECONDS = 120
SLIDE = 10


class FakeGroupState:
    """Enough of ``GroupState`` for the handler under test."""

    def __init__(
        self,
        value: tuple[Any, ...] | None = None,
        timed_out: bool = False,
        watermark_ms: int = 0,
    ) -> None:
        self._value = value
        self.hasTimedOut = timed_out
        self.removed = False
        self.timeout_timestamp: int | None = None
        self._watermark_ms = watermark_ms

    @property
    def exists(self) -> bool:
        return self._value is not None

    @property
    def get(self) -> tuple[Any, ...]:
        assert self._value is not None
        return self._value

    def update(self, value: tuple[Any, ...]) -> None:
        self._value = value

    def remove(self) -> None:
        self._value = None
        self.removed = True

    def setTimeoutTimestamp(self, timestamp: int) -> None:
        # Spark raises here when the timestamp is behind the watermark; the fake
        # reproduces that so the guard is genuinely exercised.
        if timestamp < self._watermark_ms:
            raise ValueError(
                f"Timeout timestamp ({timestamp}) cannot be earlier than the "
                f"current watermark ({self._watermark_ms})"
            )
        self.timeout_timestamp = timestamp

    def getCurrentWatermarkMs(self) -> int:
        return self._watermark_ms


def _row(offset_seconds: int, *, score: float = 0.999) -> dict[str, Any]:
    start = ORIGIN + timedelta(seconds=offset_seconds)
    return {
        "machine_id": "M-014",
        "line_id": "LINE-A",
        "window_start": pd.Timestamp(start),
        "window_end": pd.Timestamp(start + timedelta(seconds=60)),
        "anomaly_score": score,
        "score_threshold": 0.9927653712984937,
        "model_name": "one_class_svm",
        "model_version": "2.0.0",
        "top_contributors": [{"feature": "power_per_rpm_mean", "z_score": 4.2}],
    }


def _run(
    batches: list[list[dict[str, Any]]],
    *,
    state: FakeGroupState | None = None,
    consecutive_to_open: int = 2,
    watermark_ms: int = 0,
) -> tuple[list[dict[str, Any]], FakeGroupState]:
    """Feed batches through the handler, returning every alert it emitted."""
    handler = make_alert_state_handler(
        consecutive_to_open=consecutive_to_open, gap_seconds=GAP_SECONDS
    )
    group_state = state or FakeGroupState(watermark_ms=watermark_ms)
    emitted: list[dict[str, Any]] = []
    for rows in batches:
        frames = iter([pd.DataFrame(rows)]) if rows else iter([])
        for frame in handler(("M-014",), frames, group_state):
            emitted.extend(frame.to_dict("records"))
    return emitted, group_state


def _alerts(emitted: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [json.loads(record["payload"]) for record in emitted]


class TestHysteresis:
    def test_one_window_does_not_open_an_alert(self) -> None:
        """A single window above the threshold is usually noise."""
        emitted, _state = _run([[_row(0)]])
        assert emitted == []

    def test_two_consecutive_windows_open_exactly_one_alert(self) -> None:
        emitted, _state = _run([[_row(0)], [_row(SLIDE)]])
        alerts = _alerts(emitted)
        assert len(alerts) == 1
        assert alerts[0]["consecutive_windows"] == 2
        assert alerts[0]["status"] == "NEW"

    def test_a_long_run_still_produces_one_alert(self) -> None:
        """One degradation is one alert, not the hundred windows it spans."""
        batches = [[_row(index * SLIDE)] for index in range(100)]
        emitted, _state = _run(batches)
        assert len(_alerts(emitted)) == 1

    def test_a_long_run_in_a_single_batch_also_produces_one(self) -> None:
        emitted, _state = _run([[_row(index * SLIDE) for index in range(100)]])
        assert len(_alerts(emitted)) == 1


class TestUpdateModeDeduplication:
    """``telemetry.scored`` is written in update mode, so the same window
    arrives several times. The state must treat it as one logical window."""

    def test_repeated_updates_of_one_window_do_not_open_an_alert(self) -> None:
        same_window = [[_row(0, score=0.994)], [_row(0, score=0.996)], [_row(0, score=0.999)]]
        emitted, _state = _run(same_window)
        assert emitted == [], "repeated updates of a single window must count once"

    def test_repeated_updates_within_one_batch_count_once(self) -> None:
        emitted, _state = _run([[_row(0, score=0.994), _row(0, score=0.999)]])
        assert emitted == []

    def test_updates_of_an_earlier_window_never_reopen(self) -> None:
        """A late update of an already-counted window must not restart the run."""
        emitted, state = _run([[_row(0)], [_row(SLIDE)]])
        assert len(_alerts(emitted)) == 1
        more, _state = _run([[_row(0, score=0.9999)], [_row(SLIDE, score=0.9999)]], state=state)
        assert more == []

    def test_the_alert_uses_the_opening_window(self) -> None:
        """Two updates then a second window: the alert is anchored on the run's
        start, so a replay produces the same identifier."""
        emitted, _state = _run([[_row(0), _row(0, score=0.998)], [_row(SLIDE)]])
        alert = _alerts(emitted)[0]
        assert alert["window_start"] == "2026-04-01T06:00:10.000Z"
        assert alert["detected_at"] == alert["window_end"]


class TestEpisodeBoundaries:
    def test_a_gap_beyond_the_threshold_starts_a_new_episode(self) -> None:
        first = [[_row(0)], [_row(SLIDE)]]
        after_gap = GAP_SECONDS + 4 * SLIDE
        second = [[_row(after_gap)], [_row(after_gap + SLIDE)]]
        emitted, _state = _run(first + second)

        alerts = _alerts(emitted)
        assert len(alerts) == 2
        assert alerts[0]["alert_id"] != alerts[1]["alert_id"]

    def test_a_gap_within_the_threshold_continues_the_episode(self) -> None:
        emitted, _state = _run([[_row(0)], [_row(SLIDE)], [_row(SLIDE + GAP_SECONDS - 10)]])
        assert len(_alerts(emitted)) == 1

    def test_a_timeout_clears_the_state(self) -> None:
        state = FakeGroupState(value=(1, 5, 100), timed_out=True)
        emitted, state = _run([[]], state=state)
        assert emitted == []
        assert state.removed

    def test_a_timeout_behind_the_watermark_is_clamped(self) -> None:
        """The defect the crash-recovery test surfaced.

        After a restart the watermark is restored from the checkpoint while the
        replayed batch still carries older windows. Asking Spark for a timeout
        behind the watermark raises INVALID_TIMEOUT_TIMESTAMP and kills the
        query -- which is exactly what happened, and never on the happy path.
        """
        far_ahead = int((ORIGIN.timestamp() + 10_000) * 1000)
        _emitted, state = _run([[_row(0)]], watermark_ms=far_ahead)
        assert state.timeout_timestamp is not None
        assert state.timeout_timestamp > far_ahead

    def test_the_timeout_is_armed_on_event_time(self) -> None:
        """Processing time would close episodes an accelerated replay never ended."""
        _emitted, state = _run([[_row(0)]])
        expected_ms = int((ORIGIN.timestamp() + GAP_SECONDS) * 1000)
        assert state.timeout_timestamp == expected_ms


class TestAlertContract:
    def test_the_identifier_is_the_deterministic_derivation(self) -> None:
        """Validated by Alert.__post_init__, so an emitted alert proves it."""
        from telemetry_core.ids import derive_alert_id

        emitted, _state = _run([[_row(0)], [_row(SLIDE)]])
        alert = _alerts(emitted)[0]
        assert alert["alert_id"] == derive_alert_id(
            machine_id="M-014",
            window_start=ORIGIN + timedelta(seconds=SLIDE),
            model_name="one_class_svm",
            model_version="2.0.0",
        )

    def test_replaying_the_same_run_produces_the_same_identifier(self) -> None:
        """What makes at-least-once delivery harmless downstream."""
        first, _state = _run([[_row(0)], [_row(SLIDE)]])
        second, _state = _run([[_row(0)], [_row(SLIDE)]])
        assert _alerts(first)[0]["alert_id"] == _alerts(second)[0]["alert_id"]

    def test_severity_rises_with_the_score(self) -> None:
        low, _state = _run([[_row(0, score=0.9928)], [_row(SLIDE, score=0.9928)]])
        high, _state = _run([[_row(0, score=0.99999)], [_row(SLIDE, score=0.99999)]])
        assert _alerts(low)[0]["severity"] == "MEDIUM"
        assert _alerts(high)[0]["severity"] == "CRITICAL"

    def test_contributors_are_carried_through(self) -> None:
        emitted, _state = _run([[_row(0)], [_row(SLIDE)]])
        alert = _alerts(emitted)[0]
        assert alert["top_contributors"][0]["feature"] == "power_per_rpm_mean"


def test_an_empty_batch_changes_nothing() -> None:
    emitted, state = _run([[]])
    assert emitted == []
    assert not state.exists


@pytest.mark.parametrize("threshold", [1, 3])
def test_the_opening_threshold_is_configurable(threshold: int) -> None:
    batches = [[_row(index * SLIDE)] for index in range(threshold)]
    emitted, _state = _run(batches, consecutive_to_open=threshold)
    assert len(_alerts(emitted)) == 1


class TestProgressSummary:
    """The duration field silently logged null for every batch.

    ``batchDuration`` is not a key of the dict form of a progress record, so the
    one number that says whether a batch outran its trigger interval was never
    recorded. Spark puts it in the per-phase ``durationMs`` map instead.
    """

    def test_the_batch_duration_comes_from_the_phase_map(self) -> None:
        summary = summarise_progress(
            {
                "name": "scoring",
                "batchId": 7,
                "numInputRows": 150,
                "durationMs": {"triggerExecution": 2431, "addBatch": 1900},
                "stateOperators": [{"numRowsTotal": 240, "numRowsDroppedByWatermark": 0}],
            }
        )

        assert summary["batch_duration_ms"] == 2431
        assert summary["add_batch_ms"] == 1900
        assert summary["state_rows"] == 240

    def test_an_explicit_batch_duration_still_wins(self) -> None:
        summary = summarise_progress(
            {"name": "scoring", "batchId": 1, "batchDuration": 99, "durationMs": {}}
        )

        assert summary["batch_duration_ms"] == 99

    def test_a_progress_record_without_timings_does_not_raise(self) -> None:
        """Spark omits phases on an empty batch; logging must survive that."""
        summary = summarise_progress({"name": "alerting", "batchId": 0})

        assert summary["batch_duration_ms"] is None
        assert summary["state_rows"] == 0


class TestModelProvenance:
    """The alert must carry which artefact produced it.

    telemetry.scored has always carried model.trained_at and
    model.artifact_sha256 -- the Spark schema declares both -- but the alerting
    projection selected only name and version, so every published alert had them
    null. artifact_sha256 is the proof that the model which scored a window is
    the binary evaluated in Phase 2; without it an alert cannot be tied back to
    an artefact months later, which is the whole reason the field exists.
    """

    @staticmethod
    def _with_provenance(offset: int, trained_at: Any, sha: Any) -> dict[str, Any]:
        # The alert is built from the window that TRIPS the hysteresis -- the
        # second consecutive one -- so that is the row whose provenance is read.
        row = _row(offset)
        row["model_trained_at"] = trained_at
        row["model_artifact_sha256"] = sha
        return row

    def test_the_artefact_identity_reaches_the_alert(self) -> None:
        opening = self._with_provenance(10, "2026-09-07T00:36:44.723Z", "0" * 64)

        emitted, _state = _run([[_row(0)], [opening]])

        alert = json.loads(emitted[0]["payload"])
        assert alert["model"]["artifact_sha256"] == "0" * 64
        assert alert["model"]["trained_at"] == "2026-09-07T00:36:44.723Z"

    def test_an_absent_provenance_stays_null(self) -> None:
        """The fields are optional in the contract, so their absence is not an
        error -- it must not become the string "None" either."""
        emitted, _state = _run([[_row(0)], [_row(10)]])

        alert = json.loads(emitted[0]["payload"])
        assert alert["model"]["trained_at"] is None
        assert alert["model"]["artifact_sha256"] is None

    def test_a_nan_from_arrow_is_not_read_as_text(self) -> None:
        """Arrow renders a missing column as NaN, and str(nan) is "nan" -- which
        would pass silently into the contract and fail its 64-hex check there
        instead of here."""
        opening = self._with_provenance(10, float("nan"), float("nan"))

        emitted, _state = _run([[_row(0)], [opening]])

        alert = json.loads(emitted[0]["payload"])
        assert alert["model"]["artifact_sha256"] is None
        assert alert["model"]["trained_at"] is None

    def test_a_malformed_timestamp_does_not_cost_the_alert(self) -> None:
        """Provenance is metadata; the alert is the operational signal."""
        opening = self._with_provenance(10, "not-a-timestamp", None)

        emitted, _state = _run([[_row(0)], [opening]])

        assert len(emitted) == 1
        assert json.loads(emitted[0]["payload"])["model"]["trained_at"] is None
