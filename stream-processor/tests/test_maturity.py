"""Window maturity: the correction of D-37.

The defect these tests pin down was measured, not imagined. In update mode a
window is republished at every trigger that changes it, so it is normally
published while still filling. The model was trained on complete 60 s windows,
and a live run measured partial windows being flagged anomalous **100 % of the
time** (383 of 383 with 30-39 samples) against **1.0 %** for complete ones --
producing 16 CRITICAL alerts for 15 machines in five minutes.

The rule under test is event-time coverage, not the watermark. Those are
different things and the distinction is the whole point of the fix:

* the **watermark** is one stream-wide lateness bound, derived from the newest
  event time across every machine, and it governs state eviction;
* **maturity** is a property of one window of one machine, derived from that
  window's own contents.

So these tests never assert anything about the watermark, and deliberately
include a case where the two disagree.
"""

from __future__ import annotations

from typing import Any

import pandas as pd
from telemetry_core.enums import SkipReason
from telemetry_core.features import FEATURE_NAMES, MIN_SAMPLES_FOR_SCORING

from conftest import make_samples
from stream_processor.features_spark import (
    aggregate_windows,
    derive_features,
    prepare_signals,
)
from stream_processor.policy import admission_skip_reason, window_is_mature

# The production converter, deliberately: a second definition here could drift
# from the one the job actually uses, and the resolution trap it exists to avoid
# (Arrow returns microseconds, not nanoseconds) already cost one silent failure.
from stream_processor.scoring import _epoch_seconds

# One machine sampling once a second into a 60 s window, as the simulator does.
WINDOW_SECONDS = 60.0
ORIGIN = 1_000_000.0
WINDOW_END = ORIGIN + WINDOW_SECONDS


def _mature(*, samples: int, step: float = 1.0, first: float = ORIGIN, **kwargs: float) -> bool:
    """Maturity of a window holding ``samples`` readings ``step`` apart.

    ``first`` is where the readings start; leaving it at ORIGIN means the window
    is covered from its beginning, and raising it models a window the stream
    entered part-way through.
    """
    return window_is_mature(
        window_start_epoch=ORIGIN,
        window_end_epoch=WINDOW_END,
        first_event_epoch=first,
        last_event_epoch=first + step * (samples - 1),
        sample_count=samples,
        **kwargs,
    )


class TestPartialWindowIsNotScored:
    """partial window -> NOT SCORED."""

    def test_a_half_filled_window_is_immature(self) -> None:
        """30 samples of a 60 s window: exactly the case measured at 100 %
        anomalous. It passes the sample-count gate and must still be refused."""
        assert _mature(samples=30) is False

    def test_a_nearly_filled_window_is_still_immature(self) -> None:
        """50 samples measured 46.9 % anomalous -- better, and still wrong."""
        assert _mature(samples=50) is False

    def test_the_skip_reason_says_why(self) -> None:
        reason = admission_skip_reason(
            sample_count=50, running_ratio=1.0, machine_state="RUNNING", is_mature=False
        )

        assert reason is SkipReason.WINDOW_NOT_MATURE

    def test_an_immature_window_is_published_not_dropped(self) -> None:
        """The contract is unchanged: a window that cannot be scored is still
        emitted, with a reason. Silence and "nothing is wrong" must not look
        alike."""
        assert (
            admission_skip_reason(
                sample_count=50, running_ratio=1.0, machine_state="RUNNING", is_mature=False
            )
            is not None
        )


class TestCompleteWindowIsScored:
    """complete / mature window -> SCORED."""

    def test_a_full_window_is_mature(self) -> None:
        assert _mature(samples=60) is True

    def test_one_missing_sample_at_the_tail_is_tolerated(self) -> None:
        """A dropped reading must not disqualify an otherwise complete window;
        that is what the tolerance is for."""
        assert _mature(samples=59) is True

    def test_a_mature_window_passes_admission(self) -> None:
        assert (
            admission_skip_reason(
                sample_count=60, running_ratio=1.0, machine_state="RUNNING", is_mature=True
            )
            is None
        )

    def test_maturity_does_not_override_the_other_gates(self) -> None:
        """Maturity is added to the gate, not substituted for it: a mature
        window on a stopped machine is still refused, and for the older reason."""
        assert (
            admission_skip_reason(
                sample_count=60, running_ratio=0.5, machine_state="RUNNING", is_mature=True
            )
            is SkipReason.MACHINE_NOT_RUNNING
        )


class TestTheRuleAdaptsToTheSamplingRate:
    """Why coverage, and not a sample count or a fixed number of seconds."""

    def test_a_slow_machine_completes_its_window_with_few_samples(self) -> None:
        """One reading every 5 s fills a 60 s window with 12 samples. A rule
        counting samples would call this permanently immature; a rule with a
        threshold in seconds would need configuring per machine."""
        assert _mature(samples=12, step=5.0) is True

    def test_a_fast_machine_is_not_admitted_early_by_sheer_volume(self) -> None:
        """Ten readings a second reach 300 samples in half the window. Volume is
        not coverage, and a sample-count rule would admit this."""
        assert _mature(samples=300, step=0.1) is False

    def test_tolerance_is_expressed_in_the_machines_own_steps(self) -> None:
        # 55 samples at 1 Hz leaves a 5 s tail: outside two steps, inside six.
        assert _mature(samples=55) is False
        assert _mature(samples=55, tolerance_steps=6.0) is True


class TestEdgeCases:
    def test_exactly_the_minimum_sample_count_is_not_enough(self) -> None:
        """The two gates ask different questions. MIN_SAMPLES_FOR_SCORING asks
        how many readings there are; maturity asks where they are."""
        assert _mature(samples=MIN_SAMPLES_FOR_SCORING) is False
        assert (
            admission_skip_reason(
                sample_count=MIN_SAMPLES_FOR_SCORING,
                running_ratio=1.0,
                machine_state="RUNNING",
                is_mature=False,
            )
            is SkipReason.WINDOW_NOT_MATURE
        )

    def test_a_single_sample_spans_no_interval(self) -> None:
        """No cadence can be measured from one reading, so nothing may be
        concluded -- and concluding "mature" would be the dangerous direction."""
        assert (
            window_is_mature(
                window_start_epoch=ORIGIN,
                window_end_epoch=WINDOW_END,
                first_event_epoch=ORIGIN,
                last_event_epoch=ORIGIN,
                sample_count=1,
            )
            is False
        )

    def test_an_empty_window_is_immature(self) -> None:
        assert (
            window_is_mature(
                window_start_epoch=ORIGIN,
                window_end_epoch=WINDOW_END,
                first_event_epoch=None,
                last_event_epoch=None,
                sample_count=0,
            )
            is False
        )

    def test_simultaneous_readings_span_no_interval(self) -> None:
        """Many samples all bearing the same event time: a real possibility with
        a misconfigured producer, and no cadence to divide by."""
        assert (
            window_is_mature(
                window_start_epoch=ORIGIN,
                window_end_epoch=WINDOW_END,
                first_event_epoch=ORIGIN,
                last_event_epoch=ORIGIN,
                sample_count=40,
            )
            is False
        )


class TestOrderAndLateness:
    """Properties that make the rule safe under replay, restart and late data."""

    def test_arrival_order_cannot_change_the_verdict(self) -> None:
        """Maturity reads min and max event time, which are order-free
        aggregates. This is the property a watermark-based rule cannot have:
        the watermark depends on when data showed up, so the same input replayed
        with different batch boundaries would score a different set of windows.
        """
        in_order = window_is_mature(
            window_start_epoch=ORIGIN,
            window_end_epoch=WINDOW_END,
            first_event_epoch=ORIGIN,
            last_event_epoch=ORIGIN + 59.0,
            sample_count=60,
        )
        # Same window, same aggregates, whatever sequence produced them.
        shuffled = window_is_mature(
            window_start_epoch=ORIGIN,
            window_end_epoch=WINDOW_END,
            last_event_epoch=ORIGIN + 59.0,
            first_event_epoch=ORIGIN,
            sample_count=60,
        )

        assert in_order is shuffled is True

    def test_late_data_completing_the_tail_makes_a_window_mature(self) -> None:
        """A sample held back by a producer outage and delivered inside the
        watermark extends the coverage, and the window becomes scoreable. The
        verdict follows the data, not the clock."""
        before = _mature(samples=50)
        after = window_is_mature(
            window_start_epoch=ORIGIN,
            window_end_epoch=WINDOW_END,
            first_event_epoch=ORIGIN,
            last_event_epoch=ORIGIN + 59.0,
            sample_count=51,
        )

        assert before is False
        assert after is True

    def test_maturity_is_granted_and_never_revoked(self) -> None:
        """Late data can only extend a window's coverage, never shorten it, so a
        window that was scored cannot become unscoreable on a later update --
        which is what keeps the alert state machine from oscillating."""
        mature_now = _mature(samples=60)
        # A further late arrival inside the window can only sit at or before the
        # existing maximum, so last_event_epoch cannot go backwards.
        still_mature = window_is_mature(
            window_start_epoch=ORIGIN,
            window_end_epoch=WINDOW_END,
            first_event_epoch=ORIGIN,
            last_event_epoch=ORIGIN + 59.0,
            sample_count=61,
        )

        assert mature_now is still_mature is True

    def test_a_window_whose_machine_stopped_never_matures(self) -> None:
        """The machine fell silent 25 s before the window closed. The global
        watermark keeps advancing on other machines' traffic and would happily
        declare this window "past"; its own coverage says the data is missing,
        and that is the answer that matters."""
        assert _mature(samples=35) is False


class TestThroughSpark:
    """The rule is fed by a Spark aggregation, so the wiring is tested too.

    The pure-Python cases above prove the rule; they prove nothing about whether
    ``aggregate_windows`` actually hands it the right two columns. A mistake
    there would be invisible: every window would simply look complete.
    """

    @staticmethod
    def _windows(spark: Any, samples: pd.DataFrame) -> pd.DataFrame:
        aggregated = aggregate_windows(
            prepare_signals(spark.createDataFrame(samples)),
            window_duration="60 seconds",
            slide_duration="60 seconds",
            watermark_delay="0 seconds",
        )
        derived = derive_features(aggregated).toPandas()
        derived["verdict"] = [
            window_is_mature(
                window_start_epoch=start,
                window_end_epoch=end,
                first_event_epoch=first,
                last_event_epoch=last,
                sample_count=int(count),
            )
            for start, end, first, last, count in zip(
                _epoch_seconds(derived["window_start"]),
                _epoch_seconds(derived["window_end"]),
                _epoch_seconds(derived["event_time_first"]),
                _epoch_seconds(derived["event_time_last"]),
                derived["sample_count"],
                strict=True,
            )
        ]
        return derived

    def test_a_window_cut_short_is_rejected_end_to_end(self, spark: Any) -> None:
        """Half a window of real samples, aggregated by Spark, must come out
        immature -- the measured 100 %-anomalous case, refused at the gate."""
        samples = make_samples(seconds=30, seed=7)

        windows = self._windows(spark, samples)

        assert len(windows) > 0
        assert not windows["verdict"].any()

    def test_a_full_window_is_accepted_end_to_end(self, spark: Any) -> None:
        samples = make_samples(seconds=60, seed=7)

        windows = self._windows(spark, samples)

        assert len(windows) > 0
        assert windows["verdict"].all()

    def test_shuffling_the_input_changes_nothing(self, spark: Any) -> None:
        """min and max over event time are order-free, so a different partition
        layout cannot change which windows get scored. Without this the fix
        would reintroduce exactly the non-determinism D-26 removed."""
        samples = make_samples(seconds=60, seed=7)
        shuffled = samples.sample(frac=1.0, random_state=99).reset_index(drop=True)

        ordered = self._windows(spark, samples).sort_values("machine_id")
        mixed = self._windows(spark, shuffled).sort_values("machine_id")

        assert list(ordered["verdict"]) == list(mixed["verdict"])

    def test_the_coverage_columns_are_not_features(self, spark: Any) -> None:
        """They must never reach the model: the artefact is verified against
        FEATURE_NAMES, and the 52 features stay the trailing columns."""
        windows = self._windows(spark, make_samples(seconds=60, seed=7))

        assert "event_time_first" not in FEATURE_NAMES
        assert "event_time_last" not in FEATURE_NAMES
        columns = [c for c in windows.columns if c != "verdict"]
        assert columns[-len(FEATURE_NAMES) :] == list(FEATURE_NAMES)


class TestHeadTruncationIsTheColdStartStorm:
    """A window the stream entered part-way through is not complete either.

    Found by measurement, not by reasoning. A tail-only rule left every
    machine's opening windows scoreable: 15 machines all alerted CRITICAL on the
    same ``window_start``, each holding exactly 44 samples, because the run began
    15 seconds into that window. One synchronised false storm at every cold
    start.
    """

    def test_a_window_missing_its_beginning_is_immature(self) -> None:
        """44 samples of a 60 s window, all at the end: the exact measured case."""
        assert _mature(samples=44, first=ORIGIN + 16.0) is False

    def test_the_last_partly_covered_opening_window_is_also_refused(self) -> None:
        """55 samples with a 5 s head gap -- the third opening window, which a
        tail-only rule admitted and which scored 19.5 % anomalous."""
        assert _mature(samples=55, first=ORIGIN + 5.0) is False

    def test_the_first_fully_covered_window_is_accepted(self) -> None:
        """Once the stream has run a full window, coverage is complete and
        scoring resumes. The cold start costs windows, not the whole stream."""
        assert _mature(samples=60, first=ORIGIN) is True

    def test_a_head_gap_within_the_machines_cadence_is_fine(self) -> None:
        """A slow machine's first reading legitimately sits one step in."""
        assert _mature(samples=12, step=5.0, first=ORIGIN + 4.0) is True

    def test_both_ends_are_checked_not_just_the_worse_one(self) -> None:
        head_only = _mature(samples=50, first=ORIGIN + 10.0)  # covers to the end
        tail_only = _mature(samples=50, first=ORIGIN)  # covers from the start
        assert head_only is False
        assert tail_only is False
