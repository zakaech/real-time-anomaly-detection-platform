"""Calibration, threshold selection, and the operator-facing metrics."""

from __future__ import annotations

import numpy as np
import numpy.typing as npt
import pandas as pd
import pytest

from conftest import ORIGIN, make_labels, make_samples
from ml_training.calibration import EcdfCalibrator
from ml_training.config.settings import TrainingSettings
from ml_training.dataset.labels import align_window_labels, build_episode_table
from ml_training.evaluation import (
    OperatingPoint,
    alert_events,
    episode_outcomes,
    select_threshold_for_budget,
    window_metrics,
)
from ml_training.features.pandas_builder import build_window_features
from ml_training.models.detectors import IsolationForestDetector
from ml_training.training import build_pipeline, prepare_splits


class TestCalibration:
    def test_it_maps_training_scores_onto_a_uniform_scale(self) -> None:
        """The point of the ECDF: a quantile becomes an alert budget."""
        rng = np.random.default_rng(0)
        raw = rng.normal(10.0, 3.0, size=20_000)
        calibrator = EcdfCalibrator.fit(raw)
        calibrated = calibrator.transform(raw)

        assert calibrated.min() >= 0.0
        assert calibrated.max() <= 1.0
        # Roughly 0.5 % of the reference distribution sits above 0.995.
        assert float(np.mean(calibrated >= 0.995)) == pytest.approx(0.005, abs=0.002)

    def test_it_is_monotonic(self) -> None:
        rng = np.random.default_rng(1)
        calibrator = EcdfCalibrator.fit(rng.normal(size=5_000))
        probe = np.linspace(-5.0, 5.0, 200)
        calibrated = calibrator.transform(probe)
        assert np.all(np.diff(calibrated) >= -1e-12)

    def test_scores_beyond_the_reference_are_clamped_not_extrapolated(self) -> None:
        """Beyond everything seen is "at least as extreme as everything"."""
        calibrator = EcdfCalibrator.fit(np.random.default_rng(2).normal(size=1_000))
        assert calibrator.transform(np.array([1e6]))[0] == 1.0
        assert calibrator.transform(np.array([-1e6]))[0] == 0.0

    def test_two_models_become_comparable(self) -> None:
        """Different raw scales, same meaning after calibration."""
        rng = np.random.default_rng(3)
        first = rng.normal(0.0, 1.0, size=10_000)
        second = rng.normal(500.0, 250.0, size=10_000)

        first_calibrated = EcdfCalibrator.fit(first).transform(first)
        second_calibrated = EcdfCalibrator.fit(second).transform(second)
        assert float(np.mean(first_calibrated >= 0.99)) == pytest.approx(
            float(np.mean(second_calibrated >= 0.99)), abs=0.005
        )

    def test_nan_stays_nan(self) -> None:
        """A missing score must not be read as a low one."""
        calibrator = EcdfCalibrator.fit(np.random.default_rng(4).normal(size=100))
        assert np.isnan(calibrator.transform(np.array([np.nan]))[0])

    def test_it_round_trips_through_json(self) -> None:
        calibrator = EcdfCalibrator.fit(np.random.default_rng(5).normal(size=1_000))
        restored = EcdfCalibrator.from_dict(calibrator.to_dict())
        probe = np.linspace(-3.0, 3.0, 50)
        np.testing.assert_allclose(calibrator.transform(probe), restored.transform(probe))

    def test_an_empty_sample_is_refused(self) -> None:
        with pytest.raises(ValueError, match="empty"):
            EcdfCalibrator.fit(np.array([]))


class TestAlertEvents:
    def _frame(self, scores: list[float], *, slide: int = 10) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "machine_id": ["M-001"] * len(scores),
                "window_start": [index * slide for index in range(len(scores))],
                "score": scores,
            }
        )

    def test_consecutive_windows_collapse_into_one_event(self) -> None:
        """One degradation is one alert, not the eighty windows it spans."""
        frame = self._frame([0.1, 0.99, 0.99, 0.99, 0.1, 0.1])
        events = alert_events(frame, threshold=0.9, slide_seconds=10)
        assert len(events) == 1
        assert int(events.iloc[0]["windows"]) == 3

    def test_a_lone_window_is_not_an_alert(self) -> None:
        """Hysteresis: a single window above the threshold is usually noise."""
        frame = self._frame([0.1, 0.99, 0.1, 0.1])
        assert alert_events(frame, threshold=0.9, slide_seconds=10).empty

    def test_a_gap_splits_two_events(self) -> None:
        frame = self._frame([0.99, 0.99, 0.1, 0.1, 0.99, 0.99])
        assert len(alert_events(frame, threshold=0.9, slide_seconds=10)) == 2

    def test_events_are_counted_per_machine(self) -> None:
        frame = pd.DataFrame(
            {
                "machine_id": ["M-001", "M-001", "M-002", "M-002"],
                "window_start": [0, 10, 0, 10],
                "score": [0.99, 0.99, 0.99, 0.99],
            }
        )
        assert len(alert_events(frame, threshold=0.9, slide_seconds=10)) == 2


class TestThresholdSelection:
    def _points(self) -> list[OperatingPoint]:
        return [
            OperatingPoint(
                quantile=quantile,
                threshold=quantile,
                alert_windows=0,
                alert_events=0,
                alert_events_per_hour=rate * 10,
                alert_events_per_machine_hour=rate,
                window_precision=0.1,
                window_recall=recall,
                window_f1=0.2,
                episode_recall=recall,
                episodes_detected=0,
                episodes_total=10,
            )
            for quantile, rate, recall in (
                (0.95, 4.0, 0.95),
                (0.99, 1.2, 0.80),
                (0.995, 0.4, 0.60),
                (0.999, 0.05, 0.30),
            )
        ]

    def test_it_takes_the_most_permissive_affordable_point(self) -> None:
        """Within the budget, a lower threshold catches more; raising it trades
        recall for a reduction nobody asked for."""
        chosen = select_threshold_for_budget(self._points(), budget_per_machine_hour=0.5)
        assert chosen.quantile == 0.995

    def test_a_generous_budget_selects_a_lower_threshold(self) -> None:
        chosen = select_threshold_for_budget(self._points(), budget_per_machine_hour=2.0)
        assert chosen.quantile == 0.99

    def test_an_unattainable_budget_falls_back_to_the_strictest(self) -> None:
        """Reported as unattainable rather than silently overshooting it."""
        chosen = select_threshold_for_budget(self._points(), budget_per_machine_hour=0.001)
        assert chosen.quantile == 0.999


class TestWindowMetrics:
    def test_confusion_counts_add_up(self) -> None:
        truth = np.array([True, True, False, False, False])
        scores = np.array([0.9, 0.1, 0.95, 0.2, 0.3])
        metrics = window_metrics(truth, scores, 0.5)
        assert (metrics.true_positives, metrics.false_positives) == (1, 1)
        assert (metrics.true_negatives, metrics.false_negatives) == (2, 1)
        assert metrics.precision == pytest.approx(0.5)
        assert metrics.recall == pytest.approx(0.5)
        assert metrics.f1 == pytest.approx(0.5)

    def test_no_prediction_gives_zero_precision_not_a_crash(self) -> None:
        truth = np.array([True, False])
        metrics = window_metrics(truth, np.array([0.1, 0.1]), 0.9)
        assert metrics.precision == 0.0
        assert metrics.recall == 0.0


class TestEpisodeOutcomes:
    def test_an_episode_is_detected_when_any_covering_window_fires(self) -> None:
        scored = pd.DataFrame(
            {
                "machine_id": ["M-001"] * 3,
                "window_start": [0, 10, 20],
                "window_end_time": pd.to_datetime(
                    [ORIGIN + pd.Timedelta(seconds=s) for s in (60, 70, 80)], utc=True
                ),
                "score": [0.1, 0.99, 0.2],
                "episode_id": ["ep-1"] * 3,
            }
        )
        episodes = pd.DataFrame(
            {
                "episode_id": ["ep-1"],
                "machine_id": ["M-001"],
                "anomaly_type": ["OVERHEAT"],
                "started_at": [ORIGIN],
                "ended_at": [ORIGIN + pd.Timedelta(seconds=30)],
                "duration_seconds": [30.0],
            }
        )
        outcomes = episode_outcomes(scored, episodes, threshold=0.9)
        assert len(outcomes) == 1
        assert outcomes[0].detected
        assert outcomes[0].detection_latency_seconds == pytest.approx(70.0)

    def test_an_undetected_episode_has_no_latency(self) -> None:
        scored = pd.DataFrame(
            {
                "machine_id": ["M-001"],
                "window_start": [0],
                "window_end_time": pd.to_datetime([ORIGIN], utc=True),
                "score": [0.1],
                "episode_id": ["ep-1"],
            }
        )
        episodes = pd.DataFrame(
            {
                "episode_id": ["ep-1"],
                "machine_id": ["M-001"],
                "anomaly_type": ["OVERHEAT"],
                "started_at": [ORIGIN],
                "ended_at": [ORIGIN + pd.Timedelta(seconds=30)],
                "duration_seconds": [30.0],
            }
        )
        outcome = episode_outcomes(scored, episodes, threshold=0.9)[0]
        assert not outcome.detected
        assert outcome.detection_latency_seconds is None


class TestLabelAlignment:
    def test_a_window_below_the_fraction_is_not_anomalous(self) -> None:
        """A short spike in a sixty-second window barely moves the aggregates."""
        # 600 s so the second, deliberately short episode exists at all.
        samples = make_samples(seconds=600, machines=("M-002",))
        features = build_window_features(samples, window_seconds=60, slide_seconds=60)
        labels = align_window_labels(
            make_labels(samples),
            features,
            window_seconds=60,
            slide_seconds=60,
            min_anomaly_fraction=0.9,
        )
        partial = labels.loc[(labels["anomaly_fraction"] > 0) & (labels["anomaly_fraction"] < 0.9)]
        assert not partial.empty
        assert not partial["is_anomaly"].any()

    def test_windows_without_a_label_are_normal(self) -> None:
        samples = make_samples(seconds=300)
        features = build_window_features(samples, window_seconds=60, slide_seconds=60)
        labels = align_window_labels(
            make_labels(samples),
            features,
            window_seconds=60,
            slide_seconds=60,
            min_anomaly_fraction=0.25,
        )
        untouched = labels.loc[labels["episode_id"].isna()]
        assert not untouched.empty
        assert not untouched["is_anomaly"].any()

    def test_episode_table_summarises_each_occurrence(self) -> None:
        samples = make_samples(seconds=600)
        episodes = build_episode_table(make_labels(samples))
        assert set(episodes["episode_id"]) == {"ep-test-001", "ep-test-002"}
        assert (episodes["duration_seconds"] > 0).all()


class TestDeterminism:
    def test_the_same_seed_gives_the_same_scores(self) -> None:
        samples = make_samples(seconds=900)
        settings = TrainingSettings(
            train_end_hours=0.06, validation_end_hours=0.11, window_seconds=30, slide_seconds=10
        )
        splits = prepare_splits(samples, make_labels(samples), settings)

        def score() -> npt.NDArray[np.float64]:
            pipeline = build_pipeline(
                IsolationForestDetector(n_estimators=80, max_samples=128, random_state=42), seed=42
            )
            pipeline.fit(splits.train.X)
            return np.asarray(pipeline.score_samples(splits.validation.X))

        np.testing.assert_array_equal(score(), score())

    def test_a_different_seed_gives_different_scores(self) -> None:
        samples = make_samples(seconds=900)
        settings = TrainingSettings(
            train_end_hours=0.06, validation_end_hours=0.11, window_seconds=30, slide_seconds=10
        )
        splits = prepare_splits(samples, make_labels(samples), settings)

        def score(seed: int) -> npt.NDArray[np.float64]:
            pipeline = build_pipeline(
                IsolationForestDetector(n_estimators=80, max_samples=128, random_state=seed),
                seed=seed,
            )
            pipeline.fit(splits.train.X)
            return np.asarray(pipeline.score_samples(splits.validation.X))

        assert not np.array_equal(score(1), score(2))
