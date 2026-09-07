"""Metrics, at two levels, plus the operator's view.

Window-level precision, recall and F1 give the precision-recall curve. They are
not, on their own, the right way to judge this system: a five-second spike
inside a sixty-second window barely moves the aggregates, so window recall
understates detection of short events and overstates nothing.

Episode-level recall is what an exploitant asks for -- was the failure caught,
and how early. And the operating point is chosen from a third quantity
altogether: **how many alerts land on a human per hour**. A threshold that
maximises F1 can be unusable if it produces an alert every two minutes, so both
are reported and the gap between them is part of the result.

An alert event is a contiguous run of windows above the threshold on one
machine, and a run must last at least two windows to count (the hysteresis of
decision D-12). One degradation therefore counts as one alert, not as the eighty
windows it spans -- which is what an operator actually receives.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any

import numpy as np
import numpy.typing as npt
import pandas as pd
from sklearn.metrics import average_precision_score, precision_recall_curve

__all__ = [
    "EpisodeOutcome",
    "OperatingPoint",
    "WindowMetrics",
    "alert_events",
    "confusion_counts",
    "episode_outcomes",
    "false_positive_profile",
    "missed_episode_profile",
    "operating_points",
    "precision_recall_points",
    "select_threshold_for_budget",
    "window_metrics",
]

#: Runs shorter than this are not reported as alerts: a single window above the
#: threshold is usually noise, and the streaming policy will require the same
#: confirmation.
MIN_CONSECUTIVE_WINDOWS = 2


@dataclass
class WindowMetrics:
    threshold: float
    true_positives: int
    false_positives: int
    true_negatives: int
    false_negatives: int
    precision: float
    recall: float
    f1: float
    average_precision: float

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class EpisodeOutcome:
    episode_id: str
    machine_id: str
    anomaly_type: str | None
    duration_seconds: float
    scorable_windows: int
    max_score: float
    detected: bool
    detection_latency_seconds: float | None


@dataclass
class OperatingPoint:
    quantile: float
    threshold: float
    alert_windows: int
    alert_events: int
    alert_events_per_hour: float
    alert_events_per_machine_hour: float
    window_precision: float
    window_recall: float
    window_f1: float
    episode_recall: float
    episodes_detected: int
    episodes_total: int
    median_detection_latency_seconds: float | None = None
    extras: dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


def confusion_counts(
    y_true: npt.NDArray[Any], predicted: npt.NDArray[Any]
) -> tuple[int, int, int, int]:
    truth = np.asarray(y_true, dtype=bool)
    flagged = np.asarray(predicted, dtype=bool)
    return (
        int(np.sum(truth & flagged)),
        int(np.sum(~truth & flagged)),
        int(np.sum(~truth & ~flagged)),
        int(np.sum(truth & ~flagged)),
    )


def window_metrics(
    y_true: npt.NDArray[Any], scores: npt.NDArray[Any], threshold: float
) -> WindowMetrics:
    """Precision, recall, F1 and PR-AUC at one threshold.

    Average precision rather than ROC-AUC: with a small minority of anomalous
    windows, the false-positive rate barely moves when the negatives dominate,
    so ROC-AUC flatters even a poor model (docs/07 section 4.1).
    """
    truth = np.asarray(y_true, dtype=bool)
    flagged = np.asarray(scores, dtype="float64") >= threshold
    tp, fp, tn, fn = confusion_counts(truth, flagged)

    precision = tp / (tp + fp) if (tp + fp) else 0.0
    recall = tp / (tp + fn) if (tp + fn) else 0.0
    f1 = 2 * precision * recall / (precision + recall) if (precision + recall) else 0.0
    average_precision = (
        float(average_precision_score(truth, np.nan_to_num(scores, nan=0.0)))
        if truth.any() and (~truth).any()
        else float("nan")
    )
    return WindowMetrics(
        threshold=float(threshold),
        true_positives=tp,
        false_positives=fp,
        true_negatives=tn,
        false_negatives=fn,
        precision=float(precision),
        recall=float(recall),
        f1=float(f1),
        average_precision=average_precision,
    )


def precision_recall_points(
    y_true: npt.NDArray[Any], scores: npt.NDArray[Any], *, max_points: int = 400
) -> dict[str, list[float]]:
    """Precision-recall curve, thinned to a size a report can carry."""
    truth = np.asarray(y_true, dtype=bool)
    if not truth.any() or not (~truth).any():
        return {"precision": [], "recall": [], "threshold": []}

    precision, recall, thresholds = precision_recall_curve(truth, np.nan_to_num(scores, nan=0.0))
    # precision_recall_curve returns one more precision/recall than thresholds.
    precision, recall = precision[:-1], recall[:-1]
    if precision.size > max_points:
        keep = np.linspace(0, precision.size - 1, max_points).astype(int)
        precision, recall, thresholds = precision[keep], recall[keep], thresholds[keep]
    return {
        "precision": precision.tolist(),
        "recall": recall.tolist(),
        "threshold": thresholds.tolist(),
    }


def alert_events(frame: pd.DataFrame, *, threshold: float, slide_seconds: int) -> pd.DataFrame:
    """Group consecutive above-threshold windows into alert events, per machine.

    Returns one row per event with its machine, span and peak score. A run of
    fewer than :data:`MIN_CONSECUTIVE_WINDOWS` windows is dropped: a lone window
    above the threshold is usually noise, and reporting it would inflate the
    alert rate an operator is told to expect.
    """
    flagged = frame.loc[frame["score"] >= threshold, ["machine_id", "window_start", "score"]]
    if flagged.empty:
        return pd.DataFrame(
            columns=["machine_id", "first_window", "last_window", "windows", "max_score"]
        )

    flagged = flagged.sort_values(["machine_id", "window_start"])
    gap = flagged.groupby("machine_id")["window_start"].diff()
    starts_run = gap.isna() | (gap != slide_seconds)
    flagged = flagged.assign(run_id=starts_run.cumsum())

    grouped = flagged.groupby("run_id")
    events = pd.DataFrame(
        {
            "machine_id": grouped["machine_id"].first(),
            "first_window": grouped["window_start"].min(),
            "last_window": grouped["window_start"].max(),
            "windows": grouped.size(),
            "max_score": grouped["score"].max(),
        }
    ).reset_index(drop=True)
    return events.loc[events["windows"] >= MIN_CONSECUTIVE_WINDOWS].reset_index(drop=True)


def episode_outcomes(
    scored: pd.DataFrame, episodes: pd.DataFrame, *, threshold: float
) -> list[EpisodeOutcome]:
    """Was each episode caught, and how long after it started.

    Episodes with no scorable window are excluded by the caller rather than
    counted as missed: a fault occurring entirely while a machine is stopped is
    undetectable by design, and charging it to the model would be dishonest in
    the flattering direction for whoever chose the admission rule.
    """
    if episodes.empty:
        return []

    outcomes: list[EpisodeOutcome] = []
    by_episode = scored.loc[scored["episode_id"].notna()].groupby("episode_id")
    # dict(by_episode) would fail: a pandas GroupBy exposes `.keys` as the
    # grouping column name, so dict() dispatches through it and calls a string.
    grouped: dict[Any, pd.DataFrame] = dict(iter(by_episode))

    for row in episodes.itertuples(index=False):
        windows = grouped.get(row.episode_id)
        if windows is None or windows.empty:
            outcomes.append(
                EpisodeOutcome(
                    episode_id=str(row.episode_id),
                    machine_id=str(row.machine_id),
                    anomaly_type=None if row.anomaly_type is None else str(row.anomaly_type),
                    duration_seconds=float(row.duration_seconds),  # type: ignore[arg-type]
                    scorable_windows=0,
                    max_score=float("nan"),
                    detected=False,
                    detection_latency_seconds=None,
                )
            )
            continue

        above = windows.loc[windows["score"] >= threshold]
        latency: float | None = None
        if not above.empty:
            first_alert = above["window_end_time"].min()
            latency = float((first_alert - row.started_at).total_seconds())

        outcomes.append(
            EpisodeOutcome(
                episode_id=str(row.episode_id),
                machine_id=str(row.machine_id),
                anomaly_type=None if row.anomaly_type is None else str(row.anomaly_type),
                duration_seconds=float(row.duration_seconds),  # type: ignore[arg-type]
                scorable_windows=len(windows),
                max_score=float(windows["score"].max()),
                detected=bool(not above.empty),
                detection_latency_seconds=latency,
            )
        )
    return outcomes


def _duration_hours(frame: pd.DataFrame) -> float:
    span = float(frame["window_start"].max()) - float(frame["window_start"].min())
    return max(span / 3600.0, 1e-9)


def operating_points(
    scored: pd.DataFrame,
    episodes: pd.DataFrame,
    *,
    quantiles: list[float],
    slide_seconds: int,
) -> list[OperatingPoint]:
    """Sweep the threshold and report what each choice costs and buys."""
    scores = scored["score"].to_numpy(dtype="float64")
    truth = scored["is_anomaly"].to_numpy(dtype=bool)
    hours = _duration_hours(scored)
    machines = max(int(scored["machine_id"].nunique()), 1)
    scorable_episodes = episodes.loc[
        episodes["episode_id"].isin(set(scored["episode_id"].dropna()))
    ]

    points: list[OperatingPoint] = []
    for quantile in quantiles:
        threshold = float(np.quantile(scores[np.isfinite(scores)], quantile))
        metrics = window_metrics(truth, scores, threshold)
        events = alert_events(scored, threshold=threshold, slide_seconds=slide_seconds)
        outcomes = episode_outcomes(scored, scorable_episodes, threshold=threshold)
        detected = [outcome for outcome in outcomes if outcome.detected]
        latencies = [
            outcome.detection_latency_seconds
            for outcome in detected
            if outcome.detection_latency_seconds is not None
        ]

        points.append(
            OperatingPoint(
                quantile=float(quantile),
                threshold=threshold,
                alert_windows=int(np.sum(scores >= threshold)),
                alert_events=len(events),
                alert_events_per_hour=float(len(events) / hours),
                alert_events_per_machine_hour=float(len(events) / hours / machines),
                window_precision=metrics.precision,
                window_recall=metrics.recall,
                window_f1=metrics.f1,
                episode_recall=float(len(detected) / len(outcomes)) if outcomes else 0.0,
                episodes_detected=len(detected),
                episodes_total=len(outcomes),
                median_detection_latency_seconds=(
                    float(np.median(latencies)) if latencies else None
                ),
            )
        )
    return points


def select_threshold_for_budget(
    points: list[OperatingPoint], *, budget_per_machine_hour: float
) -> OperatingPoint:
    """The most permissive threshold that still fits the operator's budget.

    Most permissive on purpose: within the budget, a lower threshold catches
    more. Raising it further would trade recall for an alert rate nobody asked
    to reduce.
    """
    affordable = [
        point for point in points if point.alert_events_per_machine_hour <= budget_per_machine_hour
    ]
    if not affordable:
        # Nothing fits: return the strictest point and let the caller report
        # that the budget is unattainable rather than silently overshoot it.
        return max(points, key=lambda point: point.quantile)
    return min(affordable, key=lambda point: point.quantile)


def false_positive_profile(scored: pd.DataFrame, *, threshold: float) -> dict[str, Any]:
    """Where the false alarms come from: which machines, which hours, which state."""
    false_positives = scored.loc[(scored["score"] >= threshold) & (~scored["is_anomaly"])]
    if false_positives.empty:
        return {"count": 0, "by_machine": {}, "by_hour_utc": {}, "by_machine_state": {}}

    hours = false_positives["window_start_time"].dt.hour
    return {
        "count": len(false_positives),
        "share_of_alerts": float(
            len(false_positives) / max(int((scored["score"] >= threshold).sum()), 1)
        ),
        "by_machine": dict(
            false_positives["machine_id"].value_counts().head(10).astype(int).items()
        ),
        "by_hour_utc": {str(k): int(v) for k, v in hours.value_counts().sort_index().items()},
        "by_machine_state": dict(
            false_positives["machine_state"].value_counts().astype(int).items()
        ),
        "median_anomaly_fraction": float(false_positives["anomaly_fraction"].median()),
    }


def missed_episode_profile(outcomes: list[EpisodeOutcome]) -> dict[str, Any]:
    """What the detector misses, grouped by anomaly type and duration."""
    missed = [outcome for outcome in outcomes if not outcome.detected]
    if not missed:
        return {"count": 0, "by_type": {}, "median_duration_seconds": None}

    frame = pd.DataFrame([asdict(outcome) for outcome in missed])
    detected_frame = pd.DataFrame([asdict(outcome) for outcome in outcomes if outcome.detected])
    by_type: dict[str, dict[str, int]] = {}
    for anomaly_type in sorted({str(outcome.anomaly_type) for outcome in outcomes}):
        total = sum(1 for outcome in outcomes if str(outcome.anomaly_type) == anomaly_type)
        missed_count = sum(1 for outcome in missed if str(outcome.anomaly_type) == anomaly_type)
        by_type[anomaly_type] = {
            "total": total,
            "missed": missed_count,
            "detected": total - missed_count,
        }

    return {
        "count": len(missed),
        "by_type": by_type,
        "median_duration_seconds": float(frame["duration_seconds"].median()),
        "median_duration_detected_seconds": (
            float(detected_frame["duration_seconds"].median()) if not detected_frame.empty else None
        ),
        "median_max_score": float(frame["max_score"].median()),
    }
