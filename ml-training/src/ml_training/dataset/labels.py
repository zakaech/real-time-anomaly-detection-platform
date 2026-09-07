"""Aligning ground truth with windows -- after the features are built, never before.

The join happens here and only here. Features are computed from
``telemetry.raw`` alone; this module attaches labels to the result by
``(machine_id, window_start)``. The two frames are returned separately and are
never concatenated, so no code path can accidentally hand a label to an
estimator.

Two levels of truth come out, because they answer different questions:

* **Window level** -- needed for a precision-recall curve. A window counts as
  anomalous when at least ``min_anomaly_fraction`` of its samples are. Labelling
  on any overlap would mark a window containing a two-second spike out of sixty
  as anomalous, and a model that misses it would be penalised for failing to
  detect something its aggregates barely register.
* **Episode level** -- what an operator actually cares about. Nobody asks
  whether 43 of 60 anomalous points were flagged; they ask whether the bearing
  failure was caught, and how early.
"""

from __future__ import annotations

import pandas as pd

from ml_training.dataset.windows import assign_windows

__all__ = ["align_window_labels", "build_episode_table"]


def align_window_labels(
    labels: pd.DataFrame,
    windows: pd.DataFrame,
    *,
    window_seconds: int,
    slide_seconds: int,
    min_anomaly_fraction: float,
) -> pd.DataFrame:
    """Attach ground truth to windows, keyed by ``(machine_id, window_start)``.

    Returns:
        One row per window of ``windows``, carrying ``anomalous_samples``,
        ``anomaly_fraction``, ``is_anomaly``, and the dominant ``episode_id`` and
        ``anomaly_type``. Windows with no matching label get zero and ``False``:
        the simulator emits labels only for anomalous samples, so absence is the
        definition of normal.
    """
    key = ["machine_id", "window_start"]
    base = windows[[*key, "sample_count"]].copy()

    if labels.empty:
        base["anomalous_samples"] = 0.0
        base["episode_id"] = None
        base["anomaly_type"] = None
    else:
        anomalous = labels.loc[labels["is_anomaly"]].copy()
        expanded = assign_windows(
            anomalous, window_seconds=window_seconds, slide_seconds=slide_seconds
        )
        grouped = expanded.groupby(key, sort=True)
        summary = pd.DataFrame(
            {
                "anomalous_samples": grouped.size().astype("float64"),
                # An episode never overlaps another on the same machine (the
                # scheduler retires one before starting the next), so "first" is
                # unambiguous rather than a silent pick among several.
                "episode_id": grouped["episode_id"].first(),
                "anomaly_type": grouped["anomaly_type"].first(),
            }
        ).reset_index()
        base = base.merge(summary, on=key, how="left")
        base["anomalous_samples"] = base["anomalous_samples"].fillna(0.0)

    base["anomaly_fraction"] = (base["anomalous_samples"] / base["sample_count"]).where(
        base["sample_count"] > 0, 0.0
    )
    base["is_anomaly"] = base["anomaly_fraction"] >= min_anomaly_fraction
    return base.drop(columns=["sample_count"])


def build_episode_table(labels: pd.DataFrame) -> pd.DataFrame:
    """Summarise each anomaly occurrence: machine, type, span, sample count.

    The span is derived from the labelled samples rather than from the episode's
    declared end, because the declared end is not published and the observable
    truth is what the evaluation must use.
    """
    if labels.empty:
        return pd.DataFrame(
            columns=[
                "episode_id",
                "machine_id",
                "anomaly_type",
                "started_at",
                "ended_at",
                "labelled_samples",
                "duration_seconds",
            ]
        )

    anomalous = labels.loc[labels["is_anomaly"] & labels["episode_id"].notna()]
    grouped = anomalous.groupby("episode_id", sort=True)
    episodes = pd.DataFrame(
        {
            "machine_id": grouped["machine_id"].first(),
            "anomaly_type": grouped["anomaly_type"].first(),
            "started_at": grouped["event_time"].min(),
            "ended_at": grouped["event_time"].max(),
            "labelled_samples": grouped.size(),
        }
    ).reset_index()
    episodes["duration_seconds"] = (
        episodes["ended_at"] - episodes["started_at"]
    ).dt.total_seconds()
    return episodes
