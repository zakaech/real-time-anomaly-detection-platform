"""Assigning samples to windows.

Sliding windows are built by expansion: a sample belongs to every window that
covers it, so with a 60 s window and a 10 s slide each sample appears in six
rows. That is the same relation Spark's ``window(event_time, "60 seconds",
"10 seconds")`` expresses, written explicitly so the pandas and Spark
translations can be compared row for row.

Tumbling windows are the same function with ``slide == window``. Training uses
them because overlapping windows are near-duplicates: they multiply the row
count sixfold without adding information, and they would bias the calibration
towards whatever the overlapping region happens to contain.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

__all__ = ["NANOS_PER_SECOND", "assign_windows", "epoch_seconds"]

NANOS_PER_SECOND = 1_000_000_000


def epoch_seconds(times: pd.Series) -> pd.Series:
    """Convert a UTC timestamp column to whole epoch seconds."""
    return (times.astype("int64") // NANOS_PER_SECOND).astype("int64")


def assign_windows(
    samples: pd.DataFrame,
    *,
    window_seconds: int,
    slide_seconds: int,
    time_column: str = "event_time",
) -> pd.DataFrame:
    """Return one row per (sample, covering window).

    Window starts are aligned on multiples of ``slide_seconds`` from the epoch,
    not on the first sample of the dataset. Anchoring on the data would make the
    window boundaries depend on when a run happened to begin, and two exports of
    the same history would produce different windows.

    Raises:
        ValueError: if the window is not a whole multiple of the slide, which
            would leave samples covered by an inconsistent number of windows.
    """
    if window_seconds % slide_seconds != 0:
        raise ValueError(
            f"window_seconds ({window_seconds}) must be a whole multiple of "
            f"slide_seconds ({slide_seconds})"
        )
    if samples.empty:
        return samples.assign(window_start=pd.Series(dtype="int64"))

    occurrences = window_seconds // slide_seconds
    seconds = epoch_seconds(samples[time_column]).to_numpy()
    base = (seconds // slide_seconds) * slide_seconds

    row_count = len(samples)
    positions = np.tile(np.arange(row_count), occurrences)
    shifts = np.repeat(np.arange(occurrences) * slide_seconds, row_count)

    expanded = samples.iloc[positions].copy()
    expanded["window_start"] = np.tile(base, occurrences) - shifts
    return expanded
