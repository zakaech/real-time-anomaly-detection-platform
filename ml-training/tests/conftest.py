"""Fixtures: a small synthetic history that exercises the awkward cases.

Deliberately includes missing readings, a frozen sensor and a short window, so
the conformance and imputation tests meet the situations that actually differ
between engines rather than only the easy path.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
from telemetry_core.schemas import SENSOR_NAMES

_REPO_ROOT = Path(__file__).resolve().parents[2]

ORIGIN = datetime(2026, 4, 1, 6, 0, 0, tzinfo=UTC)
MACHINES = ("M-001", "M-002", "M-003")


@pytest.fixture(scope="session")
def repo_root() -> Path:
    return _REPO_ROOT


def make_samples(
    *,
    seconds: int = 600,
    machines: tuple[str, ...] = MACHINES,
    seed: int = 7,
    null_probability: float = 0.02,
    freeze_vibration: bool = True,
) -> pd.DataFrame:
    """A coherent-enough sample frame; realism is the simulator's job, not this one."""
    rng = np.random.default_rng(seed)
    rows: list[dict[str, object]] = []

    for machine_index, machine_id in enumerate(machines):
        load = 0.6
        for second in range(seconds):
            load = float(np.clip(load + rng.normal(0.0, 0.02), 0.05, 1.0))
            rpm = 1400.0 + 120.0 * load + rng.normal(0.0, 3.0) + 40.0 * machine_index
            power = 1.0 + 0.0065 * rpm * (0.6 + 0.4 * load) + rng.normal(0.0, 0.1)
            vibration = 0.5 + 1.2 * load + rng.normal(0.0, 0.05)
            pressure = 3.5 + 2.4 * load + rng.normal(0.0, 0.04)
            temperature = 21.0 + 4.0 * power + rng.normal(0.0, 0.3)

            # A frozen vibration sensor for one machine over one stretch: zero
            # variance, which is where standard deviation, z-score and
            # correlation all become undefined.
            if freeze_vibration and machine_id == machines[-1] and 200 <= second < 320:
                vibration = 1.75

            readings: dict[str, float | None] = {
                "temperature_c": temperature,
                "vibration_mm_s": vibration,
                "pressure_bar": pressure,
                "power_kw": power,
                "rotation_rpm": rpm,
            }
            for sensor in SENSOR_NAMES:
                if rng.random() < null_probability:
                    readings[sensor] = None

            state = "RUNNING"
            if 400 <= second < 430 and machine_id == machines[0]:
                state = "IDLE"

            rows.append(
                {
                    "event_id": f"{machine_id}-{second:06d}",
                    "machine_id": machine_id,
                    "line_id": "LINE-A",
                    "event_time": ORIGIN + timedelta(seconds=second),
                    "machine_state": state,
                    **readings,
                }
            )

    frame = pd.DataFrame(rows)
    frame["event_time"] = pd.to_datetime(frame["event_time"], utc=True)
    return frame


def make_labels(samples: pd.DataFrame, *, machine_id: str = "M-002") -> pd.DataFrame:
    """Two episodes on one machine, of clearly different lengths."""
    windows = [
        ("ep-test-001", 120, 240, "BEARING_WEAR"),
        ("ep-test-002", 450, 465, "OVERHEAT"),
    ]
    rows: list[dict[str, object]] = []
    machine_samples = samples.loc[samples["machine_id"] == machine_id]
    for episode_id, start, end, anomaly_type in windows:
        started_at = ORIGIN + timedelta(seconds=start)
        span = machine_samples.loc[
            (machine_samples["event_time"] >= started_at)
            & (machine_samples["event_time"] < ORIGIN + timedelta(seconds=end))
        ]
        for event_id, event_time in zip(span["event_id"], span["event_time"], strict=True):
            rows.append(
                {
                    "event_id": event_id,
                    "machine_id": machine_id,
                    "event_time": event_time,
                    "is_anomaly": True,
                    "anomaly_type": anomaly_type,
                    "episode_id": episode_id,
                    "episode_started_at": started_at,
                    "emitted_at": event_time + timedelta(milliseconds=50),
                }
            )
    frame = pd.DataFrame(rows)
    frame["event_time"] = pd.to_datetime(frame["event_time"], utc=True)
    return frame


@pytest.fixture
def samples() -> pd.DataFrame:
    return make_samples()


@pytest.fixture
def labels(samples: pd.DataFrame) -> pd.DataFrame:
    return make_labels(samples)
