"""Shared fixtures. One SparkSession for the whole session: starting a JVM per
test would dominate the runtime and teach us nothing."""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd
import pytest
from telemetry_core.schemas import SENSOR_NAMES

_REPO_ROOT = Path(__file__).resolve().parents[2]

ORIGIN = datetime(2026, 4, 1, 6, 0, 0, tzinfo=UTC)
ARTIFACT_DIR = _REPO_ROOT / "ml-training" / "artifacts" / "one_class_svm" / "2.0.0"


@pytest.fixture(scope="session")
def repo_root() -> Path:
    return _REPO_ROOT


@pytest.fixture(scope="session")
def spark() -> Iterator[Any]:
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.appName("stream-processor-tests")
        .master("local[2]")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def make_samples(
    *,
    seconds: int = 300,
    machines: tuple[str, ...] = ("M-001", "M-002"),
    seed: int = 7,
    null_probability: float = 0.03,
    freeze_vibration: bool = True,
) -> pd.DataFrame:
    """A deterministic sample frame that exercises the awkward cases.

    Deliberately contains missing readings and a frozen sensor: those are where
    engines disagree, and a conformance test that only walks the happy path
    proves nothing.
    """
    import numpy as np

    rng = np.random.default_rng(seed)
    rows: list[dict[str, object]] = []

    for index, machine_id in enumerate(machines):
        load = 0.6
        for second in range(seconds):
            load = float(np.clip(load + rng.normal(0.0, 0.02), 0.05, 1.0))
            rpm = 1400.0 + 120.0 * load + rng.normal(0.0, 3.0) + 40.0 * index
            power = 1.0 + 0.0065 * rpm * (0.6 + 0.4 * load) + rng.normal(0.0, 0.1)
            vibration = 0.5 + 1.2 * load + rng.normal(0.0, 0.05)
            pressure = 3.5 + 2.4 * load + rng.normal(0.0, 0.04)
            temperature = 21.0 + 4.0 * power + rng.normal(0.0, 0.3)

            if freeze_vibration and machine_id == machines[-1] and 120 <= second < 200:
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
            if 250 <= second < 270 and machine_id == machines[0]:
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


@pytest.fixture
def samples() -> pd.DataFrame:
    return make_samples()


@pytest.fixture(scope="session")
def artifact_dir() -> Path:
    if not (ARTIFACT_DIR / "model.joblib").is_file():
        # The artefact is committed (D-48), so a checkout has it. The skip covers
        # the one legitimate case: the component images copy `ml-training/src`
        # but not `artifacts/`, so a run inside a container without the bind
        # mount finds no binary, and failing would report a missing volume as a
        # broken model.
        pytest.skip(
            f"no artefact at {ARTIFACT_DIR}. It is committed (D-48), so this "
            "means the suite is running somewhere the repository tree is not "
            "mounted -- inside a component image, mount ml-training/artifacts."
        )
    return ARTIFACT_DIR
