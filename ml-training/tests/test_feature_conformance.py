"""The pandas translation must reproduce the shared reference exactly enough.

This is the test that makes the Phase 1 promise real, and the template Phase 3
will reuse for Spark: same reference, same tolerance, same awkward windows.

**Bit-exact equality is not achievable and not required.** ``math.fsum`` in the
reference and pandas' pairwise summation add in different orders, so results
differ in the last few bits. The tolerance below is tight enough that any real
semantic difference -- a ddof mismatch, a positional "last", a correlation
computed with the single-pass formula -- fails it by many orders of magnitude.
"""

from __future__ import annotations

import math

import numpy as np
import pandas as pd
import pytest
from telemetry_core.features import FEATURE_NAMES, WindowSample, compute_features
from telemetry_core.schemas import SensorReadings

from conftest import ORIGIN, make_samples
from ml_training.features.pandas_builder import build_window_features

#: Relative tolerance. A genuine semantic divergence -- ddof=0 against ddof=1 --
#: shows up around 1e-2 on sixty points, six orders of magnitude above this.
RELATIVE_TOLERANCE = 1e-9
ABSOLUTE_TOLERANCE = 1e-9

WINDOW_SECONDS = 60


def _optional(value: object) -> float | None:
    """A missing reading stays None; pandas hands it over as NaN."""
    if value is None:
        return None
    number = float(value)  # type: ignore[arg-type]
    return None if math.isnan(number) else number


def _reference_features(samples: pd.DataFrame, window_seconds: int) -> pd.DataFrame:
    """Run the shared reference implementation window by window."""
    rows: list[dict[str, object]] = []
    seconds = samples["event_time"].astype("int64") // 1_000_000_000
    frame = samples.assign(window_start=(seconds // window_seconds) * window_seconds)

    for (machine_id, window_start), group in frame.groupby(
        ["machine_id", "window_start"], sort=True
    ):
        window_samples = [
            WindowSample(
                event_time=row.event_time,  # type: ignore[arg-type]
                readings=SensorReadings(
                    temperature_c=_optional(row.temperature_c),
                    vibration_mm_s=_optional(row.vibration_mm_s),
                    pressure_bar=_optional(row.pressure_bar),
                    power_kw=_optional(row.power_kw),
                    rotation_rpm=_optional(row.rotation_rpm),
                ),
            )
            for row in group.itertuples(index=False)
        ]
        rows.append(
            {
                "machine_id": machine_id,
                "window_start": window_start,
                **compute_features(window_samples),
            }
        )
    return pd.DataFrame(rows)


def _compare(reference: pd.DataFrame, actual: pd.DataFrame) -> list[str]:
    """Return a readable description of every disagreement."""
    merged = reference.merge(
        actual, on=["machine_id", "window_start"], suffixes=("_ref", "_pandas"), how="inner"
    )
    assert len(merged) == len(reference), "the two implementations produced different windows"

    problems: list[str] = []
    for name in FEATURE_NAMES:
        expected = merged[f"{name}_ref"]
        produced = merged[f"{name}_pandas"]
        for index in range(len(merged)):
            left = expected.iloc[index]
            right = produced.iloc[index]
            left_missing = left is None or (isinstance(left, float) and math.isnan(left))
            right_missing = right is None or (isinstance(right, float) and math.isnan(right))

            if left_missing != right_missing:
                problems.append(
                    f"{name} @ {merged['machine_id'].iloc[index]}/"
                    f"{merged['window_start'].iloc[index]}: "
                    f"reference={'None' if left_missing else left}, "
                    f"pandas={'NaN' if right_missing else right}"
                )
                continue
            if left_missing:
                continue
            if not math.isclose(
                float(left), float(right), rel_tol=RELATIVE_TOLERANCE, abs_tol=ABSOLUTE_TOLERANCE
            ):
                problems.append(
                    f"{name} @ {merged['machine_id'].iloc[index]}/"
                    f"{merged['window_start'].iloc[index]}: "
                    f"reference={left!r}, pandas={right!r}"
                )
    return problems


class TestConformance:
    def test_pandas_matches_the_reference_on_a_full_history(self) -> None:
        samples = make_samples(seconds=600, seed=11)
        actual = build_window_features(
            samples, window_seconds=WINDOW_SECONDS, slide_seconds=WINDOW_SECONDS
        )
        reference = _reference_features(samples, WINDOW_SECONDS)

        problems = _compare(reference, actual)
        assert not problems, "\n".join(problems[:20])

    def test_it_matches_with_heavy_missing_data(self) -> None:
        """Pairwise deletion and per-signal exclusion must agree exactly."""
        samples = make_samples(seconds=300, seed=3, null_probability=0.25)
        actual = build_window_features(
            samples, window_seconds=WINDOW_SECONDS, slide_seconds=WINDOW_SECONDS
        )
        reference = _reference_features(samples, WINDOW_SECONDS)
        assert not _compare(reference, actual)

    def test_it_matches_when_a_sensor_is_frozen(self) -> None:
        """Zero variance is where stddev, z-score and correlation all go undefined."""
        samples = make_samples(seconds=360, seed=5, null_probability=0.0)
        actual = build_window_features(
            samples, window_seconds=WINDOW_SECONDS, slide_seconds=WINDOW_SECONDS
        )
        reference = _reference_features(samples, WINDOW_SECONDS)

        frozen_windows = actual.loc[actual["vibration_mm_s_stddev"] == 0.0]
        assert not frozen_windows.empty, "the fixture no longer produces a frozen sensor"
        assert frozen_windows["vibration_mm_s_zscore_last"].isna().all()
        assert frozen_windows["corr_vibration_mm_s__rotation_rpm"].isna().all()
        assert not _compare(reference, actual)

    def test_it_matches_on_short_windows(self) -> None:
        """Fewer than two samples is where several features become undefined."""
        samples = make_samples(seconds=61, seed=2, null_probability=0.0)
        actual = build_window_features(
            samples, window_seconds=WINDOW_SECONDS, slide_seconds=WINDOW_SECONDS
        )
        reference = _reference_features(samples, WINDOW_SECONDS)
        assert not _compare(reference, actual)

    def test_column_order_matches_the_specification(self) -> None:
        """A matrix has no column names: order is the contract."""
        samples = make_samples(seconds=120, seed=1)
        actual = build_window_features(
            samples, window_seconds=WINDOW_SECONDS, slide_seconds=WINDOW_SECONDS
        )
        assert list(actual.columns[-len(FEATURE_NAMES) :]) == list(FEATURE_NAMES)

    def test_arrival_order_does_not_change_the_result(self) -> None:
        """Late data arrives out of order; a slope must not flip sign because of it."""
        samples = make_samples(seconds=180, seed=4)
        shuffled = samples.sample(frac=1.0, random_state=99).reset_index(drop=True)

        ordered = build_window_features(
            samples, window_seconds=WINDOW_SECONDS, slide_seconds=WINDOW_SECONDS
        )
        scrambled = build_window_features(
            shuffled, window_seconds=WINDOW_SECONDS, slide_seconds=WINDOW_SECONDS
        )
        pd.testing.assert_frame_equal(ordered, scrambled)


class TestSpecificSemantics:
    """Hand-computed checks on the two definitions engines most often differ on."""

    def _tiny_frame(self) -> pd.DataFrame:
        times = [ORIGIN + pd.Timedelta(seconds=offset) for offset in (0, 10, 20)]
        return pd.DataFrame(
            {
                "machine_id": ["M-001"] * 3,
                "event_time": pd.to_datetime(times, utc=True),
                "machine_state": ["RUNNING"] * 3,
                "temperature_c": [10.0, 20.0, 30.0],
                "vibration_mm_s": [1.0, 2.0, 3.0],
                "pressure_bar": [5.0, 5.0, 5.0],
                "power_kw": [10.0, 20.0, 30.0],
                "rotation_rpm": [1000.0, 1000.0, 1000.0],
            }
        )

    def test_stddev_is_ddof_one(self) -> None:
        features = build_window_features(
            self._tiny_frame(), window_seconds=WINDOW_SECONDS, slide_seconds=WINDOW_SECONDS
        )
        # 10/20/30 -> ddof=1 gives 10.0; ddof=0 would give 8.165
        assert features["temperature_c_stddev"].iloc[0] == pytest.approx(10.0)

    def test_last_follows_event_time(self) -> None:
        features = build_window_features(
            self._tiny_frame(), window_seconds=WINDOW_SECONDS, slide_seconds=WINDOW_SECONDS
        )
        assert features["temperature_c_deviation_from_mean"].iloc[0] == pytest.approx(10.0)
        assert features["temperature_c_slope_per_second"].iloc[0] == pytest.approx(1.0)

    def test_correlation_matches_the_hand_computed_value(self) -> None:
        features = build_window_features(
            self._tiny_frame(), window_seconds=WINDOW_SECONDS, slide_seconds=WINDOW_SECONDS
        )
        # Temperature and power are collinear here.
        assert features["corr_temperature_c__power_kw"].iloc[0] == pytest.approx(1.0)
        # Rotation is flat, so its correlations are undefined rather than zero.
        assert np.isnan(features["corr_power_kw__rotation_rpm"].iloc[0])


class TestSlidingWindows:
    def test_each_sample_lands_in_the_expected_number_of_windows(self) -> None:
        samples = make_samples(seconds=300, machines=("M-001",), seed=8, null_probability=0.0)
        features = build_window_features(samples, window_seconds=60, slide_seconds=10)

        full = features.loc[features["sample_count"] == 60]
        assert not full.empty
        # Window starts are aligned on multiples of the slide.
        assert (features["window_start"] % 10 == 0).all()

    def test_sliding_and_tumbling_agree_on_shared_windows(self) -> None:
        """A window is defined by its bounds, not by the cadence that produced it."""
        samples = make_samples(seconds=300, machines=("M-001",), seed=9, null_probability=0.0)
        tumbling = build_window_features(samples, window_seconds=60, slide_seconds=60)
        sliding = build_window_features(samples, window_seconds=60, slide_seconds=10)

        shared = tumbling.merge(sliding, on=["machine_id", "window_start"], suffixes=("_t", "_s"))
        assert not shared.empty
        for name in FEATURE_NAMES:
            left = shared[f"{name}_t"].to_numpy(dtype="float64")
            right = shared[f"{name}_s"].to_numpy(dtype="float64")
            np.testing.assert_allclose(left, right, rtol=1e-9, atol=1e-9, equal_nan=True)
