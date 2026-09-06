"""The three anomaly families, forced deterministically through scenarios.

Scenarios rather than random draws: an assertion about a spike is only
meaningful if the spike is guaranteed to happen, at a known instant, with a
known intensity.
"""

from __future__ import annotations

from datetime import timedelta

import pytest
from telemetry_core.enums import AnomalyType, MachineState

from conftest import RUN_START, make_clock
from event_simulator.anomalies.episode import Episode, format_episode_id
from event_simulator.config.fleet import FleetConfig, ScenarioSpec
from event_simulator.generation.fleet_runner import FleetRunner, RunLimits
from event_simulator.generation.rng import RngRegistry
from event_simulator.generation.state import GeneratedSample

_MACHINE = "M-001"


def _fleet_with_scenario(
    fleet: FleetConfig,
    *,
    anomaly_type: AnomalyType,
    offset_seconds: float,
    duration_seconds: float,
    intensity: float | None = None,
    machine_id: str = _MACHINE,
) -> FleetConfig:
    """A fleet whose only anomaly is the one under test."""
    anomalies = fleet.anomalies.model_copy(
        update={
            "episodes_per_hour": 0.0,  # no random episodes; scenarios still fire
            "scenarios": [
                ScenarioSpec(
                    machine_id=machine_id,
                    type=anomaly_type,
                    start_offset_seconds=offset_seconds,
                    duration_seconds=duration_seconds,
                    intensity=intensity,
                )
            ],
        }
    )
    late_data = fleet.late_data.model_copy(update={"enabled": False})
    return fleet.model_copy(update={"anomalies": anomalies, "late_data": late_data})


def _samples(fleet: FleetConfig, *, ticks: int, seed: int = 4) -> list[GeneratedSample]:
    runner = FleetRunner(fleet=fleet, clock=make_clock(), rng=RngRegistry(seed))
    collected: list[GeneratedSample] = []
    for batch in runner.run(RunLimits(max_events=ticks * len(fleet.machines))):
        collected.extend(batch.samples)
    return [sample for sample in collected if sample.machine_id == _MACHINE]


def _split(samples: list[GeneratedSample]) -> tuple[list[GeneratedSample], list[GeneratedSample]]:
    inside = [s for s in samples if s.ground_truth.is_anomaly]
    outside = [s for s in samples if not s.ground_truth.is_anomaly]
    return inside, outside


class TestEpisode:
    def test_interval_is_half_open(self) -> None:
        """Otherwise two consecutive episodes both claim the boundary instant."""
        episode = Episode(
            episode_id="ep-1",
            machine_id=_MACHINE,
            anomaly_type=AnomalyType.OVERHEAT,
            family="SPIKE",
            started_at=RUN_START,
            ends_at=RUN_START + timedelta(seconds=10),
            intensity=5.0,
            sensor="temperature_c",
        )
        assert episode.is_active(RUN_START)
        assert episode.is_active(RUN_START + timedelta(seconds=9))
        assert not episode.is_active(RUN_START + timedelta(seconds=10))

    def test_progress_is_clamped(self) -> None:
        episode = Episode(
            episode_id="ep-1",
            machine_id=_MACHINE,
            anomaly_type=AnomalyType.OVERHEAT,
            family="SPIKE",
            started_at=RUN_START,
            ends_at=RUN_START + timedelta(seconds=10),
            intensity=5.0,
            sensor="temperature_c",
        )
        assert episode.progress(RUN_START) == 0.0
        assert episode.progress(RUN_START + timedelta(seconds=5)) == pytest.approx(0.5)
        assert episode.progress(RUN_START + timedelta(seconds=99)) == 1.0

    def test_zero_length_episode_is_rejected(self) -> None:
        with pytest.raises(ValueError, match="ends_at"):
            Episode(
                episode_id="ep-1",
                machine_id=_MACHINE,
                anomaly_type=AnomalyType.OVERHEAT,
                family="SPIKE",
                started_at=RUN_START,
                ends_at=RUN_START,
                intensity=1.0,
            )

    def test_identifier_is_readable(self) -> None:
        assert format_episode_id("M-014", RUN_START, 3) == "ep-20260302-M014-003"


class TestSpikeFamily:
    def test_overheat_raises_temperature_then_returns(self, fleet: FleetConfig) -> None:
        configured = _fleet_with_scenario(
            fleet,
            anomaly_type=AnomalyType.OVERHEAT,
            offset_seconds=60,
            duration_seconds=10,
            intensity=22.0,
        )
        inside, outside = _split(_samples(configured, ticks=180))
        assert inside, "the scenario produced no anomalous sample"

        peak_inside = max(s.readings.temperature_c or 0.0 for s in inside)
        peak_outside = max(s.readings.temperature_c or 0.0 for s in outside)
        assert peak_inside > peak_outside + 10.0

    def test_the_excursion_is_shaped_not_rectangular(self, fleet: FleetConfig) -> None:
        """A step would be trivially detectable by a first difference."""
        configured = _fleet_with_scenario(
            fleet,
            anomaly_type=AnomalyType.OVERHEAT,
            offset_seconds=60,
            duration_seconds=12,
            intensity=22.0,
        )
        inside, _ = _split(_samples(configured, ticks=180))
        temperatures = [s.readings.temperature_c or 0.0 for s in inside]
        assert len(temperatures) >= 6
        # Peaks in the middle rather than at either edge.
        peak_index = temperatures.index(max(temperatures))
        assert 0 < peak_index < len(temperatures) - 1

    def test_pressure_drop_lowers_pressure(self, fleet: FleetConfig) -> None:
        configured = _fleet_with_scenario(
            fleet,
            anomaly_type=AnomalyType.PRESSURE_DROP,
            offset_seconds=60,
            duration_seconds=12,
            intensity=-3.0,
        )
        inside, outside = _split(_samples(configured, ticks=180))
        assert inside
        assert min(s.readings.pressure_bar or 0.0 for s in inside) < min(
            s.readings.pressure_bar or 0.0 for s in outside
        )

    def test_a_spike_leaves_no_thermal_trail(self, fleet: FleetConfig) -> None:
        """A measurement excursion must not become part of the machine history.

        Persisting it would make each episode change the machine for the rest of
        the run, and episodes would stop being independent occurrences.
        """
        configured = _fleet_with_scenario(
            fleet,
            anomaly_type=AnomalyType.OVERHEAT,
            offset_seconds=60,
            duration_seconds=10,
            intensity=25.0,
        )
        samples = _samples(configured, ticks=200)
        after = [
            s.readings.temperature_c or 0.0
            for s in samples
            if not s.ground_truth.is_anomaly and s.event_time > RUN_START + timedelta(seconds=75)
        ]
        before = [
            s.readings.temperature_c or 0.0
            for s in samples
            if not s.ground_truth.is_anomaly and s.event_time < RUN_START + timedelta(seconds=55)
        ]
        assert after and before
        assert abs(sum(after) / len(after) - sum(before) / len(before)) < 4.0


class TestDriftFamily:
    def test_bearing_wear_moves_several_sensors_together(self, fleet: FleetConfig) -> None:
        """The multivariate case: no single sensor leaves its nominal range.

        This is what a per-sensor threshold structurally cannot see, and the
        reason the platform computes ``power_per_rpm``.
        """
        configured = _fleet_with_scenario(
            fleet,
            anomaly_type=AnomalyType.BEARING_WEAR,
            offset_seconds=60,
            duration_seconds=300,
            intensity=0.8,
        )
        samples = _samples(configured, ticks=420)
        inside = [
            s
            for s in samples
            if s.ground_truth.is_anomaly and s.machine_state is MachineState.RUNNING
        ]
        outside = [
            s
            for s in samples
            if not s.ground_truth.is_anomaly and s.machine_state is MachineState.RUNNING
        ]
        assert len(inside) > 50 and len(outside) > 20

        late = inside[-30:]

        def ratio(sample: GeneratedSample) -> float:
            power = sample.readings.power_kw or 0.0
            rpm = sample.readings.rotation_rpm or 1.0
            return power / rpm

        mean_late_ratio = sum(ratio(s) for s in late) / len(late)
        mean_base_ratio = sum(ratio(s) for s in outside) / len(outside)
        assert mean_late_ratio > mean_base_ratio

        mean_late_vibration = sum((s.readings.vibration_mm_s or 0.0) for s in late) / len(late)
        mean_base_vibration = sum((s.readings.vibration_mm_s or 0.0) for s in outside) / len(
            outside
        )
        assert mean_late_vibration > mean_base_vibration

    def test_bearing_wear_grows_monotonically_over_the_episode(self, fleet: FleetConfig) -> None:
        configured = _fleet_with_scenario(
            fleet,
            anomaly_type=AnomalyType.BEARING_WEAR,
            offset_seconds=30,
            duration_seconds=300,
            intensity=0.8,
        )
        inside = [
            s
            for s in _samples(configured, ticks=400)
            if s.ground_truth.is_anomaly and s.machine_state is MachineState.RUNNING
        ]
        assert len(inside) > 60
        first_third = inside[: len(inside) // 3]
        last_third = inside[-len(inside) // 3 :]

        def mean_vibration(items: list[GeneratedSample]) -> float:
            return sum((s.readings.vibration_mm_s or 0.0) for s in items) / len(items)

        assert mean_vibration(last_third) > mean_vibration(first_third)

    def test_motor_stall_drops_speed_and_raises_specific_consumption(
        self, fleet: FleetConfig
    ) -> None:
        configured = _fleet_with_scenario(
            fleet,
            anomaly_type=AnomalyType.MOTOR_STALL,
            offset_seconds=60,
            duration_seconds=120,
            intensity=-0.25,
        )
        samples = _samples(configured, ticks=240)
        inside = [
            s
            for s in samples
            if s.ground_truth.is_anomaly and s.machine_state is MachineState.RUNNING
        ]
        outside = [
            s
            for s in samples
            if not s.ground_truth.is_anomaly and s.machine_state is MachineState.RUNNING
        ]
        assert inside and outside

        late = inside[-20:]
        mean_late_rpm = sum((s.readings.rotation_rpm or 0.0) for s in late) / len(late)
        mean_base_rpm = sum((s.readings.rotation_rpm or 0.0) for s in outside) / len(outside)
        assert mean_late_rpm < mean_base_rpm


class TestSensorFailureFamily:
    def test_stuck_sensor_reports_a_frozen_value(self, fleet: FleetConfig) -> None:
        configured = _fleet_with_scenario(
            fleet,
            anomaly_type=AnomalyType.SENSOR_STUCK,
            offset_seconds=60,
            duration_seconds=90,
            intensity=0.0,
        )
        inside, _ = _split(_samples(configured, ticks=200))
        values = {s.readings.vibration_mm_s for s in inside}
        assert len(inside) > 30
        assert len(values) == 1, f"expected one frozen value, saw {sorted(v or 0 for v in values)}"

    def test_stuck_sensor_leaves_other_sensors_alone(self, fleet: FleetConfig) -> None:
        """The machine is fine; only the instrument failed."""
        configured = _fleet_with_scenario(
            fleet,
            anomaly_type=AnomalyType.SENSOR_STUCK,
            offset_seconds=60,
            duration_seconds=90,
        )
        inside, _ = _split(_samples(configured, ticks=200))
        assert len({s.readings.temperature_c for s in inside}) > 5

    def test_dropout_reports_null_not_zero(self, fleet: FleetConfig) -> None:
        """Zero is a valid physical value; conflating the two invents anomalies."""
        configured = _fleet_with_scenario(
            fleet,
            anomaly_type=AnomalyType.SENSOR_DROPOUT,
            offset_seconds=60,
            duration_seconds=60,
        )
        inside, outside = _split(_samples(configured, ticks=200))
        assert inside
        assert all(s.readings.pressure_bar is None for s in inside)
        assert all(s.readings.pressure_bar is not None for s in outside)

    def test_readings_recover_after_the_episode(self, fleet: FleetConfig) -> None:
        configured = _fleet_with_scenario(
            fleet,
            anomaly_type=AnomalyType.SENSOR_DROPOUT,
            offset_seconds=60,
            duration_seconds=60,
        )
        samples = _samples(configured, ticks=220)
        recovered = [
            s
            for s in samples
            if not s.ground_truth.is_anomaly and s.event_time > RUN_START + timedelta(seconds=125)
        ]
        assert recovered
        assert all(s.readings.pressure_bar is not None for s in recovered)


class TestScheduling:
    def test_ground_truth_is_attached_to_every_anomalous_sample(self, fleet: FleetConfig) -> None:
        configured = _fleet_with_scenario(
            fleet,
            anomaly_type=AnomalyType.OVERHEAT,
            offset_seconds=30,
            duration_seconds=20,
            intensity=20.0,
        )
        inside, _ = _split(_samples(configured, ticks=120))
        assert inside
        for sample in inside:
            assert sample.ground_truth.anomaly_type is AnomalyType.OVERHEAT
            assert sample.ground_truth.episode_id is not None
            assert sample.ground_truth.episode_started_at is not None

    def test_all_anomalous_samples_share_one_episode_id(self, fleet: FleetConfig) -> None:
        configured = _fleet_with_scenario(
            fleet,
            anomaly_type=AnomalyType.OVERHEAT,
            offset_seconds=30,
            duration_seconds=20,
            intensity=20.0,
        )
        inside, _ = _split(_samples(configured, ticks=120))
        assert len({s.ground_truth.episode_id for s in inside}) == 1

    def test_random_episodes_occur_on_the_shipped_configuration(self, fleet: FleetConfig) -> None:
        samples: list[GeneratedSample] = []
        runner = FleetRunner(fleet=fleet, clock=make_clock(), rng=RngRegistry(21))
        for batch in runner.run(RunLimits(max_events=3000 * len(fleet.machines))):
            samples.extend(batch.samples)
        episodes = {
            s.ground_truth.episode_id for s in samples if s.ground_truth.episode_id is not None
        }
        assert episodes, "the configured Poisson rate produced no episode at all"

    def test_anomalies_remain_a_minority(self, fleet: FleetConfig) -> None:
        """Normal behaviour must dominate, or the detector has nothing to model."""
        samples: list[GeneratedSample] = []
        runner = FleetRunner(fleet=fleet, clock=make_clock(), rng=RngRegistry(21))
        for batch in runner.run(RunLimits(max_events=2000 * len(fleet.machines))):
            samples.extend(batch.samples)
        anomalous = sum(1 for s in samples if s.ground_truth.is_anomaly)
        assert 0 < anomalous < 0.25 * len(samples)
