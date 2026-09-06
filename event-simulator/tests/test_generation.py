"""Signal generation: nominal ranges, determinism, and sensor coherence."""

from __future__ import annotations

import json
from itertools import pairwise
from pathlib import Path

import pytest
from telemetry_core.enums import MachineState

from conftest import RUN_START, load_json, make_clock
from event_simulator.config.fleet import FleetConfig
from event_simulator.generation.fleet_runner import FleetRunner, RunLimits
from event_simulator.generation.physics import PHYSICAL_LIMITS
from event_simulator.generation.rng import RngRegistry
from event_simulator.generation.state import GeneratedSample


def _run(fleet: FleetConfig, *, seed: int = 42, ticks: int = 120) -> list[GeneratedSample]:
    runner = FleetRunner(fleet=fleet, clock=make_clock(), rng=RngRegistry(seed))
    samples: list[GeneratedSample] = []
    for batch in runner.run(RunLimits(max_events=ticks * len(fleet.machines))):
        samples.extend(batch.samples)
    return samples


def _without_anomalies(fleet: FleetConfig) -> FleetConfig:
    return fleet.model_copy(
        update={"anomalies": fleet.anomalies.model_copy(update={"enabled": False})}
    )


class TestPhysicalLimits:
    def test_limits_mirror_the_contract(self, schema_dir: Path) -> None:
        """The constants are a copy of the JSON Schema; this keeps them honest."""
        schema = load_json(schema_dir / "telemetry-raw.v1.json")
        readings = schema["properties"]["readings"]["properties"]
        for sensor, (low, high) in PHYSICAL_LIMITS.items():
            assert readings[sensor]["minimum"] == low, sensor
            assert readings[sensor]["maximum"] == high, sensor

    def test_no_reading_ever_leaves_the_physical_range(self, fleet: FleetConfig) -> None:
        for sample in _run(fleet, ticks=200):
            for sensor, value in sample.readings.as_mapping().items():
                if value is None:
                    continue
                low, high = PHYSICAL_LIMITS[sensor]
                assert low <= value <= high, f"{sensor}={value}"

    def test_clamping_never_fires_on_the_shipped_configuration(self, fleet: FleetConfig) -> None:
        """A clamp is a configuration bug, not an anomaly.

        It would mean an intensity was set high enough to push a sensor outside
        the physically valid range, making the anomaly trivially detectable and
        the evaluation worthless.
        """
        runner = FleetRunner(fleet=fleet, clock=make_clock(), rng=RngRegistry(3))
        for _ in runner.run(RunLimits(max_events=200 * len(fleet.machines))):
            pass
        assert runner.clamped_readings == 0


class TestNominalRanges:
    def test_running_samples_stay_within_declared_nominal_ranges(self, fleet: FleetConfig) -> None:
        """Without anomalies, a RUNNING machine reads inside its declared band.

        Other states are excluded on purpose: a stopped pump reading zero
        pressure is correct and outside nominal, which is exactly why
        ``machine_state`` is published.
        """
        clean = _without_anomalies(fleet)
        profiles = {machine.machine_id: machine.profile for machine in clean.resolved_machines()}

        checked = 0
        for sample in _run(clean, ticks=180):
            if sample.machine_state is not MachineState.RUNNING:
                continue
            nominal = profiles[sample.machine_id].nominal.as_mapping()
            for sensor, value in sample.readings.as_mapping().items():
                assert value is not None
                bounds = nominal[sensor]
                assert bounds.min <= value <= bounds.max, (
                    f"{sample.machine_id} {sensor}={value:.3f} outside [{bounds.min}, {bounds.max}]"
                )
            checked += 1

        assert checked > 500, "not enough RUNNING samples to make the check meaningful"


class TestDeterminism:
    def test_same_seed_produces_identical_samples(self, fleet: FleetConfig) -> None:
        first = _run(fleet, seed=99, ticks=40)
        second = _run(fleet, seed=99, ticks=40)
        assert first == second

    def test_different_seed_produces_different_samples(self, fleet: FleetConfig) -> None:
        first = _run(fleet, seed=99, ticks=40)
        second = _run(fleet, seed=100, ticks=40)
        assert first != second

    def test_disabling_anomalies_leaves_the_noise_untouched(self, fleet: FleetConfig) -> None:
        """The end-to-end consequence of independent RNG streams.

        A machine that had no episode must produce byte-identical readings
        whether anomalies are enabled for the fleet or not. If the streams were
        shared, turning anomalies on would perturb every machine.
        """
        with_anomalies = _run(fleet, seed=5, ticks=30)
        without = _run(_without_anomalies(fleet), seed=5, ticks=30)

        anomalous_machines = {
            sample.machine_id for sample in with_anomalies if sample.ground_truth.is_anomaly
        }
        compared = 0
        for left, right in zip(with_anomalies, without, strict=True):
            if left.machine_id in anomalous_machines:
                continue
            assert left == right
            compared += 1
        assert compared > 0


class TestSensorCoherence:
    def test_power_tracks_rotation(self, fleet: FleetConfig) -> None:
        """Sensors are derived from one hidden state, not drawn independently."""
        samples = [
            sample
            for sample in _run(_without_anomalies(fleet), seed=17, ticks=200)
            if sample.machine_id == "M-001" and sample.machine_state is MachineState.RUNNING
        ]
        assert len(samples) > 50

        rpm = [s.readings.rotation_rpm for s in samples]
        power = [s.readings.power_kw for s in samples]
        assert all(value is not None for value in rpm + power)

        mean_rpm = sum(v for v in rpm if v is not None) / len(rpm)
        mean_power = sum(v for v in power if v is not None) / len(power)
        covariance = sum(
            (r - mean_rpm) * (p - mean_power)
            for r, p in zip(rpm, power, strict=True)
            if r is not None and p is not None
        )
        assert covariance > 0, "power and speed must move together"

    def test_specific_consumption_is_stable_without_wear(self, fleet: FleetConfig) -> None:
        """power_per_rpm is the feature the whole design leans on.

        On a healthy machine it must be roughly constant; the Phase 2 model can
        only use its rise as a wear signature if it does not wander on its own.
        """
        samples = [
            sample
            for sample in _run(_without_anomalies(fleet), seed=23, ticks=200)
            if sample.machine_id == "M-003" and sample.machine_state is MachineState.RUNNING
        ]
        ratios = [
            sample.readings.power_kw / sample.readings.rotation_rpm
            for sample in samples
            if sample.readings.power_kw is not None
            and sample.readings.rotation_rpm is not None
            and sample.readings.rotation_rpm > 1.0
        ]
        assert len(ratios) > 50
        mean = sum(ratios) / len(ratios)
        spread = max(ratios) - min(ratios)
        assert spread < 0.5 * mean

    def test_temperature_lags_rather_than_jumps(self, fleet: FleetConfig) -> None:
        """Thermal inertia gives the signal a memory, so a drift is a trend."""
        samples = [
            sample
            for sample in _run(_without_anomalies(fleet), seed=31, ticks=120)
            if sample.machine_id == "M-002"
        ]
        temperatures = [
            sample.readings.temperature_c
            for sample in samples
            if sample.readings.temperature_c is not None
        ]
        steps = [abs(later - earlier) for earlier, later in pairwise(temperatures)]
        # Consecutive changes stay small relative to the operating level.
        assert max(steps) < 5.0


class TestLifecycle:
    def test_every_operating_state_occurs(self, fleet: FleetConfig) -> None:
        """Decision D-21: machine_state must carry information for Phase 3."""
        observed = {sample.machine_state for sample in _run(fleet, seed=13, ticks=4000)}
        assert MachineState.RUNNING in observed
        assert MachineState.IDLE in observed
        assert MachineState.STARTING in observed
        assert MachineState.MAINTENANCE in observed

    def test_running_dominates(self, fleet: FleetConfig) -> None:
        samples = _run(fleet, seed=13, ticks=2000)
        running = sum(1 for s in samples if s.machine_state is MachineState.RUNNING)
        assert running / len(samples) > 0.6

    def test_stopped_machines_barely_turn(self, fleet: FleetConfig) -> None:
        samples = [
            sample
            for sample in _run(fleet, seed=13, ticks=4000)
            if sample.machine_state is MachineState.MAINTENANCE
        ]
        assert samples
        for sample in samples:
            assert sample.readings.rotation_rpm is not None
            assert sample.readings.rotation_rpm < 10.0


class TestGroundTruthAttachment:
    def test_normal_samples_carry_no_episode(self, fleet: FleetConfig) -> None:
        clean = _without_anomalies(fleet)
        for sample in _run(clean, ticks=60):
            assert sample.ground_truth.is_anomaly is False
            assert sample.ground_truth.episode_id is None
            assert sample.ground_truth.anomaly_type is None

    def test_generated_sample_is_not_serialisable_to_the_wire(self) -> None:
        """A sanity check on the split: the internal type is not a message.

        It has no ``to_dict``, so it cannot be handed to the codec by mistake.
        """
        assert not hasattr(GeneratedSample, "to_dict")
        with pytest.raises(TypeError):
            json.dumps(
                GeneratedSample(
                    machine_id="M-001",
                    line_id="LINE-A",
                    event_time=RUN_START,
                    machine_state=MachineState.RUNNING,
                    readings=None,  # type: ignore[arg-type]
                    firmware_version=None,
                    ground_truth=None,  # type: ignore[arg-type]
                )
            )
