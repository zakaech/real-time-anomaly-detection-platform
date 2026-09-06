"""The committed fleet must load, and a broken one must fail loudly."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml
from telemetry_core.errors import ConfigurationError

from event_simulator.config.fleet import FleetConfig, load_fleet


def _write(tmp_path: Path, document: dict[str, object]) -> Path:
    path = tmp_path / "fleet.yaml"
    path.write_text(yaml.safe_dump(document), encoding="utf-8")
    return path


class TestCommittedFleet:
    def test_it_loads(self, fleet: FleetConfig) -> None:
        assert len(fleet.machines) == 15
        assert set(fleet.profiles) == {"spindle", "pump", "conveyor"}

    def test_machine_ids_match_the_contract_pattern(self, fleet: FleetConfig) -> None:
        for machine in fleet.machines:
            assert machine.machine_id.startswith("M-")
            assert len(machine.machine_id) == 5
            assert machine.line_id.startswith("LINE-")

    def test_defaults_are_merged_into_profiles(self, fleet: FleetConfig) -> None:
        """A profile overrides only what differs; the rest comes from defaults."""
        conveyor = fleet.profiles["conveyor"].simulation
        spindle = fleet.profiles["spindle"].simulation

        # Overridden in the conveyor profile...
        assert conveyor.lifecycle.running_seconds == 1800
        assert spindle.lifecycle.running_seconds == 900
        # ...while the rest of the lifecycle block still comes from defaults.
        assert conveyor.lifecycle.idle_probability == spindle.lifecycle.idle_probability
        assert conveyor.noise.temperature_c == spindle.noise.temperature_c

    def test_profile_specific_wear_override(self, fleet: FleetConfig) -> None:
        assert fleet.profiles["pump"].simulation.wear.rate_per_hour == 0.0018
        assert fleet.profiles["spindle"].simulation.wear.rate_per_hour == 0.0025

    def test_resolved_machines_carry_their_profile(self, fleet: FleetConfig) -> None:
        resolved = fleet.resolved_machines()
        assert len(resolved) == len(fleet.machines)
        assert resolved[0].machine_id == "M-001"
        assert resolved[0].profile.machine_type == "CNC_SPINDLE"

    def test_labels_are_off_by_default(self, fleet: FleetConfig) -> None:
        """Decision D-22: normal is the complement, not a published fact."""
        assert fleet.labels.emit_normal_labels is False

    def test_three_anomaly_families_are_declared(self, fleet: FleetConfig) -> None:
        families = {spec.family for spec in fleet.anomalies.types}
        assert families == {"SPIKE", "DRIFT", "SENSOR_FAILURE"}

    def test_drift_can_target_hidden_state(self, fleet: FleetConfig) -> None:
        """BEARING_WEAR names no sensor, so it moves every coupled sensor."""
        bearing = next(spec for spec in fleet.anomalies.types if spec.type.value == "BEARING_WEAR")
        assert bearing.family == "DRIFT"
        assert bearing.sensor is None


class TestValidation:
    def test_missing_file_is_a_configuration_error(self, tmp_path: Path) -> None:
        with pytest.raises(ConfigurationError, match="cannot read"):
            load_fleet(tmp_path / "absent.yaml")

    def test_invalid_yaml_is_reported(self, tmp_path: Path) -> None:
        path = tmp_path / "fleet.yaml"
        path.write_text("profiles: [unclosed", encoding="utf-8")
        with pytest.raises(ConfigurationError, match="not valid YAML"):
            load_fleet(path)

    def test_unknown_profile_reference_is_rejected(self, fleet_path: Path, tmp_path: Path) -> None:
        document = yaml.safe_load(fleet_path.read_text(encoding="utf-8"))
        document["machines"][0]["profile"] = "does-not-exist"
        with pytest.raises(ConfigurationError, match="unknown profile"):
            load_fleet(_write(tmp_path, document))

    def test_duplicate_machine_id_is_rejected(self, fleet_path: Path, tmp_path: Path) -> None:
        document = yaml.safe_load(fleet_path.read_text(encoding="utf-8"))
        document["machines"][1]["machine_id"] = document["machines"][0]["machine_id"]
        with pytest.raises(ConfigurationError, match="duplicate machine_id"):
            load_fleet(_write(tmp_path, document))

    def test_machine_id_must_match_the_contract_pattern(
        self, fleet_path: Path, tmp_path: Path
    ) -> None:
        """The simulator cannot emit an id the schema would reject."""
        document = yaml.safe_load(fleet_path.read_text(encoding="utf-8"))
        document["machines"][0]["machine_id"] = "MACHINE-1"
        with pytest.raises(ConfigurationError, match="machine_id"):
            load_fleet(_write(tmp_path, document))

    def test_unknown_sensor_in_an_anomaly_is_rejected(
        self, fleet_path: Path, tmp_path: Path
    ) -> None:
        document = yaml.safe_load(fleet_path.read_text(encoding="utf-8"))
        document["anomalies"]["types"][0]["sensor"] = "humidity_pct"
        with pytest.raises(ConfigurationError, match="unknown sensor"):
            load_fleet(_write(tmp_path, document))

    def test_spike_without_a_sensor_is_rejected(self, fleet_path: Path, tmp_path: Path) -> None:
        document = yaml.safe_load(fleet_path.read_text(encoding="utf-8"))
        document["anomalies"]["types"][0]["sensor"] = None
        with pytest.raises(ConfigurationError, match="requires an explicit sensor"):
            load_fleet(_write(tmp_path, document))

    def test_unknown_key_is_rejected(self, fleet_path: Path, tmp_path: Path) -> None:
        """A typo in a config file has no symptom other than a silent default."""
        document = yaml.safe_load(fleet_path.read_text(encoding="utf-8"))
        document["labels"]["emit_normal_lables"] = True
        with pytest.raises(ConfigurationError):
            load_fleet(_write(tmp_path, document))

    def test_inverted_bounds_are_rejected(self, fleet_path: Path, tmp_path: Path) -> None:
        document = yaml.safe_load(fleet_path.read_text(encoding="utf-8"))
        document["anomalies"]["types"][0]["duration_seconds"] = {"min": 20, "max": 4}
        with pytest.raises(ConfigurationError, match="min"):
            load_fleet(_write(tmp_path, document))

    def test_scenario_on_unknown_machine_is_rejected(
        self, fleet_path: Path, tmp_path: Path
    ) -> None:
        document = yaml.safe_load(fleet_path.read_text(encoding="utf-8"))
        document["anomalies"]["scenarios"] = [
            {
                "machine_id": "M-999",
                "type": "OVERHEAT",
                "start_offset_seconds": 1,
                "duration_seconds": 5,
            }
        ]
        with pytest.raises(ConfigurationError, match="unknown machine"):
            load_fleet(_write(tmp_path, document))
