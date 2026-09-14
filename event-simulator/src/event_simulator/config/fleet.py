"""Typed model of ``fleet.yaml``, validated at load time.

Pydantic here, frozen dataclasses for wire messages: the same reasoning as
decision D-13, applied in the other direction. A message needs byte-exact
control over its representation; a configuration file needs coercion from
loosely typed YAML and a clear failure at startup. Using one tool for both would
make one of the two jobs harder.

The file separates ``nominal`` from ``simulation`` per profile, and the
separation matters (decision D-19):

* ``nominal`` is what a plant declares about a machine. It is the only block
  that ever leaves the simulator: it is extracted into the Flyway seed for the
  ``machine`` table.
* ``simulation`` is the true physics: noise sigmas, coupling coefficients, wear
  rate. Nobody knows these in production. Publishing them to the platform would
  leak the answer into the design of the detector.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator, model_validator
from telemetry_core.enums import AnomalyType
from telemetry_core.errors import ConfigurationError
from telemetry_core.schemas import SENSOR_NAMES

__all__ = [
    "AnomaliesConfig",
    "AnomalyFamily",
    "AnomalySpec",
    "Bounds",
    "FleetConfig",
    "LabelsConfig",
    "LateDataConfig",
    "MachineProfile",
    "MachineSpec",
    "ResolvedMachine",
    "ScenarioSpec",
    "SimulationConfig",
    "load_fleet",
]

MACHINE_ID_PATTERN = r"^M-[0-9]{3}$"
LINE_ID_PATTERN = r"^LINE-[A-Z]$"

AnomalyFamily = Literal["SPIKE", "DRIFT", "SENSOR_FAILURE"]


class _Model(BaseModel):
    """Base: reject unknown keys and forbid mutation.

    ``extra="forbid"`` is deliberate and differs from the environment settings in
    telemetry-core, where prefixes overlap. A configuration file has no such
    excuse: a misspelled key there is a typo whose only symptom would be a
    silently applied default.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)


class Bounds(_Model):
    """An inclusive interval."""

    min: float
    max: float

    @model_validator(mode="after")
    def _ordered(self) -> Bounds:
        if self.min > self.max:
            raise ValueError(f"min ({self.min}) must not exceed max ({self.max})")
        return self

    def sample_between(self, unit: float) -> float:
        """Map ``unit`` in [0, 1] onto the interval."""
        return self.min + (self.max - self.min) * unit


class NominalRanges(_Model):
    """Declared operating ranges -- the plant's own view of the machine."""

    temperature_c: Bounds
    vibration_mm_s: Bounds
    pressure_bar: Bounds
    power_kw: Bounds
    rotation_rpm: Bounds

    def as_mapping(self) -> dict[str, Bounds]:
        return {name: getattr(self, name) for name in SENSOR_NAMES}


class LoadConfig(_Model):
    """Hidden load factor, an Ornstein-Uhlenbeck process.

    Mean-reverting rather than white noise, so the load wanders smoothly instead
    of jumping every second. Independent draws would make every derived sensor
    jitter in lockstep with nothing physical behind it.
    """

    mean: float = Field(ge=0.0, le=1.0)
    sigma: float = Field(gt=0.0)
    reversion: float = Field(gt=0.0, le=1.0)
    daily_amplitude: float = Field(ge=0.0, le=1.0)


class WearConfig(_Model):
    """Cumulative wear: the slow trend, and the reason power_per_rpm drifts up."""

    initial: float = Field(ge=0.0)
    rate_per_hour: float = Field(ge=0.0)
    max: float = Field(gt=0.0)


class AmbientConfig(_Model):
    """Daily seasonality, entering the system through ambient temperature."""

    mean_c: float
    daily_amplitude_c: float = Field(ge=0.0)
    peak_hour_utc: int = Field(ge=0, le=23)


class NoiseConfig(_Model):
    """Per-sensor measurement noise, one standard deviation."""

    temperature_c: float = Field(gt=0.0)
    vibration_mm_s: float = Field(gt=0.0)
    pressure_bar: float = Field(gt=0.0)
    power_kw: float = Field(gt=0.0)
    rotation_rpm: float = Field(gt=0.0)

    def as_mapping(self) -> dict[str, float]:
        return {name: float(getattr(self, name)) for name in SENSOR_NAMES}


class LifecycleConfig(_Model):
    """Machine state machine (decision D-21).

    Without it ``machine_state`` would be a constant and the field would carry no
    information; neither "only RUNNING windows are scored" nor contextual
    anomalies would be possible.
    """

    running_seconds: float = Field(gt=0.0)
    idle_seconds: float = Field(gt=0.0)
    starting_seconds: float = Field(gt=0.0)
    maintenance_seconds: float = Field(gt=0.0)
    idle_probability: float = Field(ge=0.0, le=1.0)
    maintenance_probability: float = Field(ge=0.0, le=1.0)

    @model_validator(mode="after")
    def _probabilities_sum_to_one(self) -> LifecycleConfig:
        total = self.idle_probability + self.maintenance_probability
        if abs(total - 1.0) > 1e-6:
            raise ValueError(f"idle_probability + maintenance_probability must be 1.0, got {total}")
        return self


class Coefficients(_Model):
    """Coupling between the hidden state and the observable sensors.

    These are what make the data multivariate. ``power_wear_per_rpm`` is the
    important one: it makes power rise at constant speed as wear accumulates,
    which is the friction signature that ``power_per_rpm`` was designed to catch
    while both power and speed individually stay inside their nominal range.
    """

    power_base_kw: float
    power_per_rpm: float = Field(gt=0.0)
    power_wear_per_rpm: float = Field(ge=0.0)
    vibration_base_mm_s: float = Field(ge=0.0)
    vibration_per_load: float = Field(ge=0.0)
    vibration_per_wear: float = Field(ge=0.0)
    pressure_base_bar: float = Field(ge=0.0)
    pressure_per_load: float = Field(ge=0.0)
    temperature_per_kw: float = Field(ge=0.0)


class SimulationConfig(_Model):
    """True physics of a profile. Never leaves the simulator."""

    rated_rpm: float = Field(gt=0.0)
    thermal_inertia: float = Field(gt=0.0, le=1.0)
    load: LoadConfig
    wear: WearConfig
    ambient: AmbientConfig
    noise: NoiseConfig
    lifecycle: LifecycleConfig
    coefficients: Coefficients


class MachineProfile(_Model):
    machine_type: str = Field(min_length=1)
    criticality: Literal["LOW", "MEDIUM", "HIGH"]
    nominal: NominalRanges
    simulation: SimulationConfig


class MachineSpec(_Model):
    machine_id: str = Field(pattern=MACHINE_ID_PATTERN)
    line_id: str = Field(pattern=LINE_ID_PATTERN)
    profile: str = Field(min_length=1)
    firmware_version: str | None = None


class ResolvedMachine(_Model):
    """A machine with its profile already attached."""

    machine_id: str
    line_id: str
    firmware_version: str | None
    profile_name: str
    profile: MachineProfile


class AnomalySpec(_Model):
    """One configurable anomaly type: family, target, duration, intensity."""

    type: AnomalyType
    family: AnomalyFamily
    weight: float = Field(gt=0.0)
    sensor: str | None = None
    duration_seconds: Bounds
    intensity: Bounds

    @field_validator("sensor")
    @classmethod
    def _known_sensor(cls, value: str | None) -> str | None:
        if value is not None and value not in SENSOR_NAMES:
            raise ValueError(f"unknown sensor {value!r}; expected one of {list(SENSOR_NAMES)}")
        return value

    @model_validator(mode="after")
    def _sensor_required_by_family(self) -> AnomalySpec:
        # A DRIFT episode may act on hidden wear rather than on a single sensor,
        # which is what makes BEARING_WEAR show up on several sensors at once.
        # Every other family targets one specific measurement.
        if self.family in ("SPIKE", "SENSOR_FAILURE") and self.sensor is None:
            raise ValueError(f"family {self.family} requires an explicit sensor")
        if self.duration_seconds.min <= 0:
            raise ValueError("duration_seconds.min must be strictly positive")
        return self


class ScenarioSpec(_Model):
    """A deterministic episode, injected regardless of the random draw."""

    machine_id: str = Field(pattern=MACHINE_ID_PATTERN)
    type: AnomalyType
    start_offset_seconds: float = Field(ge=0.0)
    duration_seconds: float = Field(gt=0.0)
    intensity: float | None = None


class AnomaliesConfig(_Model):
    enabled: bool = True
    episodes_per_hour: float = Field(ge=0.0)
    min_gap_seconds: float = Field(ge=0.0)
    types: list[AnomalySpec]
    scenarios: list[ScenarioSpec] = Field(default_factory=list)

    @model_validator(mode="after")
    def _types_are_usable(self) -> AnomaliesConfig:
        if self.enabled and not self.types:
            raise ValueError("anomalies are enabled but no type is declared")
        declared = {spec.type for spec in self.types}
        for scenario in self.scenarios:
            if scenario.type not in declared:
                raise ValueError(
                    f"scenario references anomaly type {scenario.type} which is not declared"
                )
        return self

    def spec_for(self, anomaly_type: AnomalyType) -> AnomalySpec:
        for spec in self.types:
            if spec.type is anomaly_type:
                return spec
        raise KeyError(f"no anomaly specification for {anomaly_type}")


class DelayBounds(_Model):
    probability: float = Field(ge=0.0, le=1.0)
    delay_seconds: Bounds


class OutageConfig(_Model):
    probability_per_hour: float = Field(ge=0.0)
    duration_seconds: Bounds


class LateDataConfig(_Model):
    """Publication delay, never back-dating (decision D-23)."""

    enabled: bool = True
    jitter: DelayBounds
    outage: OutageConfig


class LabelsConfig(_Model):
    emit_normal_labels: bool = False


class FleetConfig(_Model):
    profiles: dict[str, MachineProfile]
    machines: list[MachineSpec]
    anomalies: AnomaliesConfig
    late_data: LateDataConfig
    labels: LabelsConfig

    @model_validator(mode="after")
    def _machines_are_consistent(self) -> FleetConfig:
        if not self.machines:
            raise ValueError("the fleet declares no machine")

        seen: set[str] = set()
        for machine in self.machines:
            if machine.machine_id in seen:
                raise ValueError(f"duplicate machine_id {machine.machine_id}")
            seen.add(machine.machine_id)
            if machine.profile not in self.profiles:
                raise ValueError(
                    f"machine {machine.machine_id} references unknown profile "
                    f"{machine.profile!r}; declared profiles are {sorted(self.profiles)}"
                )

        for scenario in self.anomalies.scenarios:
            if scenario.machine_id not in seen:
                raise ValueError(f"scenario targets unknown machine {scenario.machine_id}")
        return self

    def resolved_machines(self) -> list[ResolvedMachine]:
        """Machines with their profile attached, in declaration order.

        Declaration order matters: it is not used to seed randomness -- streams
        are keyed by machine identity so that reordering the file changes
        nothing -- but it fixes the publication order within a tick, which keeps
        runs comparable.
        """
        return [
            ResolvedMachine(
                machine_id=machine.machine_id,
                line_id=machine.line_id,
                firmware_version=machine.firmware_version,
                profile_name=machine.profile,
                profile=self.profiles[machine.profile],
            )
            for machine in self.machines
        ]


def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    """Merge ``override`` onto ``base`` recursively, without mutating either."""
    merged = dict(base)
    for key, value in override.items():
        existing = merged.get(key)
        if isinstance(existing, dict) and isinstance(value, dict):
            merged[key] = _deep_merge(existing, value)
        else:
            merged[key] = value
    return merged


def _apply_defaults(document: dict[str, Any]) -> dict[str, Any]:
    """Fold ``defaults.simulation`` into every profile.

    Profiles override only what differs, so a change to shared physics is made
    once. The merge happens before validation, which means a profile can rely on
    a default for a required field without the model needing optional fields
    everywhere.
    """
    defaults = document.get("defaults") or {}
    default_simulation = defaults.get("simulation") or {}

    profiles = document.get("profiles") or {}
    if not isinstance(profiles, dict):
        raise ConfigurationError("fleet.yaml: 'profiles' must be a mapping")

    resolved: dict[str, Any] = {}
    for name, profile in profiles.items():
        if not isinstance(profile, dict):
            raise ConfigurationError(f"fleet.yaml: profile {name!r} must be a mapping")
        merged = dict(profile)
        merged["simulation"] = _deep_merge(default_simulation, profile.get("simulation") or {})
        resolved[name] = merged

    document = dict(document)
    document["profiles"] = resolved
    document.pop("defaults", None)
    return document


def load_fleet(path: Path) -> FleetConfig:
    """Read and validate ``fleet.yaml``.

    Raises:
        ConfigurationError: if the file is missing, is not valid YAML, or does
            not satisfy the model. The component must refuse to start rather
            than run on a half-understood fleet.
    """
    try:
        raw = path.read_text(encoding="utf-8")
    except OSError as exc:
        raise ConfigurationError(f"cannot read fleet configuration at {path}: {exc}") from exc

    try:
        document = yaml.safe_load(raw)
    except yaml.YAMLError as exc:
        raise ConfigurationError(f"fleet configuration at {path} is not valid YAML: {exc}") from exc

    if not isinstance(document, dict):
        raise ConfigurationError(
            f"fleet configuration at {path} must be a mapping, got {type(document).__name__}"
        )

    try:
        return FleetConfig.model_validate(_apply_defaults(document))
    except ValidationError as exc:
        problems = "; ".join(
            f"{'.'.join(str(part) for part in error['loc'])}: {error['msg']}"
            for error in exc.errors()
        )
        raise ConfigurationError(f"invalid fleet configuration at {path}: {problems}") from exc
