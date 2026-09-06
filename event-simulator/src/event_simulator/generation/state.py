"""Hidden machine state and the internal sample that carries ground truth.

The distinction this module encodes is the one the whole simulator rests on:

* :class:`HiddenState` is the truth about the machine -- its load, its wear, its
  thermal state. No consumer of the platform ever sees it. In a real plant
  nobody knows these values; that is precisely why anomaly detection is
  unsupervised.
* :class:`GeneratedSample` is what the simulator produces internally. It carries
  both the observable readings **and** the ground truth, because the simulator
  is the only component entitled to know both.

Two explicit projections then split it (``publishing/projections.py``). The
separation cannot leak by accident: ``TelemetryRaw`` has no field in which a
label could be placed, so the type system enforces what a code review would
otherwise have to catch.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime

from telemetry_core.enums import AnomalyType, MachineState
from telemetry_core.schemas import SensorReadings

__all__ = ["GeneratedSample", "HiddenState", "SampleGroundTruth", "with_sensor"]


def with_sensor(readings: SensorReadings, sensor: str, value: float | None) -> SensorReadings:
    """Return a copy of ``readings`` with one sensor replaced.

    Written out field by field rather than with ``**kwargs``: an unpacked
    dictionary would silently accept a misspelled sensor name and construct a
    reading that never reaches the wire.
    """
    values = readings.as_mapping()
    if sensor not in values:
        raise KeyError(f"unknown sensor {sensor!r}")
    values[sensor] = value
    return SensorReadings(
        temperature_c=values["temperature_c"],
        vibration_mm_s=values["vibration_mm_s"],
        pressure_bar=values["pressure_bar"],
        power_kw=values["power_kw"],
        rotation_rpm=values["rotation_rpm"],
    )


@dataclass(frozen=True, slots=True)
class HiddenState:
    """Latent physical state of one machine.

    ``load`` and ``wear`` are what make the five sensors move together. Drawing
    each sensor independently would produce a dataset with no multivariate
    structure at all, and a model trained on it could not beat a per-sensor
    threshold -- which is the comparison docs/07 makes mandatory.
    """

    load: float
    wear: float
    temperature_c: float
    machine_state: MachineState
    state_seconds_remaining: float
    ticks: int = 0

    def advanced(self, **changes: object) -> HiddenState:
        return replace(self, **changes)  # type: ignore[arg-type]


@dataclass(frozen=True, slots=True)
class SampleGroundTruth:
    """Evaluation-only annotation. Never published on ``telemetry.raw``."""

    is_anomaly: bool
    anomaly_type: AnomalyType | None = None
    episode_id: str | None = None
    episode_started_at: datetime | None = None

    @classmethod
    def normal(cls) -> SampleGroundTruth:
        return cls(is_anomaly=False)


@dataclass(frozen=True, slots=True)
class GeneratedSample:
    """One simulated measurement, with its ground truth attached.

    Internal to the simulator. Only :mod:`event_simulator.publishing.projections`
    turns it into wire messages, and it does so through two separate functions so
    that the telemetry path never has a reference to the label.
    """

    machine_id: str
    line_id: str
    event_time: datetime
    machine_state: MachineState
    readings: SensorReadings
    firmware_version: str | None
    ground_truth: SampleGroundTruth
