"""The three anomaly families, as interchangeable strategies.

**Strategy pattern.** Each family is a class behind one interface, so adding a
family is a new class rather than another branch in the generator. That matters
here beyond tidiness: the families differ in *where* they act, and mixing that
decision into the generator would hide it.

Two levels of action, and the distinction is physical rather than technical:

* **State-level** -- the machine genuinely changes. Bearing wear increases
  friction, so power, vibration and temperature all move together, because they
  are all derived from the hidden state. This is what produces a multivariate
  anomaly that no single-sensor threshold can see.
* **Reading-level** -- only the measurement changes. A stuck sensor reports a
  frozen value while the machine keeps running normally. Nothing physical
  happened; the instrument failed.

Anomalies stay inside the physically valid ranges of the contract. A temperature
of 812 C is a broken sensor, not a machine anomaly, and a detector that "finds"
it has learned nothing (docs/07-ml-methodology.md section 1, guard 2).
"""

from __future__ import annotations

import math
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import ClassVar

from telemetry_core.enums import AnomalyType
from telemetry_core.schemas import SensorReadings

from event_simulator.anomalies.episode import Episode
from event_simulator.config.fleet import AnomalyFamily
from event_simulator.generation.state import HiddenState, with_sensor

__all__ = [
    "AnomalyInjector",
    "DriftInjector",
    "InjectionContext",
    "SensorFailureInjector",
    "SpikeInjector",
    "injector_for",
]


@dataclass
class InjectionContext:
    """What an injector needs, plus a scratch pad scoped to the episode.

    ``memory`` exists for the one family that is genuinely stateful: a stuck
    sensor must keep reporting the value it froze on. Passing that state in
    explicitly keeps the injectors themselves shareable and free of per-machine
    state, which would otherwise make a single injector instance unusable across
    a fleet.
    """

    episode: Episode
    progress: float
    memory: dict[str, float]


class AnomalyInjector(ABC):
    """One anomaly family."""

    family: ClassVar[AnomalyFamily]

    def affect_state(self, state: HiddenState, context: InjectionContext) -> HiddenState:
        """Alter the machine's physical state. Default: no physical change."""
        return state

    @abstractmethod
    def affect_readings(
        self, readings: SensorReadings, context: InjectionContext
    ) -> SensorReadings:
        """Alter the measured values."""


class SpikeInjector(AnomalyInjector):
    """A short, sharp excursion on one sensor.

    Shaped as a raised sine over the episode rather than a rectangular pulse: a
    step would be trivially detectable by a first difference, which would make
    the evaluation flattering and meaningless.
    """

    family = "SPIKE"

    def affect_readings(
        self, readings: SensorReadings, context: InjectionContext
    ) -> SensorReadings:
        sensor = context.episode.sensor
        if sensor is None:  # pragma: no cover - forbidden by config validation
            raise ValueError("a SPIKE episode requires a target sensor")

        current = readings.as_mapping()[sensor]
        if current is None:
            # A sensor that is already reporting nothing cannot spike. Leaving it
            # null keeps the two failure modes distinguishable downstream.
            return readings

        shape = math.sin(math.pi * context.progress)
        return with_sensor(readings, sensor, current + context.episode.intensity * shape)


class DriftInjector(AnomalyInjector):
    """A progressive degradation, growing monotonically over the episode.

    Two behaviours, chosen by whether the episode names a sensor:

    * ``sensor is None`` (BEARING_WEAR): acts on hidden **wear**. Power,
      vibration and temperature then rise together through the physical
      coupling, while each stays inside its own nominal range. This is the
      anomaly a per-sensor threshold structurally cannot see, and the reason the
      ``power_per_rpm`` feature exists.
    * ``sensor`` named (MOTOR_STALL): multiplicative loss on that sensor. Speed
      falls while power is unchanged, so the specific consumption climbs -- the
      signature of a motor labouring against a load.
    """

    family = "DRIFT"

    def affect_state(self, state: HiddenState, context: InjectionContext) -> HiddenState:
        if context.episode.sensor is not None:
            return state
        added_wear = context.episode.intensity * context.progress
        return state.advanced(wear=state.wear + added_wear)

    def affect_readings(
        self, readings: SensorReadings, context: InjectionContext
    ) -> SensorReadings:
        sensor = context.episode.sensor
        if sensor is None:
            # Wear already moved the physics; the readings follow from it.
            return readings

        current = readings.as_mapping()[sensor]
        if current is None:
            return readings

        factor = 1.0 + context.episode.intensity * context.progress
        degraded = with_sensor(readings, sensor, max(0.0, current * factor))

        # A stalling motor also shakes. Without this the anomaly would be
        # visible on a single axis, which is not what a stall looks like.
        vibration = degraded.vibration_mm_s
        if vibration is not None:
            boost = 1.0 + 0.5 * abs(context.episode.intensity) * context.progress
            degraded = with_sensor(degraded, "vibration_mm_s", vibration * boost)
        return degraded


class SensorFailureInjector(AnomalyInjector):
    """Instrument failure: the machine is fine, the measurement is not.

    ``SENSOR_STUCK`` freezes the sensor at the value it held when the episode
    began -- a flatline, which a variance-based feature notices and a
    threshold-based one does not. ``SENSOR_DROPOUT`` reports ``null``.

    Null rather than zero, always. Zero is a valid physical value; conflating
    the two manufactures anomalies that never happened (docs/02 section 4).
    """

    family = "SENSOR_FAILURE"

    _FROZEN_KEY = "frozen_value"

    def affect_readings(
        self, readings: SensorReadings, context: InjectionContext
    ) -> SensorReadings:
        sensor = context.episode.sensor
        if sensor is None:  # pragma: no cover - forbidden by config validation
            raise ValueError("a SENSOR_FAILURE episode requires a target sensor")

        if context.episode.anomaly_type is AnomalyType.SENSOR_DROPOUT:
            return with_sensor(readings, sensor, None)

        current = readings.as_mapping()[sensor]
        frozen = context.memory.get(self._FROZEN_KEY)
        if frozen is None:
            if current is None:
                # Nothing to freeze yet; wait for a real value.
                return readings
            frozen = current
            context.memory[self._FROZEN_KEY] = frozen
        return with_sensor(readings, sensor, frozen)


_INJECTORS: dict[AnomalyFamily, AnomalyInjector] = {
    "SPIKE": SpikeInjector(),
    "DRIFT": DriftInjector(),
    "SENSOR_FAILURE": SensorFailureInjector(),
}


def injector_for(family: AnomalyFamily) -> AnomalyInjector:
    """Return the strategy for ``family``.

    Instances are stateless and shared across the fleet; per-episode state lives
    in :class:`InjectionContext`.
    """
    try:
        return _INJECTORS[family]
    except KeyError as exc:  # pragma: no cover - forbidden by config validation
        raise ValueError(f"no injector for anomaly family {family!r}") from exc
