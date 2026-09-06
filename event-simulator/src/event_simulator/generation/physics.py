"""The coupling that turns a hidden state into five coherent measurements.

This module is where the dataset earns its value. Five independently noised
signals would contain no multivariate structure, and the Phase 2 model could not
beat a per-sensor threshold -- the comparison docs/07-ml-methodology.md makes
mandatory. So everything is derived from two hidden variables:

    rotation_rpm = rated x motion x (0.85 + 0.15 x load)
    power_kw     = base + k1 x rpm x load + k2 x wear x rpm
    vibration    = base + k3 x load + k4 x wear
    pressure_bar = (base + k5 x load) x motion
    temperature  = ambient(t) + k6 x power, reached through a first-order lag

The second power term is the one that matters: as wear accumulates, friction
makes power rise **at constant speed**, while power and speed each stay inside
their own nominal range. That is the anomaly a per-sensor threshold structurally
cannot see, and exactly what the ``power_per_rpm`` feature was designed to
catch.

Everything here is a pure function of state, configuration and an injected
generator, which is what makes the whole simulation reproducible from a seed.
"""

from __future__ import annotations

import math
from datetime import datetime

from numpy.random import Generator
from telemetry_core.enums import MachineState
from telemetry_core.schemas import SENSOR_NAMES, SensorReadings

from event_simulator.config.fleet import LifecycleConfig, MachineProfile
from event_simulator.generation.state import HiddenState

__all__ = [
    "PHYSICAL_LIMITS",
    "SECONDS_PER_DAY",
    "ambient_temperature",
    "clamp_to_physical_limits",
    "daily_cycle",
    "motion_factor",
    "next_load",
    "observe",
    "steady_state_temperature",
]

SECONDS_PER_DAY = 86_400.0

#: Absolute bounds from contracts/json-schema/telemetry-raw.v1.json.
#: Mirrored rather than imported because the schema is a JSON document, and kept
#: honest by a test that reads the schema and compares.
PHYSICAL_LIMITS: dict[str, tuple[float, float]] = {
    "temperature_c": (-50.0, 300.0),
    "vibration_mm_s": (0.0, 100.0),
    "pressure_bar": (0.0, 50.0),
    "power_kw": (0.0, 500.0),
    "rotation_rpm": (0.0, 6000.0),
}

#: How much the machine is actually turning, per operating state.
_IDLE_MOTION = 0.35
_STARTING_FLOOR = 0.20

#: Vibration behaves differently from the rest: a machine shakes more while
#: starting than while running steadily. That is the whole point of publishing
#: machine_state -- high vibration during STARTING is normal, and the same
#: reading while RUNNING is not (docs/02 section 4).
_VIBRATION_MULTIPLIER: dict[MachineState, float] = {
    MachineState.RUNNING: 1.0,
    MachineState.IDLE: 0.9,
    MachineState.STARTING: 1.6,
    MachineState.MAINTENANCE: 0.15,
    MachineState.STOPPED: 0.05,
    MachineState.UNKNOWN: 1.0,
}


def daily_cycle(at: datetime, peak_hour_utc: int) -> float:
    """Daily seasonality in [-1, 1], peaking at ``peak_hour_utc``."""
    seconds = at.hour * 3600.0 + at.minute * 60.0 + at.second + at.microsecond / 1_000_000.0
    phase = 2.0 * math.pi * (seconds / SECONDS_PER_DAY - peak_hour_utc / 24.0)
    return math.cos(phase)


def ambient_temperature(at: datetime, profile: MachineProfile) -> float:
    ambient = profile.simulation.ambient
    return ambient.mean_c + ambient.daily_amplitude_c * daily_cycle(at, ambient.peak_hour_utc)


def motion_factor(
    machine_state: MachineState, seconds_remaining: float, lifecycle: LifecycleConfig
) -> float:
    """Fraction of rated motion for the current operating state.

    STARTING ramps from a floor to full over its own duration, which produces the
    transient that makes startup vibration legitimately high.
    """
    if machine_state is MachineState.RUNNING:
        return 1.0
    if machine_state is MachineState.IDLE:
        return _IDLE_MOTION
    if machine_state is MachineState.STARTING:
        elapsed = max(0.0, lifecycle.starting_seconds - seconds_remaining)
        ramp = min(1.0, elapsed / lifecycle.starting_seconds)
        return _STARTING_FLOOR + (1.0 - _STARTING_FLOOR) * ramp
    return 0.0


def next_load(
    state: HiddenState, at: datetime, profile: MachineProfile, generator: Generator
) -> float:
    """Advance the hidden load one tick, as an Ornstein-Uhlenbeck process.

    Mean-reverting rather than white noise: a plant's load wanders smoothly over
    minutes. Redrawing it independently every second would make every derived
    sensor jitter with nothing physical behind the movement, and windowed
    features would see noise where they should see structure.
    """
    load_config = profile.simulation.load
    seasonal = load_config.daily_amplitude * daily_cycle(
        at, profile.simulation.ambient.peak_hour_utc
    )
    target = load_config.mean + seasonal
    innovation = float(generator.normal(0.0, load_config.sigma))
    updated = state.load + load_config.reversion * (target - state.load) + innovation
    return min(1.0, max(0.0, updated))


def steady_state_temperature(profile: MachineProfile, at: datetime) -> float:
    """Temperature a machine settles at under its mean load.

    Used to initialise the thermal state. Starting from an arbitrary value would
    produce a warm-up transient at the beginning of every run that looks like an
    anomaly and is not one.
    """
    simulation = profile.simulation
    coefficients = simulation.coefficients
    mean_load = simulation.load.mean
    rpm = simulation.rated_rpm * (0.85 + 0.15 * mean_load)
    power = (
        coefficients.power_base_kw
        + coefficients.power_per_rpm * rpm * (0.55 + 0.45 * mean_load)
        + coefficients.power_wear_per_rpm * simulation.wear.initial * rpm
    )
    return ambient_temperature(at, profile) + coefficients.temperature_per_kw * power


def observe(
    state: HiddenState, profile: MachineProfile, at: datetime, generator: Generator
) -> SensorReadings:
    """Compute the five measurements implied by the hidden state.

    Draw order is fixed and unconditional: rpm, power, vibration, pressure,
    temperature. A conditional draw would make the noise sequence depend on the
    machine's state, and two runs with the same seed but different state
    trajectories would stop being comparable.
    """
    simulation = profile.simulation
    coefficients = simulation.coefficients
    noise = simulation.noise
    motion = motion_factor(state.machine_state, state.state_seconds_remaining, simulation.lifecycle)

    rpm = simulation.rated_rpm * motion * (0.85 + 0.15 * state.load)
    rpm += float(generator.normal(0.0, noise.rotation_rpm)) * max(motion, 0.05)

    power = (
        coefficients.power_base_kw * (0.15 + 0.85 * motion)
        + coefficients.power_per_rpm * rpm * (0.55 + 0.45 * state.load)
        + coefficients.power_wear_per_rpm * state.wear * rpm
    )
    power += float(generator.normal(0.0, noise.power_kw))

    vibration = (
        coefficients.vibration_base_mm_s
        + coefficients.vibration_per_load * state.load * motion
        + coefficients.vibration_per_wear * state.wear
    )
    vibration *= _VIBRATION_MULTIPLIER[state.machine_state] * (0.1 + 0.9 * motion)
    vibration += float(generator.normal(0.0, noise.vibration_mm_s))

    pressure = (
        coefficients.pressure_base_bar + coefficients.pressure_per_load * state.load
    ) * motion
    pressure += float(generator.normal(0.0, noise.pressure_bar))

    # Thermal inertia: temperature chases its target instead of jumping to it,
    # which is what gives the signal a memory and makes a slow drift detectable
    # as a trend rather than as noise.
    target_temperature = ambient_temperature(at, profile) + coefficients.temperature_per_kw * max(
        power, 0.0
    )
    temperature = state.temperature_c + simulation.thermal_inertia * (
        target_temperature - state.temperature_c
    )
    temperature += float(generator.normal(0.0, noise.temperature_c))

    readings = SensorReadings(
        temperature_c=temperature,
        vibration_mm_s=vibration,
        pressure_bar=pressure,
        power_kw=power,
        rotation_rpm=rpm,
    )
    clamped, _ = clamp_to_physical_limits(readings)
    return clamped


def clamp_to_physical_limits(readings: SensorReadings) -> tuple[SensorReadings, int]:
    """Clamp to the contract's absolute bounds and report how often it bit.

    A clamp firing is a configuration bug, not an anomaly: an intensity was set
    high enough to push a sensor outside the physically valid range, which would
    make the anomaly trivially detectable and the evaluation worthless
    (docs/07 section 1, guard 2). The count is logged so the bug is visible
    rather than silently absorbed.
    """
    values = readings.as_mapping()
    clamped_count = 0
    for name in SENSOR_NAMES:
        value = values[name]
        if value is None:
            continue
        low, high = PHYSICAL_LIMITS[name]
        if value < low:
            values[name] = low
            clamped_count += 1
        elif value > high:
            values[name] = high
            clamped_count += 1

    if clamped_count == 0:
        return readings, 0

    return (
        SensorReadings(
            temperature_c=values["temperature_c"],
            vibration_mm_s=values["vibration_mm_s"],
            pressure_bar=values["pressure_bar"],
            power_kw=values["power_kw"],
            rotation_rpm=values["rotation_rpm"],
        ),
        clamped_count,
    )
