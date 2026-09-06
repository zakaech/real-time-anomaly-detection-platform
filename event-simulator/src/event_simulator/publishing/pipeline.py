"""Generation to publication, with delay applied in between.

Kept separate from ``main`` so the whole path -- generate, project, delay,
publish -- can be exercised in tests against a fake producer, with no broker and
no waiting. That is what makes the schema-conformance and no-leak tests cover the
real code path rather than a simplified stand-in.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime

from telemetry_core.logging import get_logger
from telemetry_core.timeutil import format_instant

from event_simulator.config.fleet import FleetConfig
from event_simulator.generation.clock import Clock
from event_simulator.generation.fleet_runner import FleetRunner, RunLimits
from event_simulator.generation.rng import RngRegistry
from event_simulator.generation.state import GeneratedSample
from event_simulator.publishing.late_buffer import LateBuffer, LateDataPolicy
from event_simulator.publishing.producer import TelemetryPublisher
from event_simulator.publishing.projections import (
    new_event_id,
    to_telemetry_label,
    to_telemetry_raw,
)

__all__ = ["PipelineResult", "SimulationPipeline"]

_LOGGER = get_logger(__name__)


@dataclass
class PipelineResult:
    """What a run actually did. Every field is counted, never estimated."""

    ticks: int = 0
    samples_generated: int = 0
    telemetry_published: int = 0
    labels_published: int = 0
    anomalous_samples: int = 0
    delayed_samples: int = 0
    buffered_at_exit: int = 0
    clamped_readings: int = 0
    first_event_time: datetime | None = None
    last_event_time: datetime | None = None
    episodes: set[str] = field(default_factory=set)

    def as_dict(self) -> dict[str, object]:
        return {
            "ticks": self.ticks,
            "samples_generated": self.samples_generated,
            "telemetry_published": self.telemetry_published,
            "labels_published": self.labels_published,
            "anomalous_samples": self.anomalous_samples,
            "delayed_samples": self.delayed_samples,
            "buffered_at_exit": self.buffered_at_exit,
            "clamped_readings": self.clamped_readings,
            "distinct_episodes": len(self.episodes),
            "first_event_time": (
                format_instant(self.first_event_time) if self.first_event_time else None
            ),
            "last_event_time": (
                format_instant(self.last_event_time) if self.last_event_time else None
            ),
        }


class SimulationPipeline:
    """Runs the fleet and publishes what it produces."""

    def __init__(
        self,
        *,
        fleet: FleetConfig,
        clock: Clock,
        rng: RngRegistry,
        publisher: TelemetryPublisher,
    ) -> None:
        self._fleet = fleet
        self._clock = clock
        self._rng = rng
        self._publisher = publisher
        self._runner = FleetRunner(fleet=fleet, clock=clock, rng=rng)
        self._late_policy = LateDataPolicy(
            config=fleet.late_data, rng=rng, tick_interval=clock.tick_interval
        )
        self._buffer = LateBuffer()
        self._result = PipelineResult()

    @property
    def result(self) -> PipelineResult:
        return self._result

    def run(
        self,
        limits: RunLimits | None = None,
        should_stop: Callable[[], bool] | None = None,
    ) -> PipelineResult:
        for batch in self._runner.run(limits, should_stop):
            self._result.ticks += 1

            for sample in batch.samples:
                self._result.samples_generated += 1
                if sample.ground_truth.is_anomaly:
                    self._result.anomalous_samples += 1
                    if sample.ground_truth.episode_id is not None:
                        self._result.episodes.add(sample.ground_truth.episode_id)

                delay = self._late_policy.delay_for(sample)
                if delay.total_seconds() > 0.0:
                    self._result.delayed_samples += 1
                    self._buffer.hold(sample, delay)
                else:
                    self._publish(sample)

            # Release held samples whose delay has elapsed in simulated time.
            # They are published after this tick's fresh samples, which is what
            # makes them genuinely out of order inside their partition.
            for released in self._buffer.release_due(batch.event_time):
                self._publish(released)

        self._result.buffered_at_exit = len(self._buffer)
        self._result.clamped_readings = self._runner.clamped_readings

        if self._result.buffered_at_exit:
            # Not published: their delay has not elapsed. Emitting them now
            # would compress an outage into an instantaneous burst at shutdown,
            # which is an artefact of stopping rather than of the network.
            _LOGGER.info(
                "late_buffer_discarded_at_exit",
                samples=self._result.buffered_at_exit,
            )

        return self._result

    def _publish(self, sample: GeneratedSample) -> None:
        event_id = new_event_id(self._rng.stream("event_id", sample.machine_id))
        ingest_time = self._clock.wall_now()

        telemetry = to_telemetry_raw(sample, event_id=event_id, ingest_time=ingest_time)
        if self._publisher.publish_telemetry(telemetry):
            self._result.telemetry_published += 1

        label = to_telemetry_label(
            sample,
            event_id=event_id,
            emitted_at=ingest_time,
            emit_normal_labels=self._fleet.labels.emit_normal_labels,
        )
        if label is not None and self._publisher.publish_label(label):
            self._result.labels_published += 1

        if self._result.first_event_time is None:
            self._result.first_event_time = sample.event_time
        self._result.last_event_time = (
            sample.event_time
            if self._result.last_event_time is None
            else max(self._result.last_event_time, sample.event_time)
        )
