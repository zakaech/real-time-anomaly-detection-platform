"""Controlled generation of late data, for the streaming watermark.

The simulator produces the data; it makes no windowing decision. What matters
here is that the delay is a *publication* delay: ``event_time`` keeps the instant
of measurement, so a delayed sample stays internally consistent. Back-dating
would write a past timestamp onto a value computed for the present.
"""

from __future__ import annotations

from datetime import timedelta
from itertools import pairwise

from telemetry_core.config import KafkaTopics
from telemetry_core.enums import MachineState
from telemetry_core.schemas import SensorReadings
from telemetry_core.timeutil import parse_instant

from conftest import RUN_START, FakeProducer, make_clock
from event_simulator.config.fleet import Bounds, FleetConfig
from event_simulator.generation.fleet_runner import RunLimits
from event_simulator.generation.rng import RngRegistry
from event_simulator.generation.state import GeneratedSample, SampleGroundTruth
from event_simulator.publishing.late_buffer import LateBuffer, LateDataPolicy
from event_simulator.publishing.pipeline import SimulationPipeline
from event_simulator.publishing.producer import TelemetryPublisher

_READINGS = SensorReadings(70.0, 2.0, 5.0, 12.0, 1450.0)


def _sample(machine_id: str = "M-001", offset_seconds: float = 0.0) -> GeneratedSample:
    return GeneratedSample(
        machine_id=machine_id,
        line_id="LINE-A",
        event_time=RUN_START + timedelta(seconds=offset_seconds),
        machine_state=MachineState.RUNNING,
        readings=_READINGS,
        firmware_version=None,
        ground_truth=SampleGroundTruth.normal(),
    )


def _late_config(fleet: FleetConfig, **overrides: object) -> FleetConfig:
    return fleet.model_copy(update={"late_data": fleet.late_data.model_copy(update=overrides)})


class TestPolicy:
    def test_disabled_means_no_delay(self, fleet: FleetConfig) -> None:
        config = _late_config(fleet, enabled=False).late_data
        policy = LateDataPolicy(
            config=config, rng=RngRegistry(1), tick_interval=timedelta(seconds=1)
        )
        for index in range(200):
            assert policy.delay_for(_sample(offset_seconds=index)) == timedelta(0)

    def test_certain_jitter_delays_every_sample_within_bounds(self, fleet: FleetConfig) -> None:
        config = _late_config(
            fleet,
            jitter=fleet.late_data.jitter.model_copy(update={"probability": 1.0}),
            outage=fleet.late_data.outage.model_copy(update={"probability_per_hour": 0.0}),
        ).late_data
        policy = LateDataPolicy(
            config=config, rng=RngRegistry(1), tick_interval=timedelta(seconds=1)
        )
        bounds = config.jitter.delay_seconds
        for index in range(100):
            delay = policy.delay_for(_sample(offset_seconds=index)).total_seconds()
            assert bounds.min <= delay <= bounds.max

    def test_zero_probability_means_no_jitter(self, fleet: FleetConfig) -> None:
        config = _late_config(
            fleet,
            jitter=fleet.late_data.jitter.model_copy(update={"probability": 0.0}),
            outage=fleet.late_data.outage.model_copy(update={"probability_per_hour": 0.0}),
        ).late_data
        policy = LateDataPolicy(
            config=config, rng=RngRegistry(1), tick_interval=timedelta(seconds=1)
        )
        assert all(policy.delay_for(_sample(offset_seconds=i)) == timedelta(0) for i in range(100))

    def test_an_outage_holds_a_run_of_consecutive_samples(self, fleet: FleetConfig) -> None:
        """The case that actually stresses a watermark.

        Per-event jitter is easy to absorb; a machine that goes silent and then
        flushes its buffer produces a whole run of samples far behind the
        stream's high-water mark.
        """
        config = _late_config(
            fleet,
            jitter=fleet.late_data.jitter.model_copy(update={"probability": 0.0}),
            outage=fleet.late_data.outage.model_copy(
                update={"probability_per_hour": 3600.0}  # certain on the first tick
            ),
        ).late_data
        policy = LateDataPolicy(
            config=config, rng=RngRegistry(2), tick_interval=timedelta(seconds=1)
        )

        delays = [policy.delay_for(_sample(offset_seconds=i)).total_seconds() for i in range(60)]
        held = [delay for delay in delays if delay > 0]
        assert len(held) > 10
        # Within one outage the release instant is fixed, so the remaining delay
        # shrinks by one second per tick.
        decreasing = [earlier > later for earlier, later in pairwise(held)]
        assert all(decreasing[: len(held) - 1])

    def test_delays_are_per_machine(self, fleet: FleetConfig) -> None:
        config = _late_config(
            fleet,
            jitter=fleet.late_data.jitter.model_copy(update={"probability": 0.0}),
            outage=fleet.late_data.outage.model_copy(update={"probability_per_hour": 3600.0}),
        ).late_data
        policy = LateDataPolicy(
            config=config, rng=RngRegistry(2), tick_interval=timedelta(seconds=1)
        )
        policy.delay_for(_sample("M-001"))
        policy.delay_for(_sample("M-002"))
        assert policy.machines_in_outage() == 2


class TestBuffer:
    def test_nothing_is_released_before_its_instant(self) -> None:
        buffer = LateBuffer()
        buffer.hold(_sample(offset_seconds=0), timedelta(seconds=10))
        assert buffer.release_due(RUN_START + timedelta(seconds=5)) == []
        assert len(buffer) == 1

    def test_release_happens_at_the_due_instant(self) -> None:
        buffer = LateBuffer()
        sample = _sample(offset_seconds=0)
        buffer.hold(sample, timedelta(seconds=10))
        released = buffer.release_due(RUN_START + timedelta(seconds=10))
        assert released == [sample]
        assert len(buffer) == 0

    def test_release_is_ordered_by_due_instant(self) -> None:
        buffer = LateBuffer()
        late = _sample(offset_seconds=0)
        early = _sample(offset_seconds=1)
        buffer.hold(late, timedelta(seconds=30))
        buffer.hold(early, timedelta(seconds=2))
        released = buffer.release_due(RUN_START + timedelta(seconds=60))
        assert released == [early, late]

    def test_drain_returns_everything_still_held(self) -> None:
        buffer = LateBuffer()
        buffer.hold(_sample(offset_seconds=0), timedelta(seconds=30))
        buffer.hold(_sample(offset_seconds=1), timedelta(seconds=40))
        assert len(buffer.drain()) == 2
        assert len(buffer) == 0


class TestObservedLateness:
    def test_delayed_samples_are_published_out_of_order(
        self, fleet: FleetConfig, topics: KafkaTopics
    ) -> None:
        """The observable outcome: within one machine's stream, event_time goes
        backwards on the wire, which is what the watermark has to absorb."""
        configured = _late_config(
            fleet,
            jitter=fleet.late_data.jitter.model_copy(
                update={"probability": 0.25, "delay_seconds": Bounds(min=5.0, max=15.0)}
            ),
            outage=fleet.late_data.outage.model_copy(update={"probability_per_hour": 0.0}),
        )
        producer = FakeProducer()
        pipeline = SimulationPipeline(
            fleet=configured,
            clock=make_clock(),
            rng=RngRegistry(9),
            publisher=TelemetryPublisher(producer=producer, topics=topics),
        )
        result = pipeline.run(RunLimits(max_events=200 * len(configured.machines)))

        assert result.delayed_samples > 0

        payloads = producer.payloads_for(topics.telemetry_raw)
        by_machine: dict[str, list[str]] = {}
        for payload in payloads:
            by_machine.setdefault(payload["machine_id"], []).append(payload["event_time"])

        regressions = sum(
            1
            for times in by_machine.values()
            for earlier, later in pairwise(times)
            if parse_instant(later) < parse_instant(earlier)
        )
        assert regressions > 0

    def test_event_time_is_never_back_dated(self, fleet: FleetConfig, topics: KafkaTopics) -> None:
        """Delay applies to publication, never to the measurement instant.

        Every event_time must still fall on the tick grid.
        """
        producer = FakeProducer()
        pipeline = SimulationPipeline(
            fleet=fleet,
            clock=make_clock(),
            rng=RngRegistry(9),
            publisher=TelemetryPublisher(producer=producer, topics=topics),
        )
        pipeline.run(RunLimits(max_events=100 * len(fleet.machines)))

        for payload in producer.payloads_for(topics.telemetry_raw):
            offset = (parse_instant(payload["event_time"]) - RUN_START).total_seconds()
            assert offset.is_integer(), offset

    def test_samples_still_held_at_exit_are_reported(
        self, fleet: FleetConfig, topics: KafkaTopics
    ) -> None:
        configured = _late_config(
            fleet,
            jitter=fleet.late_data.jitter.model_copy(
                update={"probability": 1.0, "delay_seconds": Bounds(min=500.0, max=600.0)}
            ),
            outage=fleet.late_data.outage.model_copy(update={"probability_per_hour": 0.0}),
        )
        producer = FakeProducer()
        pipeline = SimulationPipeline(
            fleet=configured,
            clock=make_clock(),
            rng=RngRegistry(9),
            publisher=TelemetryPublisher(producer=producer, topics=topics),
        )
        result = pipeline.run(RunLimits(max_events=20 * len(configured.machines)))

        assert result.buffered_at_exit > 0
        assert result.telemetry_published < result.samples_generated
