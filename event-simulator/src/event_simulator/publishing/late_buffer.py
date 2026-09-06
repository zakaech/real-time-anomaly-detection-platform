"""Publication delay: the mechanism that produces genuinely late data.

Decision D-23. The alternative -- back-dating ``event_time`` at publication --
was rejected because it writes a past timestamp onto a value computed for the
present, so the sample stops being internally consistent. Here the sample keeps
the instant at which it was measured; only its *publication* is held back,
exactly as a buffering gateway behaves after a network drop.

Two shapes, and the second is the one that matters:

* **jitter** -- individual samples delayed by a few seconds. Frequent, small,
  and easy for any watermark to absorb.
* **outage** -- a machine goes silent for tens of seconds, then flushes its
  buffer in one burst. This is what actually stresses a watermark, because it
  produces a run of samples that are all far behind the stream's high-water
  mark and arrive out of order within a single partition.

Phase 3 will need both to justify a watermark value rather than guess one; this
module only produces the data, it makes no windowing decision.
"""

from __future__ import annotations

import heapq
from dataclasses import dataclass, field
from datetime import datetime, timedelta

from event_simulator.config.fleet import LateDataConfig
from event_simulator.generation.rng import RngRegistry
from event_simulator.generation.state import GeneratedSample

__all__ = ["LateBuffer", "LateDataPolicy", "PendingSample"]

_SECONDS_PER_HOUR = 3600.0


@dataclass(frozen=True, slots=True, order=True)
class PendingSample:
    """A sample waiting for its release instant.

    ``sequence`` breaks ties so the heap never has to compare two
    ``GeneratedSample`` values, which are not ordered.
    """

    release_at: datetime
    sequence: int
    sample: GeneratedSample = field(compare=False)


class LateDataPolicy:
    """Decides how long each sample's publication is held back."""

    def __init__(
        self, *, config: LateDataConfig, rng: RngRegistry, tick_interval: timedelta
    ) -> None:
        self._config = config
        self._rng = rng
        self._tick_seconds = tick_interval.total_seconds()
        self._outage_until: dict[str, datetime] = {}
        self._outage_probability = (
            config.outage.probability_per_hour * self._tick_seconds / _SECONDS_PER_HOUR
        )

    def delay_for(self, sample: GeneratedSample) -> timedelta:
        """Return the publication delay for ``sample``; zero means publish now."""
        if not self._config.enabled:
            return timedelta(0)

        machine_id = sample.machine_id
        generator = self._rng.stream("lateness", machine_id)

        outage_end = self._outage_until.get(machine_id)
        if outage_end is not None:
            if sample.event_time < outage_end:
                # Still cut off: the sample waits for the link to come back, so
                # the whole silent period is released together.
                return outage_end - sample.event_time
            del self._outage_until[machine_id]

        if self._outage_probability > 0.0 and float(generator.random()) < self._outage_probability:
            duration = self._config.outage.duration_seconds.sample_between(
                float(generator.random())
            )
            outage_end = sample.event_time + timedelta(seconds=duration)
            self._outage_until[machine_id] = outage_end
            return outage_end - sample.event_time

        if float(generator.random()) < self._config.jitter.probability:
            seconds = self._config.jitter.delay_seconds.sample_between(float(generator.random()))
            return timedelta(seconds=seconds)

        return timedelta(0)

    def machines_in_outage(self) -> int:
        return len(self._outage_until)


class LateBuffer:
    """Holds delayed samples until their release instant, in simulated time."""

    __slots__ = ("_heap", "_sequence")

    def __init__(self) -> None:
        self._heap: list[PendingSample] = []
        self._sequence = 0

    def __len__(self) -> int:
        return len(self._heap)

    def hold(self, sample: GeneratedSample, delay: timedelta) -> None:
        self._sequence += 1
        heapq.heappush(
            self._heap,
            PendingSample(
                release_at=sample.event_time + delay,
                sequence=self._sequence,
                sample=sample,
            ),
        )

    def release_due(self, now: datetime) -> list[GeneratedSample]:
        """Pop every sample whose release instant has passed.

        Released in release order, which for a burst means the machine's samples
        come out grouped and behind the stream's current position -- the shape
        Phase 3 has to cope with.
        """
        released: list[GeneratedSample] = []
        while self._heap and self._heap[0].release_at <= now:
            released.append(heapq.heappop(self._heap).sample)
        return released

    def drain(self) -> list[GeneratedSample]:
        """Release everything still held, regardless of its release instant."""
        remaining = [entry.sample for entry in sorted(self._heap)]
        self._heap = []
        return remaining
