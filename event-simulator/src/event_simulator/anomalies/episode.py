"""An anomaly episode: one occurrence, with a beginning, an end and a strength.

The episode -- not the individual sample -- is the unit that matters for
evaluation. A plant operator does not care that 43 of 60 anomalous points were
flagged; they care that the bearing failure was caught, and how early
(docs/07-ml-methodology.md section 4.1). ``episode_id`` is what lets Phase 2
group samples back into occurrences and measure detection latency.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from telemetry_core.enums import AnomalyType

from event_simulator.config.fleet import AnomalyFamily

__all__ = ["Episode", "format_episode_id"]


def format_episode_id(machine_id: str, started_at: datetime, sequence: int) -> str:
    """Build a readable, stable episode identifier.

    Readable on purpose: it is the identifier a human will grep for in Kafka
    when reconciling a detection against the ground truth.
    """
    compact_machine = machine_id.replace("-", "")
    return f"ep-{started_at.strftime('%Y%m%d')}-{compact_machine}-{sequence:03d}"


@dataclass(frozen=True, slots=True)
class Episode:
    """One anomaly occurrence on one machine."""

    episode_id: str
    machine_id: str
    anomaly_type: AnomalyType
    family: AnomalyFamily
    started_at: datetime
    ends_at: datetime
    intensity: float
    sensor: str | None = None

    def __post_init__(self) -> None:
        if self.ends_at <= self.started_at:
            raise ValueError(f"episode {self.episode_id}: ends_at must be after started_at")

    @property
    def duration_seconds(self) -> float:
        return (self.ends_at - self.started_at).total_seconds()

    def is_active(self, at: datetime) -> bool:
        """Half-open interval ``[started_at, ends_at)``.

        Half-open so two consecutive episodes on the same machine cannot both
        claim the boundary instant, which would make a sample ambiguous.
        """
        return self.started_at <= at < self.ends_at

    def progress(self, at: datetime) -> float:
        """Position within the episode, clamped to [0, 1].

        Drives the shape of every injector: a spike peaks in the middle, a drift
        grows monotonically towards the end.
        """
        elapsed = (at - self.started_at).total_seconds()
        return min(1.0, max(0.0, elapsed / self.duration_seconds))
