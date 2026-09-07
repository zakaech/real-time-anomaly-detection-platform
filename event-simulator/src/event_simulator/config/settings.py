"""Environment-driven settings for the simulator process.

Split from ``fleet.yaml`` on purpose. The fleet describes *what is simulated* and
is versioned with the code; these settings describe *how this run behaves* and
change per invocation. Mixing them would mean editing a committed file to run a
replay.

Kafka connection settings are not redefined here: they come from
``telemetry_core.config.KafkaSettings``, which already refuses to start without
an explicit broker address.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Literal

from pydantic import Field, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

__all__ = ["SimulatorSettings"]

RunMode = Literal["realtime", "replay"]


class SimulatorSettings(BaseSettings):
    """How this particular run of the simulator behaves."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_prefix="SIMULATOR_",
        extra="ignore",
        frozen=True,
        # An empty variable means "unset", not "invalid". Compose expands an
        # undefined variable to an empty string -- `FOO: ${FOO:-}` yields
        # FOO="" -- so without this an optional field like a seed or a run
        # duration fails validation and the container refuses to start, on
        # exactly the command the README documents.
        env_ignore_empty=True,
    )

    mode: RunMode = "realtime"

    speed_factor: float | None = Field(
        default=None,
        description=(
            "Simulated seconds produced per real second. Ignored in realtime "
            "mode, which is always 1.0. None in replay means unbounded: produce "
            "as fast as the process can."
        ),
    )

    tick_interval_seconds: float = Field(default=1.0, gt=0.0)

    seed: int | None = Field(
        default=None,
        description=(
            "Master seed. When absent one is drawn and logged, so an "
            "interesting run can still be reproduced afterwards."
        ),
    )

    fleet_path: Path = Path("config/fleet.yaml")

    replay_start: datetime | None = Field(
        default=None,
        description=(
            "Event time of tick 0 in replay mode. Defaults to now minus the run "
            "duration, so a replay ends at the present instant."
        ),
    )

    duration_seconds: float | None = Field(default=None, gt=0.0)
    max_events: int | None = Field(default=None, gt=0)

    flush_timeout_seconds: float = Field(default=30.0, gt=0.0)
    client_id: str = Field(default="event-simulator", min_length=1)

    @field_validator(
        "seed", "speed_factor", "duration_seconds", "max_events", "replay_start", mode="before"
    )
    @classmethod
    def _empty_string_means_unset(cls, value: object) -> object:
        """Treat an empty environment variable as absent.

        Docker Compose interpolates an unset variable to the empty string, so
        `SIMULATOR_SEED: ${SIMULATOR_SEED:-}` arrives as "" rather than not
        arriving at all. Without this, running the simulator with default
        settings would fail validation on a value nobody set.
        """
        if isinstance(value, str) and value.strip() == "":
            return None
        return value

    @model_validator(mode="after")
    def _replay_is_bounded(self) -> SimulatorSettings:
        # An unbounded replay would keep advancing event time past the present
        # and publish samples dated in the future, which is nonsense rather than
        # a useful default.
        if self.mode == "replay" and self.duration_seconds is None and self.max_events is None:
            raise ValueError(
                "replay mode requires SIMULATOR_DURATION_SECONDS or SIMULATOR_MAX_EVENTS: "
                "without a bound the run would generate event times in the future"
            )
        if self.speed_factor is not None and self.speed_factor <= 0:
            raise ValueError("speed_factor must be strictly positive when set")
        return self

    @property
    def effective_speed_factor(self) -> float | None:
        """Realtime is speed 1 by definition; replay honours the setting."""
        return 1.0 if self.mode == "realtime" else self.speed_factor
