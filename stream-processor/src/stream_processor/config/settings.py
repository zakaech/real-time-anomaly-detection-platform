"""Streaming settings, validated at startup.

Nothing here has a default that hides an environment: broker addresses come from
``telemetry_core.config.KafkaSettings``, which has no default on purpose. What
does default is the shape of the computation -- window, slide, watermark -- and
those defaults are the values Phase 2 actually trained and evaluated with. A
mismatch between them and the artefact would produce a model scoring a
distribution it never saw, so they are asserted against the artefact at startup
rather than trusted.
"""

from __future__ import annotations

from pathlib import Path

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

__all__ = ["StreamSettings"]


class StreamSettings(BaseSettings):
    """How the streaming job behaves."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_prefix="STREAM_",
        extra="ignore",
        frozen=True,
        # An empty variable means "unset", not "invalid". Compose expands an
        # undefined variable to an empty string -- `FOO: ${FOO:-}` yields
        # FOO="" -- so without this an optional field like a seed or a run
        # duration fails validation and the container refuses to start, on
        # exactly the command the README documents.
        env_ignore_empty=True,
    )

    app_name: str = Field(default="anomaly-stream-processor")
    #: local[*] for development (decision D-11). The code makes no
    #: single-executor assumption, so this is the only line that changes to run
    #: against a cluster.
    master: str = Field(default="local[2]")

    # --- windowing: must match what the model was trained on ----------------
    window_seconds: int = Field(default=60, gt=0)
    slide_seconds: int = Field(default=10, gt=0)

    #: 90 s, not the 30 s of the Phase 0 design (decision D-36 / ADR-003).
    #: The simulator buffers a machine for 20-75 s during an outage and adds up
    #: to 9 s of jitter, so the configured worst case is about 84 s. A 30 s
    #: watermark would drop exactly the data that was generated to exercise this
    #: mechanism. The value is specific to the simulated environment and must be
    #: recalibrated from measurement on real industrial data.
    watermark_seconds: int = Field(default=90, gt=0)

    #: Fixed trigger keeps batch duration predictable. Without one, a job
    #: restarting after an outage tries to swallow the whole backlog in a single
    #: batch, exhausts memory, fails, and retries the same thing.
    trigger_seconds: int = Field(default=10, gt=0)

    #: Rate limiting, which is what backpressure actually is in Structured
    #: Streaming -- spark.streaming.backpressure.enabled is a DStream setting and
    #: does nothing here. None means unlimited, which is only safe on a topic
    #: that is already drained.
    max_offsets_per_trigger: int | None = Field(default=20_000)

    #: Ignored once a checkpoint exists. The most common source of "why is my
    #: job not replaying from the beginning".
    starting_offsets: str = Field(default="earliest")

    # --- alerting policy ----------------------------------------------------
    #: Hysteresis (decision D-12). One window above the threshold is usually
    #: noise; two consecutive ones is a signal.
    consecutive_windows_to_open: int = Field(default=2, ge=1)
    #: Silence after which an alert episode is considered over. Aligned with the
    #: simulator's min_gap_seconds so two generated episodes cannot be merged
    #: into one by the grouping rule.
    alert_gap_seconds: int = Field(default=120, gt=0)

    # --- window maturity (decision D-37 / ADR-004) --------------------------
    #: How many of the machine's own sampling intervals may be missing from the
    #: end of a window before it stops counting as complete.
    #:
    #: This is **not** the watermark and does not replace it. The watermark is a
    #: stream-wide lateness bound used to evict state; maturity is a per-window
    #: property computed from that window's own event-time coverage. Raising
    #: this admits windows whose tail is missing, which is exactly the defect
    #: D-37 records: the model was trained on complete windows, and a partial
    #: one was measured being flagged anomalous ~100 % of the time against 1.0 %
    #: for a complete one.
    maturity_tolerance_steps: float = Field(default=2.0, gt=0)

    # --- paths --------------------------------------------------------------
    checkpoint_root: Path = Field(default=Path("/checkpoints"))
    artifact_dir: Path = Field(default=Path("/models/one_class_svm/2.0.0"))

    #: Route producer-delayed samples to ``telemetry.late``. Must be off for a
    #: backfill: during an accelerated replay every sample carries a large
    #: ingest lag by construction, and the whole history would be diverted.
    late_detection_enabled: bool = Field(default=True)

    #: Refuse to start if the artefact was built for a different feature set.
    #: Disabling this is only ever useful in a test that deliberately swaps the
    #: artefact.
    verify_artifact: bool = Field(default=True)

    # --- runtime limits, used by tests and by bounded demonstration runs -----
    max_batches: int | None = Field(default=None)
    run_seconds: float | None = Field(default=None)

    @model_validator(mode="after")
    def _shape_is_consistent(self) -> StreamSettings:
        if self.window_seconds % self.slide_seconds != 0:
            raise ValueError(
                f"window_seconds ({self.window_seconds}) must be a whole multiple of "
                f"slide_seconds ({self.slide_seconds}): otherwise a sample is covered "
                "by an inconsistent number of windows"
            )
        if self.watermark_seconds < self.slide_seconds:
            raise ValueError(
                "watermark_seconds below slide_seconds would evict window state "
                "before the window can even be updated once"
            )
        return self

    @property
    def scoring_checkpoint(self) -> Path:
        """One checkpoint per query. Sharing one corrupts both states."""
        return self.checkpoint_root / "scoring"

    @property
    def alerting_checkpoint(self) -> Path:
        return self.checkpoint_root / "alerting"

    @property
    def window_duration(self) -> str:
        return f"{self.window_seconds} seconds"

    @property
    def slide_duration(self) -> str:
        return f"{self.slide_seconds} seconds"

    @property
    def watermark_delay(self) -> str:
        return f"{self.watermark_seconds} seconds"

    @property
    def trigger_interval(self) -> str:
        return f"{self.trigger_seconds} seconds"
