"""Training settings, and the split boundaries the evaluation depends on.

The split fractions live here rather than being passed around, because they are
the single most consequential choice in the component: get them wrong and every
number in the report is optimistic. They are expressed as hours from the start
of the dataset so a split is reproducible from the manifest alone.
"""

from __future__ import annotations

from pathlib import Path

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

__all__ = ["TrainingSettings"]


class TrainingSettings(BaseSettings):
    """How a training run behaves."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_prefix="TRAINING_",
        extra="ignore",
        frozen=True,
        # An empty variable means "unset", not "invalid". Compose expands an
        # undefined variable to an empty string -- `FOO: ${FOO:-}` yields
        # FOO="" -- so without this an optional field like a seed or a run
        # duration fails validation and the container refuses to start, on
        # exactly the command the README documents.
        env_ignore_empty=True,
    )

    seed: int = Field(default=20260907, ge=0)

    data_dir: Path = Path("../data")
    artifacts_dir: Path = Path("artifacts")
    reports_dir: Path = Path("reports")

    window_seconds: int = Field(default=60, gt=0)
    #: Training uses tumbling windows, evaluation the sliding cadence of the
    #: streaming job. Overlapping training windows are near-duplicates that
    #: inflate the sample count without adding information and would bias the
    #: calibration; evaluation must match inference or the alert budget is wrong
    #: by the overlap factor.
    slide_seconds: int = Field(default=10, gt=0)

    #: Temporal boundaries, in hours from the first event of the dataset. Never
    #: random: two windows either side of a random cut are near-identical, which
    #: is a leak.
    train_end_hours: float = Field(default=28.0, gt=0)
    validation_end_hours: float = Field(default=38.0, gt=0)

    #: A window counts as anomalous when at least this share of its samples are
    #: (decision D-31). "Any overlap" would label a window containing a 2-second
    #: spike out of 60 as anomalous, and we would be measuring a failure that is
    #: not one.
    min_anomaly_fraction: float = Field(default=0.25, gt=0.0, le=1.0)

    #: Alert budget used to pick the operating threshold, per machine per hour.
    #: The threshold is chosen from what an operator can absorb, not from the F1
    #: maximum -- both are reported so the gap is visible.
    alert_budget_per_machine_hour: float = Field(default=0.5, gt=0.0)

    #: One-Class SVM trains in O(n^2)-O(n^3). Above this many windows it is
    #: subsampled, and the run measures the cost at several sizes rather than
    #: asserting that it does not scale.
    ocsvm_max_training_rows: int = Field(default=8000, gt=0)

    @model_validator(mode="after")
    def _splits_are_ordered(self) -> TrainingSettings:
        if self.validation_end_hours <= self.train_end_hours:
            raise ValueError(
                "validation_end_hours must be greater than train_end_hours: "
                "the threshold has to be chosen on data the model did not fit"
            )
        if self.window_seconds % self.slide_seconds != 0:
            raise ValueError("window_seconds must be a whole multiple of slide_seconds")
        return self
