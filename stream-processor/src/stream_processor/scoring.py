"""Vectorised scoring and message encoding, as one pandas UDF.

**Why vectorised.** A plain Python UDF crosses the JVM/Python boundary once per
row and calls ``predict`` on a single sample. Here the whole Arrow batch is
converted once, the model is called once on a matrix, and the RBF kernel
evaluation is a single BLAS call instead of a few hundred. The model is also
loaded once per worker process rather than per row.

**Why one UDF rather than two.** The alternative -- score in one UDF, encode in
another -- means two Python round-trips per batch and an intermediate Arrow type
carrying nested structs. The decision logic stays testable regardless, because
it lives in :mod:`stream_processor.policy` as plain functions; this module is
only the wrapper that feeds them.

**NaN.** A feature Spark could not compute arrives as ``null`` inside
``array<double>`` and becomes ``NaN`` in a float64 matrix -- confirmed by probe.
The pipeline's imputer handles it, and its missingness indicator turns the
absence into a signal rather than erasing it.

**Errors.** A failure to *load* the artefact propagates and fails the job, which
is the intent: scoring with an unverified model is worse than not scoring. A
failure while scoring a batch also propagates, because it means a type or data
bug, and masking it would produce a plant that looks healthy.
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

import numpy as np
import numpy.typing as npt
import pandas as pd
from pyspark.sql import Column
from pyspark.sql import functions as F
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import StringType
from telemetry_core.codec import encode
from telemetry_core.features import FEATURE_NAMES
from telemetry_core.schemas import ModelRef
from telemetry_core.timeutil import parse_instant, utc_now

from stream_processor.model import ModelSpec, load_model
from stream_processor.policy import (
    DEFAULT_MATURITY_TOLERANCE_STEPS,
    admission_skip_reason,
    build_scored_event,
    to_utc_datetime,
    window_is_mature,
)

__all__ = ["FEATURE_ARRAY_COLUMN", "features_array", "make_scored_encoder"]

FEATURE_ARRAY_COLUMN = "__features"

#: Arrow hands timestamps over in whatever resolution it chose -- datetime64[us]
#: in practice, not the [ns] one might assume -- so dividing a raw integer view
#: by 1e9 is wrong by a factor of a thousand. Converting through pandas keeps
#: the arithmetic independent of that choice.
_EPOCH = pd.Timestamp("1970-01-01", tz="UTC")


def _epoch_seconds(values: pd.Series) -> npt.NDArray[np.float64]:
    """Timestamps as epoch seconds, whatever resolution Arrow used."""
    stamps = pd.to_datetime(values, utc=True)
    return np.asarray((stamps - _EPOCH).dt.total_seconds(), dtype="float64")


def features_array() -> Column:
    """Pack the 52 feature columns into one ``array<double>``.

    A user-defined function taking 52 separate arguments would be unreadable and
    fragile to reorder; one array keeps the canonical order explicit and makes
    the worker side a single ``np.vstack``.
    """
    return F.array(*[F.col(name).cast("double") for name in FEATURE_NAMES]).alias(
        FEATURE_ARRAY_COLUMN
    )


def _model_reference(metadata: dict[str, Any]) -> ModelRef:
    trained_at = metadata.get("trained_at")
    return ModelRef(
        name=str(metadata["model_name"]),
        version=str(metadata["model_version"]),
        trained_at=parse_instant(trained_at) if isinstance(trained_at, str) else None,
        artifact_sha256=metadata.get("artifact_sha256"),
    )


def make_scored_encoder(
    spec: ModelSpec,
    *,
    maturity_tolerance_steps: float = DEFAULT_MATURITY_TOLERANCE_STEPS,
) -> Any:
    """Build the UDF that scores a batch of windows and encodes the messages.

    ``spec`` is captured by the closure and is deliberately tiny -- a path and a
    flag -- because it is what crosses to the workers. The model itself is
    loaded there, once per process. ``maturity_tolerance_steps`` is captured the
    same way: a float, not a settings object.
    """

    # The iterator-of-batches form is the one PySpark infers from the
    # annotations below; its typed overloads do not describe it, so the call
    # is checked at runtime by Spark instead.
    @pandas_udf(StringType())  # type: ignore[call-overload]
    def encode_scored(
        batches: Iterator[tuple[pd.Series, ...]],
    ) -> Iterator[pd.Series]:
        # Iterator-of-batches form: the model is fetched once for the whole
        # partition rather than once per Arrow batch.
        loaded = load_model(spec)
        model_ref = _model_reference(loaded.metadata)
        threshold = loaded.threshold

        for (
            machine_id,
            line_id,
            window_start,
            window_end,
            sample_count,
            running_ratio,
            machine_state,
            event_time_first,
            event_time_last,
            feature_arrays,
        ) in batches:
            rows = len(machine_id)
            if rows == 0:
                yield pd.Series([], dtype="object")
                continue

            matrix: npt.NDArray[np.float64] = np.vstack(
                [np.asarray(item, dtype="float64") for item in feature_arrays]
            )

            # Maturity is decided per window from its own event-time coverage
            # (D-37). Epoch seconds rather than timestamps: the rule is
            # arithmetic on instants, and pandas hands these over in whatever
            # resolution Arrow chose.
            start_epoch = _epoch_seconds(window_start)
            end_epoch = _epoch_seconds(window_end)
            first_epoch = _epoch_seconds(event_time_first)
            last_epoch = _epoch_seconds(event_time_last)
            mature = [
                window_is_mature(
                    window_start_epoch=start_epoch[index],
                    window_end_epoch=end_epoch[index],
                    first_event_epoch=first_epoch[index],
                    last_event_epoch=last_epoch[index],
                    sample_count=int(sample_count.iloc[index]),
                    tolerance_steps=maturity_tolerance_steps,
                )
                for index in range(rows)
            ]

            # Only admitted windows are scored. Scoring the rest would feed the
            # model a distribution it was never trained on, and the result would
            # be a number with no meaning rather than an obvious failure.
            admitted = np.array(
                [
                    admission_skip_reason(
                        sample_count=int(sample_count.iloc[index]),
                        running_ratio=float(running_ratio.iloc[index]),
                        machine_state=str(machine_state.iloc[index]),
                        is_mature=mature[index],
                    )
                    is None
                    for index in range(rows)
                ]
            )

            raw_scores = np.full(rows, np.nan)
            scores = np.full(rows, np.nan)
            if admitted.any():
                frame = pd.DataFrame(matrix[admitted], columns=list(FEATURE_NAMES))
                frame.insert(0, "machine_id", machine_id.to_numpy()[admitted])
                raw_scores[admitted] = loaded.model.raw_scores(frame)
                scores[admitted] = loaded.model.scores(frame)

            scored_at = utc_now()
            payloads: list[str] = []
            for index in range(rows):
                is_admitted = bool(admitted[index])
                contributions = loaded.contributions(matrix[index]) if is_admitted else None
                event = build_scored_event(
                    machine_id=str(machine_id.iloc[index]),
                    line_id=str(line_id.iloc[index]),
                    window_start=to_utc_datetime(window_start.iloc[index]),
                    window_end=to_utc_datetime(window_end.iloc[index]),
                    sample_count=int(sample_count.iloc[index]),
                    running_ratio=float(running_ratio.iloc[index]),
                    machine_state=str(machine_state.iloc[index]),
                    is_mature=mature[index],
                    features=matrix[index],
                    model_ref=model_ref,
                    raw_score=None if not is_admitted else float(raw_scores[index]),
                    score=None if not is_admitted else float(scores[index]),
                    threshold=threshold,
                    contributions=contributions,
                    scored_at=scored_at,
                )
                payloads.append(encode(event).decode("utf-8"))

            yield pd.Series(payloads, dtype="object")

    return encode_scored
