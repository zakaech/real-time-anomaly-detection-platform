"""Per-batch instrumentation, collected by polling rather than by callback.

The obvious approach is a ``StreamingQueryListener``. It was tried and removed:
a Python listener needs the Py4J callback server, and with three concurrent
queries the driver spent its time in that gateway instead of running batches --
observed as three queries reported active, zero batches completed and 0.4 % CPU
over four minutes. The same pipeline with no listener drained a 216 000-message
backlog in twelve batches.

``query.recentProgress`` returns the same records by pulling them, keeps the last
hundred, and needs no callback server. Batches are de-duplicated by identifier,
so a slow poll reports every batch exactly once rather than missing some or
repeating others.

The fields kept are the ones that say whether the job is healthy (docs/04
section 5.3): input above processed means it is falling behind; a batch longer
than the trigger interval means the same thing earlier; a state row count that
only grows means the watermark is not evicting; and rows dropped by the
watermark are the number that should drive the watermark value itself.
"""

from __future__ import annotations

from typing import Any

from telemetry_core.logging import get_logger

__all__ = ["ProgressCollector", "summarise_progress"]

_LOGGER = get_logger("stream_processor.monitoring")


def summarise_progress(progress: dict[str, Any]) -> dict[str, Any]:
    """Reduce a Spark progress record to the fields worth logging."""
    state_operators = progress.get("stateOperators") or []
    sources = progress.get("sources") or []
    # ``batchDuration`` is absent from the dict form of a progress record, so
    # reading it alone logged a null duration for every batch. The per-phase
    # ``durationMs`` map carries the real number: ``triggerExecution`` is the
    # whole trigger, which is what "how long did this batch take" means.
    durations = progress.get("durationMs") or {}
    batch_duration = progress.get("batchDuration")
    if batch_duration is None:
        batch_duration = durations.get("triggerExecution")
    return {
        "query": progress.get("name"),
        "batch_id": progress.get("batchId"),
        "num_input_rows": progress.get("numInputRows"),
        "input_rows_per_second": progress.get("inputRowsPerSecond"),
        "processed_rows_per_second": progress.get("processedRowsPerSecond"),
        "batch_duration_ms": None if batch_duration is None else int(batch_duration),
        "add_batch_ms": durations.get("addBatch"),
        "state_rows": sum(int(op.get("numRowsTotal", 0)) for op in state_operators),
        "state_bytes": sum(int(op.get("memoryUsedBytes", 0)) for op in state_operators),
        "dropped_by_watermark": sum(
            int(op.get("numRowsDroppedByWatermark", 0)) for op in state_operators
        ),
        "watermark": (progress.get("eventTime") or {}).get("watermark"),
        "sink_rows": (progress.get("sink") or {}).get("numOutputRows"),
        "source_end_offsets": [source.get("endOffset") for source in sources],
    }


class ProgressCollector:
    """Polls every query for progress records and logs each batch once."""

    def __init__(self) -> None:
        self._seen: set[tuple[str, int]] = set()
        self.batches: int = 0
        self.summaries: list[dict[str, Any]] = []

    def poll(self, queries: list[Any]) -> None:
        for query in queries:
            name = str(query.name or query.id)
            for progress in query.recentProgress:
                record: dict[str, Any] = (
                    progress if isinstance(progress, dict) else progress.asDict(True)
                )
                key = (name, int(record.get("batchId", -1)))
                if key in self._seen:
                    continue
                self._seen.add(key)
                summary = summarise_progress(record)
                summary["query"] = name
                self.batches += 1
                self.summaries.append(summary)
                _LOGGER.info("batch_progress", **summary)

    def totals(self) -> dict[str, Any]:
        """Aggregate what was observed, for the end-of-run line."""
        by_query: dict[str, dict[str, Any]] = {}
        for summary in self.summaries:
            name = str(summary["query"])
            bucket = by_query.setdefault(
                name,
                {"batches": 0, "input_rows": 0, "sink_rows": 0, "dropped_by_watermark": 0},
            )
            bucket["batches"] += 1
            bucket["input_rows"] += int(summary.get("num_input_rows") or 0)
            bucket["sink_rows"] += int(summary.get("sink_rows") or 0)
            bucket["dropped_by_watermark"] += int(summary.get("dropped_by_watermark") or 0)
        return by_query
