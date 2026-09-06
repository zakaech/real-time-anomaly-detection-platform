"""Structured JSON logging.

Every component emits the same envelope on stdout (docs/08 section 3)::

    {"timestamp":"2026-09-06T14:23:41.905Z","level":"info","service":"...",
     "version":"0.4.1","event":"alert_published","machine_id":"M-014", ...}

Three properties are deliberate:

* **Structured fields, not interpolated sentences.** ``"alert published for
  M-014"`` can only be searched with a fragile regular expression; a
  ``machine_id`` field can be queried directly, and correlated across the four
  languages in this platform.
* **stdout only, never a file.** That is the container contract: collection,
  rotation and shipping belong to the orchestrator, not to the process.
* **A ``trace_id`` carried in context variables.** Bound once per message or per
  request, it then appears on every subsequent line without being threaded
  through call signatures -- and it is the same identifier returned in HTTP error
  responses, so a screenshot from a user leads straight to the log line.

The timestamp uses this project's instant format rather than structlog's
built-in one, so log timestamps and message timestamps are byte-identical and
can be compared without normalisation.
"""

from __future__ import annotations

import logging
import sys
import uuid
from collections.abc import MutableMapping
from typing import Any

import structlog
from structlog.contextvars import bind_contextvars, clear_contextvars, merge_contextvars
from structlog.typing import EventDict, FilteringBoundLogger, Processor, WrappedLogger

from telemetry_core.timeutil import utc_now

__all__ = [
    "TRACE_ID_KEY",
    "bind_trace_id",
    "clear_context",
    "configure_logging",
    "get_logger",
    "new_trace_id",
]

TRACE_ID_KEY = "trace_id"


def _add_timestamp(_logger: WrappedLogger, _method: str, event_dict: EventDict) -> EventDict:
    """Stamp the event with an ISO-8601 UTC instant in the project's format."""
    from telemetry_core.timeutil import format_instant

    event_dict["timestamp"] = format_instant(utc_now())
    return event_dict


def _service_context(service: str, version: str) -> Processor:
    """Build a processor that tags every line with the emitting service."""

    def processor(_logger: WrappedLogger, _method: str, event_dict: EventDict) -> EventDict:
        event_dict["service"] = service
        event_dict["version"] = version
        return event_dict

    return processor


def configure_logging(
    *,
    service: str,
    version: str,
    level: str = "INFO",
) -> None:
    """Configure structlog and the standard library for JSON output on stdout.

    Idempotent: calling it twice reconfigures rather than stacking processors.

    Args:
        service: Component name, e.g. ``"stream-processor"``.
        version: Component version, so a log line identifies the build it came
            from. Without it, correlating a behaviour change with a deployment
            is guesswork.
        level: Minimum level name, e.g. ``"INFO"`` or ``"DEBUG"``.
    """
    numeric_level = logging.getLevelName(level.upper())
    if not isinstance(numeric_level, int):
        numeric_level = logging.INFO

    # Route the standard library through the same handler, so a third-party
    # library's warning lands in the same JSON stream instead of escaping as
    # unstructured text.
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=numeric_level,
        force=True,
    )

    processors: list[Processor] = [
        merge_contextvars,
        structlog.stdlib.add_log_level,
        _service_context(service, version),
        _add_timestamp,
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        # sort_keys=False keeps the envelope fields (timestamp, level, service)
        # first, which is what a human scanning a terminal actually reads.
        structlog.processors.JSONRenderer(sort_keys=False),
    ]

    structlog.configure(
        processors=processors,
        wrapper_class=structlog.make_filtering_bound_logger(numeric_level),
        logger_factory=structlog.PrintLoggerFactory(file=sys.stdout),
        cache_logger_on_first_use=True,
    )


def get_logger(name: str) -> FilteringBoundLogger:
    """Return a logger bound to ``name``."""
    logger: FilteringBoundLogger = structlog.get_logger(name)
    return logger


def new_trace_id() -> str:
    """Generate a correlation identifier.

    Sixteen hexadecimal characters rather than a full UUID: compact enough to be
    read aloud or copied from a screenshot, wide enough that a collision within a
    debugging window is not a practical concern.
    """
    return uuid.uuid4().hex[:16]


def bind_trace_id(trace_id: str | None = None) -> str:
    """Bind a trace identifier to the current context, generating one if absent.

    Call once per consumed message or per incoming request. Every log line
    emitted afterwards in the same context carries it automatically.

    Returns:
        The bound trace identifier, to be propagated in Kafka headers or HTTP
        responses.
    """
    resolved = trace_id or new_trace_id()
    bind_contextvars(**{TRACE_ID_KEY: resolved})
    return resolved


def clear_context() -> None:
    """Drop every context-bound value.

    Call at the end of each unit of work. Context variables outlive the message
    that set them when a worker thread is reused, and a stale ``trace_id`` is
    worse than none: it attributes one message's logs to another.
    """
    clear_contextvars()


def bind(**values: Any) -> None:
    """Bind arbitrary fields to the current logging context."""
    bound: MutableMapping[str, Any] = dict(values)
    bind_contextvars(**bound)
