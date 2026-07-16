"""structlog JSON logging with automatic correlation context.

Every log line carries correlation_id / span ids from the current ObsContext
(via a structlog processor), so Fluent Bit-shipped logs join to events and
traces without any effort at call sites.

Usage:
    configure_logging()
    log = structlog.get_logger()
    log.info("plan created", step_count=3)
"""
from __future__ import annotations

import logging
import sys

import structlog

from .config import get_settings
from .context import _current  # intentional: read-only peek, no default-binding side effect


def _add_obs_context(_logger, _method, event_dict: dict) -> dict:
    ctx = _current.get()
    if ctx is not None:
        event_dict.setdefault("correlation_id", ctx.correlation_id)
        event_dict.setdefault("span_id", ctx.span_id)
        if ctx.request_id:
            event_dict.setdefault("request_id", ctx.request_id)
    return event_dict


def configure_logging() -> None:
    settings = get_settings()
    level = getattr(logging, settings.log_level.upper(), logging.INFO)

    shared_processors = [
        structlog.contextvars.merge_contextvars,
        _add_obs_context,
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
    ]
    renderer = (
        structlog.processors.JSONRenderer()
        if settings.log_json
        else structlog.dev.ConsoleRenderer()
    )

    structlog.configure(
        processors=[*shared_processors, renderer],
        wrapper_class=structlog.make_filtering_bound_logger(level),
        logger_factory=structlog.PrintLoggerFactory(sys.stdout),
        cache_logger_on_first_use=True,
    )

    # route stdlib logging (uvicorn, confluent_kafka, libs) through the same format
    formatter = structlog.stdlib.ProcessorFormatter(
        foreign_pre_chain=shared_processors,
        processors=[structlog.stdlib.ProcessorFormatter.remove_processors_meta, renderer],
    )
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)
    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(level)
