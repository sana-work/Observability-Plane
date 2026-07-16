"""W3C traceparent inject/extract for Kafka message headers.

Every produced message carries `traceparent` (+ `tracestate` if set) and the
`correlation_id`, so the Enrichment Consumer and any downstream service can
continue the same trace.
"""
from __future__ import annotations

from opentelemetry import trace
from opentelemetry.context import Context
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

_propagator = TraceContextTextMapPropagator()


def inject_trace_headers(correlation_id: str) -> list[tuple[str, bytes]]:
    carrier: dict[str, str] = {}
    _propagator.inject(carrier)  # uses the current active span
    headers = [(k, v.encode()) for k, v in carrier.items()]
    headers.append(("correlation_id", correlation_id.encode()))
    return headers


def extract_trace_context(headers: list[tuple[str, bytes]] | None) -> Context:
    carrier = {
        k: v.decode()
        for k, v in (headers or [])
        if k in ("traceparent", "tracestate") and v is not None
    }
    return _propagator.extract(carrier)


def current_trace_ids() -> tuple[str | None, str | None]:
    """(trace_id, span_id) of the active OTEL span as hex strings, if recording."""
    span = trace.get_current_span()
    sc = span.get_span_context()
    if not sc.is_valid:
        return None, None
    return format(sc.trace_id, "032x"), format(sc.span_id, "016x")
