"""OTEL tracing → Grafana Tempo (OTLP gRPC), with FastAPI/httpx/asyncpg auto-instrumentation.

This is the *infrastructure* trace layer (Tempo). The AI-quality trace layer
(Trace Explorer) is built from Kafka events — both share correlation_id.
"""
from __future__ import annotations

import logging

from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.trace.sampling import ParentBasedTraceIdRatio

from .config import get_settings

logger = logging.getLogger("ai_obs_sdk.tracing")
_initialized = False


def init_tracing(app=None, db_engine=None) -> None:
    """Call once at service startup (before the app starts serving).

    app: optional FastAPI instance → server spans per route.
    Auto-instruments outbound httpx and asyncpg globally.
    """
    global _initialized
    settings = get_settings()
    if _initialized or not settings.enabled or not settings.tracing_enabled:
        return

    resource = Resource.create(
        {
            "service.name": settings.service_name,
            "service.namespace": "ai-services-platform",
            "deployment.environment": settings.environment,
            "lob": settings.lob,
        }
    )
    provider = TracerProvider(
        resource=resource,
        sampler=ParentBasedTraceIdRatio(settings.trace_sample_ratio),
    )
    provider.add_span_processor(
        BatchSpanProcessor(OTLPSpanExporter(endpoint=settings.otlp_endpoint, insecure=True))
    )
    trace.set_tracer_provider(provider)

    if app is not None:
        from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor

        FastAPIInstrumentor.instrument_app(app, excluded_urls="/metrics,/health,/ready")

    try:
        from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor

        HTTPXClientInstrumentor().instrument()
    except Exception:  # noqa: BLE001 — httpx not used by every service
        logger.debug("httpx instrumentation skipped")

    try:
        from opentelemetry.instrumentation.asyncpg import AsyncPGInstrumentor

        AsyncPGInstrumentor().instrument()
    except Exception:  # noqa: BLE001
        logger.debug("asyncpg instrumentation skipped")

    _initialized = True
    logger.info("tracing initialised → %s", settings.otlp_endpoint)


def get_tracer():
    return trace.get_tracer(get_settings().service_name)
