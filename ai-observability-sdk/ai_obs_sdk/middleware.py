"""ASGI middleware: binds ObsContext per request, emits REQUEST_* events,
and wires the Prometheus /metrics endpoint.

Accepted inbound headers (all optional):
    X-Correlation-ID  — propagated end-to-end; generated here if absent
    X-Request-ID      — caller's own request id
    X-User-ID / X-SOE-ID — user identity, carried RAW (unhashed) by platform
                            decision; exposure governed by store-level RBAC
    X-Usecase-ID / X-Tenant-ID — business scoping
The response always echoes X-Correlation-ID so clients can quote it in
support tickets ("show me trace <id>").
"""
from __future__ import annotations

import time
from uuid import uuid4

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request

from .context import ObsContext, bind_context, reset_context
from .contracts import EventType
from .emitter import emit_event

_SKIP_PATHS = {"/metrics", "/health", "/ready", "/livez"}


class ObservabilityMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        if request.url.path in _SKIP_PATHS:
            return await call_next(request)

        ctx = ObsContext(
            correlation_id=request.headers.get("X-Correlation-ID") or str(uuid4()),
            request_id=request.headers.get("X-Request-ID"),
            usecase_id=request.headers.get("X-Usecase-ID"),
            tenant_id=request.headers.get("X-Tenant-ID"),
            user_id=request.headers.get("X-User-ID") or request.headers.get("X-SOE-ID"),
        )
        token = bind_context(ctx)
        start = time.perf_counter()
        emit_event(
            EventType.REQUEST_RECEIVED,
            payload={"method": request.method, "path": request.url.path},
        )
        try:
            response = await call_next(request)
        except Exception as exc:
            emit_event(
                EventType.REQUEST_FAILED,
                status="failed",
                latency_ms=(time.perf_counter() - start) * 1000,
                error_code=type(exc).__name__,
                payload={"method": request.method, "path": request.url.path},
            )
            reset_context(token)
            raise
        latency_ms = (time.perf_counter() - start) * 1000
        emit_event(
            EventType.REQUEST_COMPLETED if response.status_code < 500 else EventType.REQUEST_FAILED,
            status="success" if response.status_code < 500 else "failed",
            latency_ms=latency_ms,
            http_status=response.status_code,
            payload={"method": request.method, "path": request.url.path},
        )
        response.headers["X-Correlation-ID"] = ctx.correlation_id
        reset_context(token)
        return response


def init_observability(app) -> None:
    """One call in each service's startup — the whole SDK in one line.

    from ai_obs_sdk import init_observability
    app = FastAPI()
    init_observability(app)
    """
    from .config import get_settings
    from .log_config import configure_logging
    from .tracing import init_tracing

    settings = get_settings()
    configure_logging()
    init_tracing(app)
    app.add_middleware(ObservabilityMiddleware)

    if settings.metrics_enabled:
        from prometheus_fastapi_instrumentator import Instrumentator

        Instrumentator(excluded_handlers=["/metrics", "/health", "/ready"]).instrument(
            app
        ).expose(app, endpoint="/metrics")
