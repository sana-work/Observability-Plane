"""Request-scoped observability context via contextvars.

ObservabilityMiddleware binds one ObsContext per request; decorators and
emit_event() read it implicitly so call sites never thread IDs by hand.
Works identically under asyncio and threads.
"""
from __future__ import annotations

import contextvars
import secrets
from dataclasses import dataclass, field, replace
from uuid import uuid4


def new_span_id() -> str:
    """16 hex chars — matches the W3C traceparent span-id width."""
    return secrets.token_hex(8)


@dataclass
class ObsContext:
    correlation_id: str = field(default_factory=lambda: str(uuid4()))
    request_id: str | None = None
    trace_id: str | None = None
    span_id: str = field(default_factory=new_span_id)
    parent_span_id: str | None = None
    usecase_id: str | None = None
    agent_id: str | None = None
    tenant_id: str | None = None
    user_id: str | None = None  # raw SOE ID — kept unhashed by platform decision

    def child(self) -> "ObsContext":
        """New span under the current one — used by decorators for nesting."""
        return replace(self, parent_span_id=self.span_id, span_id=new_span_id())


_current: contextvars.ContextVar[ObsContext | None] = contextvars.ContextVar(
    "ai_obs_context", default=None
)


def bind_context(ctx: ObsContext) -> contextvars.Token:
    return _current.set(ctx)


def reset_context(token: contextvars.Token) -> None:
    _current.reset(token)


def get_context() -> ObsContext:
    """Current context, or a fresh detached one (background jobs, schedulers)."""
    ctx = _current.get()
    if ctx is None:
        ctx = ObsContext()
        _current.set(ctx)
    return ctx
