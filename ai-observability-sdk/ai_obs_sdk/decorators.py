"""@trace_llm / @trace_rag / @trace_agent / @trace_tool — one decorator per signal.

Each decorator:
  1. opens a child ObsContext span (nesting for the Trace Explorer tree),
  2. opens an OTEL span (nesting for Tempo),
  3. emits the *_STARTED event, runs the function, then emits the terminal
     event (*_COMPLETED / *_FAILED / *_TIMEOUT) with latency and domain fields.

Domain fields are supplied two ways:
  * static: decorator kwargs, e.g. @trace_tool(tool_id="svc-now", tool_type="REST")
  * dynamic: the wrapped function may return an object carrying an
    `obs_payload: dict` attribute, or the caller passes `obs_extra={...}`;
    both are merged into the terminal event's payload.

Sync and async callables both supported.
"""
from __future__ import annotations

import asyncio
import functools
import inspect
import time
from typing import Any, Callable

from .context import bind_context, get_context, reset_context
from .contracts import EventType
from .cost import estimate_cost_usd
from .emitter import emit_event
from .hashing import prompt_hash as _prompt_hash
from .tracing import get_tracer


def _merge_result_payload(result: Any, payload: dict) -> dict:
    extra = getattr(result, "obs_payload", None)
    if isinstance(extra, dict):
        payload = {**payload, **extra}
    return payload


def _make_decorator(
    started: str,
    completed: str,
    failed: str,
    span_name: str,
    static_payload_builder: Callable[[dict], dict],
    timeout_event: str | None = None,
    finalize: Callable[[Any, dict], dict] | None = None,
):
    def decorator_factory(**static_fields):
        base_payload = static_payload_builder(static_fields)

        def decorator(fn):
            def _pre():
                parent = get_context()
                token = bind_context(parent.child())
                emit_event(started, payload=base_payload, component=fn.__qualname__)
                return token, time.perf_counter()

            def _post(token, start, result, obs_extra):
                latency_ms = (time.perf_counter() - start) * 1000
                payload = _merge_result_payload(result, {**base_payload, **(obs_extra or {})})
                if finalize:
                    payload = finalize(result, payload)
                emit_event(
                    completed,
                    latency_ms=latency_ms,
                    payload=payload,
                    component=fn.__qualname__,
                )
                reset_context(token)

            def _error(token, start, exc, obs_extra):
                latency_ms = (time.perf_counter() - start) * 1000
                is_timeout = timeout_event and isinstance(
                    exc, (TimeoutError, asyncio.TimeoutError)
                )
                emit_event(
                    timeout_event if is_timeout else failed,
                    status="failed",
                    latency_ms=latency_ms,
                    error_code=type(exc).__name__,
                    payload={**base_payload, **(obs_extra or {}), "error_message": str(exc)[:500]},
                    component=fn.__qualname__,
                )
                reset_context(token)

            if inspect.iscoroutinefunction(fn):

                @functools.wraps(fn)
                async def async_wrapper(*args, obs_extra: dict | None = None, **kwargs):
                    token, start = _pre()
                    with get_tracer().start_as_current_span(span_name):
                        try:
                            result = await fn(*args, **kwargs)
                        except Exception as exc:
                            _error(token, start, exc, obs_extra)
                            raise
                    _post(token, start, result, obs_extra)
                    return result

                return async_wrapper

            @functools.wraps(fn)
            def sync_wrapper(*args, obs_extra: dict | None = None, **kwargs):
                token, start = _pre()
                with get_tracer().start_as_current_span(span_name):
                    try:
                        result = fn(*args, **kwargs)
                    except Exception as exc:
                        _error(token, start, exc, obs_extra)
                        raise
                _post(token, start, result, obs_extra)
                return result

            return sync_wrapper

        return decorator

    return decorator_factory


def _llm_finalize(result: Any, payload: dict) -> dict:
    """If the wrapped call returned token counts, add the producer-side cost estimate."""
    model = payload.get("model_name", "")
    itok, otok = payload.get("input_tokens"), payload.get("output_tokens")
    if itok is not None or otok is not None:
        payload.setdefault("total_tokens", (itok or 0) + (otok or 0))
        payload.setdefault("estimated_cost_usd", estimate_cost_usd(model, itok, otok))
    if "prompt_text" in payload:  # never emit raw prompt text — hash it
        payload["prompt_hash"] = _prompt_hash(payload.pop("prompt_text"))
    return payload


trace_llm = _make_decorator(
    started=EventType.LLM_CALL_STARTED,
    completed=EventType.LLM_CALL_COMPLETED,
    failed=EventType.LLM_CALL_FAILED,
    span_name="llm_call",
    static_payload_builder=lambda f: {
        k: v
        for k, v in {
            "model_provider": f.get("model_provider"),
            "model_name": f.get("model_name"),
            "model_version": f.get("model_version"),
            "prompt_template_id": f.get("prompt_template_id"),
            "prompt_version": f.get("prompt_version"),
            "temperature": f.get("temperature"),
        }.items()
        if v is not None
    },
    finalize=_llm_finalize,
)

trace_tool = _make_decorator(
    started=EventType.TOOL_CALL_STARTED,
    completed=EventType.TOOL_CALL_COMPLETED,
    failed=EventType.TOOL_CALL_FAILED,
    timeout_event=EventType.TOOL_CALL_TIMEOUT,
    span_name="tool_call",
    static_payload_builder=lambda f: {
        k: v
        for k, v in {
            "tool_id": f.get("tool_id"),
            "tool_name": f.get("tool_name"),
            "tool_version": f.get("tool_version"),
            "tool_type": f.get("tool_type"),  # REST | DB | ServiceNow | RAG | InternalAPI
            "called_by_agent_id": f.get("called_by_agent_id"),
        }.items()
        if v is not None
    },
)

trace_rag = _make_decorator(
    started=EventType.RAG_RETRIEVAL_STARTED,
    completed=EventType.RAG_RETRIEVAL_COMPLETED,
    failed=EventType.RAG_RETRIEVAL_FAILED,
    span_name="rag_retrieval",
    static_payload_builder=lambda f: {
        k: v
        for k, v in {
            "vector_db_index": f.get("vector_db_index"),
            "embedding_model": f.get("embedding_model"),
            "top_k": f.get("top_k"),
            "knowledge_base": f.get("knowledge_base"),
        }.items()
        if v is not None
    },
)

trace_agent = _make_decorator(
    started=EventType.AGENT_STARTED,
    completed=EventType.AGENT_COMPLETED,
    failed=EventType.AGENT_FAILED,
    timeout_event=EventType.AGENT_TIMEOUT,
    span_name="agent_run",
    static_payload_builder=lambda f: {
        k: v
        for k, v in {
            "agent_id": f.get("agent_id"),
            "agent_version": f.get("agent_version"),
            "agent_type": f.get("agent_type"),
            "agent_execution_mode": f.get("agent_execution_mode"),
        }.items()
        if v is not None
    },
)
