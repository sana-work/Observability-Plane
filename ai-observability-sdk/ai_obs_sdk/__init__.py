"""ai-observability-sdk — the one import every AI Platform service needs.

    from fastapi import FastAPI
    from ai_obs_sdk import init_observability

    app = FastAPI()
    init_observability(app)   # logging + tracing + middleware + /metrics

Then instrument the hot paths:

    from ai_obs_sdk import emit_event, trace_llm, trace_rag, trace_tool, trace_agent, get_prompt
"""
from .config import ObsSettings, get_settings
from .context import ObsContext, bind_context, get_context, reset_context
from .contracts import EventType, ObsEvent, ServiceName
from .cost import estimate_cost_usd
from .decorators import trace_agent, trace_llm, trace_rag, trace_tool
from .emitter import emit_event, get_emitter
from .hashing import prompt_hash, query_hash
from .log_config import configure_logging
from .middleware import ObservabilityMiddleware, init_observability
from .prompts import Prompt, get_prompt
from .tracing import get_tracer, init_tracing

__version__ = "0.1.0"

__all__ = [
    "init_observability",
    "ObservabilityMiddleware",
    "emit_event",
    "get_emitter",
    "trace_llm",
    "trace_rag",
    "trace_tool",
    "trace_agent",
    "get_prompt",
    "Prompt",
    "configure_logging",
    "init_tracing",
    "get_tracer",
    "ObsEvent",
    "EventType",
    "ServiceName",
    "ObsContext",
    "ObsSettings",
    "get_settings",
    "get_context",
    "bind_context",
    "reset_context",
    "prompt_hash",
    "query_hash",
    "estimate_cost_usd",
]
