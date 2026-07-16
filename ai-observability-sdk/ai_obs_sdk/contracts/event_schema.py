"""The standard telemetry envelope — every event on ai-obs-events-raw is one of these.

Vendored into ai-observability-sdk (producer side) and the Enrichment Consumer
(validation side). schema_version lets us evolve the contract safely.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Optional
from uuid import uuid4

from pydantic import BaseModel, Field, field_validator

from .event_types import VALUES as EVENT_TYPE_VALUES
from .service_names import VALUES as SERVICE_NAME_VALUES


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class ObsEvent(BaseModel):
    # --- identity ---
    event_id: str = Field(default_factory=lambda: str(uuid4()))
    schema_version: str = "1.0"
    event_type: str
    telemetry_type: str = "event"  # event | log | metric

    # --- time (UTC ISO-8601) ---
    timestamp: str = Field(default_factory=_utc_now_iso)
    emitted_at: str = Field(default_factory=_utc_now_iso)

    # --- correlation / trace ---
    correlation_id: Optional[str] = None
    request_id: Optional[str] = None
    trace_id: Optional[str] = None
    span_id: Optional[str] = None
    parent_span_id: Optional[str] = None

    # --- ownership ---
    service_name: str
    component: Optional[str] = None
    environment: str  # prod | staging | dev
    application_id: Optional[str] = None
    lob: Optional[str] = None
    tenant_id: Optional[str] = None
    # Raw user id (SOE ID) — retained unhashed by platform decision (2026-07):
    # required for audit trails and the "Requests/Errors by SOEID" dashboards.
    # Exposure is governed by per-LOB RBAC on the stores and compliance
    # retention, not by hashing.
    user_id: Optional[str] = None

    # --- outcome ---
    status: str  # success | failed | ...
    latency_ms: Optional[float] = None
    error_code: Optional[str] = None
    http_status: Optional[int] = None

    # --- domain payload (LLM/RAG/agent/tool/feedback/doc-specific fields) ---
    payload: dict[str, Any] = Field(default_factory=dict)

    @field_validator("event_type")
    @classmethod
    def known_event_type(cls, v: str) -> str:
        if v not in EVENT_TYPE_VALUES:
            raise ValueError(f"unknown event_type: {v!r}")
        return v

    @field_validator("service_name")
    @classmethod
    def known_service_name(cls, v: str) -> str:
        if v not in SERVICE_NAME_VALUES:
            raise ValueError(f"unknown service_name: {v!r}")
        return v

    @field_validator("telemetry_type")
    @classmethod
    def known_telemetry_type(cls, v: str) -> str:
        if v not in {"event", "log", "metric"}:
            raise ValueError(f"telemetry_type must be event|log|metric, got {v!r}")
        return v
