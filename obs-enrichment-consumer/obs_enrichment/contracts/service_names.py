"""Controlled vocabulary for the `service_name` field — the 8 AI services."""
from enum import Enum


class ServiceName(str, Enum):
    AGENTIC_ORCHESTRATION = "agentic-orchestration"
    AGENT_EXECUTOR = "agent-executor"
    GSSP_GS = "gssp-gs"
    GSSP_QS = "gssp-qs"
    GSSP_RS = "gssp-rs"
    CONSUMER_SERVICE = "consumer-service"
    DATA_INGESTION = "data-ingestion"
    USER_FEEDBACK = "user-feedback"


assert len(ServiceName) == 8, f"expected 8 services, got {len(ServiceName)}"

VALUES: frozenset[str] = frozenset(s.value for s in ServiceName)
