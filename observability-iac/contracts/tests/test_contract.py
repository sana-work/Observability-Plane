"""Contract tests — run in CI before any infra is applied (ci/deploy.yml step 1)."""
import pytest
from pydantic import ValidationError

from contracts.event_schema import ObsEvent
from contracts.event_types import EventType
from contracts.service_names import ServiceName


def test_catalog_has_50_event_types():
    assert len(EventType) == 50


def test_eight_services():
    assert len(ServiceName) == 8


def test_valid_event_roundtrips():
    e = ObsEvent(
        event_type=EventType.LLM_CALL_COMPLETED.value,
        service_name=ServiceName.GSSP_GS.value,
        environment="prod",
        status="success",
        correlation_id="CORR_abc123",
        latency_ms=1840.0,
        payload={"model_name": "gemini-1.5-pro", "input_tokens": 512, "output_tokens": 148},
    )
    assert e.event_id  # auto-generated uuid
    assert e.schema_version == "1.0"
    assert ObsEvent(**e.model_dump()).event_type == EventType.LLM_CALL_COMPLETED.value


def test_unknown_event_type_rejected():
    with pytest.raises(ValidationError):
        ObsEvent(event_type="NOT_A_REAL_TYPE", service_name="gssp-gs",
                 environment="prod", status="success")


def test_unknown_service_rejected():
    with pytest.raises(ValidationError):
        ObsEvent(event_type=EventType.REQUEST_RECEIVED.value, service_name="mystery-svc",
                 environment="prod", status="success")
