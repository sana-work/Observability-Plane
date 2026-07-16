"""Contract tests — the CI gate. If these fail, the SDK and the Enrichment
Consumer no longer agree on the wire format: do not merge."""
import json

import pytest
from pydantic import ValidationError

from ai_obs_sdk.contracts import EVENT_TYPE_VALUES, EventType, ObsEvent, ServiceName


def test_event_type_catalog_is_frozen_at_50():
    assert len(EventType) == 50


def test_service_catalog_is_frozen_at_8():
    assert len(ServiceName) == 8


def test_minimal_valid_event_roundtrips():
    e = ObsEvent(
        event_type=EventType.REQUEST_RECEIVED,
        service_name=ServiceName.GSSP_GS,
        environment="dev",
        status="success",
    )
    parsed = json.loads(e.model_dump_json())
    assert parsed["schema_version"] == "1.0"
    assert parsed["event_id"]
    assert parsed["timestamp"].endswith("+00:00") or parsed["timestamp"].endswith("Z")
    # the consumer must be able to re-validate what the SDK produced
    assert ObsEvent.model_validate(parsed).event_type == "REQUEST_RECEIVED"


def test_unknown_event_type_rejected():
    with pytest.raises(ValidationError):
        ObsEvent(
            event_type="NOT_A_REAL_EVENT",
            service_name=ServiceName.GSSP_GS,
            environment="dev",
            status="success",
        )


def test_unknown_service_name_rejected():
    with pytest.raises(ValidationError):
        ObsEvent(
            event_type=EventType.REQUEST_RECEIVED,
            service_name="rogue-service",
            environment="dev",
            status="success",
        )


def test_every_event_type_is_emittable():
    for et in EVENT_TYPE_VALUES:
        ObsEvent(
            event_type=et,
            service_name=ServiceName.AGENT_EXECUTOR,
            environment="dev",
            status="success",
        )
