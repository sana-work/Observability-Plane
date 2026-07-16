from ai_obs_sdk import ObsContext, bind_context, emit_event, reset_context
from ai_obs_sdk.contracts import EventType


def test_emit_fills_envelope_from_settings_and_context(fake_emitter):
    ctx = ObsContext(correlation_id="corr-123", tenant_id="t-9", user_id="SOE12345")
    token = bind_context(ctx)
    try:
        emit_event(
            EventType.LLM_CALL_COMPLETED,
            latency_ms=42.5,
            payload={"model_name": "gemini-1.5-pro", "input_tokens": 10, "output_tokens": 5},
        )
    finally:
        reset_context(token)

    assert len(fake_emitter.events) == 1
    e = fake_emitter.events[0]
    assert e.service_name == "gssp-gs"
    assert e.lob == "test-lob"
    assert e.application_id == "app-test-001"
    assert e.correlation_id == "corr-123"
    assert e.tenant_id == "t-9"
    assert e.user_id == "SOE12345"
    assert e.latency_ms == 42.5
    assert e.payload["model_name"] == "gemini-1.5-pro"


def test_emit_never_raises_on_bad_input(fake_emitter):
    # unknown event type fails contract validation — must be swallowed, not raised
    emit_event("TOTALLY_BOGUS_EVENT")
    assert fake_emitter.events == []


def test_detached_context_gets_fresh_correlation_id(fake_emitter):
    import ai_obs_sdk.context as ctx_mod

    ctx_mod._current.set(None)
    emit_event(EventType.KAFKA_MESSAGE_PRODUCED)
    assert fake_emitter.events[0].correlation_id  # auto-generated, never null
