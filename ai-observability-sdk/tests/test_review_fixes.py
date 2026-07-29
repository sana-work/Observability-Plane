"""Regression tests for the issues found in the 2026-07 code review.

Each test name carries the finding id so a future reader can trace the
behaviour back to the review that demanded it.
"""
import json

import pytest
from pydantic import ValidationError

from ai_obs_sdk import ObsContext, bind_context, emit_event, reset_context, trace_llm
from ai_obs_sdk.config import ObsSettings
from ai_obs_sdk.contracts import EventType
from ai_obs_sdk.emitter import _coerce_json_safe, emitter_stats, serialize


# ---------------------------------------------------------------- C1
def test_c1_invalid_service_name_rejected_at_startup():
    """An invalid (not merely missing) service name must fail at config load —
    otherwise every emit fails contract validation and is silently swallowed."""
    with pytest.raises(ValidationError) as exc:
        ObsSettings(service_name="gssp_gs", lob="wealth", application_id="app-1")
    assert "not one of the 8 platform services" in str(exc.value)


def test_c1_valid_service_name_accepted():
    s = ObsSettings(service_name="gssp-gs", lob="wealth", application_id="app-1")
    assert s.service_name == "gssp-gs"


# ---------------------------------------------------------------- H4
def test_h4_partial_sasl_config_rejected():
    with pytest.raises(ValidationError) as exc:
        ObsSettings(
            service_name="gssp-gs", lob="w", application_id="a",
            kafka_sasl_mechanism="SCRAM-SHA-512", kafka_sasl_username="u",
        )
    assert "incomplete SASL config" in str(exc.value)


def test_h4_sasl_over_plaintext_transport_rejected():
    with pytest.raises(ValidationError) as exc:
        ObsSettings(
            service_name="gssp-gs", lob="w", application_id="a",
            kafka_sasl_mechanism="SCRAM-SHA-512", kafka_sasl_username="u",
            kafka_sasl_password="p",  # security protocol left at PLAINTEXT
        )
    assert "SASL credentials set but" in str(exc.value)


def test_h4_complete_sasl_config_accepted():
    s = ObsSettings(
        service_name="gssp-gs", lob="w", application_id="a",
        kafka_security_protocol="SASL_SSL", kafka_sasl_mechanism="SCRAM-SHA-512",
        kafka_sasl_username="u", kafka_sasl_password="p",
    )
    assert s.kafka_sasl_username == "u"


# ---------------------------------------------------------------- H1
def test_h1_swallowed_validation_failure_is_counted(fake_emitter):
    before = emitter_stats()["invalid"]
    emit_event("TOTALLY_BOGUS_EVENT")          # fails contract validation
    assert fake_emitter.events == []           # nothing produced
    assert emitter_stats()["invalid"] == before + 1   # ...but it IS visible


# ---------------------------------------------------------------- H2
def test_h2_non_serializable_payload_is_coerced_not_dropped(fake_emitter):
    class Weird:
        def __str__(self):
            return "weird-object"

    emit_event(
        EventType.LLM_CALL_COMPLETED,
        payload={"finish_reason": Weird(), "tags": {"a", "b"}, "n": 3},
    )
    assert len(fake_emitter.events) == 1
    raw = serialize(fake_emitter.events[0])          # must not raise
    body = json.loads(raw)
    assert body["payload"]["finish_reason"] == "weird-object"
    assert sorted(body["payload"]["tags"]) == ["a", "b"]
    assert body["payload"]["n"] == 3


def test_h2_coercion_preserves_json_native_types():
    out = _coerce_json_safe({"a": 1, "b": [1, "x"], "c": {"d": None}, "e": True})
    assert out == {"a": 1, "b": [1, "x"], "c": {"d": None}, "e": True}


# ---------------------------------------------------------------- H3 / M2
def test_h3_span_tree_is_consistent_with_tracing_enabled(fake_emitter):
    """The bug: span_id came from the live OTEL span while parent_span_id came
    from the ObsContext — two different id spaces, so parent pointers matched
    no emitted span. Every other test runs with tracing off, which is why this
    went unnoticed; this test installs a real tracer provider."""
    from opentelemetry import trace
    from opentelemetry.sdk.trace import TracerProvider

    if not isinstance(trace.get_tracer_provider(), TracerProvider):
        trace.set_tracer_provider(TracerProvider())

    parent = ObsContext(correlation_id="corr-tree")
    token = bind_context(parent)
    try:
        @trace_llm(model_name="gemini-1.5-pro")
        def call():
            return type("R", (), {"obs_payload": {"input_tokens": 1, "output_tokens": 1}})()

        with trace.get_tracer("test").start_as_current_span("request"):
            call()
    finally:
        reset_context(token)

    started, completed = fake_emitter.events
    # both halves of one logical operation share one span id...
    assert started.span_id == completed.span_id
    # ...which is the ObsContext child span, parented at the request span
    assert started.parent_span_id == parent.span_id
    assert started.span_id != parent.span_id
    # the OTEL span is still recoverable for Tempo joins
    assert started.trace_id is not None
    assert "otel_span_id" in started.payload


def test_h3_failure_events_share_the_same_span_as_started(fake_emitter):
    parent = ObsContext(correlation_id="corr-fail")
    token = bind_context(parent)
    try:
        @trace_llm(model_name="gemini-1.5-pro")
        def boom():
            raise ValueError("nope")

        with pytest.raises(ValueError):
            boom()
    finally:
        reset_context(token)

    started, failed = fake_emitter.events
    assert started.span_id == failed.span_id          # was inconsistent before
    assert failed.parent_span_id == parent.span_id


# ---------------------------------------------------------------- M6
def test_m6_skip_paths_shared_between_middleware_and_tracing():
    from ai_obs_sdk.config import OPERATIONAL_PATHS
    from ai_obs_sdk.middleware import _SKIP_PATHS

    assert set(_SKIP_PATHS) == set(OPERATIONAL_PATHS)
    assert "/livez" in OPERATIONAL_PATHS
