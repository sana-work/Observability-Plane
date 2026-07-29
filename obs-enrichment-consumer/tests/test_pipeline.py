
import pytest

from obs_enrichment.control_plane import SpendResult
from obs_enrichment.errors import DeadLetterError
from obs_enrichment.pipeline import process
from tests.conftest import make_raw


def test_valid_event_flows_and_is_enriched(deps):
    event = process(make_raw(), None, deps)
    assert event.payload["app_owner_team"] == "obs-platform"
    assert event.payload["app_criticality"] == "low"
    assert event.payload["usecase_id"] == "uc-1"
    assert "enriched_at" in event.payload
    assert event.user_id == "SOE12345"  # raw, untouched by redaction


def test_not_json_dead_letters(deps):
    with pytest.raises(DeadLetterError) as exc:
        process(b"\x00garbage", None, deps)
    assert "stage1-validate" in exc.value.reason


def test_unknown_event_type_dead_letters(deps):
    with pytest.raises(DeadLetterError):
        process(make_raw(event_type="NOT_A_TYPE"), None, deps)


def test_traceparent_header_fills_trace_ids(deps):
    headers = [("traceparent", b"00-" + b"a" * 32 + b"-" + b"b" * 16 + b"-01")]
    event = process(make_raw(), headers, deps)
    assert event.trace_id == "a" * 32
    assert event.parent_span_id == "b" * 16


def test_pii_redacted_in_payload_but_not_user_id(deps):
    raw = make_raw(payload={"free_text": "mail me at jane.doe@corp.com or 415-555-1234"})
    event = process(raw, None, deps)
    assert "jane.doe@corp.com" not in event.payload["free_text"]
    assert "[REDACTED:email]" in event.payload["free_text"]
    assert event.user_id == "SOE12345"


def test_error_normalised_to_catalog_code(deps):
    raw = make_raw(
        event_type="LLM_CALL_FAILED", status="failed",
        error_code="ResourceExhausted", payload={"error_message": "429 RateLimit from vertex"},
    )
    event = process(raw, None, deps)
    assert event.error_code == "L0001"
    assert event.payload["raw_error_code"] == "ResourceExhausted"
    assert event.payload["error_category"] == "llm"
    assert event.payload["error_retryable"] is True


def test_cost_recomputed_from_control_plane_and_spend_recorded(deps, cp):
    raw = make_raw(
        event_type="LLM_CALL_COMPLETED",
        payload={"model_name": "gemini-1.5-pro", "input_tokens": 1000, "output_tokens": 200,
                 "estimated_cost_usd": 99.0},  # bogus SDK estimate must be overwritten
    )
    event = process(raw, None, deps)
    assert event.payload["estimated_cost_usd"] == pytest.approx(0.00125 + 0.001)
    assert event.payload["cost_source"] == "control_plane"
    assert cp.spend_calls == [("app-1234", "gemini-1.5-pro", pytest.approx(0.00225))]


def test_budget_alert_marked_on_crossing(deps, cp):
    cp.spend_results = [SpendResult("monthly", 85.0, 100.0, alert_crossed=True, cap_crossed=False)]
    raw = make_raw(
        event_type="LLM_CALL_COMPLETED",
        payload={"model_name": "gemini-1.5-pro", "input_tokens": 10, "output_tokens": 10},
    )
    event = process(raw, None, deps)
    assert event.payload["budget_alert"]["limit_usd"] == 100.0


def test_oversized_payload_field_offloaded_to_s3(deps):
    big = "x" * 500  # threshold is 100 in test settings
    event = process(make_raw(payload={"prompt": big, "small": "ok"}), None, deps)
    assert event.payload["prompt"].startswith("s3://fake-bucket/redacted-prompts/")
    assert event.payload["prompt_bytes"] == 500
    assert event.payload["small"] == "ok"
    assert list(deps.archiver.objects.values()) == [big]


def test_quality_sampling_marks_a_fraction(deps):
    marked = 0
    for _ in range(100):
        e = process(make_raw(event_type="LLM_CALL_COMPLETED",
                             payload={"model_name": "gemini-1.5-pro", "input_tokens": 1}), None, deps)
        marked += bool(e.payload.get("quality_sample"))
    assert 30 <= marked <= 70  # 50% sampling, seeded rng


def test_stage_failure_degrades_not_dies(deps, cp):
    cp.application = lambda app_id: (_ for _ in ()).throw(RuntimeError("db down"))
    event = process(make_raw(), None, deps)  # must not raise
    assert "app_owner_team" not in event.payload
    assert "enriched_at" in event.payload


def test_slo_observed_for_terminal_request_events(deps, cp):
    process(make_raw(status="failed", event_type="REQUEST_FAILED", latency_ms=10.0), None, deps)
    assert len(cp.slo_upserts) == 1
    slo_id, _day, good, total, b1, _b6 = cp.slo_upserts[0]
    assert (slo_id, good, total) == ("slo-avail", 0, 1)
    assert b1 == pytest.approx(100.0)  # 100% bad / 1% budget
