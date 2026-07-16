import pytest

from ai_obs_sdk import trace_llm, trace_rag, trace_tool


class FakeLLMResult:
    def __init__(self):
        self.text = "hello"
        self.obs_payload = {
            "input_tokens": 100,
            "output_tokens": 20,
            "finish_reason": "stop",
        }


def test_trace_llm_emits_started_and_completed_with_cost(fake_emitter):
    @trace_llm(model_provider="vertexai", model_name="gemini-1.5-pro")
    def call_model(prompt: str):
        return FakeLLMResult()

    call_model("what is 2+2")

    types = [e.event_type for e in fake_emitter.events]
    assert types == ["LLM_CALL_STARTED", "LLM_CALL_COMPLETED"]
    done = fake_emitter.events[1]
    assert done.payload["total_tokens"] == 120
    assert done.payload["estimated_cost_usd"] > 0
    assert done.latency_ms is not None
    # child span nests under the request span
    assert done.parent_span_id == fake_emitter.events[0].parent_span_id


async def test_trace_rag_async_failure_emits_failed(fake_emitter):
    @trace_rag(vector_db_index="kb-main", top_k=5)
    async def retrieve(q: str):
        raise ValueError("index unavailable")

    with pytest.raises(ValueError):
        await retrieve("query")

    types = [e.event_type for e in fake_emitter.events]
    assert types == ["RAG_RETRIEVAL_STARTED", "RAG_RETRIEVAL_FAILED"]
    assert fake_emitter.events[1].status == "failed"
    assert fake_emitter.events[1].error_code == "ValueError"


def test_trace_tool_timeout_maps_to_timeout_event(fake_emitter):
    @trace_tool(tool_id="svc-now", tool_type="REST")
    def slow_tool():
        raise TimeoutError("deadline")

    with pytest.raises(TimeoutError):
        slow_tool()

    assert [e.event_type for e in fake_emitter.events] == [
        "TOOL_CALL_STARTED",
        "TOOL_CALL_TIMEOUT",
    ]


def test_obs_extra_merges_into_terminal_payload(fake_emitter):
    @trace_tool(tool_id="db-query", tool_type="DB")
    def run_query():
        return {"rows": 3}

    run_query(obs_extra={"http_status_upstream": 200})
    assert fake_emitter.events[1].payload["http_status_upstream"] == 200
