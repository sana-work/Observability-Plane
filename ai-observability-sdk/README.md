# ai-observability-sdk

Shared observability SDK for the 8 AI Platform services (Phase 1 of the
Observability Plane roadmap). One import gives a service:

| Capability | API | Backing infra |
|---|---|---|
| Event emission (fire-and-forget) | `emit_event()`, `@trace_llm/_rag/_tool/_agent` | Kafka `ai-obs-events-raw` |
| Distributed tracing | `init_tracing()` (auto via `init_observability`) | OTEL → Grafana Tempo (OTLP gRPC) |
| Structured JSON logs w/ correlation_id | `configure_logging()` (auto) | stdout → Fluent Bit → ES |
| HTTP metrics | `/metrics` endpoint (auto) | Prometheus scrape |
| Request context binding | `ObservabilityMiddleware` (auto) | — |
| Versioned prompts | `get_prompt()` | control-plane `prompt_template_registry` |

## Install

```bash
pip install -e ".[dev]"          # local dev
pip install ai-observability-sdk # from the internal index once published
```

## Quick start (any FastAPI service)

```python
from fastapi import FastAPI
from ai_obs_sdk import init_observability

app = FastAPI()
init_observability(app)   # logging + tracing + middleware + /metrics — done
```

Set the required env vars (see `.env.example`); only three have no default:
`AI_OBS_SERVICE_NAME`, `AI_OBS_LOB`, `AI_OBS_APPLICATION_ID`.

## Instrumenting the four signal types

### LLM calls (GSSP GS, QS, Agent Executor, Agentic Orchestration)

```python
from ai_obs_sdk import trace_llm, get_prompt

@trace_llm(model_provider="vertexai", model_name="gemini-1.5-pro")
async def generate(prompt: str, **kw):
    resp = await vertex_client.generate(prompt, **kw)
    # attach domain fields by giving the result an obs_payload dict
    resp.obs_payload = {
        "input_tokens": resp.usage.prompt_tokens,
        "output_tokens": resp.usage.candidates_tokens,
        "finish_reason": resp.finish_reason,
        "time_to_first_token_ms": resp.ttft_ms,
    }
    return resp
# → emits LLM_CALL_STARTED / LLM_CALL_COMPLETED (or _FAILED) with
#   latency_ms, total_tokens, estimated_cost_usd computed automatically

prompt = get_prompt("qa-answer", version="active")   # versioned, hashed, TTL-cached
```

### RAG retrieval (GSSP QS / RS)

```python
from ai_obs_sdk import trace_rag, query_hash

@trace_rag(vector_db_index="kb-wealth-main", embedding_model="text-embedding-004", top_k=8)
async def retrieve(query: str):
    chunks = await pgvector_search(query)
    result = RetrievalResult(chunks)
    result.obs_payload = {
        "query_hash": query_hash(query),
        "chunk_count": len(chunks),
        "no_result_flag": not chunks,
        "avg_relevance_score": result.avg_score,
    }
    return result
```

### Tool calls (Agent Executor)

```python
from ai_obs_sdk import trace_tool

@trace_tool(tool_id="servicenow-tickets", tool_type="ServiceNow", called_by_agent_id="agent-42")
async def create_ticket(payload: dict): ...
# TimeoutError → TOOL_CALL_TIMEOUT automatically
```

### Agent runs + custom events

```python
from ai_obs_sdk import trace_agent, emit_event, EventType

@trace_agent(agent_id="planner-v2", agent_type="planner")
async def run_agent(task): ...

emit_event(EventType.AGENT_HANDOFF, payload={"from_agent": "planner", "to_agent": "executor"})
emit_event(EventType.FEEDBACK_SUBMITTED, payload={"rating": 4, "thumbs": "up"})
```

### Kafka consumers / background jobs (Consumer Service)

No middleware there — bind context from the message headers:

```python
from ai_obs_sdk import ObsContext, bind_context, emit_event, EventType
from ai_obs_sdk.kafka_headers import extract_trace_context

def on_message(msg):
    headers = dict(msg.headers() or [])
    ctx = ObsContext(correlation_id=(headers.get("correlation_id") or b"").decode() or None)
    token = bind_context(ctx)
    emit_event(EventType.KAFKA_MESSAGE_CONSUMED, payload={"topic": msg.topic()})
    ...
```

## Hard rules (enforced by design + tests)

1. **Never block, never raise.** `emit_event` swallows every error; a full
   local queue drops events with a warning. Observability cannot take the
   business path down.
2. **User identity is carried raw.** By platform decision, `user_id` (the SOE
   ID from `X-User-ID`/`X-SOE-ID`) is emitted unhashed — audit trails and the
   "by SOEID" dashboards need it. Access is governed by per-LOB RBAC on the
   stores. Prompts are still hashed (`prompt_hash`), not embedded, and the
   Enrichment Consumer's GLiNER stage still redacts PII inside free text.
3. **Partition key = correlation_id.** All events of one request are ordered
   on one partition.
4. **The contract is vendored, not imported.** `ai_obs_sdk/contracts/` is a
   byte-for-byte copy of `observability-iac/contracts/`; CI fails on drift.

## Testing

```bash
pytest                       # full suite
pytest tests/test_contract.py   # the merge gate
```

## Local end-to-end smoke

```bash
# 1. broker + topic
docker run -d --name kafka -p 9092:9092 apache/kafka:3.7.0
docker exec kafka /opt/kafka/bin/kafka-topics.sh --create --topic ai-obs-events-raw \
  --bootstrap-server localhost:9092 --partitions 3

# 2. run any service with .env pointing at localhost:9092, hit an endpoint

# 3. watch events arrive
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic ai-obs-events-raw --from-beginning
```
