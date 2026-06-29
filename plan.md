# Observability Plane — Implementation Plan (Detailed)

> **Status:** Build plan · **Architecture:** Kafka Direct Path (no OIS) · **AI-quality layer:** custom build (Langfuse undecided)
> **Source of truth:** `Observability Arch Latest.png` + `2026-06-01_kafka-direct-path-architecture.md` + `2026-06-01_observability-plane-architecture_v2-refined.md`
> **Companion explainer:** `2026-06-12_observability-plane-team-explainer.md`

> **⚠️ Interim storage note (2026-06-12): Snowflake is not yet onboarded.** Until it is, **PostgreSQL stands in for Snowflake** as the event firehose + analytics/long-term store, via a dedicated **`obs_events` schema** (separate from the `observability` control-plane schema). **Everywhere this doc says *Snowflake*, read *PostgreSQL `obs_events.*`* for now.** Differences while on Postgres: **bounded retention** (monthly partitions, ~90-day hot window + S3 archive) instead of true forever, and JSONB instead of VARIANT. **Swap-back path** when Snowflake onboards is in §5.9 / §15.6 — the `event_id`/`correlation_id` model is identical, so the swap is contained.

## Table of Contents
0. Scope & Non-Goals
1. Target Architecture
2. Components (Build vs Adopt)
3. Workstreams & Dependency Graph
4. Phased Delivery Plan (0–7)
5. Detailed Implementation — How It Actually Gets Built
   - 5.1 Kafka topics · 5.2 Event contract · 5.3 Shared SDK · 5.4 Per-service instrumentation
   - 5.5 Enrichment Consumer · 5.6 Storage Consumer · 5.7 PostgreSQL · 5.8 Elasticsearch · 5.9 Event firehose (Postgres `obs_events`; Snowflake deferred) · 5.10 S3 · 5.11 Runtime state (no Redis — interim)
   - 5.12 **Custom AI Quality & Trace Layer** (replaces Langfuse) · 5.13 Supporting telemetry · 5.14 Custom Dashboard · 5.15 Chatbot
6. Capacity & Cost Planning
7. Testing & Validation
8. Rollout & Cutover
9. Self-Monitoring & Runbooks
10. Security, PII & Governance
11. Risks & Mitigations
12. RACI / Ownership
13. Definition of Done
14. Milestones
15. Open Decisions
Appendix — source docs

---

## 0. Scope & Non-Goals

### Building
A platform-wide Observability Plane for the 8 AI services. It is **one Kafka-native pipeline** serving **two concern-areas**, unified by `correlation_id`:

- **AI Quality & Trace** (custom build) — LLM/RAG/agent trace trees, prompt registry + versioning, LLM-as-judge evaluations, quality scores, feedback linking, a Trace Explorer UI.
- **Platform / Infrastructure** — standardized events, metrics, Kafka health, cost/budget governance, SLO, business KPIs, anomalies.

### Explicitly OUT (decided)
- ❌ **No OIS / `POST /v1/ingest`.** Services produce **directly to Kafka** via a shared `confluent-kafka` emitter. (`2026-05-28_observability-ingestion-service-plan.md` is superseded.)
- ❌ **No Langfuse (for now).** The AI-quality/trace layer is **custom-built on the same pipeline + stores** (§5.12). Langfuse remains an open decision (§15) — nothing here depends on it.
- ❌ **No Sentry.** Error grouping = **Elasticsearch fingerprinting**.
- ⚠️ **Event firehose: PostgreSQL `obs_events` schema (interim).** The target state is Snowflake = firehose / Postgres = control-plane-only, but **Snowflake isn't onboarded yet**, so for now Postgres holds *both* — control plane in `observability.*` and the event firehose/analytics in a **separate `obs_events.*` schema** (bounded retention, see §5.9). Swap to Snowflake later (§15.6).
- ⚠️ **No Redis yet.** Runtime-state uses interim stand-ins: budget accumulator → Postgres `observability.budget_accumulator`, registry cache → in-process TTL cache, dedup → store-level idempotency (§5.11). Swap to Redis later (§15.7).

### Guiding principles
1. **Buy the commodity, build the domain glue.** Adopt OTEL, kminion, GLiNER, kube-prometheus-stack, structlog, Fluent Bit, Grafana Tempo. Build the Enrichment Consumer, Storage Consumer, Eval Service, Custom Dashboard (incl. Trace Explorer), Chatbot.
2. **Durable from produce time.** Fire-and-forget to Kafka; consumers commit only after success; failures → replayable dead-letter. No single component failure loses data.
3. **PII-safe before storage.** GLiNER redaction in the Enrichment Consumer before any write; emitters never log raw bodies.
4. **One contract.** A single `ObsEvent` envelope, one `event_type` vocabulary (the 50-type catalog), `correlation_id` on every event, W3C `traceparent` on every hop.
5. **Everything as code.** Topics, ES templates, ILM, DDL, dashboards, prompts in `observability-iac/`, deployed by CI.
6. **No new system if the pipeline already carries the data.** The AI-quality layer reuses the existing events/stores rather than standing up a parallel trace database.

---

## 1. Target Architecture

```
                          ┌─────────────────────────── Custom AI Quality & Trace Layer ──────────────────────────┐
                          │  prompt_registry (PG)   Trace Explorer (dashboard)   obs-eval-service (LLM-as-judge)  │
                          └───────▲────────────────────────▲───────────────────────────▲──────────────────────────┘
                                  │ get_prompt()            │ reads traces+scores       │ consumes processed, writes scores
8 Services ──emit_event()──► ai-obs-events-raw ──► Enrichment Consumer ──► ai-obs-events-processed ──┬─► Storage Consumer ─► ES (hot, per-LOB)
 confluent-kafka,            7d·12p·rf3·lz4        9-stage pipeline         3d·12p·rf3                │                       PostgreSQL obs_events.* (interim←Snowflake)
 fire-and-forget                                  (validate→trace-ctx→PII   key=correlation_id        │                       S3 (payloads)
        │ OTEL spans                              →enrich→errmap→cost→S3                              ├─► obs-eval-service ─► ES quality-scores + obs_events.quality_scores
        ▼                                         →SLO→quality-hook)                                  │
   Grafana Tempo                                         │ invalid                                    └─► control-plane rows ─► PostgreSQL observability.*
                                                         ▼                                               budget accumulator ─► PostgreSQL (no Redis yet)
                                                  ai-obs-dead-letter (14d, replayable)
```

Presentation: **Custom Dashboard Service** (FastAPI + React + Tremor; includes Trace Explorer, replacing Langfuse Web) · **Kibana** · **Observability Chatbot**.
Supporting: **structlog→Fluent Bit→ES** · **OTEL→Grafana Tempo** · **prometheus-fastapi-instrumentator** · **kminion** · **kube-prometheus-stack** (Grafana off).

---

## 2. Components (Build vs Adopt)

| # | Component | B/A | Repo / Location | Notes |
|---|---|---|---|---|
| C1 | `ai-observability-sdk` (shared Python pkg) | Build | new pkg, vendored in 8 services | emit + trace + log + prompt fetch + decorators |
| C2 | Enrichment Consumer | Build | `obs-enrichment-consumer` | 9-stage pipeline, GLiNER |
| C3 | Storage Consumer | Build | `obs-storage-consumer` | fan-out ES/`obs_events`(PG)/S3, dedup |
| C4 | **obs-eval-service** (LLM-as-judge) | Build | `obs-eval-service` | custom evals (replaces Langfuse evals) |
| C5 | Per-service instrumentation | Build | the 8 service repos | events + spans + prompt usage |
| C6 | Custom Dashboard Service (+ **Trace Explorer**) | Build | `obs-dashboard` | replaces Grafana **and** Langfuse Web |
| C7 | Observability Chatbot | Build | `obs-chatbot` | NL access |
| C8 | `observability-iac` | Build | new repo | topics, ES templates, DDL, dashboards, prompts, CI |
| I1 | Kafka topics | Config | existing cluster | raw/processed/dead-letter |
| I2 | structlog + Fluent Bit | Adopt | SDK config + DaemonSet | logs → ES |
| I3 | OpenTelemetry + Grafana Tempo | Adopt | SDK + Tempo Helm | cross-service traces |
| I4 | prometheus-fastapi-instrumentator | Adopt | 1 line/service | `/metrics` |
| I5 | kminion | Adopt | 1 container | Kafka + consumer lag |
| I6 | kube-prometheus-stack (`grafana.enabled=false`) | Adopt | Helm | infra metrics |
| I7 | Elasticsearch/Kibana, S3, PostgreSQL (`observability` + `obs_events`) | Provision | existing + new schemas | Snowflake + Redis deferred (§5.9, §5.11) |
| A1 | Anomaly Detection (Isolation Forest + Kibana ML) | Build (P7) | `obs-anomaly` | advanced |
| A2 | Offline Batch RCA | Build (P7) | CronJob | advanced |

> **Removed vs prior plan:** Langfuse (adopt) → replaced by C4 (eval service) + C6 Trace Explorer + the `prompt_registry` tables.

---

## 3. Workstreams & Dependency Graph

| WS | Name | Owner (proposed) | Blocks | Blocked by |
|---|---|---|---|---|
| WS-A | Foundation & Infra | Platform + Data Eng | all | — |
| WS-B | Shared SDK | Platform Eng | WS-C, WS-D | WS-A contract |
| WS-C | Per-service instrumentation | each service team | WS-F, WS-G | WS-B |
| WS-D | Enrichment Consumer | Platform Eng | WS-E, WS-Q | WS-A, WS-B |
| WS-E | Storage Consumer + stores | Data Eng | WS-F, WS-G, WS-Q | WS-D |
| WS-Q | **AI Quality layer** (eval svc, prompt reg, trace explorer) | ML + Frontend | WS-G | WS-E |
| WS-F | Presentation (dashboard + Kibana) | Frontend + Platform | WS-G | WS-E |
| WS-G | Chatbot | ML/Platform | — | WS-E, WS-F, WS-Q |
| WS-H | Advanced (anomaly + RCA) | ML Eng | — | WS-E |
| WS-X | Governance, IaC, CI/CD, self-monitoring | Platform Eng | cross-cutting | — |

```
WS-A ─► WS-B ─► WS-C ─┐
   └──► WS-D ─► WS-E ─┼─► WS-Q ─┐
                      ├─► WS-F ──┼─► WS-G
                      └─► WS-H   ┘
```

---

## 4. Phased Delivery Plan

> Weeks are indicative effort, not committed dates. Phases 2–4 and 3–Q overlap.

### Phase 0 — Foundation (Weeks 1–3) · WS-A, WS-X
**Exit:** every downstream component has a frozen contract and live infra to target.

Tasks:
- [ ] Freeze `ObsEvent` model + `EventType` enum (50-type catalog) + W3C header format → `observability-iac/contracts/` (§5.2).
- [ ] Create Kafka topics (§5.1) and verify partitions/retention.
- [ ] Apply PostgreSQL `observability` schema + seed `error_code_catalog`, `metric_catalog` (30 metrics), prompt-registry tables (§5.7).
- [ ] Apply Elasticsearch index templates + ILM via IaC (§5.8).
- [ ] Apply the `obs_events` schema DDL (§5.9 — interim event store; Snowflake deferred).
- [ ] Create S3 buckets + lifecycle (§5.10).
- [ ] (No Redis) confirm interim stand-ins: `observability.budget_accumulator` applied, in-process registry cache in the consumer, store-level dedup (§5.11).
- [ ] Scaffold `observability-iac` repo with `ci/deploy.yml` (applies templates/DDL/prompts on merge to main).
- [ ] Provision Grafana Tempo (S3 backend) + Prometheus + kminion + Fluent Bit DaemonSet (§5.13).

**Acceptance:** `kafka-topics --describe` correct; `\dn` shows `observability`; ES `_index_template` present; `obs_events` schema + tables present; S3 buckets encrypted; CI green; Tempo/Prometheus reachable.

### Phase 1 — Shared SDK (Weeks 3–6) · WS-B
**Exit:** one import gives a service emitter + tracing + logging + prompt fetch + decorators.

Tasks: build `ai-observability-sdk` (§5.3) — `emit_event`, `init_tracing`, `configure_logging`, `ObservabilityMiddleware`, `@trace_llm/@trace_rag/@trace_agent` decorators (custom, replace `@observe`), `get_prompt()`, `ObsContext`, enums; unit + contract tests.

**Acceptance:** scratch service emits a valid event to `ai-obs-events-raw`; OTEL span in Tempo; structlog line carries `correlation_id`; `get_prompt("x")` returns active version; decorator emits `LLM_CALL_*` with `span_id`/`parent_span_id` set.

### Phase 2 — Per-service instrumentation (Weeks 4–8) · WS-C
**Exit:** all 8 services emit the standard event set + AI-quality spans, driven by `2026-05-26_observability-coverage-master.md`.

Per service (§5.4): add SDK; wire `main.py`; add middleware; emit the events in the checklist; decorate LLM/RAG/agent functions; replace any local prompt loading with `get_prompt()`.

**Acceptance:** every request emits `REQUEST_RECEIVED → AGENT_STARTED → (LLM_CALL_COMPLETED|TOOL_CALL_COMPLETED) → AGENT_COMPLETED → RESPONSE_DELIVERED` on one `correlation_id`; Trace Explorer (once P-Q lands) shows nested trees with token/cost/latency; feedback events link to traces; **no plaintext SOE_ID / raw body** in the raw topic sample.

### Phase 3 — Enrichment Consumer (Weeks 6–10) · WS-D
**Exit:** raw → processed: validated, redacted, enriched, costed, SLO-evaluated (§5.5).

**Acceptance:** raw→processed within SLA; malformed → dead-letter with reason; redaction verified on adversarial payloads; `estimated_cost` on `LLM_CALL_COMPLETED`; one `daily_slo_compliance` row/app/day; GLiNER loads once/pod.

### Phase 4 — Storage Consumer + stores (Weeks 8–12) · WS-E
**Exit:** processed → ES (hot, per-LOB) + PostgreSQL `obs_events.*` (interim event store) + S3 (payloads); control-plane rows + budget accumulator → `observability.*` (no Redis, §5.11); §5.6.

**Acceptance:** ES indices searchable in Kibana; `obs_events.events` row counts match throughput; S3 has redacted payloads; idempotent on `event_id` (dedup verified); kminion shows both consumer groups; dead-letter replay verified.

### Phase Q — Custom AI Quality & Trace Layer (Weeks 10–15, overlaps P4–P5) · WS-Q
**Exit:** trace trees, prompt versioning, and evals all work without Langfuse (§5.12).

Tasks:
- [ ] Prompt registry tables + dashboard CRUD + version diff + A/B config.
- [ ] `obs-eval-service` worker (faithfulness, hallucination, answer-relevance), sampled, writes `ai-obs-quality-scores-*` + `obs_events.quality_scores`.
- [ ] Trace Explorer API `GET /api/v1/trace/{correlation_id}` + React waterfall/tree page.
- [ ] Feedback→trace join surfaced in Trace Explorer.

**Acceptance:** Trace Explorer renders a full span waterfall for any `correlation_id` (timing, tokens, cost, prompt version, eval scores, linked feedback); editing a prompt creates a new version with a new `prompt_hash`; eval scores appear on RAG traces within the sampling window.

### Phase 5 — Presentation (Weeks 12–16) · WS-F
**Exit:** dashboards + Kibana live with COIN-JWT + per-LOB RBAC (§5.14). Pages: Platform Overview, Cost Governance, Business KPIs, Kafka Health, RAG Quality, Anomaly View, Feedback Trends, **Trace Explorer**.

### Phase 6 — Chatbot (Weeks 15–19) · WS-G
**Exit:** NL queries answered with source attribution + dashboard deep-links (§5.15).

### Phase 7 — Advanced: Anomaly + RCA (Weeks 19–24, optional) · WS-H
> Not on `Observability Arch Latest.png`; follow-on. Anomaly Detection (Isolation Forest + Kibana ML) → `ai-obs-anomalies-*` → Anomaly View; nightly RCA CronJob → S3 `rca-reports/` + weekly digest.

---

## 5. Detailed Implementation — How It Actually Gets Built

### 5.1 Kafka topics
```bash
kafka-topics.sh --bootstrap-server kafka:9092 --create --topic ai-obs-events-raw \
  --partitions 12 --replication-factor 3 \
  --config retention.ms=604800000 --config compression.type=lz4 \
  --config min.insync.replicas=2
kafka-topics.sh --bootstrap-server kafka:9092 --create --topic ai-obs-events-processed \
  --partitions 12 --replication-factor 3 --config retention.ms=259200000 \
  --config compression.type=lz4 --config min.insync.replicas=2
kafka-topics.sh --bootstrap-server kafka:9092 --create --topic ai-obs-dead-letter \
  --partitions 3 --replication-factor 3 --config retention.ms=1209600000
```
- **Partition key = `correlation_id`** → all events of one request land on one partition (ordered, same-consumer processing).
- **Consumer groups:** `obs-enrichment-consumer` (raw), `obs-storage-consumer` + `obs-eval-consumer` (processed — two independent groups read the same processed topic).
- 12 partitions sizes for ≥12 parallel consumers per group; revisit when sustained lag appears.

### 5.2 Event contract (`observability-iac/contracts/`)
```python
# event_schema.py
from pydantic import BaseModel, Field, field_validator
from typing import Any, Optional
from datetime import datetime
from uuid import uuid4

class ObsEvent(BaseModel):
    # identity
    event_id:        str = Field(default_factory=lambda: str(uuid4()))
    schema_version:  str = "1.0"
    event_type:      str                 # ∈ EventType enum
    telemetry_type:  str                 # "event" | "log" | "metric"
    # time
    timestamp:       str                 # UTC ISO-8601
    emitted_at:      str
    # correlation / trace
    correlation_id:  Optional[str] = None
    request_id:      Optional[str] = None
    trace_id:        Optional[str] = None
    span_id:         Optional[str] = None
    parent_span_id:  Optional[str] = None
    # ownership
    service_name:    str                 # ∈ ServiceName enum
    component:       Optional[str] = None
    environment:     str                 # prod|staging|dev
    application_id:  Optional[str] = None
    lob:             Optional[str] = None
    tenant_id:       Optional[str] = None
    user_hash:       Optional[str] = None     # NEVER raw SOE_ID
    # outcome
    status:          str                 # success|failed|...
    latency_ms:      Optional[float] = None
    error_code:      Optional[str] = None
    http_status:     Optional[int] = None
    # domain payload (LLM/RAG/agent/tool/feedback/doc-specific fields)
    payload:         dict[str, Any] = {}

    @field_validator("event_type")
    @classmethod
    def known_type(cls, v):
        from .event_types import EventType
        if v not in EventType.__members__.values():
            raise ValueError(f"unknown event_type {v}")
        return v
```
`event_types.py` = the **50-type enum** (Request 4 · Orchestration 6 · Kafka 4 · Agent 8 · LLM 5 · Tool 4 · RAG 5 · Guardrail 4 · Feedback 3 · Document 7) from `2026-05-21_phase1-foundation-schema-catalogs-storage.md`. `service_names.py` = the 8 service IDs.

### 5.3 Shared SDK (`ai-observability-sdk`)
```
ai_observability_sdk/
├── obs_emitter.py     # emit_event() — confluent-kafka producer (fire-and-forget) + log fallback
├── telemetry.py       # init_tracing() — OTEL FastAPI/httpx/asyncpg + W3C + Tempo OTLP
├── logging_config.py  # configure_logging() — structlog JSON + contextvars
├── middleware.py      # ObservabilityMiddleware + prometheus instrumentator
├── tracing_decorators.py  # @trace_llm / @trace_rag / @trace_agent / @trace_tool
├── prompts.py         # get_prompt(prompt_id, version="active") + prompt_hash
├── context.py         # ObsContext dataclass
├── contracts/         # vendored ObsEvent, EventType, ServiceName
└── tests/
```
**Producer config:** `acks=1, retries=3, retry.backoff.ms=100, compression.type=lz4, linger.ms=5, batch.size=65536, delivery.timeout.ms=5000`. On failure → `log.warning("obs_emit_kafka_failed", obs_fallback_event=...)` so Fluent Bit still lands it in ES.

**The custom trace decorators** (replace Langfuse `@observe`) wrap a function in an OTEL span, capture `span_id`/`parent_span_id`, time it, and emit the matching event with the AI-specific payload:
```python
# tracing_decorators.py (sketch)
def trace_llm(fn):
    async def wrapper(*a, ctx, **kw):
        with tracer.start_as_current_span("llm.generate") as span:
            t0 = time.perf_counter()
            try:
                res = await fn(*a, ctx=ctx, **kw)
                await emit_event(
                    telemetry_type="event", event_type="LLM_CALL_COMPLETED", status="success",
                    service_name=ctx.service_name, environment=ctx.environment,
                    correlation_id=ctx.correlation_id, application_id=ctx.application_id,
                    span_id=hex(span.context.span_id), trace_id=hex(span.context.trace_id),
                    latency_ms=(time.perf_counter()-t0)*1000,
                    payload={"model_name": res.model, "input_tokens": res.usage.input,
                             "output_tokens": res.usage.output, "finish_reason": res.finish_reason,
                             "prompt_template_id": ctx.prompt_id, "prompt_version": ctx.prompt_version,
                             "prompt_hash": ctx.prompt_hash},
                )
                return res
            except Exception as e:
                await emit_event(..., event_type="LLM_CALL_FAILED", status="failed", error_code=map_error(e))
                raise
    return wrapper
```
Service `main.py` (every service, ~6 lines):
```python
from ai_observability_sdk import configure_logging, init_tracing, ObservabilityMiddleware
configure_logging()
app = FastAPI()
tracer = init_tracing(app, service_name=settings.SERVICE_NAME, environment=settings.ENVIRONMENT)
app.add_middleware(ObservabilityMiddleware)   # binds correlation_id; exposes /metrics
```

### 5.4 Per-service instrumentation checklist
Drive from the gap matrix in `2026-05-26_observability-coverage-master.md`. `@trace_*` = add the SDK decorator; `get_prompt` = move prompt loading to the registry.

| Service | Standard events | `@trace_*` | `get_prompt` | Key gap closed |
|---|---|---|---|---|
| **Agentic Orchestration** | `REQUEST_RECEIVED`, `AUTH_COMPLETED`, `PLAN_CREATED`, `AGENT_EXECUTION_REQUEST_PRODUCED`, `KAFKA_MESSAGE_PRODUCED/CONSUMED`, `HIL_REQUEST`, `FINAL_RESPONSE_CONSUMED`, `RESPONSE_DELIVERED` | `@trace_llm` on Stellar/Vertex calls | yes | **LLM cost/tokens** (was fully missing), agent events |
| **Agent Executor** | `AGENT_STARTED`, `AGENT_STEP_STARTED/COMPLETED`, `AGENT_LOOP_ITERATION`, `AGENT_HANDOFF`, `TOOL_CALL_*`, `LLM_CALL_*`, `AGENT_COMPLETED/FAILED/TIMEOUT` | `@trace_agent`, `@trace_tool`, `@trace_llm` | yes | numeric `latency_ms`, `estimated_cost`, `event_id`, finish_reason |
| **GSSP GS** | `LLM_CALL_*`, `LLM_RATE_LIMITED`, `LLM_SAFETY_BLOCKED`, `FILE_ATTACHMENT_RECEIVED` | `@trace_llm` on generators | yes (replaces `PromptTemplateFactory`) | cost calc, file/attachment telemetry |
| **GSSP QS** | `RAG_RETRIEVAL_*`, `GUARDRAIL_EVALUATED/BLOCKED`, `CACHE_HIT/MISS`, `LLM_CALL_*` | `@trace_rag` on 5 stages | yes | RAG quality fields, cache-miss cost |
| **GSSP RS** | `RAG_RETRIEVAL_STARTED/COMPLETED`, `RAG_NO_RESULT`, `EMBEDDING_CALL_*`, `RAG_INDEX_HEALTH_CHECKED` | `@trace_rag` on `retrieve()`/`embed()` | n/a | per-stage latency, embedding model/tokens/cost |
| **Consumer Service** | `INGESTION_JOB_*`, `DOCUMENT_PARSE_*`, `DOCUMENT_EMBEDDING_CREATED`, `DOCUMENT_INDEXED`, `KAFKA_LAG_RECORDED` | `@trace_tool` on embed | n/a | **first Kafka emission**, document telemetry, queue depth |
| **Data Ingestion** | `INGESTION_JOB_*`, `DOCUMENT_*`, `AUTH_FAILED`, `HTTP_LATENCY_RECORDED` | `@trace_tool` on embed | n/a | success-path events, UTC timestamps, cost |
| **User Feedback** | `FEEDBACK_SUBMITTED`, `FEEDBACK_REVIEWED`, `FEEDBACK_INCIDENT_TRIGGERED` | — (links via `correlation_id`) | n/a | feedback→fix loop, redaction, category |

### 5.5 Enrichment Consumer (`obs-enrichment-consumer`)
**Pipeline order** (latest diagram): validate → trace-context extract → PII redact (GLiNER) → metadata enrich → error-code map → token/cost calc (+ Postgres `budget_accumulator`; no Redis yet, §5.11) → S3 archive → SLO evaluate → quality hook (mark RAG/LLM events for eval).
```
obs-enrichment-consumer/
├── main.py            # consume loop, manual commit, DLQ on failure
├── validator.py       # Pydantic ObsEvent
├── pii_redactor.py    # GLiNER (loads once at startup, ~512Mi)
├── enricher.py        # registry join (in-process TTLCache 5-min; no Redis) + cost calc
├── error_mapper.py    # error_code_catalog
├── slo_evaluator.py   # burn-rate 1h+6h → observability.daily_slo_compliance
├── s3_archiver.py     # large payloads/prompts/contexts → S3
└── settings.py
```
- Consumer: `group.id=obs-enrichment-consumer, enable.auto.commit=false, auto.offset.reset=earliest, max.poll.interval.ms=300000`. **Commit only after produce to processed succeeds.**
- Invalid → `ai-obs-dead-letter` with `{raw_event, validation_error, source_partition, source_offset, failed_at}`.
- Budget accumulator (no Redis): atomic upsert into `observability.budget_accumulator` returning the running total; on crossing `budget_limits.max_spend_usd * alert_at_pct/100` → emit `BUDGET_THRESHOLD_EXCEEDED`.
- K8s: 3 replicas, `requests cpu 500m/mem 1Gi`, `limits 2000m/2Gi`, readiness delay 30s (GLiNER load).

### 5.6 Storage Consumer (`obs-storage-consumer`)
- Reads `ai-obs-events-processed` (`group.id=obs-storage-consumer`). Per event: resolve per-LOB ES index from `event_type` → `es.index(index=..., id=event_id, document=event)`; upsert into the **`obs_events.*` analytics tables** (interim Snowflake stand-in, §5.9); archive payload to S3; write control-plane rows (budget/SLO) to `observability.*`. *(Live budget accumulation happens in the Enrichment Consumer cost stage, §5.5 — no Redis.)*
- **Idempotent:** ES `_id=event_id`; PG `ON CONFLICT (event_id) DO NOTHING` (both `observability.*` and `obs_events.*`).
- `obs_events` writer batches inserts (e.g., 5s / 1000 rows via `COPY` into a staging table then `INSERT … ON CONFLICT`) to avoid per-row latency; monthly partitions keep writes and queries fast.
- Index routing:
```python
ROUTES = {"LLM_CALL":"llm-calls","RAG_":"rag-events","TOOL_CALL":"tool-calls",
          "AGENT":"agent-steps","GUARDRAIL":"guardrail-events","FEEDBACK":"feedback",
          "DOCUMENT":"requests","REQUEST":"requests","KAFKA":"requests"}
def index_for(evt):  # ai-obs-{lob}-{cat}-{YYYY.MM.dd}
    cat = next((v for k,v in ROUTES.items() if evt["event_type"].startswith(k)), "requests")
    if evt["status"]=="failed": cat="errors"
    return f"ai-obs-{evt.get('lob','shared')}-{cat}-{today()}"
```
- K8s: 3 replicas, `requests cpu 200m/mem 256Mi`.
- `scripts/replay_dead_letter.py` re-produces `raw_event` → `ai-obs-events-raw` after a fix.

### 5.7 PostgreSQL `observability` schema (control plane only) — DDL
```sql
CREATE SCHEMA observability;

-- registries
CREATE TABLE observability.application_registry (
  application_id VARCHAR(64) PRIMARY KEY, lob VARCHAR(32), soe_id VARCHAR(64),
  owner_team VARCHAR(64), environment VARCHAR(16));
CREATE TABLE observability.agent_registry (agent_id VARCHAR(64) PRIMARY KEY, name TEXT, version TEXT, type TEXT);
CREATE TABLE observability.tool_registry  (tool_id  VARCHAR(64) PRIMARY KEY, name TEXT, type TEXT);
CREATE TABLE observability.rag_registry   (rag_id   VARCHAR(64) PRIMARY KEY, knowledge_base TEXT, embedding_model TEXT);

-- prompt registry (custom AI-quality layer — replaces Langfuse Prompt Mgmt)
CREATE TABLE observability.prompt_registry (
  prompt_id    VARCHAR(64), version INT, name TEXT, template TEXT,
  variables    JSONB, prompt_hash VARCHAR(64), status VARCHAR(16) DEFAULT 'draft',  -- draft|active|archived
  ab_variant   VARCHAR(16), traffic_pct INT DEFAULT 100,
  created_by   VARCHAR(64), created_at TIMESTAMPTZ DEFAULT now(),
  PRIMARY KEY (prompt_id, version));

-- governance / config
CREATE TABLE observability.metric_catalog (
  metric_name VARCHAR(64) PRIMARY KEY, formula TEXT, source_table TEXT, lob VARCHAR(32), unit TEXT);
CREATE TABLE observability.error_code_catalog (
  raw_pattern TEXT, error_code VARCHAR(64), category VARCHAR(32), PRIMARY KEY (error_code));
CREATE TABLE observability.budget_limits (
  application_id VARCHAR(64), environment VARCHAR(32), model_id VARCHAR(128), period VARCHAR(16),
  max_spend_usd DECIMAL(10,4), alert_at_pct INT DEFAULT 80,
  PRIMARY KEY (application_id, environment, model_id, period));
CREATE TABLE observability.daily_slo_compliance (
  compliance_date DATE, application_id VARCHAR(64), slo_type VARCHAR(64),
  target_pct NUMERIC(5,2), achieved_pct NUMERIC(5,2), error_budget_consumed_pct NUMERIC(5,2),
  burn_rate_1h NUMERIC(8,4), burn_rate_6h NUMERIC(8,4), breach_flag BOOLEAN DEFAULT FALSE,
  PRIMARY KEY (compliance_date, application_id, slo_type));
CREATE TABLE observability.alert_threshold (metric_name VARCHAR(64), comparator VARCHAR(4), threshold NUMERIC, window TEXT);
CREATE TABLE observability.dashboard_config (page VARCHAR(64), widget VARCHAR(64), spec JSONB);
CREATE TABLE observability.feedback_case (
  feedback_id VARCHAR(64) PRIMARY KEY, correlation_id VARCHAR(64), rating INT, category TEXT,
  status VARCHAR(16) DEFAULT 'open', linked_incident_id TEXT, created_at TIMESTAMPTZ DEFAULT now());

-- RBAC
GRANT USAGE ON SCHEMA observability TO dashboard_ro;
GRANT SELECT ON ALL TABLES IN SCHEMA observability TO dashboard_ro;
```
**The event firehose lives in the separate `obs_events` schema (§5.9), not in `observability`.** `observability.*` stays control-plane only; events go to `obs_events.*` (interim Snowflake stand-in) + Elasticsearch.

### 5.8 Elasticsearch — per-LOB indices + ILM
- Templates (`observability-iac/elasticsearch/index-templates/`): `ai-obs-{lob}-requests/errors/agent-steps/llm-calls/tool-calls/rag-events/guardrail-events/feedback/traces`, plus `ai-obs-anomalies-*`, `ai-obs-quality-scores-*`, `ai-obs-vector-health-*`.
- Mapping essentials: `event_id` keyword (doc `_id`), `correlation_id`/`span_id`/`parent_span_id` keyword, `timestamp` date, `latency_ms` float, `payload.*` dynamic, `error_fingerprint` keyword (for grouping).
- ILM: `hot-warm-30d` (default) and `compliance-180d` (regulated LOBs). Rollover at 50 GB / 1 day.
- Per-LOB index-level RBAC roles → no doc-level filtering overhead.

### 5.9 Event firehose — PostgreSQL `obs_events` schema (interim; Snowflake deferred)
**Snowflake is not yet onboarded**, so the firehose/analytics store the diagram labels "Snowflake" is, for now, a **separate PostgreSQL schema `obs_events`** — kept apart from the `observability` control plane so the swap-back is clean.

Tables (mirror the eventual `sf_*` set): `obs_events.events` (all enriched), `obs_events.llm_events`, `obs_events.agent_events`, `obs_events.rag_events`, `obs_events.feedback_events`, `obs_events.quality_scores`, `obs_events.slo_history`.

```sql
CREATE SCHEMA obs_events;
CREATE TABLE obs_events.events (
  event_id       VARCHAR(64),
  event_type     VARCHAR(64), telemetry_type VARCHAR(16),
  correlation_id VARCHAR(64), trace_id VARCHAR(64), span_id VARCHAR(64), parent_span_id VARCHAR(64),
  service_name   VARCHAR(64), environment VARCHAR(16), application_id VARCHAR(64), lob VARCHAR(32),
  status VARCHAR(16), latency_ms DOUBLE PRECISION, error_code VARCHAR(64),
  payload        JSONB,                       -- the VARIANT stand-in
  event_ts       TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (event_id, event_ts)
) PARTITION BY RANGE (event_ts);              -- monthly partitions, created ahead by a cron/pg_partman
CREATE INDEX ON obs_events.events (application_id, event_ts);
CREATE INDEX ON obs_events.events (correlation_id);
CREATE INDEX ON obs_events.events (event_type, event_ts);
CREATE INDEX ON obs_events.events USING GIN (payload);
-- per-domain tables (llm_events, rag_events, …) follow the same shape + domain columns promoted out of payload
GRANT USAGE ON SCHEMA obs_events TO dashboard_ro;
GRANT SELECT ON ALL TABLES IN SCHEMA obs_events TO dashboard_ro;
```

**Interim differences vs Snowflake:** **not "forever"** — keep a **~90-day hot window** in Postgres (monthly partitions; `pg_partman` creates new ones and detaches old ones, archived to S3 `raw-traces/`). JSONB instead of VARIANT. Dashboards/chatbot query `obs_events.*` on demand (no pre-aggregation); add covering indexes as query patterns emerge.

**§15.6 swap-back path when Snowflake onboards:** (1) apply the `sf_*` DDL; (2) point the Storage Consumer's analytics writer at Snowflake (dual-write for a cutover window); (3) backfill history from `obs_events.*` + S3; (4) retarget dashboard/chatbot queries from `obs_events.*` → `sf_*`; (5) shrink the Postgres window or drop `obs_events`. `event_id`/`correlation_id` identical, so no contract change.

### 5.10 S3 — payload archive
Bucket `ai-obs-payloads-{env}`, SSE-KMS, prefixes: `redacted-prompts/ redacted-responses/ raw-traces/ rag-contexts/ audit-evidence/ debug-bundles/ rca-reports/ iac-dashboards/`. Lifecycle: Standard→IA @30d, →Glacier @180d, expire per LOB compliance.

### 5.11 Runtime state — interim (no Redis yet)
**Redis/ElastiCache isn't available yet**, so the runtime-state uses in the design are replaced for now:
- **Registry metadata cache** → in-process `cachetools.TTLCache` (5-min TTL) inside each Enrichment Consumer pod (per-pod rather than shared — slightly more DB load on a cold cache, otherwise equivalent).
- **Budget accumulator** → PostgreSQL `observability.budget_accumulator` with an atomic upsert (`… ON CONFLICT … DO UPDATE SET spend_usd = spend_usd + EXCLUDED.spend_usd RETURNING spend_usd`); the running total is compared to `budget_limits` to fire `BUDGET_THRESHOLD_EXCEEDED`. ms-latency, transactional — fine for budget tracking.
- **Dedup** → store-level idempotency only (ES `_id=event_id`, PG `ON CONFLICT (event_id,event_ts) DO NOTHING`); the Redis `SETNX` fast-path is dropped (it was an optimization, not required for correctness).
- **Active correlation context / session cache / rate limits** → not needed by the pipeline now (`correlation_id` rides the event + W3C headers); deferred.

**§15.7 swap-back when Redis lands:** accumulator → `INCRBYFLOAT` (sub-ms), registry cache → shared Redis (cross-pod consistency), add the `SETNX` dedup fast-path. Contained to the enrichment/storage consumers — no contract change.

### 5.12 Custom AI Quality & Trace Layer (replaces Langfuse)
This layer is **not a separate system** — it reuses the pipeline and stores, adding three pieces:

**(a) Trace assembly + Trace Explorer (replaces Langfuse Web).**
- API in the dashboard backend: `GET /api/v1/trace/{correlation_id}` → query ES `ai-obs-{lob}-traces-*` + the LLM/RAG/AGENT/TOOL event indices for that `correlation_id`, build the tree from `span_id`/`parent_span_id`, attach `ai-obs-quality-scores-*` and `feedback_case`.
- React page: waterfall + collapsible tree; per-span timing, tokens, `estimated_cost`, model, `prompt_version`, eval scores, linked feedback. Historical traces fall back to `obs_events.*` (Postgres; Snowflake later).
```python
@router.get("/trace/{correlation_id}")
async def get_trace(correlation_id, lob, user=Depends(require_coin_token)):
    spans  = await es.search(index=f"ai-obs-{lob}-*", query={"term":{"correlation_id":correlation_id}}, size=1000)
    scores = await es.search(index="ai-obs-quality-scores-*", query={"term":{"correlation_id":correlation_id}})
    fb     = await pg.fetch("SELECT * FROM observability.feedback_case WHERE correlation_id=$1", correlation_id)
    return build_tree(spans, scores, fb)   # nests by parent_span_id
```

**(b) Prompt registry + versioning (replaces Langfuse Prompt Management).**
- `observability.prompt_registry` (§5.7); dashboard CRUD + version diff + A/B traffic split.
- SDK `get_prompt(prompt_id, version="active")` returns `(template, version, prompt_hash)`; each LLM event records `prompt_template_id`, `prompt_version`, `prompt_hash` → enables drift detection and A/B comparison from the same data.

**(c) `obs-eval-service` — LLM-as-judge (replaces Langfuse evals + old FaithfulnessScorer).**
```
obs-eval-service/
├── main.py        # consumes ai-obs-events-processed (group obs-eval-consumer)
├── sampler.py     # 100% RAG completions, 10% plain LLM (configurable)
├── judges/
│   ├── faithfulness.py     # answer grounded in retrieved context?
│   ├── hallucination.py    # facts not in context?
│   └── relevance.py        # answer addresses the query?
├── llm_client.py  # internal LLM via GSSP GS / Claude
└── writer.py      # → ai-obs-quality-scores-* (ES) + obs_events.quality_scores (Postgres; Snowflake later)
```
- Async/decoupled — never on the request path. Judge prompts live in the prompt registry (versioned). Output: `{correlation_id, event_id, faithfulness_score, hallucination_flag, relevance_score, judge_model, judge_prompt_version}`.
- Sampling + a `judge_model` cost cap keep eval spend bounded (tracked like any other LLM spend).

**What we gain/lose vs Langfuse:** one stack + one store + per-LOB RBAC for free; but we build the trace UI, prompt UI, and judges ourselves (~3–5 wks). If the team later adopts Langfuse, this layer is the fallback and the `correlation_id`/event model is already compatible. → **Open decision §15.**

### 5.13 Supporting telemetry
- **Logs:** SDK structlog JSON → Fluent Bit DaemonSet (`Mem_Buf_Limit 5MB`, JSON parser) → ES `ai-obs-{service}-logs-%Y.%m`. Emitter Kafka-fallback logs are recoverable here.
- **Traces:** OTEL auto-instrumentation (FastAPI/httpx/asyncpg) → Grafana Tempo (S3 backend, 720h). Trace↔logs correlation on `correlation_id`. (Tempo = cross-service infra spans; the AI-quality Trace Explorer = business/AI trace view from events. Both keyed on `correlation_id`.)
- **HTTP metrics:** `prometheus-fastapi-instrumentator` → `/metrics` per service → Prometheus → dashboard PromQL (`histogram_quantile(0.95, …)`).
- **Kafka lag:** kminion → Prometheus (`kminion_consumer_group_topic_partition_lag`) → Kafka Health page; watch `obs-enrichment-consumer`, `obs-storage-consumer`, `obs-eval-consumer`.
- **Infra:** kube-prometheus-stack (`grafana.enabled=false`); dashboard proxies Prometheus for pod CPU/mem/restarts + asyncpg pool gauges.

### 5.14 Custom Dashboard Service (`obs-dashboard`)
- Backend `api/v1/*` (COIN-JWT `require_coin_token`, per-LOB RBAC): `overview.py` (ES recent + `obs_events` trends), `cost_governance.py` (`budget_limits` + `obs_events.llm_events`), `kafka_health.py` (Prometheus/kminion), `rag_quality.py` (`obs_events.rag_events` + `quality_scores`), `trace.py` (§5.12a), `prompts.py` (registry CRUD), `feedback.py` (`feedback_case`). *(Analytics queries hit `obs_events.*` now; retarget to `sf_*` when Snowflake lands.)*
- Frontend React + Tremor pages, version-controlled in `observability-iac/custom-dashboard/`.
- Kibana covers operational search/error drill-down + Kibana ML log anomalies + error-fingerprint grouping.

### 5.15 Observability Chatbot (`obs-chatbot`)
`/chat` → IntentClassifier (internal LLM) → MetricSemanticLayer (`metric_catalog`) → AccessController (LOB RBAC from `application_registry`) → QueryPlanner (`obs_events` / ES / S3 / Trace Explorer API) → AnswerGenerator (value + filters + source + dashboard deep-link). Joins AI-quality + infra context on `correlation_id`.

---

## 6. Capacity & Cost Planning
- **Event volume:** estimate events/request × req/day (e.g., ~10–30 events/request). Size Kafka partitions and consumer replicas to keep p95 lag < 1 partition-second.
- **GLiNER** is the enrichment bottleneck (~512Mi/pod, CPU-bound). Scale enrichment replicas to partition count; skip >10k-char fields.
- **Event store (interim Postgres `obs_events`):** size for a ~90-day window × event volume; monthly partitions + `pg_partman` keep writes/queries fast and drop old partitions to S3. **Watch DB size** — the main driver to onboard Snowflake (§15.6). No eager aggregation.
- **Eval spend:** sampled judges + `judge_model` cap; tracked in cost governance like any LLM spend.
- **Elasticsearch** hot tier sized to ILM rollover (50 GB / 1 day) × retention; cold → S3 (and Snowflake once onboarded).

---

## 7. Testing & Validation
| Level | What | Tool |
|---|---|---|
| Unit | emitter, validator, redactor, enricher, cost calc, index router, judges, prompt fetch | pytest |
| Contract | every emitted event validates `ObsEvent`; `event_type` ∈ enum | pytest + CI gate |
| Integration | produce→enrichment→processed→storage in a Kafka test cluster | testcontainers |
| **E2E correlation** | one synthetic request → full event chain on one `correlation_id`, visible in ES + `obs_events` + Trace Explorer | E2E harness |
| AI-quality | judge scores land for sampled RAG; prompt version change yields new `prompt_hash`; Trace Explorer renders tree | eval harness |
| PII | adversarial payloads (SSN/COIN/names) redacted before any write | redaction suite |
| Load | sustained + burst; zero loss, bounded lag, GLiNER throughput | k6 + kminion |
| Failure injection | kill enrichment/storage/eval pod, drop ES, schema bug → DLQ → replay | chaos checklist |

---

## 8. Rollout & Cutover
1. **Shadow:** deploy consumers; instrument **one** service (GSSP GS) → validate full path + Trace Explorer.
2. **Per-service canary:** SDK emitter behind a per-service flag; watch raw volume + DLQ rate; roll forward service by service.
3. **Per-LOB:** enable per-LOB ES indices + Kibana orgs one LOB at a time.
4. **AI-quality:** enable `obs-eval-service` sampling at 10% → ramp; turn on prompt-registry reads per service.
5. **Dashboards live** once Storage Consumer is stable. **No OIS to decommission** — remove any stray `/v1/ingest` references.

---

## 9. Self-Monitoring & Runbooks
- Consumer lag (kminion) on all 3 groups → alert if `lag > 1000` sustained 10m. **Runbook:** scale replicas to partitions; check GLiNER CPU.
- DLQ rate > 1% of raw volume → **Runbook:** inspect dead-letter reasons, fix producer/schema, `replay_dead_letter.py`.
- Emitter Kafka-fallback log rate spike → **Runbook:** check broker health; events recoverable from ES logs.
- Enrichment p95 processing latency, GLiNER inference time per event.
- Storage write errors / dedup-conflict rate; eval-service judge error rate + spend.

---

## 10. Security, PII & Governance (Layer 6)
`correlation_id` on every event · schema enforced in CI · retention & index-level RBAC per LOB · **GLiNER redaction before any write** · `metric_catalog` as semantic source of truth · audit/compliance in S3 `audit-evidence/` · cost governance (budget caps + alerts). COIN-JWT on all dashboard/chatbot/trace/prompt endpoints; per-LOB isolation end to end. Prompt registry changes are audited (`created_by`, version history).

---

## 11. Risks & Mitigations
| Risk | Mitigation |
|---|---|
| GLiNER = enrichment bottleneck | Scale replicas to partitions; skip >10k-char fields; batch; monitor inference time |
| Custom AI-quality layer is more work than adopting Langfuse | Reuse pipeline/stores; ship Trace Explorer + judges incrementally; keep Langfuse as fallback (compatible data model) |
| 8 teams must add a Kafka producer | SDK makes it ~6 lines; `emit_event()` interface unchanged; copy-paste `main.py` block |
| Eval LLM spend unbounded | Sampling + `judge_model` cost cap; tracked in cost governance |
| Interim Postgres firehose grows unbounded | Monthly partitions + ~90-day window + drop-to-S3; onboard Snowflake (§15.6) for true long-term |
| Schema drift across teams | `schema_version` + CI contract test; DLQ catches the rest |
| Diagram vs docs inconsistencies (OIS, 38 vs 50, anomaly/RCA, PG role) | §15 + this plan as the single build reference |
| Dual-write consistency (ES/Snowflake/S3) | Single Storage Consumer, idempotent on `event_id`, Kafka as durable replay source |

---

## 12. RACI / Ownership (proposed)
| Component | Responsible | Accountable | Consulted | Informed |
|---|---|---|---|---|
| Contract + SDK | Platform Eng | Platform Lead | Service teams | All |
| Enrichment/Storage consumers | Platform Eng | Platform Lead | Data Eng | All |
| Stores (ES/Snowflake/S3/PG) | Data Eng | Data Lead | Platform | All |
| AI-quality (eval/prompt/trace) | ML + Frontend | ML Lead | Platform | Service teams |
| Per-service instrumentation | each service team | service owner | Platform | — |
| Dashboard + Chatbot | Frontend + ML | Platform Lead | LOB owners | All |
| Governance/IaC/RBAC | Platform Eng | Security | Compliance | All |

---

## 13. Definition of Done
- [ ] All 8 services emit the standard event set + AI-quality spans on one `correlation_id`/request.
- [ ] Raw→processed→stored with **zero data loss** under pod kill / store outage (replay verified).
- [ ] No plaintext PII in any store (redaction suite passes).
- [ ] `estimated_cost` on every `LLM_CALL_COMPLETED`; budget caps fire `BUDGET_THRESHOLD_EXCEEDED`.
- [ ] `daily_slo_compliance` populated; burn-rate alerts wired.
- [ ] **Trace Explorer** renders full trees; **prompt registry** versioning works; **eval scores** land for sampled RAG.
- [ ] Custom Dashboard + Kibana live with COIN-JWT + per-LOB RBAC.
- [ ] Chatbot answers the eval set with correct source attribution.
- [ ] Topics, templates, DDL, dashboards, prompts deploy from `observability-iac` CI.
- [ ] `38` vs `50` event-type count reconciled across diagram + docs.

---

## 14. Milestones
| Phase | Weeks | Exit signal |
|---|---|---|
| 0 Foundation | 1–3 | Contract frozen; infra live; CI green |
| 1 Shared SDK | 3–6 | One import → emit + trace + log + prompt works |
| 2 Instrumentation | 4–8 | Full event chain per request |
| 3 Enrichment | 6–10 | raw→processed redacted/enriched/costed; DLQ works |
| 4 Storage | 8–12 | ES + `obs_events`(PG) + S3 populated; dedup + replay verified |
| Q AI-quality | 10–15 | Trace Explorer + prompt versioning + eval scores |
| 5 Presentation | 12–16 | Dashboards + Kibana, RBAC enforced |
| 6 Chatbot | 15–19 | NL queries answered with sources |
| 7 Anomaly + RCA (optional) | 19–24 | Anomaly view + weekly RCA digest |

---

## 15. Open Decisions
1. **Langfuse vs custom AI-quality layer.** Currently **custom** (§5.12). Revisit after Phase Q: if the custom Trace Explorer/eval effort outweighs the cost of adopting Langfuse, switch — the `correlation_id`/event model is already compatible, so the swap is contained.
2. **"38" vs "50" event types.** Diagram/docs say 38; the only enumerated catalog has 50. Plan uses 50; reconcile the label across diagram + `2026-06-01_*` docs + explainer together.
3. **Anomaly Detection + Batch RCA** aren't on `Observability Arch Latest.png` (only in the refined doc) → scoped as optional Phase 7. Decide if core.
4. **PostgreSQL role (interim).** Target = control-plane only with Snowflake as the firehose; **but Snowflake isn't onboarded**, so Postgres currently holds *both* — `observability.*` (control plane) and `obs_events.*` (firehose, §5.9). Revisit on Snowflake onboarding (→ decision 6).
5. **Internal LLM for judges/chatbot** — route via GSSP GS (Vertex/Claude) and which model/cost tier.
6. **Snowflake onboarding & swap-back.** When does Snowflake become available? Until then Postgres `obs_events.*` is the event store (§5.9) with a bounded ~90-day window (vs Snowflake's forever) — watch Postgres size/cost. On onboarding, run the §5.9 swap-back: apply `sf_*`, dual-write for a cutover window, backfill from `obs_events.*` + S3, retarget dashboard/chatbot queries, then shrink/drop the Postgres firehose.
7. **Redis availability & swap-back.** No Redis yet (§5.11): budget accumulator runs on Postgres `budget_accumulator` (ms, not sub-ms), registry cache is per-pod in-process, dedup is store-level only. When Redis lands, move the accumulator to `INCRBYFLOAT`, the registry cache to shared Redis, and add the `SETNX` dedup fast-path. Watch for accumulator write contention under high LLM-call volume as the trigger to prioritize Redis.

---

### Appendix — authoritative source docs
- `Observability Arch Latest.png` — latest 6-layer Kafka-direct diagram
- `2026-06-01_kafka-direct-path-architecture.md` — no-OIS transport + Enrichment/Storage consumers
- `2026-06-01_observability-plane-architecture_v2-refined.md` — stores, SLO rules, ES/PG schemas
- `2026-05-26_observability-coverage-master.md` — per-service gap matrices (drives §5.4)
- `2026-05-21_phase1-foundation-schema-catalogs-storage.md` — 50-type event catalog + standard fields
- `2026-05-28_tool-recommendations-by-signal.md` — tool rationale
- `2026-06-12_observability-plane-team-explainer.md` — narrative explainer
- ~~`2026-05-28_observability-ingestion-service-plan.md`~~ — **superseded (OIS, not used)**
