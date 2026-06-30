# EPIC: AI Services Platform — Observability Plane

| Field | Value |
|---|---|
| **Epic ID** | OBS-EPIC-001 |
| **Team** | Platform Engineering |
| **Priority** | High |
| **Status** | Planned |
| **Date** | June 29, 2026 |
| **Architecture** | Kafka-Direct (no OIS) — services produce directly to Kafka |

---

## Summary

Build a Kafka-native, two-layer observability pipeline for 8 AI services — capturing every request, agent step, LLM call, tool invocation, guardrail decision, and user feedback across a unified, governed storage fabric.

Two concern-areas unified by `correlation_id`:
- **AI Quality & Trace** (custom build) — LLM/RAG/agent trace trees, prompt registry + versioning, LLM-as-judge evaluations, quality scores, feedback linking, Trace Explorer UI
- **Platform / Infrastructure** — standardized events, metrics, Kafka health, cost/budget governance, SLO, business KPIs

---

## Business Value

- Full cost/token visibility per LOB per model — budget governance with automated threshold alerts
- AI quality scoring (faithfulness, hallucination, relevance) on every RAG/LLM call
- Prompt versioning + A/B tracking across all 8 services from one registry
- SLO compliance tracking with error-budget burn-rate alerts
- Self-serve observability for service teams without needing on-call engineers

---

## Scope

### In Scope
- Kafka-direct pipeline (services emit directly, no OIS)
- Shared Observability SDK (`ai-observability-sdk`)
- Enrichment Consumer (9-stage pipeline with GLiNER PII redaction)
- Storage Consumer (fan-out to ES, PostgreSQL, S3)
- Custom AI Quality & Trace Layer (replaces Langfuse)
- `obs-eval-service` (LLM-as-judge)
- Custom Dashboard Service (FastAPI + React + Tremor, COIN-JWT, per-LOB RBAC)
- Observability Chatbot
- `observability-iac` repo (IaC, CI/CD)
- Per-service instrumentation for all 8 AI services

### Out of Scope
- ❌ No OIS / `POST /v1/ingest` — superseded
- ❌ No Langfuse (custom-built instead; remains open decision)
- ❌ No Sentry (error grouping = ES fingerprinting)
- ⚠️ Snowflake — **Under Evaluation** (PostgreSQL `obs_events.*` is interim stand-in)
- ⚠️ Redis — **Under Evaluation** (Postgres `budget_accumulator` + in-process TTL cache as interim)

---

## Child Stories by Phase

> **All phases are PLANNED — no work is complete.**

---

### Phase 0 — Foundation · `WS-A · WS-X` · Weeks 1–3

**Exit Signal:** Contract frozen; all infra live; CI green

| Story | Description | Acceptance Criteria |
|---|---|---|
| OBS-001 | Freeze `ObsEvent` contract + 50-type `EventType` enum | Contract in `observability-iac/contracts/`; CI rejects unknown `event_type` |
| OBS-002 | Create Kafka topics (`ai-obs-events-raw`, `ai-obs-events-processed`, `ai-obs-dead-letter`) | `kafka-topics --describe` shows correct partitions (12/12/3), retention (7d/3d/14d), rf=3, lz4 |
| OBS-003 | Apply PostgreSQL `observability.*` schema (control plane) | All registry, governance, SLO, budget, prompt tables present; CI migration green |
| OBS-004 | Apply PostgreSQL `obs_events.*` schema (interim event firehose) | `obs_events.events` with monthly partitions via pg_partman; GIN on payload; indexes on `correlation_id`, `usecase_id`, `event_type` |
| OBS-005 | Apply Elasticsearch index templates + ILM policies | `_index_template` present for all 9 categories; ILM `hot-warm-30d` and `compliance-180d` applied |
| OBS-006 | Create S3 buckets + lifecycle policies | Buckets encrypted (SSE-KMS); Standard→IA @30d, →Glacier @180d; all prefixes present |
| OBS-007 | Scaffold `observability-iac` repo + CI/CD pipeline | `ci/deploy.yml` applies DDL/templates/prompts on merge to main; CI green |
| OBS-008 | Provision Grafana Tempo + Prometheus + kminion + Fluent Bit | Tempo reachable via OTLP; Prometheus scraping; kminion emitting consumer lag; Fluent Bit DaemonSet running |

---

### Phase 1 — Shared SDK · `WS-B` · Weeks 3–6

**Exit Signal:** One import → emit + trace + log + prompt works; scratch service emits valid event to Kafka; OTEL span visible in Tempo

| Story | Description | Acceptance Criteria |
|---|---|---|
| OBS-009 | Build `emit_event()` confluent-kafka producer | Event lands on `ai-obs-events-raw`; Kafka failure falls back to structlog (recoverable via Fluent Bit) |
| OBS-010 | Build `init_tracing()` — OTEL + W3C `traceparent` + Grafana Tempo | OTEL span visible in Tempo; `traceparent` header on every outbound HTTP/Kafka message |
| OBS-011 | Build `configure_logging()` — structlog JSON + `correlation_id` context | Every log line carries `correlation_id`, `service_name`, `environment` |
| OBS-012 | Build `ObservabilityMiddleware` + Prometheus `/metrics` | Middleware binds `correlation_id`; `histogram_quantile(0.95)` available in Prometheus |
| OBS-013 | Build `@trace_llm`, `@trace_rag`, `@trace_agent`, `@trace_tool` decorators | Decorator emits correct event with `span_id`/`parent_span_id`, `latency_ms`, `estimated_cost` |
| OBS-014 | Build `get_prompt()` — prompt registry fetch + `prompt_hash` | Returns active version from `observability.prompt_registry`; records `prompt_template_id`, `prompt_version`, `prompt_hash` on event |
| OBS-015 | SDK unit + contract tests | All emitted events validate `ObsEvent`; `event_type` ∈ enum; contract test is a CI gate |

---

### Phase 2 — Per-Service Instrumentation · `WS-C` · Weeks 4–8

**Exit Signal:** Every service emits full event chain on one `correlation_id` per request; no plaintext SOE_ID or raw body in raw topic

| Story | Service | Key Events | Key Gap Closed |
|---|---|---|---|
| OBS-016 | Agentic Orchestration | `REQUEST_RECEIVED`, `AUTH_COMPLETED`, `PLAN_CREATED`, `AGENT_EXECUTION_REQUEST_PRODUCED`, `KAFKA_MESSAGE_PRODUCED/CONSUMED`, `RESPONSE_DELIVERED` | LLM cost/tokens (was fully missing) |
| OBS-017 | Agent Executor | `AGENT_STARTED/COMPLETED/FAILED`, `AGENT_STEP_*`, `AGENT_LOOP_ITERATION`, `AGENT_HANDOFF`, `TOOL_CALL_*`, `LLM_CALL_*` | `estimated_cost`, `finish_reason`, numeric `latency_ms` |
| OBS-018 | GSSP GS | `LLM_CALL_*`, `LLM_RATE_LIMITED`, `LLM_SAFETY_BLOCKED`, `FILE_ATTACHMENT_RECEIVED` | Cost calc, file/attachment telemetry, prompt registry migration |
| OBS-019 | GSSP QS | `RAG_RETRIEVAL_*`, `GUARDRAIL_EVALUATED/BLOCKED`, `CACHE_HIT/MISS`, `LLM_CALL_*` | RAG quality fields, cache-miss cost |
| OBS-020 | GSSP RS | `RAG_RETRIEVAL_STARTED/COMPLETED`, `RAG_NO_RESULT`, `EMBEDDING_CALL_*`, `RAG_INDEX_HEALTH_CHECKED` | Per-stage latency, embedding model/tokens/cost |
| OBS-021 | Consumer Service | `INGESTION_JOB_*`, `DOCUMENT_PARSE_*`, `DOCUMENT_EMBEDDING_CREATED`, `DOCUMENT_INDEXED`, `KAFKA_LAG_RECORDED` | First Kafka emission, document telemetry |
| OBS-022 | Data Ingestion | `INGESTION_JOB_*`, `DOCUMENT_*`, `AUTH_FAILED`, `HTTP_LATENCY_RECORDED` | Success-path events, UTC timestamps, cost |
| OBS-023 | User Feedback | `FEEDBACK_SUBMITTED`, `FEEDBACK_REVIEWED`, `FEEDBACK_INCIDENT_TRIGGERED` | Feedback→fix loop, redaction, category |

---

### Phase 3 — Enrichment Consumer · `WS-D` · Weeks 6–10

**Exit Signal:** raw→processed within SLA; DLQ works; GLiNER verified on adversarial payloads; `estimated_cost` present; `daily_slo_compliance` populated

| Story | Description | Acceptance Criteria |
|---|---|---|
| OBS-024 | Enrichment Consumer scaffold — consume loop, DLQ, manual commit | Malformed events → `ai-obs-dead-letter` with reason; commits only after produce to processed succeeds |
| OBS-025 | Stage 1–2: Schema Validator + Trace Context Extractor | `ObsEvent` validates; `correlation_id`/`span_id`/`parent_span_id` extracted and propagated |
| OBS-026 | Stage 3: GLiNER PII Redactor | Loads once per pod (~512Mi); adversarial payloads (SSN/COIN/names) redacted; no plaintext in `ai-obs-events-processed` |
| OBS-027 | Stage 4–5: Metadata Enricher + Error Code Mapper | Registry join via in-process TTLCache (5-min); errors normalized to `error_code_catalog` |
| OBS-028 | Stage 6: Token/Cost Calculator + Budget Accumulator | `estimated_cost` on every `LLM_CALL_COMPLETED`; atomic upsert on `budget_accumulator`; `BUDGET_THRESHOLD_EXCEEDED` fires on crossing threshold |
| OBS-029 | Stage 7: S3 Archiver | Large payloads/prompts/contexts archived to S3 before downstream write |
| OBS-030 | Stage 8–9: SLO Evaluator + Quality Hook | One `daily_slo_compliance` row/app/SLO/day; burn-rate 1h + 6h computed; RAG/LLM events marked for eval sampling |

---

### Phase 4 — Storage Consumer + Stores · `WS-E` · Weeks 8–12

**Exit Signal:** ES searchable in Kibana; `obs_events` row counts match throughput; S3 has payloads; dedup + replay verified

| Story | Description | Acceptance Criteria |
|---|---|---|
| OBS-031 | Storage Consumer — ES writer | Events land in per-LOB namespaced indices (`ai-obs-{lob}-{category}-{date}`); idempotent on `event_id` (`_id`) |
| OBS-032 | Storage Consumer — `obs_events.*` writer (batch upsert) | Rows in `obs_events.events` match throughput; `ON CONFLICT (event_id, event_ts) DO NOTHING`; batch via COPY |
| OBS-033 | Storage Consumer — S3 payload writer | Redacted payloads in `redacted-prompts/` and `redacted-responses/` prefixes |
| OBS-034 | Storage Consumer — control-plane writer (`observability.*`) | SLO compliance + budget rows land in `observability.*` |
| OBS-035 | Dead-letter replay script | `replay_dead_letter.py` re-produces `raw_event` → `ai-obs-events-raw` after a fix; verified end-to-end |
| OBS-036 | kminion consumer lag alerting | Both consumer groups visible in Prometheus; alert fires at lag > 1000 sustained 10m |

---

### Phase Q — AI Quality & Trace Layer · `WS-Q` · Weeks 10–15

**Exit Signal:** Trace Explorer renders full trees; prompt versioning works; eval scores land within sampling window

| Story | Description | Acceptance Criteria |
|---|---|---|
| OBS-037 | Prompt registry CRUD dashboard + version diff | Create/edit/archive prompt versions; version diff visible in UI; `prompt_hash` changes on edit |
| OBS-038 | A/B traffic split config in prompt registry | `traffic_pct` split config; `get_prompt()` routes to variant; both variants tracked on LLM events |
| OBS-039 | Build `obs-eval-service` — consume + sample | Reads `ai-obs-events-processed`; 100% RAG, 10% LLM sampled; sampling configurable |
| OBS-040 | Eval judges — faithfulness, hallucination, relevance | Scores written to `ai-obs-quality-scores-*` + `obs_events.quality_scores`; `judge_model` cost tracked in cost governance |
| OBS-041 | Trace Explorer API `GET /api/v1/trace/{correlation_id}` | Returns span tree built from `span_id`/`parent_span_id`; includes timing, tokens, cost, prompt version, eval scores, linked feedback |
| OBS-042 | Trace Explorer React UI — waterfall + tree | Collapsible waterfall with per-span latency, cost, model, `prompt_version`, scores, feedback; COIN-JWT gated |

---

### Phase 5 — Presentation · `WS-F` · Weeks 12–16

**Exit Signal:** Dashboards + Kibana live with COIN-JWT + per-LOB RBAC enforced

| Story | Description | Acceptance Criteria |
|---|---|---|
| OBS-043 | Custom Dashboard Service — FastAPI backend + COIN-JWT auth | All API endpoints gated by COIN-JWT; per-LOB RBAC enforced |
| OBS-044 | Platform Overview dashboard page | Recent events from ES; `obs_events` trends; request volume/error rate per LOB |
| OBS-045 | Cost Governance dashboard page | `budget_limits` vs actuals from `obs_events.llm_events`; spend trend; threshold alerts |
| OBS-046 | Kafka Health dashboard page | Consumer group lag (kminion), DLQ rate, broker health via Prometheus |
| OBS-047 | RAG Quality dashboard page | Retrieval success/no-result rates, faithfulness scores from `obs_events.rag_events` + `quality_scores` |
| OBS-048 | Feedback Trends dashboard page | Sentiment trends, category breakdown, open feedback cases from `observability.feedback_case` |
| OBS-049 | Kibana operational views + Kibana ML log anomalies | Error drill-down, `correlation_id` trace view; Kibana ML anomaly signals on log patterns |

---

### Phase 6 — Observability Chatbot · `WS-G` · Weeks 15–19

**Exit Signal:** NL queries answered with correct source attribution + dashboard deep-links

| Story | Description | Acceptance Criteria |
|---|---|---|
| OBS-050 | Intent classifier + metric semantic layer | NL query → intent; `metric_catalog` resolves metric definitions |
| OBS-051 | Access controller — LOB RBAC on chatbot queries | Chatbot respects per-LOB data boundary; queries scoped to caller's LOB |
| OBS-052 | Query planner + answer generator | Queries `obs_events` / ES / S3 / Trace Explorer API; answer includes value + filters + source + dashboard deep-link |

---

### Phase 7 — Anomaly + RCA · `WS-H` · Weeks 19–24 *(Optional)*

**Exit Signal:** Anomaly view live; weekly RCA digest generated

| Story | Description | Acceptance Criteria |
|---|---|---|
| OBS-053 | Isolation Forest anomaly detection | Anomaly signals written to `ai-obs-anomalies-*`; Kibana ML Anomaly View populated |
| OBS-054 | Nightly RCA CronJob + digest | RCA report written to S3 `rca-reports/`; weekly digest generated |

---

## Workstreams & Dependency Graph

| WS | Name | Owner | Blocks | Blocked By |
|---|---|---|---|---|
| WS-A | Foundation & Infra | Platform + Data Eng | all | — |
| WS-B | Shared SDK | Platform Eng | WS-C, WS-D | WS-A contract |
| WS-C | Per-service instrumentation | Each service team | WS-F, WS-G | WS-B |
| WS-D | Enrichment Consumer | Platform Eng | WS-E, WS-Q | WS-A, WS-B |
| WS-E | Storage Consumer + stores | Data Eng | WS-F, WS-G, WS-Q | WS-D |
| WS-Q | AI Quality layer | ML + Frontend | WS-G | WS-E |
| WS-F | Presentation (dashboard + Kibana) | Frontend + Platform | WS-G | WS-E |
| WS-G | Chatbot | ML / Platform | — | WS-E, WS-F, WS-Q |
| WS-H | Advanced (anomaly + RCA) | ML Eng | — | WS-E |
| WS-X | Governance, IaC, CI/CD | Platform Eng | cross-cutting | — |

```
WS-A ─► WS-B ─► WS-C ─┐
   └──► WS-D ─► WS-E ─┼─► WS-Q ─┐
                       ├─► WS-F ──┼─► WS-G
                       └─► WS-H   ┘
```

---

## External Dependencies

| Dependency | Status | Needed By |
|---|---|---|
| Kafka cluster access | Existing | Phase 0 |
| PostgreSQL instance | Existing | Phase 0 |
| Elasticsearch / Kibana | Existing | Phase 0 |
| COIN-JWT token service | Existing | Phase 5 |
| Internal LLM via GSSP GS | Existing | Phase Q, 6 |
| S3 bucket provisioning | Pending | Phase 0 |
| Grafana Tempo Helm deployment | Pending | Phase 0 |
| Snowflake onboarding | **Under Evaluation** | Phase 4 (swap) |
| Redis / ElastiCache | **Under Evaluation** | Phase 3 (swap) |

---

## Milestones Summary

| Phase | Name | Weeks | Exit Signal |
|---|---|---|---|
| Phase 0 | Foundation | 1–3 | Contract frozen; infra live; CI green |
| Phase 1 | Shared SDK | 3–6 | One import → emit + trace + log + prompt works |
| Phase 2 | Instrumentation | 4–8 | Full event chain per request across all 8 services |
| Phase 3 | Enrichment Consumer | 6–10 | raw→processed redacted/enriched/costed; DLQ works |
| Phase 4 | Storage Consumer + Stores | 8–12 | ES + obs_events(PG) + S3 populated; dedup + replay verified |
| Phase Q | AI Quality & Trace Layer | 10–15 | Trace Explorer + prompt versioning + eval scores |
| Phase 5 | Presentation | 12–16 | Dashboards + Kibana; COIN-JWT RBAC enforced |
| Phase 6 | Chatbot | 15–19 | NL queries answered with sources + dashboard deep-links |
| Phase 7 | Anomaly + RCA *(Optional)* | 19–24 | Anomaly view + weekly RCA digest |

---

## Risks & Mitigations

| Risk | Mitigation |
|---|---|
| GLiNER = enrichment bottleneck (~512Mi/pod, CPU-bound) | Scale replicas to partition count; skip fields > 10k chars; monitor inference time |
| Custom AI quality layer scope creep vs adopting Langfuse | Reuse pipeline/stores; ship incrementally; Langfuse remains compatible fallback |
| 8 service teams must add Kafka producer | SDK is ~6 lines in `main.py`; copy-paste block provided |
| Eval LLM spend unbounded | 10% sampling + `judge_model` cost cap; tracked in cost governance |
| Postgres `obs_events` grows unbounded | Monthly partitions + ~90-day window + archive to S3; Snowflake when approved |
| Schema drift across 8 teams | `schema_version` + CI contract gate; DLQ catches stragglers |
| Dual-write consistency (ES / PG / S3) | Single Storage Consumer, idempotent on `event_id`, Kafka as durable replay source |

---

## Definition of Done

- [ ] All 8 services emit the full event chain on one `correlation_id` per request
- [ ] Zero data loss under pod kill / store outage (DLQ replay verified)
- [ ] No plaintext PII in any store (redaction suite passes)
- [ ] `estimated_cost` on every `LLM_CALL_COMPLETED`; budget caps fire `BUDGET_THRESHOLD_EXCEEDED`
- [ ] `daily_slo_compliance` populated; burn-rate alerts wired
- [ ] Trace Explorer renders full trees; prompt registry versioning works; eval scores land for sampled RAG
- [ ] Custom Dashboard + Kibana live with COIN-JWT + per-LOB RBAC
- [ ] Chatbot answers NL queries with correct source attribution
- [ ] All IaC deployed from `observability-iac` CI — no manual infra steps

---

## Open Decisions

1. **Langfuse vs custom AI quality layer** — currently custom (Phase Q); revisit after Phase Q delivery; data model is compatible either way
2. **Snowflake onboarding timeline** — PostgreSQL `obs_events.*` is interim; watch DB size as the trigger to onboard
3. **Redis availability** — PostgreSQL `budget_accumulator` is interim; watch write contention under high LLM-call volume as the trigger
4. **Internal LLM for judges/chatbot** — route via GSSP GS; model/cost tier TBD
5. **Anomaly Detection (Phase 7)** — optional; decide if core before Phase 6 completes

---

*All phases Planned — no implementation work is complete as of June 29, 2026.*
