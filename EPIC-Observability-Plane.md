# Observability Plane — JIRA EPIC + Stories

---

## EPIC (Create this first, parent = ARCH-41247)

| Field | Value |
|---|---|
| **Issue Type** | Epic |
| **Project** | ARCHITECTURE_CrossApp |
| **Parent (Capability)** | ARCH-41247 |
| **Summary** | [AI Platform] Observability Plane — Full-Stack AI Observability, Governance & Compliance |
| **Priority** | Medium |
| **Initiative** | PDS_2026_Generative AI infrastructure and Foundational use case funding for TTS Globally_IR202602888 |
| **Labels** | observability, kafka, ai-platform, platform-engineering |
| **RAG** | Green |

### Epic Description

Build a Kafka-native, two-layer observability pipeline for 8 AI services — capturing every request, agent step, LLM call, tool invocation, guardrail decision, and user feedback across a unified, governed storage fabric. Unified by a single `correlation_id` across two concern-areas:

- **AI Quality & Trace** — LLM/RAG/agent trace trees, prompt registry + versioning, LLM-as-judge eval scores, Trace Explorer UI
- **Platform / Infrastructure** — standardized events, Kafka health, cost/budget governance, SLO tracking, business KPIs

**Architecture:** Kafka-Direct — services produce directly to Kafka (no OIS/ingestion service).
**Status:** All phases Planned. No implementation work complete.

**Acceptance Criteria:** Completion of all linked Stories across Phase 0 → Phase 7.

---

## Child Stories (Link each to the Epic above)

---

### Phase 0 — Foundation · Weeks 1–3

---

**OBS-S001**
| Field | Value |
|---|---|
| **Issue Type** | Story |
| **Summary** | [OBS] Define and freeze ObsEvent schema contract and 50-type event catalog |
| **Priority** | High |
| **Phase** | Phase 0 — Foundation |

**Description:**
Freeze the `ObsEvent` pydantic model and `EventType` enum (50 types across 10 categories: Request×4, Orchestration×6, Kafka×4, Agent×8, LLM×5, Tool×4, RAG×5, Guardrail×4, Feedback×3, Document×7) in `observability-iac/contracts/`. Mandatory envelope fields: `event_id`, `schema_version`, `event_type`, `telemetry_type`, `timestamp`, `emitted_at`, `correlation_id`, `span_id`, `parent_span_id`, `service_name`, `environment`, `usecase_id`, `lob`, `user_hash`, `status`, `latency_ms`, `payload`.

**Acceptance Criteria:**
- Contract committed to `observability-iac/contracts/`
- CI gate rejects any event with unknown `event_type`
- All 50 event types enumerated and documented

---

**OBS-S002**
| Field | Value |
|---|---|
| **Issue Type** | Story |
| **Summary** | [OBS] Provision Kafka observability topics (raw, processed, dead-letter) |
| **Priority** | High |
| **Phase** | Phase 0 — Foundation |

**Description:**
Create 3 Kafka topics on the existing cluster:
- `ai-obs-events-raw` — 12 partitions, rf=3, retention 7d, lz4, min.insync.replicas=2
- `ai-obs-events-processed` — 12 partitions, rf=3, retention 3d, lz4, min.insync.replicas=2
- `ai-obs-dead-letter` — 3 partitions, rf=3, retention 14d
Partition key = `correlation_id` (all events of one request land on one partition).

**Acceptance Criteria:**
- `kafka-topics --describe` shows correct partitions, retention, rf, compression
- Consumer groups `obs-enrichment-consumer`, `obs-storage-consumer`, `obs-eval-consumer` registered

---

**OBS-S003**
| Field | Value |
|---|---|
| **Issue Type** | Story |
| **Summary** | [OBS] Apply PostgreSQL control-plane schema (observability.*) with registries and governance tables |
| **Priority** | High |
| **Phase** | Phase 0 — Foundation |

**Description:**
Apply DDL for the `observability` schema (control plane only — NOT an event firehose). Tables: `application_registry`, `agent_registry`, `tool_registry`, `rag_registry`, `prompt_template_registry`, `error_code_catalog`, `metric_catalog`, `budget_limits`, `budget_accumulator` (interim Redis stand-in), `daily_slo_compliance`, `alert_threshold`, `dashboard_config`, `feedback_case`. Seed `error_code_catalog` and `metric_catalog` (30 metrics).

**Acceptance Criteria:**
- `\dn` shows `observability` schema
- All tables present with correct indexes and constraints
- CI migration green; `dashboard_ro` role granted

---

**OBS-S004**
| Field | Value |
|---|---|
| **Issue Type** | Story |
| **Summary** | [OBS] Apply PostgreSQL event firehose schema (obs_events.*) with monthly partitioning |
| **Priority** | High |
| **Phase** | Phase 0 — Foundation |

**Description:**
Apply DDL for the `obs_events` schema — interim event firehose while Snowflake onboarding is under evaluation. Tables: `obs_events.events` (monthly partitions via pg_partman), `obs_events.llm_events`, `obs_events.agent_events`, `obs_events.rag_events`, `obs_events.feedback_events`, `obs_events.quality_scores`, `obs_events.slo_history`. ~90-day hot window; old partitions archived to S3. Indexed by `usecase_id`, `correlation_id`, `event_type`, GIN on payload.

**Acceptance Criteria:**
- `obs_events.events` created with RANGE partitioning on `event_ts`
- Monthly partitions present and pg_partman configured
- All domain tables present; `dashboard_ro` role granted

---

**OBS-S005**
| Field | Value |
|---|---|
| **Issue Type** | Story |
| **Summary** | [OBS] Configure Elasticsearch per-LOB index templates and ILM retention policies |
| **Priority** | High |
| **Phase** | Phase 0 — Foundation |

**Description:**
Apply ES index templates for per-LOB namespaced indices: `ai-obs-{lob}-{category}-{YYYY.MM.dd}`. Categories: requests, errors, agent-steps, llm-calls, tool-calls, rag-events, guardrail-events, feedback, traces. Plus: `ai-obs-anomalies-*`, `ai-obs-quality-scores-*`. ILM: `hot-warm-30d` (default), `compliance-180d` (regulated LOBs). Rollover at 50GB/1day. Idempotent on `event_id` (`_id`).

**Acceptance Criteria:**
- `_index_template` present for all 9 categories
- Both ILM policies applied and linked to templates
- Rollover thresholds configured

---

**OBS-S006**
| Field | Value |
|---|---|
| **Issue Type** | Story |
| **Summary** | [OBS] Provision S3 payload archive buckets with SSE-KMS encryption and lifecycle policies |
| **Priority** | Medium |
| **Phase** | Phase 0 — Foundation |

**Description:**
Provision `ai-obs-payloads-{env}` S3 bucket with SSE-KMS encryption. Prefixes: `redacted-prompts/`, `redacted-responses/`, `raw-traces/`, `rag-contexts/`, `audit-evidence/`, `debug-bundles/`, `rca-reports/`, `iac-dashboards/`. Lifecycle: Standard → IA @30d → Glacier @180d.

**Acceptance Criteria:**
- Buckets created and encrypted (SSE-KMS)
- Lifecycle rules applied and verified
- All prefixes documented in `observability-iac/`

---

**OBS-S007**
| Field | Value |
|---|---|
| **Issue Type** | Story |
| **Summary** | [OBS] Scaffold observability-iac repository with automated CI/CD for infra-as-code deployments |
| **Priority** | High |
| **Phase** | Phase 0 — Foundation |

**Description:**
Create `observability-iac` repo with `ci/deploy.yml` — applies DDL, ES templates, ILM, Kafka topic configs, and dashboard configs on merge to main. All infra as code; no manual steps.

**Acceptance Criteria:**
- Repo created; CI pipeline green on merge to main
- DDL, ES templates, Kafka configs, S3 lifecycle all deployed via CI
- No manual infra steps required

---

**OBS-S008**
| Field | Value |
|---|---|
| **Issue Type** | Story |
| **Summary** | [OBS] Deploy supporting telemetry stack (Grafana Tempo, Prometheus, kminion, Fluent Bit) |
| **Priority** | Medium |
| **Phase** | Phase 0 — Foundation |

**Description:**
Deploy supporting telemetry stack: Grafana Tempo (S3 backend, 720h retention, OTLP endpoint), kube-prometheus-stack (`grafana.enabled=false` — Grafana dashboards deliberately OFF), kminion (Kafka consumer lag → Prometheus), Fluent Bit DaemonSet (structlog JSON → ES `ai-obs-{service}-logs-%Y.%m`).

**Acceptance Criteria:**
- Tempo reachable via OTLP; spans visible
- Prometheus scraping all service `/metrics` endpoints
- kminion emitting `kminion_consumer_group_topic_partition_lag`
- Fluent Bit DaemonSet running; logs landing in ES

---

### Phase 1 — Shared SDK · Weeks 3–6

---

**OBS-S009**
| Field | Value |
|---|---|
| **Issue Type** | Story |
| **Summary** | [OBS] Observability SDK — Build Kafka event emitter with fire-and-forget and fallback logging |
| **Priority** | High |
| **Phase** | Phase 1 — Shared SDK |

**Description:**
Build `obs_emitter.py` — confluent-kafka fire-and-forget producer. Config: `acks=1, retries=3, retry.backoff.ms=100, compression.type=lz4, linger.ms=5, batch.size=65536, delivery.timeout.ms=5000`. On Kafka failure → `log.warning("obs_emit_kafka_failed")` so Fluent Bit still captures it.

**Acceptance Criteria:**
- Event lands on `ai-obs-events-raw` from a scratch service
- Kafka failure falls back to structlog (log recoverable via Fluent Bit)
- Contract test validates emitted event against `ObsEvent`

---

**OBS-S010**
| Field | Value |
|---|---|
| **Issue Type** | Story |
| **Summary** | [OBS] Observability SDK — Build distributed tracing with OTEL, W3C traceparent and Grafana Tempo |
| **Priority** | High |
| **Phase** | Phase 1 — Shared SDK |

**Description:**
Build `telemetry.py` — OTEL auto-instrumentation for FastAPI/httpx/asyncpg, W3C `traceparent` propagation on every outbound HTTP call and Kafka message header, export to Grafana Tempo via OTLP.

**Acceptance Criteria:**
- OTEL span visible in Grafana Tempo
- `traceparent` header present on every outbound call and Kafka message
- `correlation_id` linked to `trace_id`

---

**OBS-S011**
| Field | Value |
|---|---|
| **Issue Type** | Story |
| **Summary** | [OBS] Observability SDK — Build structured logging, request middleware and Prometheus metrics endpoint |
| **Priority** | High |
| **Phase** | Phase 1 — Shared SDK |

**Description:**
Build `logging_config.py` (structlog JSON, contextvars, `correlation_id` on every line) and `middleware.py` (`ObservabilityMiddleware` — binds `correlation_id` from W3C header or generates new; mounts `prometheus-fastapi-instrumentator` at `/metrics`).

**Acceptance Criteria:**
- Every log line carries `correlation_id`, `service_name`, `environment`
- `/metrics` returns Prometheus histogram data
- `histogram_quantile(0.95)` available in Prometheus

---

**OBS-S012**
| Field | Value |
|---|---|
| **Issue Type** | Story |
| **Summary** | [OBS] Observability SDK — Build AI tracing decorators for LLM, RAG, Agent and Tool calls |
| **Priority** | High |
| **Phase** | Phase 1 — Shared SDK |

**Description:**
Build `tracing_decorators.py` — wraps functions in OTEL spans, captures `span_id`/`parent_span_id`, times execution, emits the matching event type with AI-specific payload (model, tokens, cost, prompt_version, finish_reason etc.).

**Acceptance Criteria:**
- Decorator emits correct event type with `span_id`/`parent_span_id`, `latency_ms`, `estimated_cost`
- Nested calls produce parent→child span tree
- `@trace_llm` records `model_name`, `input_tokens`, `output_tokens`, `prompt_hash`

---

**OBS-S013**
| Field | Value |
|---|---|
| **Issue Type** | Story |
| **Summary** | [OBS] Observability SDK — Build prompt registry fetch with version tracking and prompt_hash |
| **Priority** | Medium |
| **Phase** | Phase 1 — Shared SDK |

**Description:**
Build `prompts.py` — `get_prompt(prompt_id, version="active")` fetches from `observability.prompt_template_registry`, returns `(template, version, prompt_hash)`. Every LLM event records `prompt_template_id`, `prompt_version`, `prompt_hash`.

**Acceptance Criteria:**
- Returns active version from `observability.prompt_template_registry`
- `prompt_hash` changes when template content changes
- Emitted LLM event carries `prompt_template_id`, `prompt_version`, `prompt_hash`

---

**OBS-S014**
| Field | Value |
|---|---|
| **Issue Type** | Story |
| **Summary** | [OBS] Observability SDK — Add unit and contract tests with mandatory CI enforcement gate |
| **Priority** | High |
| **Phase** | Phase 1 — Shared SDK |

**Description:**
Write pytest unit tests for all SDK modules. Contract test validates every emitted event against `ObsEvent` pydantic model and verifies `event_type` ∈ `EventType` enum. This test must be a required CI gate — merge blocked if contract test fails.

**Acceptance Criteria:**
- All SDK modules have unit tests
- Contract test is a required CI gate
- CI blocks merge on unknown `event_type` or invalid `ObsEvent`

---

### Phase 2 — Per-Service Instrumentation · Weeks 4–8

---

**OBS-S015**
| Field | Value |
|---|---|
| **Issue Type** | Story |
| **Summary** | [OBS] Instrument Agentic Orchestration — add SDK, emit request and LLM cost events |
| **Priority** | High |
| **Phase** | Phase 2 — Instrumentation |

**Description:**
Add `ai-observability-sdk` to Agentic Orchestration. Wire `main.py` (6-line block). Emit: `REQUEST_RECEIVED`, `AUTH_COMPLETED`, `PLAN_CREATED`, `AGENT_EXECUTION_REQUEST_PRODUCED`, `KAFKA_MESSAGE_PRODUCED`, `KAFKA_MESSAGE_CONSUMED`, `FINAL_RESPONSE_CONSUMED`, `RESPONSE_DELIVERED`. Apply `@trace_llm` on Stellar/Vertex calls. Migrate prompt loading to `get_prompt()`.

**Acceptance Criteria:**
- Full event chain emitted on one `correlation_id` per request
- `@trace_llm` records cost/tokens (previously missing)
- No plaintext SOE_ID in raw topic

---

**OBS-S016**
| Field | Value |
|---|---|
| **Issue Type** | Story |
| **Summary** | [OBS] Instrument Agent Executor — add SDK, emit agent lifecycle, tool and LLM cost events |
| **Priority** | High |
| **Phase** | Phase 2 — Instrumentation |

**Description:**
Add SDK to Agent Executor. Emit: `AGENT_STARTED`, `AGENT_STEP_STARTED/COMPLETED`, `AGENT_LOOP_ITERATION`, `AGENT_HANDOFF`, `TOOL_CALL_*`, `LLM_CALL_*`, `AGENT_COMPLETED/FAILED/TIMEOUT`. Apply `@trace_agent`, `@trace_tool`, `@trace_llm`. Key gaps: numeric `latency_ms`, `estimated_cost`, `event_id`, `finish_reason`.

**Acceptance Criteria:**
- `estimated_cost` present on all `LLM_CALL_COMPLETED` events
- Nested span tree (agent→llm/tool) visible with `parent_span_id`
- `AGENT_TIMEOUT` fires correctly on timeout

---

**OBS-S017**
| Field | Value |
|---|---|
| **Issue Type** | Story |
| **Summary** | [OBS] Instrument GSSP GS — add SDK, emit LLM cost events and migrate to prompt registry |
| **Priority** | High |
| **Phase** | Phase 2 — Instrumentation |

**Description:**
Add SDK to GSSP GS. Emit: `LLM_CALL_COMPLETED`, `LLM_CALL_FAILED`, `LLM_RATE_LIMITED`, `LLM_SAFETY_BLOCKED`, `FILE_ATTACHMENT_RECEIVED`. Apply `@trace_llm` on generators. Migrate `PromptTemplateFactory` to `get_prompt()`.

**Acceptance Criteria:**
- Cost/token calc present on LLM events
- File/attachment telemetry emitted
- Prompt registry migration complete; `prompt_hash` tracked

---

**OBS-S018**
| Field | Value |
|---|---|
| **Issue Type** | Story |
| **Summary** | [OBS] Instrument GSSP QS — add SDK, emit RAG retrieval, guardrail and cache events |
| **Priority** | High |
| **Phase** | Phase 2 — Instrumentation |

**Description:**
Add SDK to GSSP QS. Emit: `RAG_RETRIEVAL_STARTED/COMPLETED`, `GUARDRAIL_EVALUATED/BLOCKED`, `CACHE_HIT/MISS`, `LLM_CALL_*`. Apply `@trace_rag` on 5 retrieval stages.

**Acceptance Criteria:**
- RAG quality fields present (`chunk_count`, `retrieval_score`, `no_result_flag`)
- Cache-miss cost tracked
- Guardrail decisions emitted with `guardrail_type` and `block_reason`

---

**OBS-S019**
| Field | Value |
|---|---|
| **Issue Type** | Story |
| **Summary** | [OBS] Instrument GSSP RS — add SDK, emit RAG retrieval and embedding cost events per stage |
| **Priority** | High |
| **Phase** | Phase 2 — Instrumentation |

**Description:**
Add SDK to GSSP RS. Emit: `RAG_RETRIEVAL_STARTED/COMPLETED`, `RAG_NO_RESULT`, `EMBEDDING_CALL_COMPLETED`, `RAG_INDEX_HEALTH_CHECKED`. Apply `@trace_rag` on `retrieve()` and `embed()`.

**Acceptance Criteria:**
- Per-stage latency present
- Embedding model, tokens, and cost tracked
- `RAG_NO_RESULT` fires with query context

---

**OBS-S020**
| Field | Value |
|---|---|
| **Issue Type** | Story |
| **Summary** | [OBS] Instrument Consumer Service — add SDK, emit document ingestion and Kafka lag events |
| **Priority** | Medium |
| **Phase** | Phase 2 — Instrumentation |

**Description:**
Add SDK to Consumer Service. Emit: `INGESTION_JOB_STARTED/COMPLETED`, `DOCUMENT_PARSE_STARTED/COMPLETED`, `DOCUMENT_EMBEDDING_CREATED`, `DOCUMENT_INDEXED`, `KAFKA_LAG_RECORDED`. Apply `@trace_tool` on embed.

**Acceptance Criteria:**
- First Kafka emission from this service verified
- Document telemetry (parse time, embedding time, chunk count) present
- `KAFKA_LAG_RECORDED` emits queue depth

---

**OBS-S021**
| Field | Value |
|---|---|
| **Issue Type** | Story |
| **Summary** | [OBS] Instrument Data Ingestion Service — add SDK, emit document parsing and ingestion job events |
| **Priority** | Medium |
| **Phase** | Phase 2 — Instrumentation |

**Description:**
Add SDK to Data Ingestion. Emit: `INGESTION_JOB_STARTED/COMPLETED`, `DOCUMENT_PARSE_*`, `DOCUMENT_INDEXED`, `AUTH_FAILED`, `HTTP_LATENCY_RECORDED`. Ensure UTC timestamps throughout.

**Acceptance Criteria:**
- Success-path events emitted
- All timestamps in UTC ISO-8601
- `AUTH_FAILED` emits with reason code

---

**OBS-S022**
| Field | Value |
|---|---|
| **Issue Type** | Story |
| **Summary** | [OBS] Instrument User Feedback Service — add SDK, emit feedback lifecycle events linked by correlation_id |
| **Priority** | Medium |
| **Phase** | Phase 2 — Instrumentation |

**Description:**
Add SDK to User Feedback. Emit: `FEEDBACK_SUBMITTED`, `FEEDBACK_REVIEWED`, `FEEDBACK_INCIDENT_TRIGGERED`. Link via `correlation_id` to the original request trace. Ensure `user_hash` only — no raw SOE_ID.

**Acceptance Criteria:**
- Feedback events linked to original request via `correlation_id`
- `user_hash` present; no raw SOE_ID in any field
- `free_text_redacted` field confirmed redacted

---

### Phase 3 — Enrichment Consumer · Weeks 6–10

---

**OBS-S023**
| Field | Value |
|---|---|
| **Issue Type** | Story |
| **Summary** | [OBS] Build Enrichment Consumer — Kafka consume loop with dead-letter routing and manual offset commit |
| **Priority** | High |
| **Phase** | Phase 3 — Enrichment Consumer |

**Description:**
Build `obs-enrichment-consumer` base: consume `ai-obs-events-raw`, manual offset commit (`enable.auto.commit=false`), commit only after successful produce to `ai-obs-events-processed`. Invalid/unprocessable events → `ai-obs-dead-letter` with `{raw_event, validation_error, source_partition, source_offset, failed_at}`. K8s: 3 replicas, cpu 500m/1Gi requests, 2000m/2Gi limits.

**Acceptance Criteria:**
- Malformed events land in `ai-obs-dead-letter` with full context
- Offsets committed only after produce to processed succeeds
- K8s deployment with readiness probe passing

---

**OBS-S024**
| Field | Value |
|---|---|
| **Issue Type** | Story |
| **Summary** | [OBS] Enrichment Consumer — Stage 1-2: Schema validation and trace context extraction |
| **Priority** | High |
| **Phase** | Phase 3 — Enrichment Consumer |

**Description:**
Stage 1: Pydantic `ObsEvent` validation — invalid → dead-letter. Stage 2: Extract and propagate `correlation_id`, `span_id`, `parent_span_id`, `traceparent` from W3C header.

**Acceptance Criteria:**
- Invalid schema → dead-letter with `validation_error` field
- `correlation_id` and span context extracted and present on processed event

---

**OBS-S025**
| Field | Value |
|---|---|
| **Issue Type** | Story |
| **Summary** | [OBS] Enrichment Consumer — Stage 3: In-process PII redaction using GLiNER NER |
| **Priority** | High |
| **Phase** | Phase 3 — Enrichment Consumer |

**Description:**
Integrate GLiNER in-process NER (no sidecar, no network hop). Hash SOE_ID, redact names/cards/emails. GLiNER loads once at pod startup (~512Mi). Skip fields > 10k chars. Readiness delay 30s for model load.

**Acceptance Criteria:**
- Adversarial payloads (SSN, COIN, names, cards) redacted before produce to processed
- No plaintext PII in `ai-obs-events-processed` (verified by redaction test suite)
- GLiNER loads once per pod; inference time monitored

---

**OBS-S026**
| Field | Value |
|---|---|
| **Issue Type** | Story |
| **Summary** | [OBS] Enrichment Consumer — Stage 4-5: Metadata enrichment and error code normalisation |
| **Priority** | High |
| **Phase** | Phase 3 — Enrichment Consumer |

**Description:**
Stage 4: Registry join via in-process `cachetools.TTLCache` (5-min TTL) — adds service, tenant, LOB labels from `observability.application_registry`. Stage 5: Map raw error strings to standard codes from `observability.error_code_catalog`.

**Acceptance Criteria:**
- `lob`, `tenant_id` enriched on every event
- Raw errors normalized to `error_code` from catalog
- Cache TTL 5-min; cold cache hits DB (no Redis yet)

---

**OBS-S027**
| Field | Value |
|---|---|
| **Issue Type** | Story |
| **Summary** | [OBS] Enrichment Consumer — Stage 6: Token and cost calculation with real-time budget tracking |
| **Priority** | High |
| **Phase** | Phase 3 — Enrichment Consumer |

**Description:**
Stage 6: Compute `estimated_cost` from token counts + currency rates on every `LLM_CALL_COMPLETED`. Atomic upsert into `observability.budget_accumulator` (interim Redis stand-in). On crossing `budget_limits.max_spend_usd × alert_at_pct/100` → emit `BUDGET_THRESHOLD_EXCEEDED`.

**Acceptance Criteria:**
- `estimated_cost` present on every `LLM_CALL_COMPLETED`
- `BUDGET_THRESHOLD_EXCEEDED` fires when threshold crossed
- `budget_accumulator` updated atomically (no double-count on retry)

---

**OBS-S028**
| Field | Value |
|---|---|
| **Issue Type** | Story |
| **Summary** | [OBS] Enrichment Consumer — Stage 7-9: S3 payload archival, SLO burn-rate evaluation and quality hook |
| **Priority** | Medium |
| **Phase** | Phase 3 — Enrichment Consumer |

**Description:**
Stage 7: Archive large payloads/prompts/contexts to S3 before downstream write. Stage 8: Compute burn-rate 1h + 6h; write 1 row/day/app/SLO to `daily_slo_compliance`. Stage 9: Mark RAG/LLM events for eval sampling (set `eval_eligible=true` flag for `obs-eval-service`).

**Acceptance Criteria:**
- Large payloads in S3 `redacted-prompts/` / `redacted-responses/`
- One `daily_slo_compliance` row per app per SLO per day
- RAG events flagged for downstream eval sampling

---

### Phase 4 — Storage Consumer + Stores · Weeks 8–12

---

**OBS-S029**
| Field | Value |
|---|---|
| **Issue Type** | Story |
| **Summary** | [OBS] Build Storage Consumer — fan-out writer to Elasticsearch, PostgreSQL and S3 |
| **Priority** | High |
| **Phase** | Phase 4 — Storage Consumer |

**Description:**
Build `obs-storage-consumer` — consumes `ai-obs-events-processed` (`group.id=obs-storage-consumer`). Fan-out per event: (1) ES index via route logic `ai-obs-{lob}-{category}-{date}`, `_id=event_id`; (2) batch upsert into `obs_events.*` via COPY + `ON CONFLICT (event_id, event_ts) DO NOTHING`; (3) S3 payload archive; (4) control-plane rows (`observability.*`). K8s: 3 replicas, cpu 200m/256Mi.

**Acceptance Criteria:**
- Events indexed in ES within SLA; searchable in Kibana
- `obs_events.events` row counts match throughput
- S3 has redacted payloads
- Idempotent on `event_id` — dedup verified by replay test

---

**OBS-S030**
| Field | Value |
|---|---|
| **Issue Type** | Story |
| **Summary** | [OBS] Implement dead-letter replay tooling and Kafka consumer lag alerting |
| **Priority** | Medium |
| **Phase** | Phase 4 — Storage Consumer |

**Description:**
Write `scripts/replay_dead_letter.py` — re-produces `raw_event` from dead-letter topic → `ai-obs-events-raw` after a fix. Wire kminion alert: lag > 1000 sustained 10m → PagerDuty/alert for both consumer groups.

**Acceptance Criteria:**
- Dead-letter replay verified end-to-end
- kminion alert fires correctly at lag threshold
- Both consumer groups (`obs-enrichment-consumer`, `obs-storage-consumer`) monitored

---

### Phase Q — AI Quality & Trace Layer · Weeks 10–15

---

**OBS-S031**
| Field | Value |
|---|---|
| **Issue Type** | Story |
| **Summary** | [OBS] Build prompt registry — version management, side-by-side diff and A/B traffic split |
| **Priority** | High |
| **Phase** | Phase Q — AI Quality |

**Description:**
Build dashboard CRUD for `observability.prompt_template_registry`. Support: create new version, diff between versions, set `status` (draft/active/archived), configure `traffic_pct` for A/B split. `prompt_hash` changes on any template edit. SDK `get_prompt()` routes to variant by traffic split.

**Acceptance Criteria:**
- Create/edit/archive prompt versions via dashboard
- Version diff visible side-by-side
- A/B split routes traffic to variants; both variants tracked on LLM events

---

**OBS-S032**
| Field | Value |
|---|---|
| **Issue Type** | Story |
| **Summary** | [OBS] Build LLM-as-judge evaluation service — faithfulness, hallucination and relevance scoring |
| **Priority** | High |
| **Phase** | Phase Q — AI Quality |

**Description:**
Build `obs-eval-service` — consumes `ai-obs-events-processed` (`group obs-eval-consumer`). Sampling: 100% RAG completions, 10% plain LLM (configurable). Judges: `faithfulness.py`, `hallucination.py`, `relevance.py`. Judge prompts versioned in `prompt_template_registry`. Output written to `ai-obs-quality-scores-*` (ES) + `obs_events.quality_scores` (PG). Async/decoupled — never on request path.

**Acceptance Criteria:**
- Eval scores land for sampled RAG within sampling window
- `faithfulness_score`, `hallucination_flag`, `relevance_score` present
- `judge_model` cost tracked in cost governance

---

**OBS-S033**
| Field | Value |
|---|---|
| **Issue Type** | Story |
| **Summary** | [OBS] Build Trace Explorer — correlation_id span waterfall API and React drill-down UI |
| **Priority** | High |
| **Phase** | Phase Q — AI Quality |

**Description:**
Build `GET /api/v1/trace/{correlation_id}` — queries ES `ai-obs-{lob}-*` + `ai-obs-quality-scores-*` + `observability.feedback_case`, builds span tree from `span_id`/`parent_span_id`. React UI: collapsible waterfall + tree with per-span timing, tokens, cost, `prompt_version`, eval scores, linked feedback. COIN-JWT gated; per-LOB RBAC enforced.

**Acceptance Criteria:**
- Trace Explorer renders full span waterfall for any `correlation_id`
- Eval scores and linked feedback visible in the tree
- COIN-JWT auth enforced; LOB isolation verified

---

### Phase 5 — Presentation · Weeks 12–16

---

**OBS-S034**
| Field | Value |
|---|---|
| **Issue Type** | Story |
| **Summary** | [OBS] Build Custom Dashboard Service — FastAPI backend, React UI with COIN-JWT auth and per-LOB RBAC |
| **Priority** | High |
| **Phase** | Phase 5 — Presentation |

**Description:**
Build `obs-dashboard` backend (FastAPI, COIN-JWT `require_coin_token`, per-LOB RBAC) and React + Tremor frontend. Replaces Grafana dashboards (kube-prometheus-stack deployed with `grafana.enabled=false`). Dashboard pages: Platform Overview, Cost Governance, Kafka Health, RAG Quality, Feedback Trends.

**Acceptance Criteria:**
- All API endpoints gated by COIN-JWT
- Per-LOB RBAC enforced — user cannot see data outside their LOB
- All 5 dashboard pages rendering with live data

---

**OBS-S035**
| Field | Value |
|---|---|
| **Issue Type** | Story |
| **Summary** | [OBS] Configure Kibana operational views, error drill-down and ML log anomaly detection |
| **Priority** | Medium |
| **Phase** | Phase 5 — Presentation |

**Description:**
Configure Kibana: operational search, error drill-down, `correlation_id` trace view querying `ai-obs-{lob}-*` indices. Enable Kibana ML on log patterns for anomaly signal detection. Per-LOB index-level RBAC roles.

**Acceptance Criteria:**
- Error drill-down and `correlation_id` search working in Kibana
- Kibana ML anomaly job running on log indices
- Per-LOB index RBAC configured

---

### Phase 6 — Observability Chatbot · Weeks 15–19

---

**OBS-S036**
| Field | Value |
|---|---|
| **Issue Type** | Story |
| **Summary** | [OBS] Build Observability Chatbot — natural language querying with LOB RBAC and source attribution |
| **Priority** | Medium |
| **Phase** | Phase 6 — Chatbot |

**Description:**
Build `obs-chatbot` (`/chat` endpoint). Pipeline: IntentClassifier (internal LLM via GSSP GS) → MetricSemanticLayer (`metric_catalog`) → AccessController (LOB RBAC from `application_registry`) → QueryPlanner (queries `obs_events` / ES / S3 / Trace Explorer API) → AnswerGenerator (value + filters + source + dashboard deep-link). Joins AI-quality + infra context on `correlation_id`.

**Acceptance Criteria:**
- NL queries answered with correct value, source attribution, and dashboard deep-link
- LOB RBAC enforced — chatbot scoped to caller's LOB
- `metric_catalog` resolves metric definitions correctly

---

### Phase 7 — Anomaly + RCA (Optional) · Weeks 19–24

---

**OBS-S037**
| Field | Value |
|---|---|
| **Issue Type** | Story |
| **Summary** | [OBS] Implement Isolation Forest anomaly detection and Anomaly View dashboard (optional) |
| **Priority** | Low |
| **Phase** | Phase 7 — Anomaly + RCA (Optional) |

**Description:**
Build anomaly detection using Isolation Forest on event metrics. Write signals to `ai-obs-anomalies-*`. Surface in Kibana ML Anomaly View dashboard page.

**Acceptance Criteria:**
- Anomaly signals written to `ai-obs-anomalies-*`
- Anomaly View dashboard page rendering in Custom Dashboard
- False-positive rate acceptable (tuned threshold)

---

**OBS-S038**
| Field | Value |
|---|---|
| **Issue Type** | Story |
| **Summary** | [OBS] Implement nightly root cause analysis CronJob and weekly digest reporting (optional) |
| **Priority** | Low |
| **Phase** | Phase 7 — Anomaly + RCA (Optional) |

**Description:**
Nightly K8s CronJob: correlate anomalies, errors, latency spikes; write RCA report to S3 `rca-reports/`. Generate weekly digest summarising top issues and recommended actions.

**Acceptance Criteria:**
- RCA report written to S3 nightly
- Weekly digest generated and delivered
- Reports linked from Anomaly View dashboard

---

## JIRA Hierarchy Summary

```
ARCH-41247 (Capability — EXISTS, do not recreate)
└── [New Epic] Observability Plane — Kafka-native AI observability pipeline
    ├── Phase 0 — Foundation        OBS-S001 → OBS-S008
    ├── Phase 1 — Shared SDK        OBS-S009 → OBS-S014
    ├── Phase 2 — Instrumentation   OBS-S015 → OBS-S022  (one story per service)
    ├── Phase 3 — Enrichment        OBS-S023 → OBS-S028
    ├── Phase 4 — Storage           OBS-S029 → OBS-S030
    ├── Phase Q — AI Quality        OBS-S031 → OBS-S033
    ├── Phase 5 — Presentation      OBS-S034 → OBS-S035
    ├── Phase 6 — Chatbot           OBS-S036
    └── Phase 7 — Anomaly (Optional) OBS-S037 → OBS-S038
```

**Total: 1 Epic + 38 Stories**

---

## Risks & Mitigations

| Risk | Mitigation |
|---|---|
| GLiNER = enrichment bottleneck (~512Mi/pod, CPU-bound) | Scale replicas to partition count; skip fields > 10k chars |
| 8 service teams must add Kafka producer | SDK is ~6 lines in `main.py`; copy-paste block provided |
| Eval LLM spend unbounded | 10% sampling + `judge_model` cost cap; tracked in cost governance |
| Postgres `obs_events` grows unbounded | Monthly partitions + ~90-day window + S3 archive; Snowflake if/when approved |
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

1. **Langfuse vs custom AI quality layer** — currently custom (Phase Q); revisit after Phase Q; data model is compatible either way
2. **Snowflake onboarding** — PostgreSQL `obs_events.*` is interim; watch DB size as the trigger
3. **Redis availability** — PostgreSQL `budget_accumulator` is interim; watch write contention as the trigger
4. **Internal LLM for judges/chatbot** — route via GSSP GS; model/cost tier TBD
5. **Phase 7 (Anomaly + RCA)** — optional; decide before Phase 6 completes

---

*All phases Planned — no implementation work complete. Capability ARCH-41247 exists; create one Epic under it, then link all 38 stories to that Epic.*
