# Observability Plane — What We're Building & Why

> **Audience:** Platform engineering team
> **Purpose:** Explain (1) what observability was missing in each of our 8 AI services, (2) the target architecture we're building, (3) how data is captured, flows, is stored, displayed and used, (4) the main metrics, and (5) the technology choices — what we picked and what we deliberately rejected.
>
> **Reference diagram:** `Observability Arch Latest.png` — *AI Services Platform Observability Plane — Detailed Architecture.*
>
> **Status note:** The architecture itself (two layers, Kafka pipeline, four stores) is the proposal we're confident in. Most tool picks in §5 are recommendations — but **the AI-quality layer tool (Langfuse vs a custom build) is deliberately still open** (§5.3). Present it as an open decision, not a done deal.

---

## 0. TL;DR

Today our 8 AI services each log to their own JSON files and a few audit tables. There is **no shared schema, no cost tracking, no distributed tracing, no PII redaction, and no central place to see what the platform is doing.** A single request crosses Orchestration → Agent Executor → GSSP QS → GSSP RS → GSSP GS and we cannot follow it end-to-end.

We are building a **two-layer Observability Plane:**

- **Layer 1 — AI Quality** *(tool not yet decided — Langfuse or custom build, see §5.3)*: LLM traces, RAG spans, agent step trees, prompt versions, evaluations, user-feedback linkage.
- **Layer 2 — Platform/Infra (OIS + Kafka):** structured events, metrics, Kafka health, cost governance, anomalies, business KPIs.

Both layers share a **`correlation_id`** so any request can be reconstructed across both. Data is captured by an OpenTelemetry SDK, streamed through **3 Kafka topics**, enriched (PII-redacted, cost-calculated, trace-stitched), then fanned out to **four stores** tuned for different jobs (Elasticsearch = hot search, PostgreSQL = control plane, Snowflake = analytics/forever, S3 = payload archive). It is presented through a **Custom Dashboard Service**, Kibana, and the AI-quality trace UI (Langfuse or custom — §5.3), and consumed by **SLO alerting, a nightly RCA engine, and an Observability Chatbot.**

---

## 0.1 How to Tell This Story (Presenter's Guide)

**The story in three sentences:**
1. *We run 8 AI services and we're flying blind — we can't follow a request across them, we don't know what any request costs, and we're logging PII in plain text.* (§1)
2. *We're building one shared pipeline: every service emits standard events, fire-and-forget, into Kafka; they're validated, PII-redacted, and cost-priced once, centrally; then each lands in the store built for its job.* (§2–§3)
3. *On top sit dashboards, SLO alerts, a nightly root-cause engine, and a chatbot — all joined by one `correlation_id` per request.* (§3.6)

**Suggested walk order (~20 min):** §1 the pain (5 min — lead with the story below, then the cross-cutting table) → §2.7 trace one event down the diagram (5 min) → §3.4 why four stores (3 min) → §5 decisions, ending on the open Langfuse-vs-custom call in §5.3 (5 min) → §6 rollout (2 min).

**Plain-English translations** — reach for these when the room glazes over:

| Term | Say instead |
|---|---|
| `correlation_id` | A parcel-tracking number: stamped on the request at the front door; every log line, event, and trace at every hop carries it |
| Kafka observability topics | A conveyor belt between the services and the processing plant — services drop events on it and move on; nothing ever waits |
| Fire-and-forget emitter | Dropping a letter in a postbox: the service never waits for delivery, so observability can never slow down or break a real request |
| Enrichment pipeline | The processing plant: every event passes the same nine stations — inspected, cleaned of PII, priced, labeled — before storage |
| Dead-letter topic | The returns shelf: malformed events are quarantined for inspection and replay instead of being silently dropped |
| The four stores | Whiteboard (Elasticsearch: fast, recent), filing cabinet (PostgreSQL: small, authoritative), warehouse archive (Snowflake: everything, forever), storage unit (S3: big boxes, cheap) |
| SLO burn rate | How fast we're spending the monthly error allowance — burning a day's worth in an hour pages someone; a slow leak just opens a ticket |

**If the room remembers only three things:**
1. **One `correlation_id` per request, on every event, in both layers** — that's what makes everything joinable.
2. **Services emit and forget; all hard work happens once, centrally** — validation, PII redaction, and cost pricing live in the pipeline, not in 8 codebases.
3. **We buy the commodity and build only the domain glue** — and the one buy-vs-build still open is the AI-quality layer: Langfuse vs custom (§5.3).

---

## 1. The Problem — What Was Missing in Each Service

> **Open with this story, not the table:** *A user reports a bad answer at 2 pm. Today, finding out why means grepping four different JSON log files on four services with no shared ID. We can't say which model call produced it, what it cost, or whether retrieval returned anything relevant — and the user's SOE_ID is sitting in those logs in plain text.* Every row below is a piece of that story.

### 1.1 The cross-cutting gaps (true for *all 8* services)

These are the gaps that block a central observability plane no matter which service you look at:

| Gap | Why it hurts | Priority |
|---|---|---|
| **No `event_id`** anywhere | Cannot de-duplicate events; chatbot drill-down and exactly-once processing impossible | P0 |
| **No `environment`** field | Cannot separate prod / staging / dev telemetry | P0 |
| **No `service_name`** as a structured field | Cannot filter or group telemetry by service in storage | P0 |
| **No `schema_version`** | Cannot evolve the event contract safely | P0 |
| **SOE_ID / user_id logged in plain text** 🔴 | PII / compliance violation in every service | P0 |
| **`estimated_cost` never calculated** | Zero cost governance, even where token counts exist | P0 |
| **`latency_ms` is a string in a log message** (6 of 8) | Not a queryable numeric — no p95/p99 dashboards | P0 |
| **No OpenTelemetry / distributed tracing** | A request cannot be followed across service boundaries | P0 |
| **No shared ingestion API** — each service writes its own JSON logs / audit tables | No single pane of glass; everything is fragmented | P0 |
| **No `/metrics` endpoint** on any service | No real-time counters / histograms | P1 |
| **6 of 8 services emit nothing to Kafka** | Their events never reach a central pipeline | P1 |

> **Bottom line:** every service captures *something* locally, but no two services agree on field names, nothing is PII-safe, nothing is cost-aware, and nothing is traceable across hops.

### 1.2 Per-service breakdown

#### 1. Agent Executor — *best instrumented, still incomplete*
- **Has:** full PostgreSQL `audit_table` trail (INVOCATION/AGENT/LLM/TOOL/ERROR), JSON logs with correlation/application/SOE context, token counts from VertexAI, end-to-end `X-Correlation-ID`.
- **Missing:** `event_id`, numeric `latency_ms`, `estimated_cost`, `environment`, `service_name`; SOE_ID in plain text; no OTEL spans; no `/metrics`; `agent_id`/`tool_id` not in log lines; `finish_reason`/`rate_limit_hit`/`safety_blocked` absent; one stray `print()` bypasses logging entirely.

#### 2. Agentic Orchestration — *the routing brain, blind on LLM + agents*
- **Has:** JSON logs, Kafka lifecycle events (`AGENT_EXECUTION_REQUEST`, `HIL_REQUEST/RESPONSE`, final/rejected), HTTP timing, centralized error codes, planner step counts.
- **Missing:** **all LLM telemetry** (tokens/model/latency for its VertexAI/Stellar calls — a critical cost hole), structured agent events (`AGENT_STARTED`, `AGENT_STEP_COMPLETED`), `environment`/`service_name`, distributed tracing, `/metrics`; SOE_ID plain text; step count logged as an English sentence, not a number.

#### 3. Consumer Service (ingestion scheduler) — *pipeline steps invisible*
- **Has:** structured JSON logs with job/document context, error codes, a timing decorator, job-status lifecycle.
- **Missing:** `event_type` only on errors so pipeline steps are invisible; **no Kafka emission at all**; no embedding token/cost capture; **document telemetry entirely absent** — `document_format`, `document_size_bytes`, `chunk_count`, `page_count`, `extraction_status`, `parser_used`; no `DOCUMENT_PARSE_*` events; **queue depth never recorded** so backlog is undetectable. Silent extraction failures produce empty embeddings nobody sees.

#### 4. Data Ingestion Service — *success paths emit nothing*
- **Has:** structured logs via `asgi-correlation-id`, job/document IDs, typed network-error handling, STATUS events.
- **Missing:** same standard-field gaps; **success-path route events missing** (bulk-change create, status query); no embedding token/cost; **no Kafka emission**; timestamps not consistently UTC ISO 8601 (breaks cross-region correlation); same document-telemetry holes as Consumer Service.

#### 5. GSSP GS (Generation Service) — *has the data, doesn't ship it*
- **Has:** the richest LLM data already — `LLMUsageMetrics` parses tokens across Vertex/OpenAI/Anthropic, confidence via logprobs, template-bound generation, hot-reload configs.
- **Missing:** `estimated_cost` **despite having token counts**; no Kafka streaming; no OTEL; no `/metrics`; `finish_reason`/`rate_limit_hit`/`safety_blocked` absent; **all file/attachment telemetry missing** (`has_attachment`, `file_count`, `image_count`, `doc_count`, `file_types`, `multimodal_flag`) even though `PartHolder` already carries filename + mime_type — fully derivable at intake, zero upstream change.

#### 6. GSSP QS (Query Service) — *RAG quality is a black box*
- **Has:** request/response/error/cache-hit logs, an `observability_type` enum, error-code registry, cache-hit token/cost-saved capture.
- **Missing:** `observability_type` is limited and not mapped to standard `event_type`; **guardrail/retrieval/generation success paths not consistently evented**; **cache misses logged without model/cost/latency** (can't measure cache effectiveness or live spend); **all RAG quality fields missing** — `retrieved_chunk_count`, `avg_relevance_score`, `no_result_flag`, `citation_coverage_pct`; no `/metrics`, no central emitter.

#### 7. GSSP RS (Retrieval Service) — *owns retrieval, measures none of it*
- **Has:** HTTP request/response logs, error-code enum, partial startup/PGVector init logs.
- **Missing:** `latency_ms` only as total HTTP time in seconds (no per-stage embed/DB/MMR timing); **embedding calls emit no model/token/latency/cost**; **retrieval runtime events absent** (`RETRIEVAL_REQUEST/RESPONSE`, `RAG_NO_RESULT`) so result count, top-k, relevance, strategy, and no-result rate are all unknown; MMR re-ranking has no logs; raw SOE/content PII risk; no `/metrics`.

#### 8. User Feedback — *the quality loop is broken at the source*
- **Has:** captures `feedback_id`, `rating`, `thumbs`; partial correlation linkage; middleware request/response logging.
- **Missing:** **`FEEDBACK_SUBMITTED` event never emitted**; feedback not reliably linked to `correlation_id` (can't join a rating to the request that earned it); `feedback_category`, `submitted_by_role`, `resolution_status`, `linked_incident_id` all absent; free-text comment **not redacted** 🔴; no Kafka emission (can't trigger incident routing); no counters. The feedback-to-fix loop has no telemetry.

---

## 2. Reading the Architecture Diagram (`Observability Arch Latest.png`)

The diagram is a **six-layer pipeline read top-to-bottom.** Each layer carries a numbered badge on the left (**1 → 6**), and data flows **downward**: an event is *produced* at the top, *transported* and *enriched* in the middle, *stored*, and finally *presented* at the bottom. A governance band runs underneath everything.

**The legend (bottom strip) defines three arrow types — know these before reading the boxes:**

| Arrow | Meaning |
|---|---|
| **──▶ solid** | **Data Flow** — the normal path of a telemetry event |
| **┄┄▶ red dashed** | **Failed / Invalid Flow** — events that fail validation/enrichment and branch to the dead-letter topic |
| **⋯⋯▶ dotted** | **Config / Control Flow** — schema, RBAC, budgets — rules applied to the pipeline, *not* telemetry data |

The colored badge numbers map to: **1** Producers · **2** Kafka Layer · **3** Processing · **4** Storage · **5** Consumers · **6** Governance.

### 2.1 Layer 1 — Producers *(blue badge)*
*"AI Services Platform Producers."* The top row is the **live business platform** — the systems that generate telemetry as a side-effect of doing their real work, shown left-to-right along the request path:

> Client / UI / API Gateway → Orchestration Service → **Kafka Event Bus (business execution path)** → Executor Service → Agent Orchestrator / Agent Runtime → LLM / Model Services → Registered Tools → RAG / Knowledge Base → Guardrails / Memory / Feedback

Spanning the **entire row** is the **Observability SDK / OpenTelemetry** band — the single instrumentation layer every producer shares. It emits standard events, logs, metrics, and spans; propagates `Correlation_ID`; runs the **GLiNER PII redactor at source**; and ships everything through a **fire-and-forget Kafka emitter** so instrumentation never blocks the business request.

> ⚠️ **Don't confuse the two Kafkas:** the *Kafka Event Bus* in this row is the **business** execution path (how the platform actually runs agents). The Kafka in Layer 2 is the separate **observability** transport. They are different clusters/topics with different jobs.

### 2.2 Layer 2 — Kafka Observability Topics *(purple badge)*
*"Durable, scalable event transport — 3 topics."* This is the buffer that decouples producers from processing:

- **`ai-obs-events-raw`** — the ingestion topic; **all 8 services, all 38 event types** land here, unvalidated.
- **Enrichment Consumer** — reads raw events, **validates, enriches, and routes** them; valid output goes to **`ai-obs-events-processed`** (the `event_type` field routes internally).
- **`ai-obs-dead-letter`** — the **red-dashed branch**; invalid / failed events land here, held for debugging and replay.
- **Retention panel (configurable):** raw = **7 days**, processed = **3 days**, dead-letter = **14 days**; with compaction + partitioning for scale & cost efficiency.

### 2.3 Layer 3 — Telemetry Processor / Enrichment Layer *(green badge)*
*"Normalize, enrich, redact, route."* This expands what the Enrichment Consumer actually does, as a **nine-stage left-to-right pipeline**:

| # | Stage | Job |
|---|---|---|
| 1 | **Schema Validator** | Validate structure and required fields |
| 2 | **Correlation_ID / Trace Context Extractor** | Extract & propagate `Correlation_ID` and parent context |
| 3 | **PII / Sensitive Data Redactor** | Detect, mask, pseudonymise sensitive data |
| 4 | **Metadata Enricher** | Add env, service, version, tenant, user, labels |
| 5 | **Error Code Mapper** | Map errors to standard codes and categories |
| 6 | **Token / Cost Calculator** | Compute tokens, cost, currency, rates |
| 7 | **S3 Archiver** | Write large payloads & artifacts to S3 |
| 8 | **SLO Evaluator** | Burn-rate computation; writes 1 row/day to PostgreSQL |
| 9 | **RAG / Quality Scorer** | Score retrieval quality & response quality |

### 2.4 Layer 4 — Data & Storage Layer *(teal badge)*
*"Persistent, durable, queryable stores."* Five stores, **each with a distinct job** (this is the "four stores, not one" decision made visual):

- **Elasticsearch / Kibana (Hot & Warm)** — recent searchable events, errors, traces, tool/LLM/RAG/feedback events, anomalies; **per-LOB namespaced indices**.
- **PostgreSQL (Control Plane)** — control plane *only*: registries, metric catalog, KPI definitions, budget config, alert thresholds, budget limits, `daily_slo_compliance`, `feedback_case`. **No pre-computed aggregates.**
- **Snowflake** *(marked **NEW**)* — **all events, forever retention**; on-demand analytics with no pre-aggregation: raw events, RAG-quality history, agent performance, feedback analytics, cross-LOB BI reporting, ML feature store, compliance & audit long-term data.
- **Amazon S3 (Object Store)** — redacted prompts/responses, raw payloads, full traces, audit evidence, multimodal document artifacts, RCA reports, debug bundles.
- **Redis (Cache / Memory)** — active `Correlation_ID` context, dedup, rate limits, session cache, real-time budget accumulator (live spend), registry cache (5-min TTL). *Recommended cache — **not** a source of truth.*

### 2.5 Layer 5 — Presentation & Consumers *(orange badge)*
*"Insights, analysis, dashboards, and access."* Three surfaces, each reading from the store that fits the question:

- **Kibana Dashboards** — operational search, error drill-down, `Correlation_ID` trace view → queries **Elasticsearch** hot events.
- **Custom Dashboard Service** — FastAPI + React + Tremor → recent ops from **Elasticsearch**, analytics & trends from **Snowflake** (on-demand), budget config from **PostgreSQL**.
- **Observability Chatbot** — natural-language Q&A → recent events from **Elasticsearch**, long-range analytics from **Snowflake**, fetches artifacts on demand from **S3**.

### 2.6 Layer 6 — Governance & Standards *(dark badge)*
The bottom band is **not a flow stage** — it's the set of rules that apply *across every layer* (drawn with dotted control-flow arrows): `Correlation_ID` on every event · standard event schema · retention & RBAC · PII redaction policy (GLiNER) · KPI & metric catalog · Snowflake on-demand analytics · no pre-computed aggregates · audit & compliance (access logs, policy decisions) · cost governance (budget limits, alerts, spend tracking).

### 2.7 Following one event through the picture
Putting it together, trace a single request straight down the diagram:

1. A user request hits **Layer 1**; the SDK emits an event — PII-redacted at source, `Correlation_ID` attached — **fire-and-forget** into `ai-obs-events-raw` (**Layer 2**).
2. The **Enrichment Consumer** runs the nine-stage **Layer 3** pipeline. A valid event becomes a `processed` event; an invalid one branches off (**red dashed**) to `dead-letter`.
3. The processed event **fans out across Layer 4** — a hot copy to Elasticsearch, everything to Snowflake (forever), big payloads to S3, live counters to Redis, and SLO/budget rows to PostgreSQL.
4. **Layer 5** reads those stores so a human (Kibana / Custom Dashboard) or the **Chatbot** can answer questions — all bounded by the **Layer 6** standards.

### 2.8 Where the AI-Quality layer fits — the two-layer view

One thing the diagram **does not draw explicitly:** the diagram is the **platform / infrastructure pipeline**. In parallel, AI-quality signals (LLM traces, RAG spans, agent step trees, prompt versions, evals) are *also* captured by a dedicated **AI-quality trace layer**. Langfuse is the lead candidate for this layer, but **a custom build is equally on the table** (§5.3) — the mechanics are the same either way: a one-line decorator on every LLM/RAG/agent function. Together the two layers form one model, bridged by the same `Correlation_ID` that flows through the pipeline above:

```
┌──────────────────────────────────────────────────────────────────────┐
│  LAYER 1 — AI Quality & Trace  (tool open: Langfuse or custom, §5.3) │
│  What:  LLM traces, RAG spans, agent step trees, prompt versions,    │
│         faithfulness/eval scores, user-feedback links                │
│  Who:   GSSP GS, GSSP QS, GSSP RS, Agent Executor, Orchestration     │
│  How:   1-line decorator (Langfuse @observe, or custom equivalent)   │
│  Store: self-hosted PostgreSQL inside our boundary (either way)      │
│  UI:    trace explorer, prompt mgmt, evals (Langfuse Web or ours)    │
├──────────────────────────────────────────────────────────────────────┤
│  LAYER 2 — Platform / Infrastructure  (OIS + Kafka)                  │
│  What:  Kafka lag, service health, document ingestion, business      │
│         KPIs, budget governance, anomaly events, guardrails          │
│  Who:   All 8 services via OpenTelemetry SDK / OIS HTTP emitter      │
│  How:   POST /v1/ingest — fire-and-forget                            │
│  Store: Elasticsearch, PostgreSQL, Snowflake, S3, Redis             │
│  UI:    Custom Dashboard Service, Kibana, Observability Chatbot      │
└──────────────────────────────────────────────────────────────────────┘
         ▲                                                    ▲
         └──────────  shared correlation_id  ─────────────────┘
        (any request reconstructable across BOTH layers)
```

**Why two layers? (This rationale holds whichever way the Langfuse decision goes.)** LLM/RAG/agent quality and infrastructure health are different problems with different shapes: one is trace trees, prompts, and eval scores; the other is counters, lag, and budgets. Forcing AI-quality signals through generic infra tooling loses the trace-tree/eval model; conversely, Kafka lag and budget caps don't belong in an LLM trace tool. The shared `correlation_id` is the bridge: the chatbot can pull the LLM trace tree from Layer 1 **and** the infra context from Layer 2 for the same request. The open question (§5.3) is only *who provides Layer 1* — Langfuse off the shelf, or our own build.

---

## 3. Data Lifecycle — Capture → Flow → Process → Store → Display → Use

This is the heart of the design. Follow one event from emission to consumption.

### 3.1 CAPTURE — how data leaves a service

Each service emits to **both layers in parallel**:

- **Layer 1:** a single decorator on LLM/RAG/agent functions (Langfuse's `@observe`, or our custom equivalent — §5.3) — it auto-captures tokens, cost, latency, model, finish_reason, prompt hash.
- **Layer 2:** an **OpenTelemetry SDK** wrapper plus a fire-and-forget `POST /v1/ingest` to the **Observability Ingestion Service (OIS)**. There are **38 standardized event types** (LLM call, agent step, tool call, RAG retrieval, guardrail decision, document parse, feedback, Kafka lag, etc.), all sharing one mandatory field envelope (`event_id`, `event_type`, `schema_version`, `timestamp`, `correlation_id`, `span_id`, `service_name`, `environment`, `application_id`, `lob`, `status`, `latency_ms`, …).

Auto-instrumentation does most of the work: FastAPI server spans, outbound `httpx` spans (service→service), and `asyncpg` query spans come for free, and **W3C `traceparent` is injected on every outbound call and Kafka message** — this is what finally makes a request traceable across all 8 services.

### 3.2 FLOW — three Kafka topics, `event_type` routes internally

| Topic | Producer | Consumer | Retention | Purpose |
|---|---|---|---|---|
| `ai-obs-events-raw` | All 8 services via SDK | Enrichment Consumer | **7 days** | All 38 event types — unvalidated, unredacted |
| `ai-obs-events-processed` | Enrichment Consumer | Storage Consumer, Anomaly Detection | **3 days** | Enriched, validated, **PII-redacted** |
| `ai-obs-dead-letter` | Enrichment Consumer | DLQ handler | **14 days** | Failed validation/enrichment — held for replay |

Every Kafka message carries the trace context as headers:
```
traceparent: 00-{32-char-trace-id}-{16-char-parent-id}-{flags}
tracestate:  intentiq={application_id};env={environment}
correlation_id: {id}
```

### 3.3 PROCESS — the Enrichment / Telemetry Processor

The Enrichment Consumer reads `raw`, runs each event through a pipeline, and writes to `processed` (or `dead-letter` on failure):

1. **W3C traceparent extractor** — stitches the event into its cross-service trace.
2. **PII redaction (GLiNER)** — hashes SOE_ID/user_id, redacts names/emails/cards/tokens in free text. *Invalid → dead-letter.*
3. **Metadata enrichment** — joins app/agent/tool/rag registries from PostgreSQL.
4. **Error-code mapping** — normalizes error taxonomy.
5. **Token cost calculation + Budget accumulator (Redis)** — computes `estimated_cost_usd`, increments a real-time `application_id:model:date` counter, fires `BUDGET_THRESHOLD_EXCEEDED` when a cap is hit.
6. **Faithfulness scorer** — RAG context-overlap / LLM-as-judge quality score (delegated to the Layer-1 evaluators — built into Langfuse, or custom LLM-as-judge if we build).
7. **SLO evaluator** — multi-window burn-rate (1h + 6h); writes one `daily_slo_compliance` row/day.
8. **S3 payload archiver** — large prompts/responses/contexts off-loaded to object storage.

### 3.4 STORE — four stores, each tuned for one job

We deliberately use **four** storage systems because "one database for everything" forces a bad trade-off between fast search, cheap retention, transactional integrity, and analytics.

| Store | Role | What lives here | Why this store |
|---|---|---|---|
| **Elasticsearch** | **Hot operational** (recent days) | Per-LOB namespaced indices: `ai-obs-{lob}-requests/errors/agent-steps/llm-calls/tool-calls/rag-events/guardrail/feedback/traces`, plus `anomalies`, `quality-scores`, `vector-health` | Sub-second full-text search and drill-down; Kibana-native; per-LOB ILM retention + index-level RBAC |
| **PostgreSQL** | **Control plane only** | Registries (app/agent/tool/rag/prompt), KPI + metric catalog, `budget_limits`, `daily_slo_compliance`, `alert_threshold`, `dashboard_config`, `feedback_case` workflow | Transactional integrity for config and small authoritative tables — *not* a firehose sink |
| **Snowflake** | **Analytics + long-term (forever)** | All raw events, LLM call details, agent steps, RAG/quality events, feedback, SLO history | Cheap infinite retention + heavy analytical queries the chatbot and trend dashboards run |
| **Amazon S3** | **Payload archive** | `redacted-prompts/`, `redacted-responses/`, `raw-traces/`, `rag-contexts/`, `audit-evidence/`, `debug-bundles/`, `rca-reports/`, `iac-dashboards/` | Cheapest cold storage for big blobs; keeps ES/PG lean |
| **Redis / ElastiCache** | **Runtime state** | Real-time cost accumulator, dedup, rate limits | Sub-millisecond counters for live budget enforcement |

**Tiering logic:** hot search → Elasticsearch (days), authoritative config → PostgreSQL, everything forever for analytics → Snowflake, raw payloads → S3. The Storage Consumer fans `processed` events out to ES (hot) + Snowflake (all) + S3 (payloads); the Anomaly Detection Service reads `processed` and writes anomalies straight to Elasticsearch.

### 3.5 DISPLAY — where humans look

| Surface | Built with | Shows |
|---|---|---|
| **Custom Dashboard Service** | FastAPI + React + Tremor (COIN JWT auth) | Platform Overview, Cost Governance, Business KPIs, Kafka Health, RAG Quality, Anomaly View, Feedback Trends |
| **Kibana** | Elasticsearch-native | Event search, error drill-down, trace explorer, Kibana ML log-anomaly explorer |
| **AI-quality trace UI** | Langfuse Web *or* custom (open — §5.3) | LLM/RAG/agent trace trees, prompt management, eval scores |

The Custom Dashboard reads **Elasticsearch for recent operational** views and **Snowflake for trends/analytics**, with `budget_limits` (PostgreSQL) overlaid on Cost Governance.

### 3.6 USE — what the data actually does

1. **SLO burn-rate alerting** — multi-window (Google SRE standard) eliminates false positives. Fast burn > 14.4× over 1h **and** > 6× over 6h → page immediately; slow burn → ticket only.
2. **Offline Batch RCA engine (nightly)** — joins errors ↔ traces ↔ KPI aggregates, ranks root-cause hypotheses (model drift, tool degradation, prompt change), writes an RCA report to S3 + Elasticsearch and a weekly digest to Slack/Email.
3. **Observability Chatbot** — natural-language → intent classification → metric semantic layer (`metric_catalog`) → RBAC/LOB check → query planner → answers from Snowflake + PostgreSQL + Elasticsearch + S3, returning value + filters + source + a dashboard link.
4. **Feedback-to-fix loop** — `FEEDBACK_SUBMITTED` linked by `correlation_id` to the exact LLM trace, opening a `feedback_case` (open → reviewed → fixed).
5. **Cost governance** — live budget caps stop runaway model spend before the invoice arrives.

---

## 4. Main Metrics

Organized by the question each group answers.

### 4.1 Golden signals (every service)
`request_count` · `error_rate` · `latency_ms` p50/p95/p99 · `throughput` (req/s) · `http_status` distribution

### 4.2 LLM metrics — *cost & quality of generation*
`input_tokens` · `output_tokens` · `total_tokens` · **`estimated_cost_usd`** · `model_name` · `model_provider` · `llm_latency_ms` · `finish_reason` · `rate_limit_hit` · `safety_blocked` · `confidence_score` · `prompt_hash` (drift)

### 4.3 Agent metrics — *behavior & reliability*
`agent_success_rate` · `step_count` · `loop_count` · `handoff_count` · `termination_reason` · `agent_latency_ms` · HIL request/response counts

### 4.4 Tool metrics
`tool_status` · `tool_latency_ms` · `retry_count` · `timeout_flag` · tool error rate by `tool_id`

### 4.5 RAG / retrieval metrics — *answer quality*
`retrieved_chunk_count` · `avg_relevance_score` · `no_result_rate` · **`faithfulness_score`** (LLM-as-judge) · `citation_coverage_pct` · `context_truncation_flag` · `embedding_latency_ms` · `embedding_drift_score` · `retrieval_recall@k` · index freshness (hours since last embed)

### 4.6 Document ingestion metrics
`document_format` · `document_size_bytes` · `page_count` · `chunk_count` · `avg_chunk_size_tokens` · `extraction_status` · `parser_used` · **queue depth** · `ingestion_job_duration`

### 4.7 Cost & budget governance
spend vs budget per `application_id`/`model`/`period` · budget utilisation % · `BUDGET_THRESHOLD_EXCEEDED` events · model cost comparison

### 4.8 Kafka health
`kafka_consumer_lag` (per topic/partition/group) · offset · partition health · `producer/consumer_latency_ms` · `message_size_bytes` · `dlq_flag`

### 4.9 SLO / error budget
availability · p95 latency · error rate vs target · `error_budget_consumed_pct` · `burn_rate_1h` · `burn_rate_6h` · `breach_flag`

### 4.10 Feedback metrics
`rating` · `thumbs` · `sentiment` · `feedback_category` · positive/negative ratio · `resolution_status`

### 4.11 Infrastructure
pod CPU/memory vs limits · pod restarts (crash-loop detection) · DB connection-pool size/idle · node disk/memory pressure

---

## 5. Technology Choices — What We Chose and What We Rejected

For every signal we asked: *is there a best-in-class tool, or do we build?* The guiding principle was **buy/adopt the commodity, build only the domain-specific glue.** Adopting the tools below replaces an estimated **~30–45 weeks** of custom engineering. One decision is intentionally still open — the AI-quality layer (Langfuse vs custom build) — covered in §5.3.

### 5.1 The chosen stack at a glance

| Signal | ✅ Chosen | ❌ Rejected | Why |
|---|---|---|---|
| LLM / RAG / agent traces, prompt mgmt, evals | 🟡 **OPEN — Langfuse (self-hosted) vs custom build** (§5.3) | *nothing rejected yet* | Langfuse would save ~30+ wks of eval/trace code; custom gives exact fit and zero dependency — trade-off in §5.3 |
| Structured logs | **structlog + Fluent Bit + Elasticsearch** | Fluentd; per-service `JSONFormatter` | structlog = async-safe contextvars + built-in JSON; Fluent Bit ~1 MB vs Fluentd ~40 MB sidecar |
| Distributed traces | **OpenTelemetry + Grafana Tempo** | **Jaeger** | Tempo stores in S3/GCS (~$5/mo @1M spans/day) vs Jaeger on Cassandra/ES (expensive); native Grafana + TraceQL |
| HTTP latency / error metrics | **prometheus-fastapi-instrumentator** | Custom `/metrics` per service | One line per service → p50/p95/p99 histograms automatically |
| Kafka consumer lag | **kminion → Prometheus** | **JMX Exporter** (complex), **Burrow** (abandoned, no Prom-native), **Confluent Control Center** (needs Confluent Platform) | kminion is modern, Prometheus-native, single container |
| PII detection / redaction | **GLiNER** (in-process NER) | **Presidio** (2 sidecars + HTTP hop), custom regex | GLiNER runs *in-process* — no sidecar, no network failure mode; zero-shot labels are plain English strings; add an entity type = add a string |
| Error aggregation | **Sentry** (self-hosted) | Raw Elasticsearch errors index | ES can't fingerprint/dedupe; Sentry gives grouping, "seen 847×, up 40% today", breadcrumbs, release tracking |
| Cost / budget governance | **Pipeline cost calculator + Redis + PostgreSQL** | Custom standalone cost engine | Enrichment stage 6 prices every call (Langfuse adds per-trace pricing for free *if* adopted); Redis sub-ms accumulator; PostgreSQL caps |
| K8s / infra metrics | **kube-prometheus-stack** (`grafana.enabled=false`) | Building infra metrics | Industry-standard; we just disable its Grafana and scrape from our own dashboard |
| Platform dashboards | **Custom Dashboard Service** (FastAPI + React + Tremor) | **Grafana** | We need COIN-JWT auth, per-LOB RBAC, and business-KPI pages Grafana can't model cleanly; one auth/UX surface for the whole org |
| Anomaly detection | **Custom Isolation Forest + Kibana ML** | All-custom anomaly stack | Kibana ML (already in our ES cluster) handles log anomalies for free; custom Isolation Forest/LSTM only for metric-level |
| Guardrail / ingestion / vector-health events | **OIS custom events / OTEL spans / pgvector CronJob** | Off-the-shelf | Domain-specific — no commodity tool exists, so we emit structured events ourselves |

### 5.2 The decisions worth defending in the room

- **The AI-quality layer is the one genuinely open call — present it that way.** Langfuse is the lead candidate (1-line `@observe`, built-in LLM-as-judge evals, self-hosted so prompts stay inside our boundary), but a custom build is a legitimate alternative, and the architecture doesn't depend on which we pick. Don't defend Langfuse as decided — walk the trade-off in §5.3 and the spike that settles it.

- **Grafana Tempo over Jaeger (traces).** Same trace quality, but Tempo's object-storage backend (S3) is roughly an order of magnitude cheaper than Jaeger on Cassandra/Elasticsearch at our span volume, with native Grafana integration and TraceQL.

- **GLiNER over Presidio (PII).** Presidio needs two sidecar services and an HTTP call per event — a network hop that can time out and block the pipeline. GLiNER loads one ~300 MB model *inside* the OIS process; no new deployment, no failure mode, and new entity types are added as plain-English strings with no retraining.

- **Custom Dashboard Service over Grafana (presentation).** This is the one place we chose to **build rather than adopt**, on purpose. We need COIN-JWT auth, per-LOB index-level RBAC, business-KPI and agent-success pages, and an Observability Chatbot integration that Grafana can't model cleanly. The trade-off (a few weeks of React + Tremor) buys one consistent, governed surface for the whole org — so we run `kube-prometheus-stack` with `grafana.enabled=false` and scrape Prometheus from our own backend.

- **kminion over JMX Exporter / Burrow / Confluent.** JMX is fiddly to configure, Burrow is abandoned and not Prometheus-native, and Confluent Control Center would force us onto Confluent Platform. kminion is a single modern container that speaks Prometheus.

- **Sentry over an Elasticsearch errors index.** Elasticsearch stores every error occurrence separately; it can't tell you "this `ReadTimeoutError` spiked 3× today." Sentry fingerprints and dedupes, tracks first/last seen, and ties an error to the release that introduced it — and we scrub PII in a `before_send` hook so nothing sensitive leaves our boundary.

- **Four stores, not one.** Elasticsearch is fast but expensive to retain; Snowflake is cheap and analytical but not sub-second; PostgreSQL is transactional but not a firehose sink; S3 is cheapest but not queryable. Routing each kind of data to the store built for it is what keeps the system both fast *and* affordable.

### 5.3 The one open decision — Langfuse vs custom build (AI-quality layer)

**The architecture is tool-agnostic on this point.** Layer 1 needs *a* trace + eval + prompt-management system; whether that's Langfuse or our own build changes the implementation, not the design. The contract is identical either way: a one-line decorator on LLM/RAG/agent functions, traces keyed by `correlation_id`, data in a self-hosted PostgreSQL inside our boundary.

| Criterion | Langfuse (self-hosted) | Custom build |
|---|---|---|
| **Time to first trace** | Days — decorator + deploy | Weeks — SDK, ingestion, trace model, UI from scratch |
| **Eval / LLM-as-judge framework** | Built in (evaluators, datasets, scores) | Build it — historically the most expensive part (~30+ wks est.) |
| **Prompt management & versioning** | Built in, with UI | Build it |
| **Fit to our world** (COIN auth, LOB RBAC, our event model) | Adapt around its model — some seams | Exact fit by construction |
| **Dependency / exit risk** | OSS core, self-hosted, data stays in our PostgreSQL — but we track upstream releases | None — we own every line |
| **Maintenance** | Upgrades and breaking changes | Permanent ownership of an internal product |
| **Cost** | Infra only | Engineering time: est. 30–45 wks initial, plus ongoing |

**How to frame it in the room:** the real question is not *"is Langfuse good?"* — it's *"do we want to own an LLM-trace product, or own only our glue code?"* Adopting buys speed and a mature eval framework; building buys exact fit and zero third-party dependency.

**Path to a decision:** a 1–2 week spike on one service — GSSP GS, which already has the richest LLM data (§1.2). Run Langfuse's decorator next to a thin custom decorator writing to PostgreSQL, then score both on: integration friction, eval quality out of the box, RBAC/auth fit, and how much of the tool we'd actually use. Week 1 of the rollout (§6) doubles as this spike.

---

## 6. Rollout Order (for reference)

| Phase | Adds | Immediate value |
|---|---|---|
| **Week 1** | AI-quality tracing spike on GSSP GS (the §5.3 bake-off: Langfuse trial vs thin custom decorator); `prometheus-fastapi-instrumentator` on all 8; kminion | LLM traces, p95 latency, and Kafka lag visible day one — and the §5.3 decision gets real data |
| **Week 2** | kube-prometheus-stack; structlog migration (1 service first) | Infra metrics + consistent log schema |
| **Week 3** | OpenTelemetry + Grafana Tempo; AI-quality tracing extended to GSSP QS + Agent Executor (whichever tool §5.3 picks) | Cross-service trace tree; RAG + agent traces |
| **Week 4** | GLiNER into OIS; Sentry + SDK everywhere | PII redaction (no new service) + error grouping |
| **Week 5** | Custom Dashboard Service (FastAPI backend + React/Tremor frontend) | Platform Overview, Kafka Health, Cost Governance, RAG Quality live |
| **Week 6–8** | Kibana ML; custom Isolation Forest; vector-health CronJob | Anomaly detection + embedding-freshness monitoring |

> **Note:** Weeks 1 and 3 assume the adopt path for the AI-quality layer. If §5.3 lands on a custom build, that one line of work stretches by several weeks — nothing else in the rollout moves.

---

## 7. Anticipated Questions — Q&A Prep

The questions you're most likely to get in the room, with the short honest answer and where the long answer lives.

**"Is Langfuse decided?"**
No — and say so up front. The two-layer architecture is the decision; *who implements Layer 1* (Langfuse vs custom build) is open. The trade-off and the spike that settles it are in §5.3. Everything else in §5 is a confident recommendation.

**"Why not one database instead of four (five with Redis)?"**
Because no single store does all the jobs: sub-second search (Elasticsearch), transactional config (PostgreSQL), cheap forever-analytics (Snowflake), cheap blob storage (S3). Any single choice forces slow queries, expensive retention, or both. §3.4 has the table; §5.2 has the defense.

**"Why are there two Kafkas?"**
Different jobs, deliberate isolation. The business Kafka runs agent execution; the observability Kafka transports telemetry. An observability incident must never block a customer request — and vice versa. See the callout in §2.1.

**"Will this slow down live requests?"**
No, by construction: the emitter is fire-and-forget and instrumentation never sits on the critical path. If the observability pipeline is down, we lose telemetry for that window — we never lose or delay a business transaction.

**"Doesn't redacting PII at the source AND in the pipeline duplicate work?"**
It's defense in depth, not duplication. Source redaction (the SDK's GLiNER pass) protects the raw topic; the pipeline stage is the enforced guarantee before anything reaches storage. The raw topic is also short-retention (7 days) and access-controlled for the window where best-effort is all we have.

**"Why build a dashboard when Grafana exists?"**
The one deliberate *build* call: COIN-JWT auth, per-LOB index-level RBAC, business-KPI pages, and chatbot integration don't fit Grafana's model. We still run kube-prometheus-stack underneath — we just turn its Grafana off. §5.2.

**"What happens to events that fail validation?"**
They branch to `ai-obs-dead-letter` (the red dashed arrow on the diagram), held 14 days for debugging and replay — failed events are quarantined, never silently dropped. §2.2.

**"38 event types — isn't that a lot to maintain?"**
It's one mandatory envelope (`event_id`, `correlation_id`, `service_name`, …) with 38 typed payloads, versioned via `schema_version`. The alternative — every service inventing its own shape — is exactly the today-state we're fixing (§1.1).

**"What does each service team actually have to do?"**
Three small things: adopt the SDK (standard envelope + fire-and-forget emitter), add one decorator to LLM/RAG/agent functions, and expose `/metrics` via the one-line instrumentator. The heavy lifting — validation, PII, cost, routing, storage — happens once, centrally.

---

### Appendix — Source documents in this repo
- `2026-05-26_observability-coverage-master.md` — full per-service gap matrices (source for §1)
- `2026-06-01_observability-plane-architecture_v2-refined.md` — Mermaid architecture, Kafka topics, ES/PG schemas, SLO rules (source for §2–§3)
- `2026-05-28_tool-recommendations-by-signal.md` — per-signal tool rationale (source for §5)
- `Observability Arch Latest.png` — the latest detailed architecture diagram
