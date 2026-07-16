# Observability Plane — Build Guide

*Companion to `observability-roadmap.html` and `Observability Arch Latest.png`.
Last updated: 2026-07-15.*

This document answers three questions:

1. **What are we building, in plain words?**
2. **What already exists in this repo, and where?**
3. **In what order do we build the rest, and how do we know each step worked?**

---

## 1. The big picture, in plain words

We run 8 AI services (agents, LLM gateways, RAG search, document ingestion,
user feedback). Today, when something goes wrong — a slow answer, a wrong
answer, a surprise cloud bill — nobody can see *why*, because each service
keeps its own logs in its own format, and nothing connects them.

The Observability Plane fixes that. Think of it as a **flight recorder for
every request**:

```
 A user asks a question
        │
        ▼
 ① The 8 services each write small "event" records describing what they did
    (request received, LLM called, 1 200 tokens used, tool timed out, ...)
    They all use ONE shared Python library — the SDK — so every event has
    the same shape and the same request ID (the "correlation_id").
        │
        ▼
 ② All events land on one Kafka topic: ai-obs-events-raw.
    Kafka is a durable queue — services drop events on it and move on;
    nothing downstream can ever slow a user request down.
        │
        ▼
 ③ One central service (the Enrichment Consumer) reads every event and
    cleans it up in 9 steps: validates the shape, redacts PII from free
    text, attaches owner/team info from registries, computes the dollar
    cost, checks budgets and SLOs. Broken events go to a "dead-letter"
    topic instead of being lost.
        │
        ▼
 ④ A second central service (the Storage Consumer) copies each clean event
    into three stores, each chosen for one job:
      • Elasticsearch — fast text search ("show me all A0001 errors today")
      • PostgreSQL obs_events.* — SQL analytics ("cost per model per week")
      • S3 — cheap archive for big things (full prompts, traces, documents)
        │
        ▼
 ⑤ People look at the data through Kibana (ops search), a custom dashboard
    (business views, cost, SLOs), and later a chatbot ("why did request X
    fail?"). Everything joins on the correlation_id.
```

Because every event from every service carries the same `correlation_id`,
you can pick any user request and replay its entire story: which agent ran,
which tools it called, which model it used, what it cost, what the user
thought of the answer.

### Decisions already made (do not re-litigate in code)

| Decision | What it means for you |
|---|---|
| **No Snowflake yet** | The SQL analytics store is a partitioned PostgreSQL schema called `obs_events.*`, holding ~90 days. Snowflake may replace it later; the swap is contained because both use the same `event_id`/`correlation_id` model. |
| **No Redis yet** | Anything that "needs Redis" uses a stand-in: budget counters live in a Postgres table (`budget_accumulator`) with an atomic function; caches are in-process TTL caches inside each pod. |
| **No Langfuse** | The AI-quality layer (prompt versions, LLM-as-judge scores, trace explorer) is a custom build on our own pipeline (Phase Q, later). |
| **Grafana = internal ops console only** (updated 2026-07-15) | Grafana is deployed with kube-prometheus-stack but **cluster-internal only**: ClusterIP, no ingress, no anonymous access, reached by the platform team via `kubectl port-forward`. It is never a stakeholder surface — LOB users use the Custom Dashboard, which queries Prometheus/ES/Postgres directly (COIN-JWT + per-LOB RBAC that Grafana can't model). Phase 5 will evaluate reusing Grafana's OSS React packages (`@grafana/ui`, `@grafana/scenes`) *inside* the custom dashboard — components only, no Grafana server involved. |
| **`user_id` is raw** | Events carry the user's SOE ID **unhashed** in the `user_id` field. Audit trails and "by SOEID" dashboards need it. Protection comes from access control on the stores, not from hashing. Prompts and free text are still hashed/redacted. |

### The two-repo split (two folders, one git repo)

- **`observability-iac/`** — "infrastructure as code". Scripts and SQL that
  *create things in shared infrastructure*: Kafka topics, database schemas,
  Elasticsearch templates, S3 buckets. Applied by CI, once per environment.
- **`ai-observability-sdk/`** — a normal Python package, published to the
  internal index. The 8 service teams `pip install` it. It contains the
  emit/trace/log code that runs inside every service.

They meet at the **contract**: the `ObsEvent` schema + the 50 event types +
the 8 service names. The contract lives in `observability-iac/contracts/`
and is *copied* (vendored) into the SDK. CI tests fail if the two copies
differ — that's how compatibility stays guaranteed.

---

## 2. What already exists (status as of 2026-07-15)

| Phase | Deliverable | Where | Status |
|---|---|---|---|
| 0 | Foundation IaC (63 files) | `observability-iac/` | ✅ Built. 14 tests green. SQL parse-validated; needs one run against a real Postgres (CI does this). |
| 1 | Shared SDK | `ai-observability-sdk/` | ✅ Built. 17 tests green. Not yet published to the internal index. |
| 2 | Instrument the 8 services | each service repo | ⬜ Not started |
| 3 | Enrichment Consumer | new repo `obs-enrichment-consumer` | ⬜ Not started |
| 4 | Storage Consumer | new repo `obs-storage-consumer` | ⬜ Not started |
| Q | AI-quality layer (prompt registry API, eval service, trace explorer) | new repos | ⬜ Not started |
| 5 | Custom dashboard + Kibana | new repo | ⬜ Not started |
| 6 | Chatbot | new repo | ⬜ Not started |

A detailed two-developer task breakdown for Phases 0–1 (useful for rebuilding
from scratch, onboarding, or estimating) is in
[TASKS-2dev-phase0-phase1.md](TASKS-2dev-phase0-phase1.md).

---

## 3. Step 0 — Stand up the foundation (Phase 0)

Everything here is **idempotent**: running a script twice is safe; the second
run changes nothing. Apply in this order, because later steps assume earlier
ones exist.

### 0.1 Local playground first (15 minutes)

Before touching any shared environment, bring the whole stack up on a laptop:

```bash
cd observability-iac
docker compose -f docker-compose.dev.yml up -d
# starts: Kafka (1 broker), Postgres 16, Elasticsearch 8, Kibana, Tempo
```

Then apply everything to it:

```bash
# Kafka topics (KAFKA_ENV=dev lowers replication to 1 for a single broker)
KAFKA_ENV=dev BOOTSTRAP=localhost:9092 ./kafka/create_topics.sh

# Control-plane database (registries, budgets, catalogs) + starter data
PGURL=postgres://postgres:obs@localhost:5432/postgres ./postgres/apply.sh --with-seed

# Event analytics database (the Snowflake stand-in)
PGURL=postgres://postgres:obs@localhost:5432/postgres ./postgres-events/apply.sh

# Elasticsearch lifecycle policies + index templates
ES_URL=http://localhost:9200 ./elasticsearch/apply.sh

# Prove nothing drifted
pytest tests/ contracts/tests -v
```

**You know it worked when:** the topic list shows 3 `ai-obs-*` topics, `psql`
shows the `observability` and `obs_events` schemas with tables, the ES verify
line lists 11 `ai-obs-*` index templates, and all tests pass.

### 0.2 What each piece is, and how to apply it for real

**(a) The contract — `contracts/`.** Three small Python files that define
what an event *is*: the `ObsEvent` envelope (every field every event must
carry), the 50 allowed `event_type` values, the 8 allowed `service_name`
values. Everything else in the platform is derived from these files. Any
change bumps `schema_version` and must update the SDK's vendored copy in the
same PR — CI enforces it.

**(b) Kafka topics — `kafka/`.** Three queues:

| Topic | Size | Kept for | Carries |
|---|---|---|---|
| `ai-obs-events-raw` | 12 partitions | 7 days | everything the services emit, unprocessed |
| `ai-obs-events-processed` | 12 partitions | 3 days | validated/redacted/costed events, ready to store |
| `ai-obs-dead-letter` | 3 partitions | 14 days | events that failed validation — kept for debugging and replay |

Messages are keyed by `correlation_id`, which guarantees all events of one
request arrive in order on one partition. Apply:
`BOOTSTRAP=<broker> ./kafka/create_topics.sh` (add `COMMAND_CONFIG=` for SASL).

**(c) Control-plane Postgres — `postgres/`.** The `observability.*` schema:
small, authoritative configuration tables. Who owns which app
(`application_registry`), which agents/tools/knowledge-bases exist
(registries), versioned prompt templates, the error-code taxonomy, metric
definitions for the chatbot, model prices, budget limits, SLO targets, and
7 pre-aggregated rollup tables for fast dashboard charts. Also
`budget_accumulator` + the `add_spend()` function — the Redis stand-in that
counts spend atomically and reports budget crossings exactly once.
Apply: `PGURL=<url> ./postgres/apply.sh --with-seed`. The seed loads the 8
services, a dev app `app-1234`, ~20 error codes, ~10 metrics, and prices.

**(d) Event firehose Postgres — `postgres-events/`.** The `obs_events.*`
schema: one big partitioned `events` table mirroring the envelope
field-for-field, plus per-domain tables (`llm_events`, `agent_events`,
`rag_events`, `feedback_events`, `quality_scores`, `slo_history`) where hot
fields are real columns so SQL is cheap. Partitions are monthly; two built-in
functions manage them (`ensure_month_partitions()` nightly to create,
`drop_old_partitions()` to remove after the S3 archiver has exported the
month). Apply: `PGURL=<url> ./postgres-events/apply.sh`.

**(e) Elasticsearch — `elasticsearch/`.** Search-optimised copies of the
events. Two lifecycle (ILM) policies decide how long data lives (30 days
default, 180 days for guardrail + quality data). Two "component templates"
hold the shared settings and the envelope field mappings. Eleven "index
templates" — one per event family (requests, errors, agent-steps, llm-calls,
tool-calls, rag-events, guardrail-events, feedback, traces, quality-scores,
anomalies) — add each family's own fields. Index names embed the line of
business (`ai-obs-{lob}-requests-...`), which is how per-LOB access control
works. Apply: `ES_URL=<url> ./elasticsearch/apply.sh`.

**(f) S3 — `s3/`.** One archive bucket, encrypted (SSE-KMS), versioned,
public access blocked, with automatic tiering (Standard → cheaper IA at 30
days → Glacier at 180). Nine prefixes for prompts, responses, traces, RAG
contexts, documents, audit evidence, debug bundles, RCA reports, dashboard
IaC. Apply: `BUCKET=<name> KMS_KEY_ID=<key> ./s3/apply.sh`.

**(g) Cluster monitoring — `infra/`.** Four files applied to Kubernetes:
- `kube-prometheus-stack-values.yaml` — metrics + alert rules
  (consumer lag > 1000 for 10 min → page; dead-letter rate > 1% → warn;
  enrichment consumer missing → page). **Grafana is included as an
  internal-only ops console** (ClusterIP, no ingress, port-forward access,
  Prometheus + Tempo datasources pre-wired) — for platform engineers during
  bring-up and incidents. Stakeholders never touch it; their surface is the
  custom dashboard.
- `tempo-values.yaml` — Grafana Tempo, the trace backend the SDK sends
  OpenTelemetry spans to (port 4317), stored on S3, 30-day retention.
- `kminion.yaml` — exports Kafka consumer-group lag as Prometheus metrics.
- `fluent-bit-configmap.yaml` — ships every pod's JSON logs to Elasticsearch
  and strips secrets (`authorization`, `api_key`, `password`) on the way.
  `user_id` is *not* stripped — raw by design.

**(h) CI — `ci/deploy.yml`.** On every PR: contract tests → policy gate
(the Phase 0 ↔ Phase 1 compatibility tests) → shell syntax → applies all SQL
to a throwaway Postgres container. On merge to main: applies everything to
dev, then (with approvals) staging/prod.

**Phase 0 is done when:** all applies succeed in dev, the policy tests are a
required CI check, and the exit checklist in `observability-iac/README.md`
is fully ticked.

---

## 4. Step 1 — The SDK (Phase 1) — built, needs publishing

The SDK is the *only* observability code service teams ever touch. One line:

```python
from ai_obs_sdk import init_observability
init_observability(app)   # logging + tracing + middleware + /metrics
```

gives a FastAPI service: JSON logs that carry the correlation_id, OTEL spans
to Tempo, `REQUEST_RECEIVED/COMPLETED/FAILED` events to Kafka, and a
Prometheus `/metrics` endpoint. Then teams decorate their hot paths:

| You have | You add | You get on Kafka |
|---|---|---|
| a function that calls an LLM | `@trace_llm(model_name=...)` | LLM_CALL_STARTED/COMPLETED with tokens, latency, estimated cost |
| a retrieval function | `@trace_rag(vector_db_index=...)` | RAG_RETRIEVAL_* with chunk counts, relevance, no-result flag |
| a tool/API call | `@trace_tool(tool_id=...)` | TOOL_CALL_* (TimeoutError becomes TOOL_CALL_TIMEOUT) |
| an agent loop | `@trace_agent(agent_id=...)` | AGENT_STARTED/COMPLETED/FAILED/TIMEOUT |
| anything else | `emit_event(EventType.X, payload={...})` | that event, envelope auto-filled |

Design guarantees (all covered by tests):

1. **Never blocks, never raises.** The Kafka producer is fire-and-forget.
   Broker down? Queue full? The event is dropped with a log warning; the
   user request is never affected.
2. **`user_id` is raw.** The middleware copies `X-User-ID`/`X-SOE-ID`
   verbatim into the event. No hashing (platform decision).
3. **Ordering.** Every message is keyed by correlation_id.
4. **The contract is vendored.** CI diffs the SDK's copy against
   `observability-iac/contracts/` and fails on drift.

Configuration is pure environment variables (prefix `AI_OBS_`, full list in
`ai-observability-sdk/.env.example`). Only three are required per service:
`AI_OBS_SERVICE_NAME`, `AI_OBS_LOB`, `AI_OBS_APPLICATION_ID`.

**Remaining Phase 1 work:**

- [ ] Publish `0.1.0` to the internal PyPI index; services pin the minor version.
- [ ] Review the pricing table in `cost.py` against real billing (it must match the `model_pricing` seed — a test enforces the sync).
- [ ] Confirm store-level RBAC design for raw `user_id`.
- [ ] Point `AI_OBS_PROMPT_REGISTRY_URL` at the real prompt API once Phase Q builds it (services keep local fallbacks until then).
- [ ] Nightly CI integration test against a real broker (docker compose).

**Phase 1 is done when:** a fresh `pip install ai-observability-sdk` + the
README quick-start emits visible events on the dev stack's raw topic.

---

## 5. Step 2 — Instrument the 8 services (Phase 2)

Go service by service, highest signal first. For each: install the SDK, add
the env block to Helm values, add `init_observability(app)`, decorate the
LLM/RAG/tool/agent call sites, delete the old ad-hoc logging, verify.

Order and what each service adds:

1. **Agentic Orchestration** (already emits to Kafka today — swap its bespoke code for the SDK): plan/handoff/HIL events.
2. **Agent Executor**: `@trace_agent` + `@trace_tool`, step events, real numeric latencies, `finish_reason`.
3. **GSSP GS** (LLM gateway — richest signal): `@trace_llm` fed from its existing `LLMUsageMetrics`, document events.
4. **GSSP QS**: `@trace_rag` + `@trace_llm`, guardrail events, semantic-cache hit/miss.
5. **GSSP RS**: embedding telemetry (today it discards token usage — stop that), per-stage latency.
6. **Consumer Service** (no HTTP — a scheduler): bind context from Kafka message headers instead of middleware (pattern in the SDK README), document-pipeline events.
7. **Data Ingestion**: success-path events (today only errors are logged), UTC timestamps, embedding cost.
8. **User Feedback**: `FEEDBACK_SUBMITTED` carrying the correlation_id of the answer being rated — this is what links user opinions to traces.

**Per-service verification:** make one real request, then read
`ai-obs-events-raw` with a console consumer and confirm the full event chain
appears under a single correlation_id, with `user_id` populated and no raw
request bodies or credentials in any payload.

---

## 6. Steps 3–7 — the consumers and the faces (Phases 3–7)

Build these in order; each unblocks the next:

- **Phase 3 — Enrichment Consumer.** New service, consumer group
  `obs-enrichment`, 3 replicas. Runs the 9 stages (validate → trace context →
  GLiNER PII redaction of free text → registry enrichment → error-code
  mapping → cost via `model_pricing` + `add_spend()` → S3 offload of big
  payloads → SLO burn rates → quality sampling hook). Commits offsets only
  *after* producing to the processed topic. Bad events → dead-letter with a
  reason; `scripts/replay_dead_letter.py` re-drives them after a fix.
- **Phase 4 — Storage Consumer.** Fan-out from the processed topic:
  Elasticsearch bulk writes with `_id = event_id` (idempotent — replays
  can't duplicate), `obs_events.*` batch COPY every 5 s / 1000 rows,
  aggregate rollup upserts, S3 pointers. kminion must now show lag for all
  consumer groups.
- **Phase Q — AI-quality layer.** Prompt registry CRUD API (what
  `get_prompt()` calls), the LLM-as-judge eval service filling
  `quality_scores`, and the Trace Explorer API + waterfall UI.
- **Phase 5 — Presentation.** Custom dashboard (FastAPI + React, COIN-JWT,
  per-LOB RBAC; ES for recent data, `obs_events` for trends, PromQL for
  infra) + the 8 Kibana dashboards.
- **Phase 6 — Chatbot.** Natural-language questions over the metric catalog
  and the stores, with per-LOB access control.
- **Phase 7 (optional) — anomaly detection + nightly RCA reports.**

---

## 7. The critical path, one line each

1. Apply Phase 0 IaC from `observability-iac/` — **done, all gates green**.
2. Publish `ai-observability-sdk` 0.1.0 — code done, publish pending.
3. Instrument Agentic Orchestration + Agent Executor; verify the event chain end-to-end.
4. Build the Enrichment Consumer — nothing downstream matters until raw→processed works.
5. Build the Storage Consumer — Kibana then gives dashboards nearly for free.
6. Only then invest in the custom dashboard, AI-quality layer, and chatbot.

---

## 8. Glossary

| Term | Meaning |
|---|---|
| **Event** | One small JSON record describing one thing that happened (an LLM call, a tool failure). Shape defined by `ObsEvent`. |
| **Envelope** | The fields every event must carry (ids, timestamps, service, status, latency...). Domain-specific extras go in `payload`. |
| **correlation_id** | One ID per user request, passed through every service, event, span, and log line. The join key for everything. |
| **LOB** | Line of business. Baked into index names and RBAC. |
| **SOE ID / `user_id`** | The user's corporate identity. Carried raw (unhashed) by platform decision. |
| **Contract** | The frozen definition of the envelope + 50 event types + 8 service names. Lives in `observability-iac/contracts/`, copied into the SDK. |
| **Vendored** | Copied into a package rather than imported from elsewhere, so the package is self-contained. Drift is caught by CI. |
| **Dead-letter (DLQ)** | Where events that fail validation go, so they're debuggable and replayable instead of lost. |
| **ILM** | Elasticsearch Index Lifecycle Management — automatic ageing/deletion of indices. |
| **Idempotent** | Safe to run twice; the second run changes nothing. All apply scripts here are. |
| **Fire-and-forget** | The producer doesn't wait for acknowledgment on the request path. Observability can lag; it must never break the product. |
| **OTEL / Tempo** | OpenTelemetry (the tracing standard) and Grafana Tempo (where the spans are stored). This is the *infrastructure* trace view; the AI-quality trace view is built from events. Same correlation_id in both. |
| **kminion** | A small exporter that turns Kafka consumer lag into Prometheus metrics. |
| **Enrichment / Storage Consumer** | The two central pipeline services: one cleans events (Phase 3), one writes them to the stores (Phase 4). |
