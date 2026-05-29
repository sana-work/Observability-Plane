# Langfuse vs Current Logs + Custom Code
## Business Case for Adopting Langfuse

---

## 1. What the Current Logs Actually Capture

Based on a review of all 8 service codebases, this is the honest state of observability today:

### Agent Executor — Best in class, still incomplete
| What exists | Gap |
|---|---|
| `DbLoggerPlugin` writes every agent/LLM/tool execution stage to `audit_table` | `user_id` / SOE_ID stored as **plain text** — PII risk |
| `prompt_tokens`, `completion_tokens`, `total_tokens` from VertexAI metadata | `estimated_cost_usd` never calculated |
| `correlation_id` flows Kafka → context var → log → audit table | `latency_ms` on LLM calls missing — LLM performance completely blind |
| `event_type` on all Kafka events | `event_type = LLM_CALL` currently logged as `OTHER` |
| `ErrorCodes` enum with structured API error responses | `environment` and `service_name` not in log records |
| Optional Kafka streaming of tool/agent events | `timestamp` is local time without timezone — cross-service correlation broken |
| X-Correlation-ID propagated end-to-end | `token_claims` logged on null `consumer_coin` — JWT PII leak |
| | `event_id` — no unique identifier per log entry |
| | Prometheus metrics endpoint removed from current code |
| | No OpenTelemetry distributed tracing |

### GSSP GS (Generation Service) — Partial
| What exists | Gap |
|---|---|
| Request/response observability log per call | `estimated_cost_usd` never calculated |
| `correlation_id` via `asgi-correlation-id` middleware | `latency_ms` not a dedicated numeric field |
| `prompt_tokens`, `completion_tokens`, `total_tokens` parsed from Vertex AI / OpenAI / Anthropic | `consumer_coin` + `app_id` absent from LLM events — cost attribution blind |
| `application_id` injected via `AppIdFilter` | Prometheus middleware wired in but **no active instrumentation** — metrics endpoint does nothing |
| Structured JSON via `logging.yaml` | `event_type = LLM_CALL` not emitted consistently |
| | SOE_ID present in every log record — PII risk |

### GSSP QS (Query Service) — Partial
| What exists | Gap |
|---|---|
| HTTP middleware logs request/response with processing time | RAG pipeline stages not logged as separate events |
| `correlation_id` propagated via X-Correlation-ID header | Token counts and cost not captured at this layer |
| `soeid`, `application_id` in every log via `AppIdFilter` | `soeid` stored in plain text in ContextVar — PII risk |
| RESPONSE / CACHED_RESPONSE log emitted on return | No retrieval quality signals (chunk count, relevance scores) |
| Structured JSON logs | Plain text log messages only at pipeline execution level |

### GSSP RS (Retrieval Service) — Minimal
| What exists | Gap |
|---|---|
| `correlation_id` via middleware | No embedding latency as numeric field |
| COIN JWT validation logs | No vector search result count or relevance score |
| Basic HTTP request/response logs | No retrieval quality signals at all |
| Exception handler structured logs | No chunk-level metadata |

### User Feedback — Minimal
| What exists | Gap |
|---|---|
| Full request body logged at HTTP middleware | **No logging at route handler level at all** |
| `application_id`, `soe_id` via `AppIdFilter` | **No logging in `auth.py`** — auth failures are completely silent |
| Structured JSON via `logging.yaml` | `feedback_id` — no unique identifier per submission |
| DB errors surfaced as HTTP errors | No link from feedback to the original agent `correlation_id` being rated |
| | No feedback workflow status (open / reviewed / fixed) |

### Data Ingestion — Partial
| What exists | Gap |
|---|---|
| HTTP middleware logs path, body, X-Correlation-ID, soeid, application_id | Nothing logged at route handler level |
| `INGESTION_JOB_CREATED`, `INGESTION_QUERY_STATUS` event types | Latency logged in seconds (float string) not milliseconds (int) |
| `job_id`, `doc_src_id`, `document_count`, `sp_status` | No per-document success/fail breakdown |
| Latency logged (WARNING if slow) | No bulk operation tracking |

### Consumer Service — Partial
| What exists | Gap |
|---|---|
| `correlation_id`, `message_status`, `event_type`, `service_name` | No full structured request/response body |
| `body_process_time_ms`, document metadata | No Kafka offset/partition/lag logged |
| Job ID, doc ID per message | No consumer group health events |

### Agentic Orchestration — Partial
| What exists | Gap |
|---|---|
| `correlation_id` in JSON log envelope | No plan-level event breakdown |
| `soeid`, `application_id` | No routing decision events |
| Kafka produce/consume events (topic, partition, offset, key) | No HIL decision tracking |
| Structured JSON via structlog | No cross-service trace joining |

---

## 2. Cross-Cutting Gaps — Every Service

These problems exist across all 8 services regardless of what each service individually logs:

| Gap | Impact |
|---|---|
| **No standard event envelope** | Each service uses different field names (`c_correlation_id` vs `correlation_id` vs `X-Correlation-ID`) — impossible to join across services |
| **No cross-service trace stitching** | `correlation_id` exists in all services but lives in separate logs/databases — no single query can show what happened end-to-end for one request |
| **No estimated cost anywhere** | Tokens are logged in Agent Executor and GSSP GS but no service calculates dollar cost |
| **No feedback-to-trace link** | User Feedback has no `correlation_id` of the response being rated — impossible to close the quality feedback loop |
| **PII in plain text** | `user_id`, `soeid` stored unmasked in Agent Executor `audit_table` and GSSP QS logs |
| **No RAG quality signals** | GSSP QS and GSSP RS log nothing about relevance scores, chunk counts, retrieval quality, or faithfulness |
| **Latency inconsistency** | Seconds (float) in Data Ingestion, milliseconds (int) in some, string in others — cannot aggregate |
| **Timestamps without timezone** | Agent Executor uses local time — cross-service correlation breaks when services run in different timezones |
| **No budget / spend tracking** | Nothing anywhere |
| **No LLM trace tree** | No parent/child span relationship between requests — impossible to see which LLM call belongs to which agent step |

---

## 3. What You Would Need to Build Without Langfuse

To achieve the same capabilities Langfuse provides out of the box, this custom work is required:

### 3.1 LLM Call Trace Tree
**What Langfuse gives you:** Visual hierarchy of request → agent → LLM call → tool call with timing waterfall. Click any span to see its prompt, response, tokens, cost, latency.

**What you would need to build:**
- Add W3C `traceparent` parent/child span IDs to every LLM call, tool call, and agent step event
- Build a tree-reconstruction query that re-assembles flat events into a hierarchy using `parent_span_id`
- Build a custom React UI that renders the hierarchy as an interactive waterfall tree
- Build a drill-down that links each node to S3 for the prompt/response payload

**Estimated effort:** 6–8 weeks for a working version. Ongoing maintenance as new event types are added.

---

### 3.2 Prompt Version Management
**What Langfuse gives you:** Prompt templates stored and versioned in Langfuse. Each LLM call records which version was used. UI shows side-by-side diff between versions. Roll back to any version instantly. A/B test two versions and compare quality scores.

**What you would need to build:**
- Extend PostgreSQL `prompt_template_registry` to store full template text + diffs
- Build a versioning API (create, activate, deprecate, roll back)
- Build a diff viewer UI in Custom Dashboard
- Build a query that joins prompt version to LLM call events to show which version was used when
- Build A/B test assignment logic and outcome tracking

**Estimated effort:** 4–6 weeks. High maintenance surface — prompt engineers will want changes constantly.

---

### 3.3 LLM-as-Judge Quality Evaluators
**What Langfuse gives you:** Configure an evaluator once (e.g. "faithfulness: does the response stay grounded in the retrieved context?"). Langfuse automatically runs a judge LLM on every trace that matches the criteria and stores the score. View score trends over time. Compare across prompt versions.

**What you would need to build:**
- Write a Kafka consumer that picks up `LLM_CALL_COMPLETED` and `RAG_GENERATION_COMPLETED` events
- Write prompts for each evaluator (faithfulness, hallucination, answer relevance, toxicity)
- Call the judge LLM, parse the score, store it back to PostgreSQL
- Build aggregation queries and dashboard pages to trend scores over time
- Handle evaluator failures, retries, rate limits
- Update evaluator prompts as your LLMs and RAG content evolve

**Estimated effort:** 3–4 weeks initial build. High ongoing maintenance — evaluator prompts need tuning as models change.

---

### 3.4 Dataset and Experiment Management
**What Langfuse gives you:** Flag any trace as a "golden example". Collect flagged traces into named datasets (e.g. "payment queries that failed"). Run any new prompt version against the full dataset and compare evaluation scores before deploying.

**What you would need to build:**
- A flagging mechanism in the Custom Dashboard to mark traces as examples
- A dataset table in PostgreSQL to store flagged traces
- A job runner that re-runs flagged traces through the current system
- A comparison UI showing before/after quality scores
- Dataset versioning as you add/remove examples

**Estimated effort:** 6–8 weeks. This is essentially building a mini-MLOps platform.

---

### 3.5 User Feedback Linked to Traces
**What Langfuse gives you:** `langfuse.score(trace_id=..., name="user_rating", value=4)` — one line in User Feedback service. The rating instantly appears on the exact trace that generated the response. Filter all traces rated 1–2 stars. See which prompt version caused low ratings.

**What you would need to build today:**
- User Feedback service currently has **no `correlation_id` of the response being rated** — this link does not exist
- First fix: Add `correlation_id` of the response to every feedback submission
- Then build: Join query across `feedback_case` + `audit_table` (Agent Executor DB) + ES LLM events to reconstruct "what was the full LLM call for this low-rated response?"
- Then build: A dashboard page showing low-rated responses with their prompts and LLM context

**Estimated effort:** 2–3 weeks just to close the feedback-to-trace link. 4–6 more weeks for a useful UI.

---

### 3.6 Cost Per Request Attribution
**What Langfuse gives you:** Every trace automatically shows total token cost. Aggregate by user, application, prompt version, or time range. No code changes required once `@observe` is in place.

**What you have today:**
- Agent Executor logs `prompt_tokens`, `completion_tokens`, `total_tokens` — but `estimated_cost_usd` is never calculated
- GSSP GS logs token counts — cost never calculated
- GSSP QS/RS/Orchestration — no token counts at all

**What you would need to build:**
- Add cost calculation to the Enrichment Consumer (this is in the plan)
- But you still need to aggregate cost **per trace** (one request may have 3 LLM calls) — requires joining all `LLM_CALL_COMPLETED` events for a `correlation_id` and summing
- Build a per-request cost view in Custom Dashboard

**Estimated effort:** 2–3 weeks on top of what the Enrichment Consumer already does.

---

## 4. Side-by-Side Comparison

| Capability | Current State | Without Langfuse | With Langfuse |
|---|---|---|---|
| **LLM call trace tree** | Not possible — logs are flat | 6–8 weeks custom build | Ready on Day 1 with `@observe` |
| **Per-call latency breakdown** | Not captured as numeric field | 4–6 weeks (span IDs + UI) | Automatic waterfall view |
| **Prompt version management** | `prompt_template_id` field only, no versioning | 4–6 weeks | Built-in UI, API, rollback |
| **Faithfulness / hallucination score** | Nothing | 3–4 weeks + ongoing maintenance | Configure once, runs automatically |
| **Feedback linked to trace** | Not linked — User Feedback has no `correlation_id` of the response | 2–3 weeks to fix + 4–6 weeks UI | `langfuse.score()` — 1 line |
| **Dataset + experiment management** | Nothing | 6–8 weeks | Built-in |
| **Cost per request** | Tokens logged but cost not calculated, not joined per trace | 2–3 weeks | Automatic |
| **Token count** | Partial (Agent Executor + GSSP GS only) | Already in your pipeline plan | Automatic for all `@observe` spans |
| **PII before storage** | SOE_ID in plain text in audit table + logs today | GLiNER via your SDK (already planned) | Raw prompts stored in Langfuse — you must pre-scrub before sending |

---

## 5. What Langfuse Does NOT Replace

These are things your custom pipeline does that Langfuse cannot:

| Your Custom Pipeline | Why Langfuse Cannot Replace It |
|---|---|
| Business context enrichment (`lob`, `application_tier`, `owner_team`) | Langfuse has no concept of your LOB/tier registry |
| SLO compliance tracking (`daily_slo_compliance`) | Not in Langfuse |
| Budget governance (`budget_limits`, spend alerts) | Not in Langfuse |
| Infrastructure signals (Kafka consumer lag, guardrail block rate, ingestion failures) | Langfuse only sees what you explicitly send via `@observe` |
| Multi-LOB data isolation and RBAC | Langfuse has projects but not your tier/LOB governance model |
| Anomaly detection (Isolation Forest / LSTM) | Not in Langfuse |
| Feedback workflow (open / reviewed / fixed status) | Langfuse has scores, not a workflow |
| Kafka event replay and durability | Not in Langfuse |

---

## 6. Recommended Decision

### Use both — they solve different problems

```
Langfuse answers:                         Your custom pipeline answers:
────────────────────────────────          ─────────────────────────────────────────
"Why did this request give a bad answer?" "Is the platform healthy right now?"
"Which prompt version performs better?"   "Are we over budget this month?"
"Is my RAG retrieval grounded?"           "Which LOB has the most errors?"
"Show me all 1-star traces from today"    "Is Kafka consumer lag increasing?"
"What changed between v3 and v4 prompt?"  "Which tool is timing out most?"
```

### If you drop Langfuse, plan to build:
- 6–8 weeks: LLM trace tree with waterfall UI
- 4–6 weeks: Prompt version management
- 3–4 weeks: LLM-as-judge evaluators + maintenance
- 6–8 weeks: Dataset and experiment management
- 2–3 weeks: Feedback-to-trace link (fixing current gap)

**Total: ~22–29 weeks of additional custom build** to match what Langfuse gives you on Day 1 with 3 environment variables and `@observe` decorators on 3 services.

### If you keep Langfuse, the integration cost is:
- 1 day: Deploy Langfuse self-hosted (Helm chart / Docker Compose)
- 1 day: Add `@observe` to Agent Executor (step execution loop)
- 1 day: Add `@observe` to GSSP GS (LLM generator functions)
- 1 day: Add `@observe` to GSSP QS (5 RAG pipeline stages)
- 1 hour: Add `langfuse.score()` to User Feedback service

**Total: ~4–5 days** of integration work.

---

## 7. One-Line Recommendation

> **Keep Langfuse for LLM debugging, prompt quality, and RAG evaluation. Build your custom pipeline for operational health, business KPIs, cost governance, and multi-LOB control. The two tools do not overlap — removing either one creates a 22–29 week rebuild.**
