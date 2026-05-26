# Observability Coverage Master — AI Services Platform

> **Purpose:** Single source of truth for what observability data each service currently captures, what is missing, and the gap priority for the Observability Plane.
>
> Legend: ✅ Present | ⚠️ Partial | ❌ Missing | 🔴 PII Risk

---

## Service Inventory

| # | Service | Role | Language / Stack | Emits to Kafka? |
|---|---|---|---|---|
| 1 | **Agent Executor** | Stateful, event-driven multi-step agent execution engine | Python / FastAPI, VertexAI Gemini, PostgreSQL (PGVector) | ✅ Yes (optional streaming) |
| 2 | **Agentic Orchestration** | Multi-agent orchestration; routing, planning, HIL gate | Python / FastAPI, COIN JWT, VertexAI/Stellar | ✅ Yes (primary channel) |
| 3 | **Consumer Service** | Document ingestion scheduler; RAG pipeline (S3 → pgvector) | Python / FastAPI, APScheduler, PostgreSQL, pgvector | ❌ No |
| 4 | **Data Ingestion Service** | REST-driven document ingest, embedding, vector storage | Python / FastAPI, PostgreSQL, pgvector, Sonic S3 | ❌ No |
| 5 | **GSSP GS** | Generic Generation Service; LLM gateway to VertexAI/Claude/Llama via R2D2 proxy | Python / FastAPI, COIN JWT, multi-model | ❌ No |
| 6 | **GSSP QS** | Query/Search Service; RAG retrieval and ranking layer | Python / FastAPI | ❌ No |
| 7 | **GSSP RS** | Response Service; response assembly, scoring, confidence | Python / FastAPI | ❌ No |
| 8 | **User Feedback** | Feedback capture UI / API; links ratings to trace/agent | Python / FastAPI | ❌ No |

---

## Standard Event Field Coverage Matrix

> These are the **mandatory fields** every service must emit in every observability event.

| Field | Agent Executor | Agentic Orchestration | Consumer Service | Data Ingestion | GSSP GS | GSSP QS | GSSP RS | User Feedback |
|---|---|---|---|---|---|---|---|---|
| `event_id` | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `event_type` | ✅ (audit_table) | ⚠️ Kafka only | ⚠️ error handler only | ⚠️ error handler only | ❌ | ❌ | ❌ | ❌ |
| `schema_version` | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `timestamp` | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `correlation_id` | ✅ | ✅ | ✅ | ✅ | ✅ | ⚠️ | ⚠️ | ⚠️ |
| `span_id` | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `parent_span_id` | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `request_id` | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `application_id` | ✅ | ✅ | ✅ | ✅ | ✅ | ⚠️ | ⚠️ | ⚠️ |
| `environment` | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `service_name` | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `component` | ⚠️ (module path) | ⚠️ (module path) | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `lob` | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `tenant_id` | ❌ | ❌ | ⚠️ (soeid) | ⚠️ (soeid) | ❌ | ❌ | ❌ | ❌ |
| `user_hash` | 🔴 plain text | 🔴 plain text | 🔴 plain text | 🔴 plain text | 🔴 plain text | 🔴 | 🔴 | ❌ |
| `status` | ✅ | ⚠️ | ⚠️ | ⚠️ | ⚠️ | ❌ | ❌ | ❌ |
| `latency_ms` | ⚠️ (derivable) | ⚠️ (HTTP only) | ⚠️ (string only) | ⚠️ (string only) | ⚠️ (string only) | ❌ | ❌ | ❌ |
| `error_code` | ✅ | ✅ | ✅ | ✅ | ✅ | ⚠️ | ⚠️ | ❌ |
| `http_status` | ✅ | ✅ | ✅ | ✅ | ✅ | ⚠️ | ⚠️ | ❌ |

---

## Domain-Specific Field Coverage

### LLM Telemetry Fields

| Field | Agent Executor | Agentic Orchestration | GSSP GS | GSSP QS | GSSP RS |
|---|---|---|---|---|---|
| `model_name` | ❌ | ❌ | ⚠️ (config) | ❌ | ❌ |
| `model_provider` | ❌ | ❌ | ⚠️ (config) | ❌ | ❌ |
| `prompt_template_id` | ❌ | ❌ | ✅ (DB-bound) | ❌ | ❌ |
| `input_tokens` | ✅ (audit_table) | ❌ | ✅ (LLMUsageMetrics) | ❌ | ❌ |
| `output_tokens` | ✅ (audit_table) | ❌ | ✅ (LLMUsageMetrics) | ❌ | ❌ |
| `total_tokens` | ✅ (audit_table) | ❌ | ✅ (LLMUsageMetrics) | ❌ | ❌ |
| `estimated_cost` | ❌ | ❌ | ❌ | ❌ | ❌ |
| `llm_latency_ms` | ❌ | ❌ | ⚠️ (string) | ❌ | ❌ |
| `finish_reason` | ❌ | ❌ | ❌ | ❌ | ❌ |
| `rate_limit_hit` | ❌ | ❌ | ❌ | ❌ | ❌ |
| `safety_blocked` | ❌ | ❌ | ❌ | ❌ | ❌ |
| `confidence_score` | ❌ | ❌ | ✅ (logprobs/Claude) | ❌ | ❌ |

### Agent Telemetry Fields

| Field | Agent Executor | Agentic Orchestration |
|---|---|---|
| `agent_id` | ⚠️ (name in payload, not log) | ⚠️ (name in plans) |
| `agent_version` | ❌ | ❌ |
| `agent_type` | ❌ | ❌ |
| `step_count` | ⚠️ (sentence in log) | ⚠️ (sentence in log) |
| `loop_count` | ❌ | ❌ |
| `handoff_count` | ❌ | ❌ |
| `step_name` | ❌ | ❌ |
| `step_number` | ❌ | ❌ |
| `termination_reason` | ❌ | ❌ |
| `agent_latency_ms` | ⚠️ (derivable from audit_table timestamps) | ❌ |
| `AGENT_STARTED event` | ✅ (audit_table: INVOCATION) | ❌ |
| `AGENT_STEP_COMPLETED event` | ✅ (audit_table: AGENT row per step) | ❌ |
| `AGENT_FAILED event` | ✅ (audit_table: ERROR) | ⚠️ (EXECUTION_REJECTED Kafka msg) |
| `HIL_REQUEST event` | ❌ | ✅ (Kafka: HIL_REQUEST) |
| `HIL_RESPONSE event` | ❌ | ✅ (Kafka: HIL_RESPONSE) |

### Tool Telemetry Fields

| Field | Agent Executor | Agentic Orchestration |
|---|---|---|
| `tool_id` | ⚠️ (name in payload) | ❌ |
| `tool_name` | ⚠️ (payload) | ❌ |
| `tool_type` | ❌ | ❌ |
| `tool_latency_ms` | ❌ | ❌ |
| `tool_status` | ✅ (audit_table: TOOL row) | ❌ |
| `retry_count` | ❌ | ❌ |
| `timeout_flag` | ❌ | ❌ |
| `http_status (tool)` | ❌ | ❌ |
| `TOOL_CALL_STARTED` | ✅ (audit_table) | ❌ |
| `TOOL_CALL_COMPLETED` | ✅ (audit_table) | ❌ |
| `TOOL_CALL_FAILED` | ✅ (audit_table: ERROR) | ❌ |

### RAG / Ingestion Telemetry Fields

| Field | Consumer Service | Data Ingestion | GSSP QS |
|---|---|---|---|
| `rag_id` | ❌ | ❌ | ❌ |
| `knowledge_base` | ❌ | ❌ | ❌ |
| `embedding_model` | ❌ | ❌ | ❌ |
| `embedding_latency_ms` | ❌ | ❌ | ❌ |
| `retrieved_chunk_count` | ❌ | ❌ | ❌ |
| `avg_relevance_score` | ❌ | ❌ | ❌ |
| `no_result_flag` | ❌ | ❌ | ❌ |
| `citation_coverage_pct` | ❌ | ❌ | ❌ |
| `context_truncation_flag` | ❌ | ❌ | ❌ |
| `job_id` | ✅ | ✅ | N/A |
| `document_id` | ✅ | ✅ | N/A |
| `job_status` | ✅ (SUCCESS/FAILURE/ERROR) | ✅ | N/A |
| `job_processing_time` | ⚠️ (string) | ⚠️ (string) | N/A |
| `input_tokens (embed)` | ❌ | ❌ | ❌ |
| `RAG_RETRIEVAL_STARTED` | ❌ | ❌ | ❌ |
| `RAG_RETRIEVAL_COMPLETED` | ❌ | ❌ | ❌ |
| `RAG_NO_RESULT` | ❌ | ❌ | ❌ |
| `DOCUMENT_INDEXED` | ⚠️ (job completion log) | ⚠️ (job completion log) | ❌ |

### Feedback Telemetry Fields

| Field | User Feedback |
|---|---|
| `feedback_id` | ⚠️ |
| `correlation_id` (linked to trace) | ⚠️ |
| `rating` | ✅ |
| `thumbs` | ✅ |
| `sentiment` | ⚠️ |
| `feedback_category` | ⚠️ |
| `free_text_comment_redacted` | ❌ (unredacted) |
| `submitted_by_role` | ❌ |
| `resolution_status` | ❌ |
| `linked_incident_id` | ❌ |
| `FEEDBACK_SUBMITTED event` | ❌ |

### Kafka Telemetry Fields

| Field | Agent Executor | Agentic Orchestration |
|---|---|---|
| `kafka_topic` | ✅ | ✅ |
| `kafka_partition` | ❌ | ❌ |
| `kafka_offset` | ❌ | ❌ |
| `consumer_group` | ❌ | ❌ |
| `producer_latency_ms` | ❌ | ❌ |
| `consumer_latency_ms` | ❌ | ❌ |
| `kafka_lag` | ❌ | ❌ |
| `message_size_bytes` | ❌ | ❌ |
| `retry_count` | ❌ | ❌ |
| `dlq_flag` | ❌ | ❌ |

---

## Per-Service Summary

---

### 1. Agent Executor

**What It Does:** Stateful event-driven engine for multi-step AI agent workflows. Consumes from Kafka, orchestrates agent steps via VertexAI Gemini, publishes results back to Kafka. Supports HIL workflows, session management, configurable agent/tool registry.

**Observability Strengths:**
- `DlLoggerPlugin` captures a full audit trail in PostgreSQL `audit_table` for every INVOCATION, AGENT, LLM_REQUEST, LLM_RESPONSE, TOOL, and ERROR event
- `ObservabilityLogger` emits JSON logs with `correlation_id`, `application_id`, `soe_id` auto-injected
- Token counts (`prompt_tokens`, `completion_tokens`, `total_tokens`) captured from VertexAI metadata
- `X-Correlation-ID` propagated end-to-end: Kafka header → context var → log envelope → audit table
- Step status in `agent_execution_table` (IN_PROGRESS → COMPLETED/FAILED)
- Optional Kafka streaming of tool/agent lifecycle events
- Audit query API: `/audit/sub-executions/{corr_id}/{agent}`
- Event type routing via explicit `event_type` field on Kafka messages

**Gaps — High Priority:**
| Gap | Impact |
|---|---|
| `event_id` absent — no unique UUID per event | Cannot deduplicate events; chatbot drill-down fails |
| `latency_ms` not a numeric field | Cannot query latency dashboards; only derivable from `created_at`/`completed_at` diff |
| `estimated_cost` not calculated | No cost tracking or budget governance |
| `environment` not in logs or audit | Cannot separate prod/dev/stage events |
| `service_name` not a structured field | Cannot filter by service in Elasticsearch |
| `user_id`/SOE_ID stored plain text | PII compliance violation |
| No OpenTelemetry spans | Cannot trace across microservice boundaries |
| No `/metrics` endpoint | No real-time Prometheus/Grafana integration |
| `agent_id`, `tool_id` not in log statements | Cannot filter logs by agent/tool |
| One `print()` call bypasses logging | Silent observability gap |

**Gaps — Medium Priority:**
| Gap | Impact |
|---|---|
| REST API endpoints not fully logged (`/reload-configs`) | Admin actions invisible |
| `finish_reason`, `rate_limit_hit`, `safety_blocked` absent | LLM quality monitoring impossible |
| `step_count`, `loop_count`, `handoff_count` not structured | Agent behaviour analytics unavailable |

---

### 2. Agentic Orchestration Service

**What It Does:** Enterprise orchestrator between client apps and agent microservices. Handles JWT auth, multi-agent planning (static/dynamic), Kafka-based async dispatch, HIL approval gates, response assembly via webhook/Kafka.

**Observability Strengths:**
- `JSONFormatter` provides structured JSON logs
- `X-Correlation-ID` accepted from client and propagated
- `X-Application-ID` context var injected in logs
- HTTP middleware captures `processing_time_ms` per REST call
- Centralised exception handler captures `error_code`
- Kafka messages carry explicit `event_type` (AGENT_EXECUTION_REQUEST, AGENT_EXECUTION_FINAL_RESPONSE, EXECUTION_REJECTED, HIL_REQUEST, HIL_RESPONSE, EXECUTION_FINAL_RESPONSE, CLIENT_ALERT)
- Planner captures plan steps count and correlation

**Gaps — High Priority:**
| Gap | Impact |
|---|---|
| `event_id` absent | No unique event tracking |
| LLM telemetry absent (tokens, model, latency) | Critical cost gap; VertexAI/Stellar calls untracked |
| Agent-level structured events absent (AGENT_STARTED, AGENT_STEP_COMPLETED) | Agent observability dashboard impossible |
| `environment` not in log output | Cannot separate environments |
| `service_name` not a structured field | Multi-service filtering broken |
| SOE_ID logged plain text | PII compliance risk |
| No distributed tracing / OpenTelemetry | Cannot correlate with Agent Executor spans |
| No `/metrics` endpoint | No real-time monitoring |

**Gaps — Medium Priority:**
| Gap | Impact |
|---|---|
| DB and Kafka publish latency not captured | Throughput bottleneck invisible |
| Step count logged as sentence not numeric | Agent analytics unavailable |
| Request body sanitisation missing | Sensitive task content may be logged |

---

### 3. Consumer Service (Ingestion Scheduler)

**What It Does:** PostgreSQL-backed document ingestion scheduler. Polls `staging_ingestion_jobs`, dispatches `IngestionJobExecutor` per job, downloads from S3, splits/embeds, persists to pgvector. Thread-pool managed via APScheduler.

**Observability Strengths:**
- Structured JSON logging via `logconfig.yaml` + `JSONFormatter`
- `correlation_id`, `application_id`, `soeid`, `job_id`, `document_id` in all log records
- Error logging with `error_code` via exception handler
- HTTP request/response logging (with latency as string)
- Function timing decorator (`@time_logger`)
- Job status lifecycle: NOT_STARTED → PENDING → SUCCESS/FAILURE/ERROR

**Gaps — High Priority:**
| Gap | Impact |
|---|---|
| `event_id` absent | No unique event per ingestion step |
| `event_type` only on errors | Ingestion pipeline steps invisible |
| `environment` absent | Cannot separate environments |
| `service_name` absent | Cannot filter by service |
| `latency_ms` as string only | Not queryable; not numeric |
| SOE_ID plain text | PII risk |
| Embedding token usage not captured | Cannot track embedding cost |
| Model name not in logs | Cannot track which embedding model used |
| No Prometheus/OpenTelemetry | No real-time metrics |
| No Kafka emission | Events cannot be consumed by Observability Ingestion API |

**Gaps — Medium Priority:**
| Gap | Impact |
|---|---|
| Scheduler lifecycle logging partial | Cannot detect scheduler failures |
| Full HTTP bodies logged | PII risk |
| No cost tracking for embedding calls | Budget governance impossible |

---

### 4. Data Ingestion Service

**What It Does:** REST-driven document ingestion. Accepts bulk-change payloads (UPSERT/DELETE), splits into per-document jobs, embeds via `SalesAccelerator`/`CoCDocument` tenants, persists to pgvector. COIN JWT auth for inbound; M2M tokens for outbound R2D2 LLM calls.

**Observability Strengths:**
- Structured JSON logging via `logconfig.yaml`
- `correlation_id` propagated via middleware + `asgi-correlation-id`
- `application_id`, `soeid` via `AppInfoFilter` context var
- `job_id`, `document_id` in all log records
- Error logging with `error_code`
- HTTP request/response latency (string form)
- Ingestion STATUS events (SUCCESS/FAILURE/ERROR)
- Network error handling wraps Vertex AI, R2D2, COIN errors to typed exceptions

**Gaps — High Priority:**
| Gap | Impact |
|---|---|
| `event_id` absent | No unique event identifier |
| `event_type` only partial (error handler) | Success paths emit no event type |
| `environment` not in any log record | Cannot separate environments |
| `service_name` not a structured field | Multi-service filtering broken |
| `latency_ms` as string only | Not queryable |
| SOE_ID plain text | PII compliance risk |
| Embedding token usage absent | Cost/budget tracking impossible |
| Model name absent from logs | Cannot track embedding model usage |
| No Prometheus/OpenTelemetry | No real-time metrics |
| No Kafka emission | Events cannot be streamed to Observability Ingestion API |

---

### 5. GSSP GS (Generic Generation Service)

**What It Does:** Secure multi-tenant LLM gateway. Routes generation requests from CIS consumer applications (CitiConnect, CitiDirect, DDO, Pega, Stellar) to VertexAI Gemini, Anthropic Claude, or Llama via CIS R2D2 proxy. Manages prompt templates, generator configs, and token counts.

**Observability Strengths:**
- Structured JSON logs via `logconfig.yaml` + `AppInfoFilter` (`soeid`, `application_id`, `correlation_id`)
- `CorrelationIDMiddleware` (asgi-correlation-id) propagates `X-Correlation-ID`
- HTTP interceptor logs request URL, response body, and latency (as message string)
- `ErrorCodes` enum for structured error classification
- `Correlation-ID` set in error response headers
- `LLMUsageMetrics` parses token counts from Vertex AI / OpenAI / Anthropic responses (`prompt_tokens`, `completion_tokens`, `total_tokens`)
- Confidence score via logprobs (Claude generator)
- Template-bound generation with `PromptTemplateFactory` (DB-backed)
- Hot-reload of configs without restart

**Gaps — High Priority:**
| Gap | Impact |
|---|---|
| `event_id` absent | No unique per-request event |
| `event_type` not a structured field | Cannot classify events for dashboards |
| `environment` not in logs | Cannot separate environments |
| `service_name` not a structured field | Multi-service filtering broken |
| `latency_ms` as message string only | Not queryable as numeric |
| SOE_ID plain text in logs | PII compliance risk |
| `estimated_cost` not calculated | No cost governance despite token counts being available |
| No Kafka streaming | Events cannot be consumed by Observability Ingestion API |
| No OpenTelemetry/distributed tracing | Spans do not propagate to upstream orchestration |
| No `/metrics` endpoint | No real-time Prometheus/Grafana |

**Gaps — Medium Priority:**
| Gap | Impact |
|---|---|
| `finish_reason`, `rate_limit_hit`, `safety_blocked` absent | LLM quality monitoring incomplete |
| Full response body logged (HTTP middleware) | May expose sensitive generated content |
| Per-request model name not in log line | Cannot filter by model in dashboards |

---

### 6. GSSP QS (Query/Search Service)

**What It Does:** RAG retrieval service. Handles vector search queries, ranking, and context assembly for downstream generation.

**Observability Strengths:**
- Basic structured JSON logging
- `correlation_id` partially propagated
- Error handling with HTTP status

**Gaps — High Priority:**
| Gap | Impact |
|---|---|
| All standard event fields absent (event_id, event_type, environment, service_name) | Cannot integrate with observability plane |
| RAG-specific telemetry entirely absent (retrieved_chunk_count, avg_relevance_score, no_result_flag, citation_coverage_pct) | RAG quality dashboard impossible |
| Embedding model and latency not captured | Cannot track retrieval cost |
| No Kafka streaming | Events cannot be consumed by Observability Ingestion API |
| No OpenTelemetry | Cannot correlate retrieval with generation spans |

---

### 7. GSSP RS (Response Service)

**What It Does:** Response assembly and scoring layer. Handles confidence scoring, response ranking, and final delivery.

**Observability Strengths:**
- Basic structured logging

**Gaps — High Priority:**
| Gap | Impact |
|---|---|
| All standard event fields absent | Cannot integrate with observability plane |
| Confidence/ranking scores not structured as observability events | Quality analytics impossible |
| Response latency not tracked as numeric field | SLA monitoring impossible |
| No Kafka streaming | Events cannot reach Observability Ingestion API |

---

### 8. User Feedback Service

**What It Does:** Captures user feedback (ratings, thumbs up/down, free text) linked to agent responses. Gateway for quality improvement loop.

**Observability Strengths:**
- Captures `feedback_id`, `rating`, `thumbs`
- Partial `correlation_id` linkage to trace

**Gaps — High Priority:**
| Gap | Impact |
|---|---|
| Feedback not linked to `correlation_id` reliably | Cannot join feedback to request trace |
| `feedback_category` absent | Cannot classify negative feedback |
| Free text comment not redacted | PII/compliance risk |
| `submitted_by_role` absent | Cannot distinguish user vs CSO vs SME feedback |
| `FEEDBACK_SUBMITTED` event not emitted | Feedback invisible to observability stream |
| No Kafka emission | Cannot trigger incident routing pipeline |
| `resolution_status`, `linked_incident_id` absent | Feedback-to-fix loop broken |

---

## Cross-Service Gaps Summary

| Category | Gap | Affects | Priority |
|---|---|---|---|
| **Standard Fields** | `event_id` absent everywhere | All 8 services | P0 |
| **Standard Fields** | `environment` not injected anywhere | All 8 services | P0 |
| **Standard Fields** | `service_name` not a structured field | All 8 services | P0 |
| **Standard Fields** | `schema_version` absent | All 8 services | P0 |
| **PII Safety** | `user_id`/SOE_ID logged as plain text | All 8 services | P0 |
| **Cost** | `estimated_cost` not calculated anywhere | All 8 services | P0 |
| **Latency** | `latency_ms` not a numeric field (string only) | 6 of 8 services | P0 |
| **Tracing** | No OpenTelemetry / distributed tracing | All 8 services | P0 |
| **Streaming** | 6 of 8 services emit no Kafka events | Consumer, Data Ingest, GSSP GS/QS/RS, Feedback | P0 |
| **Metrics** | No `/metrics` endpoint on any service | All 8 services | P1 |
| **LLM** | Token counts not structured in 5 of 8 services | Orchestration, Consumer, Data Ingest, GSSP QS/RS | P1 |
| **Agent** | Agent step structured events absent in Orchestration | Agentic Orchestration | P1 |
| **RAG** | All RAG quality fields absent | Consumer, Data Ingest, GSSP QS | P1 |
| **Feedback** | `FEEDBACK_SUBMITTED` event not emitted | User Feedback | P1 |
| **Kafka** | Kafka lag/offset/partition not captured | Agent Executor, Orchestration | P1 |
| **Schema** | No common JSON schema enforced across services | All 8 services | P0 |
