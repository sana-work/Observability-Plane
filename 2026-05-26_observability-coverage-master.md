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
| 6 | **GSSP QS** | Query Service; RAG workflow orchestration, guardrails, semantic cache, retrieval/generation clients | Python / FastAPI, PGVector cache, COIN JWT | ❌ No |
| 7 | **GSSP RS** | Retrieval Service; configurable document retrieval, embeddings, PGVector search, MMR re-ranking | Python / FastAPI, PostgreSQL/pgvector, R2D2 embeddings | ❌ No |
| 8 | **User Feedback** | Feedback capture API; persists ratings/comments and partial trace links | Python / FastAPI, PostgreSQL/PGVector, COIN JWT | ❌ No |

---

## Telemetry Signal Coverage

> Current capture is fragmented: services write local JSON logs, audit tables, or Kafka control events, but no repo currently emits a uniform log/event/metric payload to a shared ingestion API.

| Service | Logs Captured Today | Events Captured Today | Metrics Captured Today | Missing for Observability Plane |
|---|---|---|---|---|
| **Agent Executor** | `ObservabilityLogger` JSON logs with correlation/application/SOE context | PostgreSQL `audit_table` rows for INVOCATION, AGENT, LLM_REQUEST, LLM_RESPONSE, TOOL, ERROR; optional Kafka lifecycle events | Token counts from VertexAI metadata; no Prometheus endpoint | Convert audit/log/Kafka records to standard OIS JSON; add numeric latency, cost, service/env, hash user |
| **Agentic Orchestration** | `JSONFormatter` logs, HTTP request/response timing | Kafka orchestration/HIL events (`AGENT_EXECUTION_REQUEST`, `HIL_REQUEST`, final/rejected responses) | HTTP timing only; no `/metrics` | Emit planner, Kafka, HIL, auth, and response lifecycle events to OIS; add LLM token/cost capture |
| **Consumer Service** | JSON logs with job/document context and HTTP body logs | Job lifecycle status is persisted/logged but not emitted as structured events | Function timing decorator; no queue-depth metric endpoint | Emit job, document parse, embedding, queue-depth, and scheduler events to OIS |
| **Data Ingestion Service** | JSON logs with correlation/application/job context; raw request/response body logs | Error/status paths only; success path route events are mostly absent | HTTP timing string only; no `/metrics` | Emit bulk-change, status-query, document, embedding, and auth events to OIS |
| **GSSP GS** | HTTP interceptor and generator logs with correlation/application; LLM usage in generator observability logs | Limited `observability_type` enum (`REQUEST`, `RESPONSE`, `ERROR`, `OTHER`, `CACHED_RESPONSE`) | Token counts captured; cost not calculated; no `/metrics` | Emit LLM call, request, response, file attachment, rate-limit/safety events to OIS |
| **GSSP QS** | `ObservabilityLogger` request/response/error/cache-hit logs | Limited `observability_type`; retrieval/guardrail success paths are not consistently evented | Cache-hit token/cost-saved fields only; no cache-miss or live LLM cost metric | Emit query, guardrail, cache, retrieval-client, generation-client, and latency metrics to OIS |
| **GSSP RS** | HTTP request/response logs, config/DB init logs, some error events | Retrieval runtime events are not emitted; MMR and embedding success paths mostly absent | Processing time in seconds; no retrieval, embedding, result-count, or token metrics | Emit retrieval, embedding, PGVector, MMR, config-load, and result-quality events to OIS |
| **User Feedback** | Middleware logs and DB/repository behavior, but route/repository success events are sparse | Feedback record persisted; `FEEDBACK_SUBMITTED` event not emitted | No feedback counters, latency histogram, or `/metrics` | Emit feedback submission/review/auth-failure events and counters to OIS |

---

## Standard Event Field Coverage Matrix

> These are the **mandatory fields** every service must emit in every observability telemetry record.

| Field | Agent Executor | Agentic Orchestration | Consumer Service | Data Ingestion | GSSP GS | GSSP QS | GSSP RS | User Feedback |
|---|---|---|---|---|---|---|---|---|
| `event_id` | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `event_type` | ✅ (audit_table) | ⚠️ Kafka only | ⚠️ error handler only | ⚠️ error handler only | ⚠️ (`observability_type`) | ⚠️ (`observability_type`) | ⚠️ (`observability_type`) | ❌ |
| `schema_version` | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `timestamp` | ✅ | ✅ | ✅ | ⚠️ (not always UTC ISO) | ⚠️ (`time`, local) | ⚠️ (`time`, local) | ⚠️ (`time`, local) | ⚠️ |
| `correlation_id` | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ (`ObservabilityLogger`) | ⚠️ | ⚠️ |
| `span_id` | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `parent_span_id` | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `request_id` | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `application_id` | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ⚠️ (consumer app ID) | ⚠️ |
| `environment` | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `service_name` | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `component` | ⚠️ (module path) | ⚠️ (module path) | ❌ | ❌ | ⚠️ (`name` logger field) | ❌ | ❌ | ❌ |
| `lob` | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `tenant_id` | ❌ | ❌ | ⚠️ (soeid) | ⚠️ (soeid) | ❌ | ❌ | ❌ | ❌ |
| `user_hash` | 🔴 plain text | 🔴 plain text | 🔴 plain text | 🔴 plain text | 🔴 plain text | 🔴 | 🔴 | ❌ |
| `status` | ✅ | ⚠️ | ⚠️ | ⚠️ | ⚠️ | ⚠️ (errors only) | ⚠️ (HTTP/error only) | ❌ |
| `latency_ms` | ⚠️ (derivable) | ⚠️ (HTTP only) | ⚠️ (string only) | ⚠️ (string only) | ⚠️ (seconds/string) | ⚠️ (seconds/string) | ⚠️ (seconds/string) | ⚠️ (seconds/string) |
| `error_code` | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ⚠️ (nested/partial) | ❌ |
| `http_status` | ✅ | ✅ | ✅ | ✅ | ✅ | ⚠️ (errors only) | ⚠️ (errors/response) | ❌ |

---

## Domain-Specific Field Coverage

### LLM Telemetry Fields

| Field | Agent Executor | Agentic Orchestration | GSSP GS | GSSP QS | GSSP RS |
|---|---|---|---|---|---|
| `model_name` | ❌ | ❌ | ✅ (LLM call logs) | ⚠️ (cache/pricing only) | ❌ (embedding config not logged per call) |
| `model_provider` | ❌ | ❌ | ⚠️ (config) | ⚠️ (client/config) | ❌ |
| `prompt_template_id` | ❌ | ❌ | ✅ (DB-bound) | ❌ | ❌ |
| `input_tokens` | ✅ (audit_table) | ❌ | ✅ (LLMUsageMetrics) | ⚠️ (cache hit only) | ❌ (embedding usage discarded) |
| `output_tokens` | ✅ (audit_table) | ❌ | ✅ (LLMUsageMetrics) | ⚠️ (cache hit only) | ❌ (embedding usage discarded) |
| `total_tokens` | ✅ (audit_table) | ❌ | ✅ (LLMUsageMetrics) | ❌ | ❌ |
| `estimated_cost` | ❌ | ❌ | ❌ | ⚠️ (`cost_saved` on cache hit only) | ❌ |
| `llm_latency_ms` | ❌ | ❌ | ⚠️ (seconds/string) | ⚠️ (seconds/string) | ❌ |
| `finish_reason` | ❌ | ❌ | ❌ | ❌ | ❌ |
| `rate_limit_hit` | ❌ | ❌ | ❌ | ❌ | ❌ |
| `safety_blocked` | ❌ | ❌ | ❌ | ❌ | ❌ |
| `confidence_score` | ❌ | ❌ | ✅ (logprobs/Claude) | ❌ | N/A |

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

| Field | Consumer Service | Data Ingestion | GSSP QS | GSSP RS |
|---|---|---|---|---|
| `rag_id` | ❌ | ❌ | ❌ | ❌ |
| `knowledge_base` | ❌ | ❌ | ❌ | ⚠️ (retrieval config only) |
| `embedding_model` | ❌ | ❌ | ❌ | ❌ (configured, not logged per call) |
| `embedding_latency_ms` | ❌ | ❌ | ❌ | ❌ |
| `retrieved_chunk_count` | ❌ | ❌ | ❌ | ❌ (`result_count` missing at runtime) |
| `avg_relevance_score` | ❌ | ❌ | ❌ | ❌ |
| `no_result_flag` | ❌ | ❌ | ❌ | ❌ |
| `citation_coverage_pct` | ❌ | ❌ | ❌ | ❌ |
| `context_truncation_flag` | ❌ | ❌ | ❌ | N/A |
| `job_id` | ✅ | ✅ | N/A | N/A |
| `document_id` | ✅ | ✅ | N/A | N/A |
| `job_status` | ✅ (SUCCESS/FAILURE/ERROR) | ✅ | N/A | N/A |
| `job_processing_time` | ⚠️ (string) | ⚠️ (string) | N/A | N/A |
| `input_tokens (embed)` | ❌ | ❌ | ❌ | ❌ |
| `RAG_RETRIEVAL_STARTED` | ❌ | ❌ | ❌ | ❌ |
| `RAG_RETRIEVAL_COMPLETED` | ❌ | ❌ | ❌ | ❌ |
| `RAG_NO_RESULT` | ❌ | ❌ | ❌ | ❌ |
| `DOCUMENT_INDEXED` | ⚠️ (job completion log) | ⚠️ (job completion log) | N/A | N/A |

### File / Attachment Telemetry Fields

> Covers every service that accepts inbound files (multimodal LLM requests, document uploads) or processes files from object storage (S3 ingestion pipelines).

#### Inbound Request File Fields — GSSP GS (multimodal `/generate`) & Agentic Orchestration

| Field | GSSP GS | Agentic Orchestration | Notes |
|---|---|---|---|
| `has_attachment` | ❌ | ❌ | Boolean: was any file/part included in the request? |
| `file_count` | ❌ | ❌ | Total number of files/parts attached |
| `image_count` | ❌ | ❌ | Number of image parts (PNG/JPG/WEBP/HEIC) |
| `doc_count` | ❌ | ❌ | Number of document parts (PDF/DOCX/XLSX/HTML) |
| `total_file_size_bytes` | ❌ | ❌ | Sum of all attachment sizes in the request |
| `largest_file_bytes` | ❌ | ❌ | Size of the largest individual attachment |
| `file_types` | ❌ | ❌ | Array of MIME types / extensions present e.g. `["image/png","application/pdf"]` |
| `multimodal_flag` | ❌ | ❌ | True when request contains both text and at least one file |
| `requests_without_files` | ❌ | ❌ | Counter metric: requests that had zero attachments |
| `FILE_ATTACHMENT_RECEIVED event` | ❌ | ❌ | Structured event at intake capturing all above fields |

> **Source note — GSSP GS:** The `PartHolder` model (`query/models/part_holder.py`) already carries `filename`, `mime_type`, and base64 `data`. All file metrics can be derived at request time from the `parts` list without any new upstream changes.

#### Document Ingestion File Fields — Consumer Service & Data Ingestion Service

| Field | Consumer Service | Data Ingestion | Notes |
|---|---|---|---|
| `document_format` | ❌ | ❌ | File extension / MIME type: `pdf`, `docx`, `xlsx`, `html`, `txt` |
| `document_size_bytes` | ❌ | ❌ | Raw byte size of the S3 object downloaded |
| `page_count` | ❌ | ❌ | Number of pages (PDF) or sheets (XLSX); `null` for HTML/text |
| `chunk_count` | ❌ | ❌ | Number of chunks produced by the splitter after ingestion |
| `avg_chunk_size_tokens` | ❌ | ❌ | Average token count per chunk |
| `extraction_status` | ❌ | ❌ | `success` \| `partial` \| `failed` — whether text extraction succeeded |
| `parser_used` | ❌ | ❌ | Which parser handled the file: `docx`, `xlsx`, `pdf`, `html`, `openparse` |
| `s3_object_key` | ⚠️ (in some logs) | ⚠️ (in some logs) | S3 URI of the source document |
| `requests_without_files` | ❌ | ❌ | Jobs where no document could be downloaded (S3 missing / access error) |
| `DOCUMENT_PARSE_STARTED event` | ❌ | ❌ | Structured event at parse start with format + size |
| `DOCUMENT_PARSE_COMPLETED event` | ❌ | ❌ | Event with chunk_count, page_count, extraction_status |
| `DOCUMENT_PARSE_FAILED event` | ❌ | ❌ | Event with parser_used, error_code, document_format |

> **Source note — Consumer Service:** `ingestion/parsers/parse_docx.py`, `parse_xlsx.py`, `parse_store.py` handle per-format extraction. `BaseTenant.ingest()` knows document size (S3 download) and chunk count (after split). Both are computable today, just not emitted.

#### Cross-Service File Telemetry Gaps Summary

| Gap | Affects | Priority | Impact |
|---|---|---|---|
| `has_attachment` / `file_count` not captured on any inbound request | GSSP GS, Agentic Orchestration | P0 | Cannot measure multimodal adoption or file-upload volume |
| `file_types` array absent | GSSP GS | P0 | Cannot detect unsupported formats reaching LLM; no type-distribution analytics |
| `image_count` / `doc_count` breakdown missing | GSSP GS | P1 | Cannot separate image vs document traffic for cost/capacity planning |
| `total_file_size_bytes` not tracked | GSSP GS, Data Ingestion, Consumer | P1 | No payload size governance; oversized requests invisible |
| `document_format` absent from ingestion events | Consumer, Data Ingestion | P1 | Cannot detect format-specific failure rates (e.g. PDF parser breakage) |
| `chunk_count` and `page_count` not in events | Consumer, Data Ingestion | P1 | Cannot correlate ingestion cost with document complexity |
| `extraction_status` not a structured field | Consumer, Data Ingestion | P0 | Silent extraction failures produce empty embeddings — undetectable |
| `requests_without_files` not counted | All inbound services | P1 | Cannot distinguish multimodal vs text-only request patterns |
| No `FILE_ATTACHMENT_RECEIVED` event | GSSP GS, Agentic Orchestration | P1 | File receipt invisible to observability pipeline |
| `parser_used` absent | Consumer, Data Ingestion | P2 | Cannot identify which parser caused extraction failures |

---

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
| Job queue depth not captured on poll cycle | Cannot detect ingestion backlog or scheduler starvation |

**Gaps — Medium Priority:**
| Gap | Impact |
|---|---|
| Scheduler lifecycle logging partial | Cannot detect scheduler failures |
| Full HTTP bodies logged | PII risk |
| No cost tracking for embedding calls | Budget governance impossible |

**Gaps — File / Document Telemetry (High Priority):**
| Gap | Impact |
|---|---|
| `document_format` (pdf/docx/xlsx/html) not in any event | Cannot detect format-specific parser failure rates |
| `document_size_bytes` absent | No payload size governance; large documents cause silent timeouts |
| `chunk_count` not emitted after splitting | Cannot correlate ingestion cost with document complexity |
| `page_count` absent | No document complexity analytics |
| `extraction_status` not a structured field | Silent parse failures produce empty embeddings undetected |
| `parser_used` (docx/xlsx/pdf/html/openparse) absent | Cannot identify which parser is responsible for failures |
| `avg_chunk_size_tokens` not tracked | Cannot detect token-limit violations during embedding |
| `requests_without_files` not counted | S3 download failures / missing docs invisible at service level |
| No `DOCUMENT_PARSE_STARTED` / `DOCUMENT_PARSE_COMPLETED` events | Document processing pipeline steps fully invisible |
| No `QUEUE_DEPTH_RECORDED` metric/event | Backlog trend cannot be pushed to a central metrics table |

> **Source:** `BaseTenant.ingest()` downloads the S3 blob (size knowable at download). Parsers in `ingestion/parsers/` produce chunks (count available post-split). Both metrics require only one `emit_event()` call each.

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
| Success-path route events missing for bulk-change create/status query | Job creation volume and status-query behavior invisible |
| Timestamps not consistently UTC ISO 8601 | Cross-region correlation may be unreliable |

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

**Gaps — File / Attachment Telemetry (High Priority):**
| Gap | Impact |
|---|---|
| `has_attachment`, `file_count`, `image_count`, `doc_count` not captured | Multimodal request volume and file-upload patterns invisible |
| `total_file_size_bytes`, `largest_file_bytes` absent | Oversized multimodal payloads undetected; no size governance |
| `file_types` array not emitted | Cannot detect unsupported/unexpected MIME types hitting the LLM |
| `multimodal_flag` absent | Cannot split multimodal vs text-only traffic in dashboards |
| `requests_without_files` counter missing | Cannot measure what proportion of requests are text-only |
| No `FILE_ATTACHMENT_RECEIVED` structured event | File intake completely invisible to observability pipeline |

> **Note:** GSSP GS already has `PartHolder` (filename, mime_type, base64 data) in `query/models/part_holder.py`. All file metrics are derivable from the `parts` list at request intake — zero upstream changes required.

---

### 6. GSSP QS (Query Service)

**What It Does:** Central RAG workflow orchestrator for consumer applications. Accepts `/query-data` and `/conversational-query-data`, runs guardrail checks, consults a semantic PGVector cache, calls GSSP Retrieval Service for chunks, and calls GSSP GS for generation.

**Observability Strengths:**
- `ObservabilityLogger` emits structured request/response/error/cache-hit logs with `X-Correlation-ID` and `X-Application-ID`
- `observability_type` enum exists for REQUEST, RESPONSE, CACHED_RESPONSE, ERROR, and OTHER
- Error-code registry covers auth, LLM, retrieval, cache, and config errors
- Cache-hit logger captures `input_tokens`, `output_tokens`, and `cost_saved`
- HTTP middleware and timing decorator capture processing time, though not as standard `latency_ms`

**Gaps — High Priority:**
| Gap | Impact |
|---|---|
| `event_id`, `environment`, `service_name`, `component`, `request_id`, and `user_hash` absent | Cannot route/query telemetry safely in a central plane |
| `observability_type` is limited and not mapped to standard `event_type` | Dashboards cannot distinguish query, guardrail, cache, retrieval, and generation stages |
| Guardrail/retrieval/generation success paths are not consistently logged | Query pipeline can fail or degrade without stage-level visibility |
| Cache misses are not logged with model/cost/latency details | Cannot measure cache effectiveness or live LLM cost |
| RAG quality fields missing (`retrieved_chunk_count`, `avg_relevance_score`, `no_result_flag`, `citation_coverage_pct`) | RAG quality dashboard impossible |
| No `/metrics` endpoint or OIS emitter | No central counters/histograms; events cannot reach the Observability Plane |

---

### 7. GSSP RS (Retrieval Service)

**What It Does:** Foundation retrieval microservice for RAG pipelines. Exposes `/api/gssp-retrieval-service/v1/retrieve`, `/retrieve_embedding`, and `/reload-configs`; loads consumer configs, generates embeddings through Stellar/VertexAI via R2D2, retrieves chunks from PostgreSQL/pgvector, and can apply MMR re-ranking.

**Observability Strengths:**
- HTTP middleware emits REQUEST/RESPONSE-style observability logs with URL path, processing time, `X-Correlation-ID`, `X-Application-ID`, and SOE context
- Error handling uses an `ErrorCodes` enum and captures HTTP status/error descriptions on some paths
- Startup/config/PGVector initialization emit partial logs
- PGVector retriever logs init parameters such as schema, semantic-search config, and fusion strategy

**Gaps — High Priority:**
| Gap | Impact |
|---|---|
| `event_id`, `environment`, `service_name`, `component`, `request_id`, and `user_hash` absent | Retrieval logs cannot be safely aggregated or deduplicated |
| `latency_ms` is stored as seconds/string and only total HTTP time is measured | No per-stage latency for embed, DB query, MMR, or result building |
| Embedding calls do not emit model, token usage, latency, or cost | Retrieval cost and model/provider impact invisible |
| Retrieval runtime events missing (`RETRIEVAL_REQUEST`, `RETRIEVAL_RESPONSE`, `RAG_NO_RESULT`) | Cannot measure result count, top-k, relevance, strategy, or no-result rate |
| MMR re-ranking has no structured logs | Re-ranking quality and latency cannot be analyzed |
| SOE_ID and some request/error content may be logged raw | PII/compliance risk |
| No `/metrics` endpoint or OIS emitter | No central retrieval metrics or uniform event pipeline |

---

### 8. User Feedback Service

**What It Does:** Captures user feedback (ratings, thumbs up/down, free text) linked to agent responses. Gateway for quality improvement loop.

**Observability Strengths:**
- Captures `feedback_id`, `rating`, `thumbs`
- Partial `correlation_id` linkage to trace
- FastAPI middleware has request/response logging and latency context
- Repository persists feedback records through `UserFeedbackRepo.create()`

**Gaps — High Priority:**
| Gap | Impact |
|---|---|
| Route/repository success and failure events are not individually logged | Cannot derive feedback success/failure rates from telemetry |
| `http_status` absent from logs | Feedback API SLA and error-rate dashboards incomplete |
| Feedback not linked to `correlation_id` reliably | Cannot join feedback to request trace |
| `feedback_id` / `trace_id` not emitted in log records | Logs cannot be joined back to feedback DB rows or upstream traces |
| `feedback_category` absent | Cannot classify negative feedback |
| Free text comment not redacted | PII/compliance risk |
| `submitted_by_role` absent | Cannot distinguish user vs CSO vs SME feedback |
| `FEEDBACK_SUBMITTED` event not emitted | Feedback invisible to observability stream |
| No Kafka emission | Cannot trigger incident routing pipeline |
| `resolution_status`, `linked_incident_id` absent | Feedback-to-fix loop broken |
| No `/metrics` endpoint or feedback counters | Cannot alert on submission failures or volume spikes |

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
| **Ingestion API** | No service emits logs/events/metrics to a shared Observability Ingestion API | All 8 services | P0 |
| **Streaming** | 6 of 8 services emit no Kafka events | Consumer, Data Ingest, GSSP GS/QS/RS, Feedback | P1 |
| **Metrics** | No `/metrics` endpoint on any service | All 8 services | P1 |
| **Metrics** | No standard JSON metric envelope for counters/gauges/histograms | All 8 services | P0 |
| **LLM** | Token counts not structured in 5 of 8 services | Orchestration, Consumer, Data Ingest, GSSP QS/RS | P1 |
| **Agent** | Agent step structured events absent in Orchestration | Agentic Orchestration | P1 |
| **RAG** | RAG/retrieval quality fields absent | Consumer, Data Ingest, GSSP QS, GSSP RS | P1 |
| **RAG** | Retrieval/embedding runtime metrics absent despite GSSP RS owning retrieval | GSSP RS | P1 |
| **Feedback** | `FEEDBACK_SUBMITTED` event not emitted | User Feedback | P1 |
| **Kafka** | Kafka lag/offset/partition not captured | Agent Executor, Orchestration | P1 |
| **Schema** | No common JSON schema enforced across services | All 8 services | P0 |
| **PII Safety** | Full request/response bodies and raw prompt/comment fields logged in several services | Consumer, Data Ingest, GSSP GS, User Feedback | P0 |
