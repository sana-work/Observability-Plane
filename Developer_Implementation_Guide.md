# AI Services Platform Observability Plane — Developer Implementation Guide

## Purpose

This document is the developer-facing reference for building the Observability Plane.
It explains every component, what data to capture at each level, and exactly what code needs to be written.
Read this alongside the architecture document and the refined architecture diagram.

---

## Table of Contents

1. [Component Catalog](#1-component-catalog)
2. [Capture Requirements by Level](#2-capture-requirements-by-level)
3. [Observability SDK — What to Build](#3-observability-sdk--what-to-build)
4. [Kafka Layer — What to Build](#4-kafka-layer--what-to-build)
5. [Telemetry Processor — What to Build](#5-telemetry-processor--what-to-build)
6. [Storage Layer — What to Build](#6-storage-layer--what-to-build)
7. [Anomaly Detection Service — What to Build](#7-anomaly-detection-service--what-to-build)
8. [Incident Router Service — What to Build](#8-incident-router-service--what-to-build)
9. [Observability-as-Code Pipeline — What to Build](#9-observability-as-code-pipeline--what-to-build)
10. [Observability Chatbot — What to Build](#10-observability-chatbot--what-to-build)
11. [Dashboard Specifications — What to Build](#11-dashboard-specifications--what-to-build)
12. [Offline Batch RCA Engine — What to Build](#12-offline-batch-rca-engine--what-to-build)
13. [Development Sequence and Dependencies](#13-development-sequence-and-dependencies)

---

## 1. Component Catalog

### 1.1 Observability SDK

**What it is:** A shared Python library (or language-appropriate package) imported by every service in the AI platform. It wraps OpenTelemetry and emits standardized events, spans, metrics, and quality signals.

**What it does:**
- Creates spans with W3C TraceContext
- Emits structured domain events to Kafka observability topics
- Injects `correlation_id`, `span_id`, `parent_span_id`, `application_id`, `agent_id` into every event
- Redacts PII and secrets before emitting
- Captures quality signals (faithfulness, response entropy) for LLM and RAG calls

**Inputs:** Service code instrumentation calls (context managers / decorators)
**Outputs:** Kafka messages on `ai-obs-traces`, `ai-obs-events`, `ai-obs-metrics`, `ai-obs-quality`, `ai-obs-cost`

**Technology:** Python (primary), Java (secondary), OpenTelemetry SDK, confluent-kafka or kafka-python

---

### 1.2 Kafka Event Bus

**What it is:** Apache Kafka acting as the central nervous system for all observability data. Every service emits events to Kafka topics; downstream processors consume from them.

**Topics required:**

| Topic | Purpose | Producers | Consumers |
|---|---|---|---|
| `ai-obs-traces` | Distributed trace spans | All services via SDK | Telemetry Processor |
| `ai-obs-events` | Domain events (REQUEST_RECEIVED, TOOL_CALL_COMPLETED, etc.) | All services via SDK | Telemetry Processor |
| `ai-obs-metrics` | Counters, histograms, Kafka lag | All services + JMX exporter | Telemetry Processor, Grafana |
| `ai-obs-quality` | Faithfulness scores, entropy, embedding drift | LLM Wrapper, RAG Wrapper via SDK | Telemetry Processor |
| `ai-obs-cost` | Token cost events and budget snapshots | LLM Wrapper via SDK | Telemetry Processor |
| `ai-obs-anomalies` | ML-detected anomaly events | Anomaly Detection Service | Elasticsearch indexer, Grafana adapter |
| `ai-obs-incidents` | Incident triggers from feedback gate | Stream Processor | Incident Router Service |

**W3C traceparent** must be injected as a Kafka message header (not inside the payload) on every message produced.

---

### 1.3 Telemetry Processor

**What it is:** A Kafka Streams or Python Faust / Java Kafka Streams application that consumes raw observability events and transforms them into storage-ready artifacts.

**Sub-components** (each is a discrete processing step in the pipeline):

| Sub-component | Responsibility |
|---|---|
| W3C TraceContext Extractor | Reads `traceparent` header from Kafka message; propagates span context |
| PII Redactor | Removes names, emails, phone numbers, account numbers from event fields |
| Metadata Enricher | Joins event with app/agent/tool registry from PostgreSQL cache |
| Error Code Mapper | Translates raw exception types to standard error catalog codes |
| Token Cost Calculator | Computes `estimated_cost` from token counts × model pricing; writes to Redis budget accumulator |
| Faithfulness Scorer | Computes RAG faithfulness score from context/response overlap; writes to quality index |
| Rollup Generator | Aggregates events into hourly/daily PostgreSQL aggregate tables |
| SLO Evaluator | Computes burn rate for 1h and 6h windows; writes `daily_slo_compliance` |
| S3 Archiver | Writes large payloads (prompts, responses, full traces, RAG contexts) to S3 |

**Inputs:** Kafka topics `ai-obs-traces`, `ai-obs-events`, `ai-obs-quality`, `ai-obs-cost`
**Outputs:** Elasticsearch indices, PostgreSQL tables, Amazon S3 objects, Kafka `ai-obs-anomalies`

**Technology:** Python (Faust or custom consumer) or Java (Kafka Streams), Redis (budget accumulator), PostgreSQL (registry cache + aggregates), Elasticsearch client, boto3/S3 SDK

---

### 1.4 Anomaly Detection Service

**What it is:** A standalone Python service that subscribes to the enriched metric stream, runs ML models to detect anomalies, maintains per-application dynamic baselines, and publishes anomaly events.

**What it does:**
- Maintains sliding P50/P95/P99 baselines in Redis per `(application_id, metric_name)` using a 7-day window
- Runs Isolation Forest for point anomalies on `latency_ms`, `error_rate`, `token_cost`
- Runs LSTM autoencoder for temporal pattern anomalies
- Correlates anomalies across metrics using shared `correlation_id`
- Publishes `ANOMALY_DETECTED` events to `ai-obs-anomalies` Kafka topic

**Inputs:** Kafka topic `ai-obs-metrics`, `ai-obs-events` (enriched by processor)
**Outputs:** Kafka topic `ai-obs-anomalies` → Elasticsearch `ai-obs-anomalies-*` → Grafana

**Technology:** Python, scikit-learn (Isolation Forest), PyTorch or TensorFlow (LSTM), Redis, Kafka consumer

---

### 1.5 Incident Router Service

**What it is:** A lightweight consumer of the `ai-obs-incidents` Kafka topic that dispatches incident payloads to external ticketing systems (PagerDuty, Jira ServiceNow).

**What it does:**
- Evaluates routing rules (severity, application tier, incident type)
- Calls PagerDuty Events API or Jira REST API to create incidents
- Assembles a debug bundle (Elasticsearch links, S3 artifact URIs, trace links) and stores it in S3
- Writes `linked_incident_id` back to the `feedback_case` PostgreSQL table

**Inputs:** Kafka topic `ai-obs-incidents` (published by stream processor's feedback quality gate)
**Outputs:** PagerDuty/Jira incident, S3 debug bundle, PostgreSQL `feedback_case.linked_incident_id`

**Technology:** Python, PagerDuty Python SDK or Jira REST, boto3, psycopg2

---

### 1.6 Alert Rule Syncer

**What it is:** A Python script (run as a Kubernetes CronJob or CI step) that reads the `alert_threshold` PostgreSQL table and pushes alert rules to Grafana via the Grafana Alerting API.

**What it does:**
- Reads all active rows from `alert_threshold`
- Converts each row to a Grafana alert rule JSON payload
- Creates or updates the rule via Grafana HTTP API
- Reports drift (rules in Grafana not in PostgreSQL) for cleanup

**Inputs:** PostgreSQL `alert_threshold` table
**Outputs:** Grafana alert rules (via API)

**Technology:** Python, requests (HTTP), psycopg2, Grafana Alerting API v1

---

### 1.7 Offline Batch RCA Engine

**What it is:** A nightly Python job (scheduled via Kubernetes CronJob or Airflow) that correlates failure events across Elasticsearch and PostgreSQL, ranks root cause hypotheses, and produces a weekly digest.

**What it does:**
- Joins `ai-obs-errors-*` with `ai-obs-traces-*` by `correlation_id`
- Cross-references with `agg_daily_kpi_metrics` to correlate technical failures with business impact
- Scores root cause hypotheses (tool degradation, model drift, prompt change, RAG staleness)
- Writes ranked results to Elasticsearch `ai-obs-anomalies-*` and S3 `rca-reports/`
- Sends weekly digest to Slack webhook and/or email

**Inputs:** Elasticsearch `ai-obs-errors-*`, `ai-obs-traces-*`, PostgreSQL `agg_daily_kpi_metrics`, `vector_health_snapshots`
**Outputs:** S3 `rca-reports/`, Elasticsearch, Slack webhook / SES email

**Technology:** Python, elasticsearch-py, psycopg2, boto3, requests (Slack), AWS SES

---

### 1.8 Observability Chatbot

**What it is:** A natural-language interface for querying the observability data. Uses a metric semantic layer to route questions to the correct data source.

**What it does:**
- Classifies user intent (aggregate metric question, trace drill-down, RCA question, infra/SLO question)
- Enforces RBAC — users can only query data for their application/LOB
- Routes queries to PostgreSQL (aggregates), Elasticsearch (events/traces), S3 (artifacts), or Grafana (infra metrics)
- Generates structured answers with metric value, time range, applied filters, source, and dashboard link

**Inputs:** User natural-language question, user identity/role
**Outputs:** Structured answer with metric value, source attribution, dashboard link, recommended action

**Technology:** Python, LLM (gemini/gpt-4o) for intent classification + answer generation, PostgreSQL, Elasticsearch, boto3, Grafana API

---

### 1.9 Storage Components

| Component | Technology | Role |
|---|---|---|
| **Elasticsearch** | Elasticsearch 8.x | Hot operational event store; searchable logs, traces, errors, LLM/tool/RAG events |
| **PostgreSQL** | PostgreSQL 14+ | Control plane — registries, KPI definitions, aggregate tables, alert thresholds, chatbot metric catalog |
| **Amazon S3** | S3 with SSE-KMS | Object store — redacted payloads, full traces, RAG contexts, audit evidence, debug bundles, RCA reports |
| **Grafana** | Grafana 10+ | Monitoring, SLO/SLA dashboards, alerting; reads from ES, PG, CloudWatch, Kafka JMX |
| **Redis / ElastiCache** | Redis 7+ | Runtime cache — budget accumulators, agent execution state, Kafka dedup, chatbot query cache |
| **Kibana** | Kibana 8.x | Operational search UI over Elasticsearch for event and trace drill-down |

---

## 2. Capture Requirements by Level

### 2.1 Platform Request Level

**Where to instrument:** API Gateway / Orchestration Service entry point.
**Capture one event per request lifecycle transition.**

| Field | Type | Required | Description |
|---|---|---|---|
| `event_id` | string | Yes | UUID for this event |
| `event_type` | string | Yes | `REQUEST_RECEIVED`, `RESPONSE_DELIVERED`, `REQUEST_FAILED` |
| `timestamp` | ISO8601 | Yes | UTC timestamp |
| `correlation_id` | string | Yes | End-to-end trace ID; generated at request entry, propagated everywhere |
| `span_id` | string | Yes | W3C span ID for this operation |
| `parent_span_id` | string | No | Parent span (null at root) |
| `environment` | string | Yes | `prod`, `staging`, `dev` |
| `application_id` | string | Yes | CSI / application identifier |
| `app_container` | string | Yes | Application container name |
| `soe_id` | string | Yes | Service owner / business ID |
| `lob` | string | Yes | Line of business |
| `tenant_id` | string | No | Tenant identifier for multi-tenant deployments |
| `user_hash` | string | Yes | SHA-256 of user ID — never raw user ID |
| `channel` | string | Yes | `ui`, `api`, `webhook`, `batch` |
| `request_type` | string | Yes | `prompt`, `rag`, `single_agent`, `multi_agent`, `loop_agent`, `tool_augmented` |
| `status` | string | Yes | `success`, `failed`, `partial`, `timeout` |
| `latency_ms` | integer | Yes | End-to-end latency in milliseconds |
| `error_code` | string | No | Standard error code from catalog |
| `http_status` | integer | No | HTTP response code |
| `input_tokens` | integer | No | Total input tokens across all LLM calls |
| `output_tokens` | integer | No | Total output tokens |
| `total_tokens` | integer | No | Sum of input + output tokens |
| `estimated_cost` | decimal | No | Estimated USD cost |
| `feedback_available` | boolean | No | Whether feedback was submitted |

---

### 2.2 Orchestration Level

**Where to instrument:** Orchestration Service — after each internal decision.
**Emit one event per orchestration step.**

| Event Type | When to Emit | Additional Fields |
|---|---|---|
| `AUTH_COMPLETED` | After authentication check | `auth_provider`, `auth_latency_ms`, `auth_result` |
| `CONFIG_LOADED` | After config/feature-flag load | `config_version`, `feature_flags_active` |
| `PLAN_CREATED` | After planner selects agent/tool | `plan_type` (static/dynamic), `selected_agent_id`, `estimated_steps` |
| `AGENT_EXECUTION_REQUEST_PRODUCED` | After Kafka produce | `kafka_topic`, `kafka_partition`, `kafka_offset`, `produce_latency_ms` |
| `FINAL_RESPONSE_CONSUMED` | After consuming result from Kafka | `kafka_consumer_group`, `consumer_latency_ms`, `kafka_lag` |
| `RESPONSE_BUILT` | After assembling final response | `response_size_bytes`, `response_latency_ms` |
| `RESPONSE_DELIVERED` | After returning to caller | Same as request-level `status` and `latency_ms` |

---

### 2.3 Kafka Level

**Where to instrument:** SDK Kafka producer and consumer wrappers.
**Emit one metric event per produce and per consume.**

| Field | Type | Description |
|---|---|---|
| `topic` | string | Kafka topic name |
| `partition` | integer | Kafka partition number |
| `offset` | long | Kafka message offset |
| `consumer_group` | string | Consumer group ID |
| `producer_latency_ms` | integer | Time from produce call to broker acknowledgement |
| `consumer_latency_ms` | integer | Time from message timestamp to consume call |
| `kafka_lag` | long | Current consumer lag for this topic/partition |
| `message_size_bytes` | integer | Serialized message size |
| `retry_count` | integer | Number of produce/consume retries |
| `dlq_flag` | boolean | Whether message was routed to dead-letter queue |
| `traceparent` | string | W3C traceparent header value (also set as Kafka message header) |

---

### 2.4 Agent Level

**Where to instrument:** Agent Orchestrator + Agent Runtime.
**Emit one event per agent lifecycle event and one event per step.**

| Event Type | When to Emit | Additional Fields |
|---|---|---|
| `AGENT_STARTED` | Agent begins execution | `agent_id`, `agent_version`, `agent_type`, `execution_mode` |
| `AGENT_STEP_COMPLETED` | Each step finishes | `step_number`, `step_name`, `step_latency_ms`, `selected_tool_id`, `selected_rag_id` |
| `AGENT_LOOP_ITERATION` | Each loop iteration (loop-agents) | `loop_count`, `loop_max`, `loop_exit_condition` |
| `AGENT_HANDOFF` | Multi-agent handoff | `source_agent_id`, `target_agent_id`, `handoff_reason`, `handoff_latency_ms` |
| `AGENT_COMPLETED` | Successful completion | `total_steps`, `total_latency_ms`, `total_tokens`, `estimated_cost` |
| `AGENT_FAILED` | Any terminal failure | `error_code`, `error_description`, `termination_reason`, `failed_at_step` |
| `AGENT_TIMEOUT` | Execution timeout | `timeout_ms`, `steps_completed` |

**Aggregate metrics to compute per agent per hour:**
- `request_count`, `success_count`, `error_count`
- `avg_latency_ms`, `p95_latency_ms`
- `avg_step_count`, `loop_count`, `handoff_count`
- `tool_call_count`, `tool_failure_count`
- `rag_request_count`, `rag_no_result_count`
- `total_tokens`, `estimated_cost`

---

### 2.5 LLM Call Level

**Where to instrument:** LLM Wrapper — around every `generate()` / `chat()` call.
**Emit one event per model call. Never log raw prompts — store in S3 with redaction.**

| Field | Type | Required | Description |
|---|---|---|---|
| `model_provider` | string | Yes | e.g., `vertex_ai`, `azure_openai`, `anthropic` |
| `model_name` | string | Yes | e.g., `gemini-1.5-pro`, `gpt-4o`, `claude-sonnet-4-6` |
| `model_version` | string | No | Model version/snapshot ID |
| `prompt_template_id` | string | Yes | ID from `prompt_template_registry` |
| `prompt_template_version` | string | Yes | Template version |
| `prompt_hash` | string | Yes | SHA-256 of canonical prompt — for drift detection |
| `temperature` | float | No | Model temperature setting |
| `input_tokens` | integer | Yes | Input token count |
| `output_tokens` | integer | Yes | Output token count |
| `total_tokens` | integer | Yes | Sum |
| `estimated_cost` | decimal | Yes | USD cost based on model pricing |
| `latency_ms` | integer | Yes | Total model call latency |
| `time_to_first_token_ms` | integer | No | Streaming: time to first token |
| `retry_count` | integer | Yes | Number of retries |
| `rate_limit_hit` | boolean | Yes | Whether rate limit was encountered |
| `safety_blocked` | boolean | Yes | Whether model safety filter blocked output |
| `finish_reason` | string | Yes | `stop`, `length`, `safety`, `error`, `tool_call` |
| `llm_error_code` | string | No | Provider-specific error code |
| `s3_prompt_uri` | string | No | S3 URI of redacted prompt |
| `s3_response_uri` | string | No | S3 URI of redacted response |
| `response_entropy` | float | No | Shannon entropy of response tokens — quality signal |

**Do not capture:** Raw prompt text, raw response text in Elasticsearch or PostgreSQL. These go to S3 after redaction only.

---

### 2.6 Tool Call Level

**Where to instrument:** Tool Executor wrapper — around every external API/DB/service call.
**Emit one event per tool invocation.**

| Field | Type | Required | Description |
|---|---|---|---|
| `tool_id` | string | Yes | ID from `tool_registry` |
| `tool_name` | string | Yes | Human-readable tool name |
| `tool_version` | string | Yes | Tool version |
| `tool_type` | string | Yes | `rest_api`, `db_query`, `servicenow`, `internal_api`, `rag` |
| `input_schema_valid` | boolean | Yes | Whether input passed schema validation |
| `status` | string | Yes | `success`, `failed`, `timeout`, `auth_error` |
| `http_status` | integer | No | HTTP status code if REST |
| `error_code` | string | No | Standard error catalog code |
| `error_description` | string | No | Short error description (redacted) |
| `latency_ms` | integer | Yes | Tool call latency |
| `retry_count` | integer | Yes | Number of retries |
| `timeout_flag` | boolean | Yes | Whether call timed out |
| `response_size_bytes` | integer | No | Response payload size |
| `called_by_agent_id` | string | Yes | Agent that invoked the tool |
| `called_by_step_number` | integer | No | Agent step number that triggered call |

**Do not capture:** Tool request body or response body if they contain PII or sensitive data. Store references to S3 only if needed.

---

### 2.7 RAG Retrieval Level

**Where to instrument:** RAG Wrapper — across all stages of the retrieval pipeline.
**Emit one event per retrieval attempt.**

| Stage | Fields to Capture |
|---|---|
| **Query** | `query_hash` (SHA-256, not raw query), `query_type` (semantic/keyword/hybrid), `rewrite_applied` (bool) |
| **Embedding** | `embedding_model`, `embedding_latency_ms`, `embedding_error` |
| **Retrieval** | `knowledge_base`, `vector_index_name`, `top_k`, `retrieved_chunk_count`, `retrieval_latency_ms` |
| **Ranking** | `reranker_used` (bool), `reranker_model`, `rerank_latency_ms`, `avg_relevance_score`, `min_relevance_score` |
| **Grounding** | `citation_coverage_pct` (% of response grounded in retrieved context), `source_doc_count`, `s3_rag_context_uri` |
| **Context** | `context_tokens`, `context_truncation_flag`, `context_utilization_ratio` |
| **Quality** | `faithfulness_score` (computed in processor), `no_result_flag`, `low_confidence_flag` |
| **Permissions** | `access_filter_applied` (bool), `chunks_denied_count` |
| **Freshness** | `oldest_doc_age_days`, `stale_doc_flag` |
| **Failure** | `rag_error_code`, `rag_error_description` |

---

### 2.8 Guardrail Level

**Where to instrument:** Guardrails Engine — after every policy evaluation.
**Emit one event per guardrail check.**

| Field | Type | Description |
|---|---|---|
| `policy_id` | string | Policy identifier |
| `policy_version` | string | Policy version |
| `decision` | string | `allow`, `block`, `redact`, `escalate` |
| `risk_score` | float | 0.0–1.0 risk score |
| `violation_type` | string | `bias`, `pii`, `toxicity`, `off_topic`, `competitor_mention` |
| `blocked_stage` | string | `input`, `tool_call`, `rag_context`, `output` |
| `redaction_applied` | boolean | Whether content was redacted |
| `guardrail_latency_ms` | integer | Time for guardrail evaluation |
| `false_positive_feedback` | boolean | User flagged as false positive |

**Do not capture:** The actual violating text. Capture only `violation_type` and `risk_score`.

---

### 2.9 Memory Level

**Where to instrument:** Memory Module — around every read and write.

| Event Type | Fields |
|---|---|
| `MEMORY_READ` | `memory_source` (session/long_term/episodic), `memory_hit` (bool), `retrieval_latency_ms`, `item_count` |
| `MEMORY_WRITE` | `memory_source`, `items_written`, `write_latency_ms` |
| `MEMORY_MISS` | `memory_source`, `query_hash`, `miss_reason` |
| `MEMORY_EVICTED` | `memory_source`, `eviction_reason`, `item_count_evicted` |

---

### 2.10 User Feedback Level

**Where to instrument:** Feedback UI component — on submission.

| Field | Type | Required | Description |
|---|---|---|---|
| `feedback_id` | string | Yes | UUID |
| `correlation_id` | string | Yes | Links to the request trace |
| `application_id` | string | Yes | Application |
| `agent_id` | string | Yes | Agent that produced the response |
| `response_id` | string | No | Specific response ID |
| `rating` | integer | No | 1–5 star rating |
| `thumbs` | string | No | `up` or `down` |
| `sentiment` | string | Yes | `positive`, `negative`, `neutral` (derived from rating/thumbs) |
| `feedback_category` | string | No | `wrong_answer`, `incomplete`, `slow`, `tool_failed`, `unsafe`, `rag_missing`, `formatting`, `other` |
| `free_text_comment_redacted` | string | No | Redacted free text |
| `submitted_by_role` | string | Yes | `user`, `cso`, `sme`, `admin` |
| `resolution_status` | string | Yes | `open` (default) |

---

## 3. Observability SDK — What to Build

### 3.1 Package Structure

```text
ai-observability-sdk/
├── ai_obs_sdk/
│   ├── __init__.py
│   ├── context.py          ← ObsContext dataclass (correlation_id, span_id, etc.)
│   ├── tracer.py           ← ObsTracer: span creation, W3C traceparent management
│   ├── emitter.py          ← KafkaEmitter: produces events to Kafka topics
│   ├── redactor.py         ← PiiRedactor: field-level PII redaction before emit
│   ├── decorators.py       ← @observe_request, @observe_llm_call, @observe_tool_call, @observe_rag
│   ├── models.py           ← Pydantic models for each event type
│   ├── quality.py          ← FaithfulnessScorer, EntropyCalculator
│   ├── cost.py             ← TokenCostCalculator (model pricing table)
│   └── kafka_headers.py    ← W3C traceparent inject/extract for Kafka
├── tests/
└── pyproject.toml
```

### 3.2 Core Classes to Implement

#### `ObsContext` — dataclass passed through every call

```python
from dataclasses import dataclass, field
from uuid import uuid4

@dataclass
class ObsContext:
    correlation_id: str = field(default_factory=lambda: str(uuid4()))
    span_id: str = field(default_factory=lambda: str(uuid4())[:16])
    parent_span_id: str | None = None
    application_id: str = ""
    agent_id: str | None = None
    environment: str = "prod"
    lob: str = ""
    tenant_id: str | None = None
    user_hash: str | None = None
```

#### `ObsTracer` — span lifecycle management

```python
from contextlib import contextmanager
from opentelemetry import trace
from opentelemetry.propagators.textmap import DefaultGetter

class ObsTracer:
    def __init__(self, service_name: str, emitter: "KafkaEmitter"):
        self._tracer = trace.get_tracer(service_name)
        self._emitter = emitter

    @contextmanager
    def span(self, operation: str, ctx: ObsContext, event_type: str, extra: dict = None):
        with self._tracer.start_as_current_span(operation) as span:
            start_ms = time.time() * 1000
            try:
                yield span
                status = "success"
            except Exception as exc:
                status = "failed"
                span.record_exception(exc)
                raise
            finally:
                latency_ms = int(time.time() * 1000 - start_ms)
                self._emitter.emit(event_type, ctx, {
                    "status": status,
                    "latency_ms": latency_ms,
                    **(extra or {}),
                })
```

#### `KafkaEmitter` — standardized event publishing

```python
from confluent_kafka import Producer
from ai_obs_sdk.kafka_headers import inject_traceparent

class KafkaEmitter:
    def __init__(self, bootstrap_servers: str):
        self._producer = Producer({"bootstrap.servers": bootstrap_servers})

    def emit(self, event_type: str, ctx: ObsContext, payload: dict, topic: str = "ai-obs-events"):
        event = {
            "event_id": str(uuid4()),
            "event_type": event_type,
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "correlation_id": ctx.correlation_id,
            "span_id": ctx.span_id,
            "parent_span_id": ctx.parent_span_id,
            "application_id": ctx.application_id,
            "agent_id": ctx.agent_id,
            "environment": ctx.environment,
            "lob": ctx.lob,
            **payload,
        }
        headers = inject_traceparent(trace.get_current_span())
        self._producer.produce(
            topic,
            value=json.dumps(event).encode(),
            headers=headers,
        )
        self._producer.poll(0)
```

#### `kafka_headers.py` — W3C traceparent inject/extract

```python
from opentelemetry import trace
from opentelemetry.propagators.b3 import B3Format
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

_propagator = TraceContextTextMapPropagator()

def inject_traceparent(span: trace.Span) -> list[tuple[str, str]]:
    carrier = {}
    ctx = trace.set_span_in_context(span)
    _propagator.inject(carrier, context=ctx)
    return [(k, v.encode()) for k, v in carrier.items()]

def extract_traceparent(headers: list[tuple[str, bytes]]) -> trace.Context:
    carrier = {k: v.decode() for k, v in headers if k in ("traceparent", "tracestate")}
    return _propagator.extract(carrier)
```

#### `decorators.py` — convenience wrappers for instrumentation

```python
import functools

def observe_llm_call(tracer: ObsTracer):
    def decorator(fn):
        @functools.wraps(fn)
        def wrapper(self, prompt: str, ctx: ObsContext, **kwargs):
            extra = {
                "model_provider": self.provider,
                "model_name": self.model_name,
                "prompt_template_id": kwargs.get("template_id"),
                "prompt_hash": hashlib.sha256(prompt.encode()).hexdigest(),
            }
            with tracer.span("llm_call", ctx, "LLM_CALL_STARTED", extra):
                result = fn(self, prompt, ctx, **kwargs)
                extra.update({
                    "input_tokens": result.usage.input_tokens,
                    "output_tokens": result.usage.output_tokens,
                    "finish_reason": result.finish_reason,
                    "estimated_cost": cost_calculator.compute(self.model_name, result.usage),
                    "response_entropy": entropy_calculator.compute(result.text),
                })
            return result
        return wrapper
    return decorator

def observe_tool_call(tracer: ObsTracer):
    def decorator(fn):
        @functools.wraps(fn)
        def wrapper(self, input_data: dict, ctx: ObsContext, **kwargs):
            extra = {
                "tool_id": self.tool_id,
                "tool_name": self.tool_name,
                "tool_type": self.tool_type,
                "input_schema_valid": self.validate_input(input_data),
            }
            with tracer.span("tool_call", ctx, "TOOL_CALL_STARTED", extra):
                result = fn(self, input_data, ctx, **kwargs)
                extra.update({
                    "http_status": getattr(result, "status_code", None),
                    "response_size_bytes": len(str(result)),
                })
            return result
        return wrapper
    return decorator

def observe_rag_retrieval(tracer: ObsTracer):
    def decorator(fn):
        @functools.wraps(fn)
        def wrapper(self, query: str, ctx: ObsContext, **kwargs):
            extra = {
                "rag_id": self.rag_id,
                "knowledge_base": self.knowledge_base_name,
                "query_hash": hashlib.sha256(query.encode()).hexdigest(),
                "top_k": kwargs.get("top_k", self.default_top_k),
            }
            with tracer.span("rag_retrieval", ctx, "RAG_RETRIEVAL_STARTED", extra):
                result = fn(self, query, ctx, **kwargs)
                extra.update({
                    "retrieved_chunk_count": len(result.chunks),
                    "avg_relevance_score": result.avg_score,
                    "no_result_flag": len(result.chunks) == 0,
                    "context_tokens": result.context_tokens,
                    "citation_coverage_pct": result.citation_coverage,
                })
            return result
        return wrapper
    return decorator
```

#### `quality.py` — quality signal computation

```python
import math
from collections import Counter

class EntropyCalculator:
    def compute(self, text: str) -> float:
        tokens = text.split()
        if not tokens:
            return 0.0
        counts = Counter(tokens)
        total = len(tokens)
        return -sum((c / total) * math.log2(c / total) for c in counts.values())

class FaithfulnessScorer:
    def compute(self, context: str, response: str) -> float:
        # Token overlap: fraction of response tokens present in context
        ctx_tokens = set(context.lower().split())
        resp_tokens = set(response.lower().split())
        if not resp_tokens:
            return 0.0
        overlap = ctx_tokens.intersection(resp_tokens)
        return len(overlap) / len(resp_tokens)

class TokenCostCalculator:
    # Pricing table: (input_per_1k, output_per_1k) in USD
    PRICING = {
        "gpt-4o": (0.005, 0.015),
        "gemini-1.5-pro": (0.00125, 0.005),
        "claude-sonnet-4-6": (0.003, 0.015),
    }

    def compute(self, model_name: str, usage) -> float:
        pricing = self.PRICING.get(model_name, (0.01, 0.03))
        return (usage.input_tokens / 1000 * pricing[0] +
                usage.output_tokens / 1000 * pricing[1])
```

---

## 3a. Langfuse Integration — LLM, RAG & Agent Trace Layer

Langfuse is a self-hosted LLM observability platform that handles the AI quality and trace layer. It runs alongside the custom Observability SDK — services emit to both. Langfuse requires **zero new infrastructure** beyond a Docker container backed by a dedicated PostgreSQL database.

### 3a.1 Why Langfuse (not custom-built)

| What you would build custom | What Langfuse gives you instead | Time saved |
|---|---|---|
| `obs_llm_events` nested trace view | Full trace tree explorer in Langfuse UI | 4–6 weeks |
| Custom `FaithfulnessScorer` (token overlap) | LLM-as-judge evals with Gemini/Claude | 3–4 weeks |
| `PromptTemplateFactory` versioning | Langfuse Prompt Management with A/B test | 2–3 weeks |
| Feedback-to-trace correlation query | `langfuse.score(trace_id=...)` — one call | 1–2 weeks |
| RAG span (chunk count, relevance, no-result) | Native `as_type="retrieval"` span | 2–3 weeks |
| Production dataset curation for fine-tuning | Langfuse Datasets UI | Not planned today |

**Total estimated custom build avoided: 12–18 weeks of engineering.**

### 3a.2 Deployment

```yaml
# Add to platform Helm chart / docker-compose
langfuse:
  image: ghcr.io/langfuse/langfuse:latest
  environment:
    DATABASE_URL: postgresql://langfuse:${LANGFUSE_DB_PASS}@postgres/langfuse
    NEXTAUTH_SECRET: ${LANGFUSE_SECRET}
    SALT: ${LANGFUSE_SALT}
    NEXTAUTH_URL: http://langfuse.internal:3000
    LANGFUSE_ENABLE_EXPERIMENTAL_FEATURES: "true"
  ports:
    - "3000:3000"
```

Run database migrations on first deploy:
```bash
docker run --rm ghcr.io/langfuse/langfuse:latest node_modules/.bin/prisma migrate deploy
```

**Multi-tenancy:** Create one Langfuse **Project** per `application_id` (or per LOB). Each project gets independent API keys. This maps directly to your existing `application_registry`.

### 3a.3 Shared Configuration (add to each service's settings)

```python
# config/settings.py — add to existing Pydantic Settings
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    # ... existing fields ...
    LANGFUSE_PUBLIC_KEY: str = ""
    LANGFUSE_SECRET_KEY: str = ""
    LANGFUSE_HOST: str = "http://langfuse.internal:3000"
    LANGFUSE_ENABLED: bool = True          # set False in unit tests
```

```python
# shared/langfuse_client.py — one shared initialisation
from langfuse import Langfuse
from config.settings import get_settings

_settings = get_settings()

langfuse = Langfuse(
    public_key=_settings.LANGFUSE_PUBLIC_KEY,
    secret_key=_settings.LANGFUSE_SECRET_KEY,
    host=_settings.LANGFUSE_HOST,
    enabled=_settings.LANGFUSE_ENABLED,
)
```

### 3a.4 GSSP GS — LLM Call Tracing

**File to modify:** `query/generators/vertexai_generator.py` (and equivalent files for Claude, Llama generators)

```python
from langfuse.decorators import observe, langfuse_context
from shared.langfuse_client import langfuse

@observe(as_type="generation", name="llm-call")
async def generate(self, prompt: str, ctx: ObsContext) -> GenerationResult:
    result = await self.vertex_client.generate(prompt)

    langfuse_context.update_current_observation(
        model=self.model_name,                          # "gemini-1.5-pro"
        model_parameters={
            "temperature": self.temperature,
            "max_tokens": self.max_output_tokens,
        },
        # Do NOT pass raw prompt/response — use hashes only
        input={"prompt_hash": sha256(prompt.encode()).hexdigest()},
        output={"response_hash": sha256(result.text.encode()).hexdigest()},
        usage={
            "input": result.usage.input_tokens,
            "output": result.usage.output_tokens,
            "total": result.usage.total_tokens,
        },
        metadata={
            "correlation_id": ctx.correlation_id,
            "application_id": ctx.application_id,
            "prompt_template_id": self.prompt_template_id,
            "finish_reason": result.finish_reason,
            "safety_blocked": result.safety_blocked,
            "rate_limit_hit": result.rate_limit_hit,
        },
    )
    return result
```

**What this captures automatically (no extra code):** `latency_ms`, `estimated_cost_usd`, `model_name`, `input_tokens`, `output_tokens` — all the fields currently ❌ in the coverage matrix.

### 3a.5 GSSP QS — Full RAG Pipeline Trace

**File to modify:** `query/execute_pipeline.py`

```python
from langfuse.decorators import observe, langfuse_context

@observe(name="rag-pipeline")
async def execute_pipeline(self, query: str, ctx: ObsContext) -> QueryResult:
    # Set trace-level context — applies to all child spans
    langfuse_context.update_current_trace(
        session_id=ctx.session_id,
        user_id=ctx.user_hash,
        tags=[ctx.lob, ctx.environment, "rag"],
        metadata={"correlation_id": ctx.correlation_id, "application_id": ctx.application_id},
    )

    await self._run_guardrail(query, ctx)
    cache_result = await self._check_semantic_cache(query, ctx)
    if cache_result:
        return cache_result

    chunks = await self._retrieve_chunks(query, ctx)
    return await self._generate_answer(query, chunks, ctx)


@observe(name="guardrail-check")
async def _run_guardrail(self, query: str, ctx: ObsContext):
    result = await self.guardrail_client.evaluate(query)
    langfuse_context.update_current_observation(
        metadata={"decision": result.decision, "risk_score": result.risk_score},
    )
    return result


@observe(name="cache-lookup", as_type="retrieval")
async def _check_semantic_cache(self, query: str, ctx: ObsContext):
    result = await self.cache.lookup(query)
    langfuse_context.update_current_observation(
        metadata={"cache_hit": result is not None},
    )
    return result


@observe(name="retrieval", as_type="retrieval")
async def _retrieve_chunks(self, query: str, ctx: ObsContext) -> list:
    chunks = await self.retrieval_client.retrieve(query)
    langfuse_context.update_current_observation(
        input={"query_hash": sha256(query.encode()).hexdigest()},
        output={"document_count": len(chunks)},
        metadata={
            "retrieved_chunk_count": len(chunks),
            "avg_relevance_score": mean(c.score for c in chunks) if chunks else 0.0,
            "no_result_flag": len(chunks) == 0,
            "knowledge_base": self.config.knowledge_base_name,
        },
    )
    return chunks


@observe(name="generation", as_type="generation")
async def _generate_answer(self, query: str, chunks: list, ctx: ObsContext):
    # Generation tracing handled by GSSP GS's @observe on its generate()
    return await self.generation_client.generate(query, chunks)
```

**Resulting Langfuse trace tree for one RAG request:**
```
rag-pipeline  (1.24s total)
  ├── guardrail-check   (44ms)  decision=allow  risk_score=0.02
  ├── cache-lookup      (11ms)  cache_hit=false
  ├── retrieval         (337ms) chunks=5  avg_score=0.87  no_result=false
  └── generation        (851ms) gemini-1.5-pro  660 tokens  $0.00132
```

### 3a.6 Agent Executor — Agent Step Hierarchy

**File to modify:** `executor/services/agent_execution_service.py`

```python
from langfuse.decorators import observe, langfuse_context

@observe(name="agent-execution")
async def execute(self, request: AgentExecutionRequest) -> AgentResult:
    langfuse_context.update_current_trace(
        metadata={
            "correlation_id": request.correlation_id,
            "agent_id": self.agent_config.agent_id,
            "agent_version": self.agent_config.version,
        },
    )
    for step_num in range(self.max_steps):
        result = await self._execute_step(step_num, request)
        if result.done:
            break
    return result


@observe(name="agent-step")
async def _execute_step(self, step_num: int, request) -> StepResult:
    langfuse_context.update_current_observation(
        metadata={"step_number": step_num, "step_name": self.current_step_name},
    )
    # Tool and LLM calls inside the step are automatically child spans
    # because they are also decorated with @observe
    ...
```

### 3a.7 User Feedback — Link Ratings to Traces

**File to modify:** `feedback/api/v1/feedback.py`

```python
from shared.langfuse_client import langfuse

@router.post("/feedback", status_code=201)
async def submit_feedback(feedback: FeedbackRequest):
    # Existing DB write — unchanged
    record = await repo.create(feedback)

    # Link the rating to the exact Langfuse trace for this agent execution
    if feedback.correlation_id:
        langfuse.score(
            trace_id=feedback.correlation_id,
            name="user-feedback-rating",
            value=feedback.rating / 5.0,          # normalise 1–5 → 0.0–1.0
            comment=redact_pii(feedback.comment),
            data_type="NUMERIC",
        )
        if feedback.thumbs:
            langfuse.score(
                trace_id=feedback.correlation_id,
                name="user-feedback-thumbs",
                value=1.0 if feedback.thumbs == "up" else 0.0,
                data_type="BOOLEAN",
            )

    return {"feedback_id": record.id}
```

### 3a.8 Prompt Management — Replacing PromptTemplateFactory

**Where to change:** `gssp-gs/query/factories/prompt_template_factory.py`

```python
# Before — fetches from PostgreSQL prompt_template table
class PromptTemplateFactory:
    async def get_template(self, template_id: str) -> str:
        return await self.db.fetch_one("SELECT template FROM prompt_template WHERE id = $1", template_id)

# After — fetches from Langfuse (versioned, A/B testable, auditable)
from shared.langfuse_client import langfuse

class PromptTemplateFactory:
    async def get_template(self, template_id: str, label: str = "production") -> CompiledPrompt:
        prompt = langfuse.get_prompt(template_id, label=label)
        # SDK automatically attaches prompt_name + version to every trace that uses this prompt
        return prompt
```

**Langfuse Prompt Management benefits over raw DB:**
- Full version history with diff view
- Rollback to any previous version in one click
- A/B testing: route N% of traffic to `experiment` label, compare faithfulness scores
- Automatic `prompt_hash` tracking for drift detection across deployments

### 3a.9 Evaluations — Replacing FaithfulnessScorer

Configure once in Langfuse UI or via API — no custom scorer code needed:

```python
# Run during Phase 3 setup — registers evaluator that runs on all future RAG traces
from langfuse import Langfuse

langfuse = Langfuse()

# Faithfulness: is the answer grounded in the retrieved context?
langfuse.create_llm_as_judge_eval(
    name="rag-faithfulness",
    prompt_template="""
        Context: {{retrieved_context}}
        Answer: {{output}}
        Rate how faithful the answer is to the context on a scale 0.0 to 1.0.
        Return only the number.
    """,
    variables={"retrieved_context": "retrieval.output", "output": "generation.output"},
    model="gemini-1.5-flash",         # cheap model for eval
    score_name="faithfulness",
    min_score=0.0,
    max_score=1.0,
)

# Hallucination: is the model making up facts not in context?
langfuse.create_llm_as_judge_eval(
    name="hallucination-check",
    prompt_template="""
        Context: {{retrieved_context}}
        Answer: {{output}}
        Does the answer contain claims not supported by the context? Answer yes or no.
    """,
    variables={"retrieved_context": "retrieval.output", "output": "generation.output"},
    model="gemini-1.5-flash",
    score_name="hallucination",
    data_type="BOOLEAN",
)
```

These scores are stored in Langfuse, queryable via the Langfuse SDK, and can be exported to PostgreSQL `daily_rag_quality` via a nightly sync job.

### 3a.10 Nightly Langfuse → PostgreSQL Sync (for Grafana dashboards)

```python
# aggregation/langfuse_to_postgres.py — run as K8s CronJob nightly
from langfuse import Langfuse
import psycopg2
from datetime import date, timedelta

langfuse = Langfuse()

def sync_rag_quality(quality_date: date):
    # Fetch all RAG traces from yesterday
    traces = langfuse.get_traces(
        from_timestamp=quality_date,
        to_timestamp=quality_date + timedelta(days=1),
        tags=["rag"],
    )
    # Aggregate faithfulness scores per rag_id
    by_rag = {}
    for trace in traces:
        rag_id = trace.metadata.get("rag_id")
        scores = [s.value for s in trace.scores if s.name == "faithfulness"]
        if rag_id and scores:
            by_rag.setdefault(rag_id, []).extend(scores)

    # Upsert into PostgreSQL daily_rag_quality
    with psycopg2.connect(PG_DSN) as conn:
        for rag_id, scores in by_rag.items():
            conn.execute("""
                INSERT INTO daily_rag_quality (quality_date, rag_id, avg_faithfulness_score, sample_count)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (quality_date, rag_id) DO UPDATE
                SET avg_faithfulness_score = EXCLUDED.avg_faithfulness_score,
                    sample_count = EXCLUDED.sample_count
            """, (quality_date, rag_id, mean(scores), len(scores)))
```

---

## 4. Kafka Layer — What to Build

### 4.1 Topic Configuration

Deploy the following Kafka topics via IaC (Terraform or `kafka-topics.sh`):

```bash
# Example topic creation (adjust partitions/replication for production)
kafka-topics.sh --create --topic ai-obs-traces       --partitions 12 --replication-factor 3
kafka-topics.sh --create --topic ai-obs-events       --partitions 24 --replication-factor 3
kafka-topics.sh --create --topic ai-obs-metrics      --partitions 6  --replication-factor 3
kafka-topics.sh --create --topic ai-obs-quality      --partitions 6  --replication-factor 3
kafka-topics.sh --create --topic ai-obs-cost         --partitions 6  --replication-factor 3
kafka-topics.sh --create --topic ai-obs-anomalies    --partitions 6  --replication-factor 3
kafka-topics.sh --create --topic ai-obs-incidents    --partitions 3  --replication-factor 3

# Dead-letter queues for each topic
kafka-topics.sh --create --topic ai-obs-events-dlq   --partitions 6  --replication-factor 3
```

**Topic retention:** 7 days for all observability topics. Downstream sinks (ES, PG, S3) are the durable stores.

### 4.2 Message Schema

All Kafka messages must use a common envelope. Register schemas in Confluent Schema Registry if available.

```json
{
  "event_id": "uuid",
  "event_type": "string",
  "schema_version": "1.0",
  "timestamp": "ISO8601",
  "correlation_id": "string",
  "span_id": "string",
  "parent_span_id": "string | null",
  "application_id": "string",
  "lob": "string",
  "environment": "string",
  "payload": { }
}
```

**Kafka message headers** (set by SDK producer):
```
traceparent: 00-{trace_id}-{span_id}-01
tracestate: intentiq={application_id};env={environment}
content-type: application/json
schema-version: 1.0
```

### 4.3 Dead-Letter Queue Handler

Build a small consumer that reads from `*-dlq` topics, logs with context to Elasticsearch `ai-obs-errors-*`, and optionally alerts via Grafana.

```python
class DLQConsumer:
    def __init__(self, consumer: Consumer, es_client: Elasticsearch):
        self._consumer = consumer
        self._es = es_client

    def run(self):
        self._consumer.subscribe(["ai-obs-events-dlq", "ai-obs-traces-dlq"])
        while True:
            msg = self._consumer.poll(1.0)
            if msg and not msg.error():
                self._es.index(
                    index="ai-obs-errors-dlq",
                    document={
                        "timestamp": datetime.utcnow().isoformat(),
                        "topic": msg.topic(),
                        "raw_value": msg.value().decode("utf-8", errors="replace"),
                        "error_code": "DLQ_MESSAGE",
                        "headers": dict(msg.headers() or []),
                    }
                )
```

---

## 5. Telemetry Processor — What to Build

### 5.1 Application Structure

```text
telemetry-processor/
├── processor/
│   ├── __init__.py
│   ├── main.py              ← entry point; starts Kafka consumer loop
│   ├── pipeline.py          ← orchestrates all steps in order
│   ├── steps/
│   │   ├── trace_extractor.py   ← W3C traceparent extraction
│   │   ├── redactor.py          ← PII field redaction
│   │   ├── enricher.py          ← app/agent/tool metadata join
│   │   ├── error_mapper.py      ← raw exception → error catalog code
│   │   ├── cost_calculator.py   ← token cost + Redis budget accumulator
│   │   ├── faithfulness.py      ← RAG faithfulness scoring
│   │   ├── rollup.py            ← hourly/daily aggregate writes
│   │   ├── slo_evaluator.py     ← burn-rate computation
│   │   └── archiver.py          ← S3 payload storage
│   ├── sinks/
│   │   ├── elasticsearch.py     ← index routing + write
│   │   ├── postgres.py          ← aggregate + compliance writes
│   │   └── s3.py               ← payload archival
│   └── cache/
│       └── registry_cache.py    ← Redis-backed registry cache (5 min TTL)
├── tests/
└── Dockerfile
```

### 5.2 Pipeline Orchestration

```python
class TelemetryPipeline:
    def __init__(self, steps: list, sinks: list):
        self._steps = steps
        self._sinks = sinks

    def process(self, raw_event: dict, kafka_headers: dict) -> None:
        event = raw_event.copy()
        try:
            for step in self._steps:
                event = step.process(event, kafka_headers)
                if event is None:
                    return  # step decided to drop event (e.g., schema invalid)
            for sink in self._sinks:
                sink.write(event)
        except Exception as e:
            log_to_dlq(raw_event, error=str(e))
```

### 5.3 PII Redactor

```python
import re

class PiiRedactor:
    # Patterns applied to string field values
    PATTERNS = [
        (re.compile(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'), "[EMAIL]"),
        (re.compile(r'\b\d{3}[-.]?\d{3}[-.]?\d{4}\b'), "[PHONE]"),
        (re.compile(r'\b(?:\d[ -]*?){13,16}\b'), "[CARD]"),
        (re.compile(r'\b\d{3}-\d{2}-\d{4}\b'), "[SSN]"),
        (re.compile(r'\b[A-Z]{2}\d{6}[A-Z]\b'), "[PASSPORT]"),
    ]

    # Fields to skip (hashed IDs, metadata)
    SKIP_FIELDS = {"user_hash", "correlation_id", "span_id", "application_id", "event_id"}

    def process(self, event: dict, _headers: dict) -> dict:
        return self._redact_dict(event)

    def _redact_dict(self, obj):
        if isinstance(obj, dict):
            return {k: obj[k] if k in self.SKIP_FIELDS else self._redact_dict(obj[k]) for k in obj}
        if isinstance(obj, str):
            for pattern, replacement in self.PATTERNS:
                obj = pattern.sub(replacement, obj)
        if isinstance(obj, list):
            return [self._redact_dict(item) for item in obj]
        return obj
```

### 5.4 Metadata Enricher

```python
class MetadataEnricher:
    def __init__(self, registry_cache: "RegistryCache"):
        self._cache = registry_cache

    def process(self, event: dict, _headers: dict) -> dict:
        app_id = event.get("application_id")
        agent_id = event.get("agent_id")
        tool_id = event.get("tool_id")

        if app_id:
            app = self._cache.get_application(app_id)
            if app:
                event.setdefault("lob", app["lob"])
                event.setdefault("soe_id", app["soe_id"])
                event.setdefault("app_container", app["app_container"])
                event.setdefault("owner_team", app["owner_team"])

        if agent_id:
            agent = self._cache.get_agent(agent_id)
            if agent:
                event.setdefault("agent_name", agent["agent_name"])
                event.setdefault("agent_type", agent["agent_type"])
                event.setdefault("agent_version", agent["agent_version"])

        if tool_id:
            tool = self._cache.get_tool(tool_id)
            if tool:
                event.setdefault("tool_name", tool["tool_name"])
                event.setdefault("tool_sla_ms", tool["sla_ms"])
                event["sla_breached"] = (
                    event.get("latency_ms", 0) > tool["sla_ms"]
                    if tool.get("sla_ms") else False
                )
        return event
```

### 5.5 Token Cost Calculator + Budget Accumulator

```python
import redis

class TokenCostCalculator:
    PRICING = {
        "gpt-4o": (0.005, 0.015),
        "gemini-1.5-pro": (0.00125, 0.005),
        "claude-sonnet-4-6": (0.003, 0.015),
    }

    def __init__(self, redis_client: redis.Redis, pg_conn):
        self._redis = redis_client
        self._pg = pg_conn

    def process(self, event: dict, _headers: dict) -> dict:
        if event.get("event_type") not in ("LLM_CALL_COMPLETED",):
            return event

        model = event.get("model_name", "")
        pricing = self.PRICING.get(model, (0.01, 0.03))
        cost = (event.get("input_tokens", 0) / 1000 * pricing[0] +
                event.get("output_tokens", 0) / 1000 * pricing[1])
        event["estimated_cost"] = round(cost, 6)

        # Accumulate to Redis budget counter
        app_id = event.get("application_id", "")
        env = event.get("environment", "prod")
        today = datetime.utcnow().strftime("%Y-%m-%d")
        redis_key = f"budget:{app_id}:{env}:{model}:daily:{today}"
        new_total = self._redis.incrbyfloat(redis_key, cost)
        self._redis.expire(redis_key, 86400 * 2)  # TTL: 2 days

        # Check against budget_limits table
        limit = self._get_budget_limit(app_id, env, model, "daily")
        if limit:
            pct = (new_total / limit["max_spend_usd"]) * 100
            event["budget_utilization_pct"] = round(pct, 2)
            if pct >= limit["alert_at_pct"]:
                self._emit_budget_alert(app_id, model, pct, new_total, limit)

        return event

    def _get_budget_limit(self, app_id, env, model, period) -> dict | None:
        # Query PostgreSQL budget_limits with Redis caching
        ...

    def _emit_budget_alert(self, app_id, model, pct, spent, limit):
        # Produce BUDGET_THRESHOLD_EXCEEDED event to ai-obs-events topic
        ...
```

### 5.6 SLO Evaluator

```python
class SloEvaluator:
    # SLO target definitions per application (loaded from PostgreSQL)
    # burn_rate = error_budget_consumed_in_window / (window_duration / slo_period_hours)

    def __init__(self, pg_conn, redis_client: redis.Redis):
        self._pg = pg_conn
        self._redis = redis_client

    def process(self, event: dict, _headers: dict) -> dict:
        if event.get("event_type") != "REQUEST_COMPLETED":
            return event

        app_id = event.get("application_id")
        latency = event.get("latency_ms", 0)
        status = event.get("status")

        # Update rolling counters in Redis (1h and 6h windows)
        self._update_window_counters(app_id, status, latency)

        # Compute burn rates and write to PostgreSQL daily_slo_compliance
        burn_1h = self._compute_burn_rate(app_id, window_hours=1)
        burn_6h = self._compute_burn_rate(app_id, window_hours=6)
        self._write_compliance(app_id, burn_1h, burn_6h)

        return event

    def _compute_burn_rate(self, app_id: str, window_hours: int) -> float:
        # error_budget_consumed_in_window / (window_hours / slo_period_hours)
        errors = self._redis.get(f"slo:{app_id}:errors:{window_hours}h") or 0
        total = self._redis.get(f"slo:{app_id}:total:{window_hours}h") or 1
        error_rate = int(errors) / int(total)
        slo_target = self._get_slo_target(app_id)  # e.g., 0.999 = 99.9% availability
        error_budget = 1 - slo_target
        if error_budget == 0:
            return 0.0
        consumed = error_rate / error_budget
        slo_period_hours = 24 * 30  # monthly SLO
        return consumed / (window_hours / slo_period_hours)
```

### 5.7 S3 Archiver

```python
import boto3
from botocore.exceptions import ClientError

class S3Archiver:
    EVENT_TO_PREFIX = {
        "LLM_CALL_COMPLETED": "redacted-prompts",
        "RAG_RETRIEVAL_COMPLETED": "rag-contexts",
        "AGENT_COMPLETED": "raw-traces",
    }

    def __init__(self, bucket: str):
        self._s3 = boto3.client("s3")
        self._bucket = bucket

    def process(self, event: dict, _headers: dict) -> dict:
        event_type = event.get("event_type", "")
        prefix = self.EVENT_TO_PREFIX.get(event_type)
        if not prefix:
            return event

        # Build S3 key
        ts = datetime.utcnow()
        app_id = event.get("application_id", "unknown")
        correlation_id = event.get("correlation_id", "unknown")
        key = f"{prefix}/year={ts.year}/month={ts.month:02d}/day={ts.day:02d}/application_id={app_id}/correlation_id={correlation_id}/{event_type.lower()}.json"

        # Extract and clear large/sensitive payload fields before archiving
        payload = self._extract_payload(event)
        self._s3.put_object(
            Bucket=self._bucket,
            Key=key,
            Body=json.dumps(payload).encode(),
            ServerSideEncryption="aws:kms",
        )

        # Replace payload with S3 URI reference
        event["s3_payload_uri"] = f"s3://{self._bucket}/{key}"
        event.pop("raw_prompt", None)
        event.pop("raw_response", None)
        event.pop("rag_context_chunks", None)

        return event
```

### 5.8 Elasticsearch Index Router

```python
class ElasticsearchSink:
    EVENT_TO_INDEX = {
        "REQUEST_RECEIVED": "requests",
        "REQUEST_COMPLETED": "requests",
        "REQUEST_FAILED": "requests",
        "AGENT_STARTED": "agent-steps",
        "AGENT_STEP_COMPLETED": "agent-steps",
        "AGENT_FAILED": "agent-steps",
        "LLM_CALL_COMPLETED": "llm-calls",
        "LLM_CALL_FAILED": "llm-calls",
        "TOOL_CALL_COMPLETED": "tool-calls",
        "TOOL_CALL_FAILED": "tool-calls",
        "RAG_RETRIEVAL_COMPLETED": "rag-events",
        "RAG_NO_RESULT": "rag-events",
        "GUARDRAIL_EVALUATED": "guardrail-events",
        "GUARDRAIL_BLOCKED": "guardrail-events",
        "FEEDBACK_SUBMITTED": "feedback",
        "ERROR_OCCURRED": "errors",
        "ANOMALY_DETECTED": "anomalies",
    }

    def __init__(self, es_client: Elasticsearch):
        self._es = es_client

    def write(self, event: dict) -> None:
        event_type = event.get("event_type", "")
        base_index = self.EVENT_TO_INDEX.get(event_type, "events")
        lob = event.get("lob", "platform").lower().replace(" ", "-")
        ts = datetime.utcnow().strftime("%Y.%m")
        index = f"ai-obs-{lob}-{base_index}-{ts}"

        self._es.index(index=index, document=event)
```

### 5.9 Rollup Generator (PostgreSQL)

```python
class RollupGenerator:
    # Writes to hourly aggregate tables on each event
    # Uses INSERT ... ON CONFLICT DO UPDATE for idempotent upserts

    def write(self, event: dict) -> None:
        event_type = event.get("event_type")
        if event_type in ("REQUEST_COMPLETED", "REQUEST_FAILED"):
            self._upsert_hourly_application(event)
        elif event_type in ("AGENT_COMPLETED", "AGENT_FAILED"):
            self._upsert_hourly_agent(event)
        elif event_type in ("TOOL_CALL_COMPLETED", "TOOL_CALL_FAILED"):
            self._upsert_hourly_tool(event)
        elif event_type in ("LLM_CALL_COMPLETED", "LLM_CALL_FAILED"):
            self._upsert_hourly_llm(event)
        elif event_type in ("RAG_RETRIEVAL_COMPLETED", "RAG_NO_RESULT"):
            self._upsert_hourly_rag(event)

    def _upsert_hourly_application(self, event: dict):
        hour = datetime.utcnow().replace(minute=0, second=0, microsecond=0)
        sql = """
            INSERT INTO agg_hourly_application_metrics
                (hour_timestamp, application_id, request_count, success_count, error_count,
                 avg_latency_ms, p95_latency_ms, total_tokens, estimated_cost)
            VALUES (%s, %s, 1,
                %s, %s, %s, %s, %s, %s)
            ON CONFLICT (hour_timestamp, application_id)
            DO UPDATE SET
                request_count   = agg_hourly_application_metrics.request_count + 1,
                success_count   = agg_hourly_application_metrics.success_count + EXCLUDED.success_count,
                error_count     = agg_hourly_application_metrics.error_count + EXCLUDED.error_count,
                avg_latency_ms  = (agg_hourly_application_metrics.avg_latency_ms *
                                   agg_hourly_application_metrics.request_count + EXCLUDED.avg_latency_ms) /
                                  (agg_hourly_application_metrics.request_count + 1),
                total_tokens    = agg_hourly_application_metrics.total_tokens + EXCLUDED.total_tokens,
                estimated_cost  = agg_hourly_application_metrics.estimated_cost + EXCLUDED.estimated_cost
        """
        is_success = 1 if event.get("status") == "success" else 0
        with self._pg.cursor() as cur:
            cur.execute(sql, (
                hour, event["application_id"],
                is_success, 1 - is_success,
                event.get("latency_ms", 0), event.get("latency_ms", 0),
                event.get("total_tokens", 0), event.get("estimated_cost", 0),
            ))
        self._pg.commit()
```

---

## 6. Storage Layer — What to Build

### 6.1 Elasticsearch Index Templates

Create one index template per event category. Apply via CI or `observability-iac` pipeline.

```json
// File: elasticsearch/index-templates/ai-obs-requests.json
{
  "index_patterns": ["ai-obs-*-requests-*"],
  "template": {
    "settings": {
      "number_of_shards": 2,
      "number_of_replicas": 1,
      "index.lifecycle.name": "hot-warm-30d"
    },
    "mappings": {
      "properties": {
        "event_id":        { "type": "keyword" },
        "event_type":      { "type": "keyword" },
        "timestamp":       { "type": "date" },
        "correlation_id":  { "type": "keyword" },
        "span_id":         { "type": "keyword" },
        "application_id":  { "type": "keyword" },
        "lob":             { "type": "keyword" },
        "environment":     { "type": "keyword" },
        "status":          { "type": "keyword" },
        "latency_ms":      { "type": "integer" },
        "error_code":      { "type": "keyword" },
        "http_status":     { "type": "integer" },
        "total_tokens":    { "type": "long" },
        "estimated_cost":  { "type": "float" },
        "channel":         { "type": "keyword" },
        "request_type":    { "type": "keyword" }
      }
    }
  }
}
```

Create similar templates for: `llm-calls`, `tool-calls`, `rag-events`, `agent-steps`, `guardrail-events`, `feedback`, `errors`, `traces`, `anomalies`, `quality-scores`, `vector-health`.

**ILM Policies to create:**

```json
// hot-warm-30d: 30-day retention for operational events
// hot-warm-90d: 90-day retention for traces, errors, feedback
// compliance-180d: 180-day retention for guardrail events (compliance)
```

### 6.2 PostgreSQL Full Schema

All tables required, in dependency order:

```sql
-- === REGISTRIES ===

CREATE TABLE application_registry (
    application_id    VARCHAR(64) PRIMARY KEY,
    application_name  VARCHAR(256) NOT NULL,
    app_container     VARCHAR(128),
    csi_id            VARCHAR(64),
    soe_id            VARCHAR(64),
    lob               VARCHAR(64),
    owner_team        VARCHAR(128),
    support_contact   VARCHAR(256),
    environment       VARCHAR(32),
    tier              VARCHAR(16) DEFAULT 'standard', -- 'critical' | 'standard' | 'low'
    status            VARCHAR(32) DEFAULT 'active',
    created_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE agent_registry (
    agent_id          VARCHAR(64) PRIMARY KEY,
    application_id    VARCHAR(64) REFERENCES application_registry,
    agent_name        VARCHAR(256),
    agent_version     VARCHAR(32),
    agent_type        VARCHAR(64),   -- 'single' | 'multi' | 'loop' | 'tool_augmented'
    framework         VARCHAR(64),
    owner_team        VARCHAR(128),
    active_flag       BOOLEAN DEFAULT TRUE,
    created_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE tool_registry (
    tool_id           VARCHAR(64) PRIMARY KEY,
    application_id    VARCHAR(64) REFERENCES application_registry,
    tool_name         VARCHAR(256),
    tool_type         VARCHAR(64),   -- 'rest_api' | 'db_query' | 'servicenow' | 'rag'
    endpoint          VARCHAR(512),
    version           VARCHAR(32),
    owner_team        VARCHAR(128),
    sla_ms            INTEGER,
    active_flag       BOOLEAN DEFAULT TRUE,
    created_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE rag_registry (
    rag_id                VARCHAR(64) PRIMARY KEY,
    application_id        VARCHAR(64) REFERENCES application_registry,
    knowledge_base_name   VARCHAR(256),
    vector_index_name     VARCHAR(256),
    embedding_model       VARCHAR(128),
    refresh_frequency     VARCHAR(32),    -- 'hourly' | 'daily' | 'weekly' | 'manual'
    freshness_sla_hours   INTEGER DEFAULT 24,
    owner_team            VARCHAR(128),
    active_flag           BOOLEAN DEFAULT TRUE
);

CREATE TABLE prompt_template_registry (
    prompt_template_id      VARCHAR(64) PRIMARY KEY,
    agent_id                VARCHAR(64) REFERENCES agent_registry,
    template_name           VARCHAR(256),
    template_version        VARCHAR(32),
    model_name              VARCHAR(128),
    active_flag             BOOLEAN DEFAULT TRUE,
    created_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- === GOVERNANCE TABLES ===

CREATE TABLE error_code_catalog (
    error_code        VARCHAR(64) PRIMARY KEY,
    error_category    VARCHAR(64),
    severity          VARCHAR(16),   -- 'critical' | 'high' | 'medium' | 'low'
    description       TEXT,
    runbook_url       VARCHAR(512),
    owner_team        VARCHAR(128)
);

CREATE TABLE kpi_definition (
    kpi_id            VARCHAR(64) PRIMARY KEY,
    application_id    VARCHAR(64) REFERENCES application_registry,
    agent_id          VARCHAR(64),
    kpi_name          VARCHAR(256) NOT NULL,
    kpi_category      VARCHAR(64),
    formula           TEXT,
    data_source       VARCHAR(64),
    threshold_green   NUMERIC(12,4),
    threshold_yellow  NUMERIC(12,4),
    threshold_red     NUMERIC(12,4),
    owner             VARCHAR(128),
    active_flag       BOOLEAN DEFAULT TRUE,
    created_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE metric_catalog (
    metric_id         VARCHAR(64) PRIMARY KEY,
    metric_name       VARCHAR(256),
    metric_aliases    TEXT[],
    metric_category   VARCHAR(64),
    formula           TEXT,
    source_table      VARCHAR(128),
    time_grain        VARCHAR(32),   -- 'hourly' | 'daily'
    dimensions        TEXT[],
    owner             VARCHAR(128),
    active_flag       BOOLEAN DEFAULT TRUE
);

CREATE TABLE alert_threshold (
    alert_id              VARCHAR(64) PRIMARY KEY,
    metric_id             VARCHAR(64) REFERENCES metric_catalog,
    application_id        VARCHAR(64),
    agent_id              VARCHAR(64),
    tool_id               VARCHAR(64),
    threshold_value       NUMERIC(12,4),
    comparison_operator   VARCHAR(8),    -- '>' | '<' | '>=' | '<='
    window_minutes        INTEGER,
    severity              VARCHAR(16),
    notification_channel  VARCHAR(128),
    active_flag           BOOLEAN DEFAULT TRUE
);

CREATE TABLE budget_limits (
    application_id    VARCHAR(64),
    environment       VARCHAR(32),
    model_id          VARCHAR(128),
    period            VARCHAR(16),      -- 'daily' | 'monthly'
    max_spend_usd     DECIMAL(10,4),
    alert_at_pct      INT DEFAULT 80,
    PRIMARY KEY (application_id, environment, model_id, period)
);

-- === FEEDBACK ===

CREATE TABLE feedback_case (
    feedback_id           VARCHAR(64) PRIMARY KEY,
    correlation_id        VARCHAR(256),
    application_id        VARCHAR(64),
    agent_id              VARCHAR(64),
    rating                INTEGER CHECK (rating BETWEEN 1 AND 5),
    thumbs                VARCHAR(8),    -- 'up' | 'down'
    sentiment             VARCHAR(16),
    category              VARCHAR(64),
    comment_redacted      TEXT,
    status                VARCHAR(32) DEFAULT 'open',
    linked_incident_id    VARCHAR(128),
    submitted_by_role     VARCHAR(32),
    created_at            TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- === HOURLY AGGREGATES ===

CREATE TABLE agg_hourly_application_metrics (
    hour_timestamp            TIMESTAMP,
    application_id            VARCHAR(64),
    request_count             BIGINT DEFAULT 0,
    success_count             BIGINT DEFAULT 0,
    error_count               BIGINT DEFAULT 0,
    avg_latency_ms            NUMERIC(12,2),
    p95_latency_ms            NUMERIC(12,2),
    total_tokens              BIGINT DEFAULT 0,
    estimated_cost            NUMERIC(12,6) DEFAULT 0,
    budget_utilization_pct    NUMERIC(5,2),
    positive_feedback_count   BIGINT DEFAULT 0,
    negative_feedback_count   BIGINT DEFAULT 0,
    PRIMARY KEY (hour_timestamp, application_id)
);

CREATE TABLE agg_hourly_agent_metrics (
    hour_timestamp            TIMESTAMP,
    application_id            VARCHAR(64),
    agent_id                  VARCHAR(64),
    request_count             BIGINT DEFAULT 0,
    success_count             BIGINT DEFAULT 0,
    error_count               BIGINT DEFAULT 0,
    avg_latency_ms            NUMERIC(12,2),
    p95_latency_ms            NUMERIC(12,2),
    avg_step_count            NUMERIC(8,2),
    loop_count                BIGINT DEFAULT 0,
    handoff_count             BIGINT DEFAULT 0,
    tool_call_count           BIGINT DEFAULT 0,
    tool_failure_count        BIGINT DEFAULT 0,
    rag_request_count         BIGINT DEFAULT 0,
    rag_no_result_count       BIGINT DEFAULT 0,
    total_tokens              BIGINT DEFAULT 0,
    estimated_cost            NUMERIC(12,6) DEFAULT 0,
    PRIMARY KEY (hour_timestamp, application_id, agent_id)
);

CREATE TABLE agg_hourly_tool_metrics (
    hour_timestamp            TIMESTAMP,
    application_id            VARCHAR(64),
    agent_id                  VARCHAR(64),
    tool_id                   VARCHAR(64),
    call_count                BIGINT DEFAULT 0,
    success_count             BIGINT DEFAULT 0,
    failure_count             BIGINT DEFAULT 0,
    timeout_count             BIGINT DEFAULT 0,
    retry_count               BIGINT DEFAULT 0,
    avg_latency_ms            NUMERIC(12,2),
    p95_latency_ms            NUMERIC(12,2),
    sla_breach_count          BIGINT DEFAULT 0,
    PRIMARY KEY (hour_timestamp, application_id, agent_id, tool_id)
);

CREATE TABLE agg_hourly_llm_metrics (
    hour_timestamp                      TIMESTAMP,
    application_id                      VARCHAR(64),
    agent_id                            VARCHAR(64),
    model_provider                      VARCHAR(64),
    model_name                          VARCHAR(128),
    prompt_template_id                  VARCHAR(64),
    llm_call_count                      BIGINT DEFAULT 0,
    input_tokens                        BIGINT DEFAULT 0,
    output_tokens                       BIGINT DEFAULT 0,
    total_tokens                        BIGINT DEFAULT 0,
    estimated_cost                      NUMERIC(12,6) DEFAULT 0,
    alternative_model_estimated_cost    NUMERIC(12,6),
    error_count                         BIGINT DEFAULT 0,
    rate_limit_count                    BIGINT DEFAULT 0,
    safety_block_count                  BIGINT DEFAULT 0,
    avg_latency_ms                      NUMERIC(12,2),
    p95_latency_ms                      NUMERIC(12,2),
    PRIMARY KEY (hour_timestamp, application_id, agent_id, model_provider, model_name, prompt_template_id)
);

CREATE TABLE agg_hourly_rag_metrics (
    hour_timestamp            TIMESTAMP,
    application_id            VARCHAR(64),
    agent_id                  VARCHAR(64),
    rag_id                    VARCHAR(64),
    retrieval_count           BIGINT DEFAULT 0,
    no_result_count           BIGINT DEFAULT 0,
    avg_relevance_score       NUMERIC(5,4),
    avg_retrieval_latency_ms  NUMERIC(12,2),
    citation_coverage_pct     NUMERIC(5,2),
    context_truncation_count  BIGINT DEFAULT 0,
    avg_faithfulness_score    NUMERIC(5,4),
    PRIMARY KEY (hour_timestamp, application_id, agent_id, rag_id)
);

-- === DAILY AGGREGATES ===

CREATE TABLE agg_daily_feedback_metrics (
    metric_date               DATE,
    application_id            VARCHAR(64),
    agent_id                  VARCHAR(64),
    positive_feedback_count   BIGINT DEFAULT 0,
    negative_feedback_count   BIGINT DEFAULT 0,
    neutral_feedback_count    BIGINT DEFAULT 0,
    avg_rating                NUMERIC(3,2),
    top_feedback_category     VARCHAR(64),
    PRIMARY KEY (metric_date, application_id, agent_id)
);

CREATE TABLE agg_daily_kpi_metrics (
    metric_date               DATE,
    kpi_id                    VARCHAR(64),
    application_id            VARCHAR(64),
    agent_id                  VARCHAR(64),
    kpi_value                 NUMERIC(16,6),
    target_value              NUMERIC(16,6),
    status                    VARCHAR(16),   -- 'green' | 'yellow' | 'red'
    threshold_breach_flag     BOOLEAN DEFAULT FALSE,
    trend_direction           VARCHAR(8),    -- 'up' | 'down' | 'stable'
    PRIMARY KEY (metric_date, kpi_id, application_id, agent_id)
);

-- === NEW TABLES (Phase 3 Enhancements) ===

CREATE TABLE daily_slo_compliance (
    compliance_date           DATE,
    application_id            VARCHAR(64),
    slo_type                  VARCHAR(64),
    target_pct                NUMERIC(5,2),
    achieved_pct              NUMERIC(5,2),
    error_budget_consumed_pct NUMERIC(5,2),
    burn_rate_1h              NUMERIC(8,4),
    burn_rate_6h              NUMERIC(8,4),
    breach_flag               BOOLEAN DEFAULT FALSE,
    PRIMARY KEY (compliance_date, application_id, slo_type)
);

CREATE TABLE daily_rag_quality (
    quality_date              DATE,
    application_id            VARCHAR(64),
    rag_id                    VARCHAR(64),
    avg_faithfulness_score    NUMERIC(5,4),
    avg_context_utilization   NUMERIC(5,4),
    avg_retrieval_precision   NUMERIC(5,4),
    retrieval_recall_at_k     NUMERIC(5,4),
    sample_count              BIGINT,
    PRIMARY KEY (quality_date, application_id, rag_id)
);

CREATE TABLE vector_health_snapshots (
    snapshot_date             DATE,
    rag_id                    VARCHAR(64),
    knowledge_base            VARCHAR(256),
    last_indexed_at           TIMESTAMP,
    hours_since_indexed       NUMERIC(8,2),
    embedding_drift_score     NUMERIC(6,4),
    retrieval_recall_at_k     NUMERIC(5,4),
    freshness_breach_flag     BOOLEAN DEFAULT FALSE,
    PRIMARY KEY (snapshot_date, rag_id)
);
```

### 6.3 Amazon S3 Bucket Structure

**Create two buckets per environment:**

```
s3://ai-observability-{env}/        ← event artifacts (redacted payloads, traces)
s3://ai-obs-iac-{env}/              ← IaC artifacts (dashboard JSON, index templates)
```

**Bucket policy requirements:**
- SSE-KMS encryption on all objects
- Access logging enabled to a separate `ai-obs-access-logs-{env}` bucket
- Block all public access
- Object versioning enabled for `audit-evidence/` prefix only
- Lifecycle rules:
  - `redacted-prompts/`: transition to S3 IA after 30d, delete after compliance-defined period
  - `raw-traces/`: transition to Glacier after 90d, delete after 1 year
  - `debug-bundles/`: delete after 90d
  - `rca-reports/`: delete after 1 year

---

## 7. Anomaly Detection Service — What to Build

### 7.1 Application Structure

```text
anomaly-detector/
├── detector/
│   ├── __init__.py
│   ├── main.py              ← Kafka consumer loop
│   ├── baseline.py          ← Redis-backed sliding window baseline manager
│   ├── models/
│   │   ├── isolation_forest.py  ← point anomaly detection
│   │   └── lstm.py              ← temporal pattern detection
│   ├── correlator.py        ← metric correlation using correlation_id
│   └── publisher.py         ← publishes ANOMALY_DETECTED to ai-obs-anomalies
├── tests/
└── Dockerfile
```

### 7.2 Baseline Manager

```python
import redis
import json
from collections import deque

class BaselineManager:
    WINDOW_SECONDS = 7 * 24 * 3600  # 7-day sliding window

    def __init__(self, redis_client: redis.Redis):
        self._redis = redis_client

    def update(self, app_id: str, metric: str, value: float, timestamp: float):
        key = f"baseline:{app_id}:{metric}"
        entry = json.dumps({"v": value, "t": timestamp})
        self._redis.zadd(key, {entry: timestamp})
        # Remove values older than 7 days
        cutoff = timestamp - self.WINDOW_SECONDS
        self._redis.zremrangebyscore(key, "-inf", cutoff)

    def get_percentiles(self, app_id: str, metric: str) -> dict:
        key = f"baseline:{app_id}:{metric}"
        entries = self._redis.zrange(key, 0, -1)
        values = sorted([json.loads(e)["v"] for e in entries])
        if not values:
            return {"p50": None, "p95": None, "p99": None}
        n = len(values)
        return {
            "p50": values[int(n * 0.50)],
            "p95": values[int(n * 0.95)],
            "p99": values[int(n * 0.99)],
        }
```

### 7.3 Isolation Forest Detector

```python
from sklearn.ensemble import IsolationForest
import numpy as np

class IsolationForestDetector:
    CONTAMINATION = 0.05  # expect ~5% anomaly rate

    def __init__(self, baseline: BaselineManager):
        self._baseline = baseline
        self._models: dict[str, IsolationForest] = {}

    def score(self, app_id: str, metric: str, value: float) -> float:
        model_key = f"{app_id}:{metric}"
        if model_key not in self._models:
            self._models[model_key] = IsolationForest(contamination=self.CONTAMINATION)

        baseline = self._baseline.get_percentiles(app_id, metric)
        if baseline["p95"] is None:
            return 0.0  # not enough data yet

        # Feature vector: [value, ratio_to_p95, ratio_to_p99]
        features = np.array([[
            value,
            value / max(baseline["p95"], 1),
            value / max(baseline["p99"], 1),
        ]])

        # -1 = anomaly, 1 = normal; convert to 0.0–1.0 score
        prediction = self._models[model_key].fit_predict(features)
        decision_score = self._models[model_key].score_samples(features)[0]
        # Normalize: more negative = more anomalous
        normalized = max(0.0, min(1.0, (-decision_score + 0.5) / 1.0))
        return normalized
```

---

## 8. Incident Router Service — What to Build

### 8.1 Feedback Quality Gate (runs inside Telemetry Processor)

```python
class FeedbackQualityGate:
    def __init__(self, pg_conn, kafka_producer):
        self._pg = pg_conn
        self._producer = kafka_producer

    def process(self, event: dict, _headers: dict) -> dict:
        if event.get("event_type") != "FEEDBACK_SUBMITTED":
            return event

        rating = event.get("rating", 5)
        app_id = event.get("application_id")
        tier = self._get_application_tier(app_id)

        if rating <= 2 and tier == "critical":
            self._publish_incident(event, severity="high", reason="critical_negative_feedback")
        return event

    def _publish_incident(self, event: dict, severity: str, reason: str):
        incident = {
            "event_type": "INCIDENT_TRIGGERED",
            "incident_id": str(uuid4()),
            "correlation_id": event.get("correlation_id"),
            "application_id": event.get("application_id"),
            "agent_id": event.get("agent_id"),
            "severity": severity,
            "reason": reason,
            "feedback_rating": event.get("rating"),
            "timestamp": datetime.utcnow().isoformat() + "Z",
        }
        self._producer.produce("ai-obs-incidents", value=json.dumps(incident).encode())
```

### 8.2 Incident Router Consumer

```python
class IncidentRouterService:
    def __init__(self, consumer, pagerduty_client, jira_client, s3_client, pg_conn):
        self._consumer = consumer
        self._pd = pagerduty_client
        self._jira = jira_client
        self._s3 = s3_client
        self._pg = pg_conn

    def run(self):
        self._consumer.subscribe(["ai-obs-incidents"])
        while True:
            msg = self._consumer.poll(1.0)
            if msg and not msg.error():
                incident = json.loads(msg.value())
                self._route(incident)

    def _route(self, incident: dict):
        # Build debug bundle
        bundle_uri = self._assemble_debug_bundle(incident)

        # Create PagerDuty event
        if incident.get("severity") == "high":
            pd_event_id = self._pd.create_event(
                summary=f"AI Platform Incident: {incident['reason']}",
                source=incident["application_id"],
                severity="error",
                custom_details={
                    "correlation_id": incident["correlation_id"],
                    "agent_id": incident.get("agent_id"),
                    "debug_bundle": bundle_uri,
                }
            )
            # Write back linked_incident_id to PostgreSQL
            self._pg.execute(
                "UPDATE feedback_case SET linked_incident_id = %s, status = 'escalated' WHERE correlation_id = %s",
                (pd_event_id, incident["correlation_id"])
            )

    def _assemble_debug_bundle(self, incident: dict) -> str:
        bundle = {
            "incident_id": incident["incident_id"],
            "correlation_id": incident["correlation_id"],
            "elasticsearch_links": [
                f"/app/discover#/?_g=()&_a=(query:(language:kuery,query:'correlation_id:\"{incident[\"correlation_id\"]}\"'))"
            ],
            "created_at": datetime.utcnow().isoformat(),
        }
        key = f"debug-bundles/{datetime.utcnow().strftime('%Y/%m/%d')}/{incident['incident_id']}/bundle.json"
        self._s3.put_object(
            Bucket="ai-observability-prod",
            Key=key,
            Body=json.dumps(bundle).encode(),
            ServerSideEncryption="aws:kms",
        )
        return f"s3://ai-observability-prod/{key}"
```

---

## 9. Observability-as-Code Pipeline — What to Build

### 9.1 Alert Rule Syncer

```python
import psycopg2
import requests

class AlertRuleSyncer:
    def __init__(self, pg_dsn: str, grafana_url: str, grafana_token: str):
        self._pg = psycopg2.connect(pg_dsn)
        self._grafana_url = grafana_url
        self._headers = {"Authorization": f"Bearer {grafana_token}"}

    def sync(self):
        rules = self._load_rules_from_postgres()
        for rule in rules:
            grafana_rule = self._convert_to_grafana_format(rule)
            self._upsert_grafana_rule(grafana_rule)

    def _load_rules_from_postgres(self) -> list[dict]:
        with self._pg.cursor() as cur:
            cur.execute("""
                SELECT a.alert_id, a.metric_id, a.application_id, a.threshold_value,
                       a.comparison_operator, a.window_minutes, a.severity,
                       a.notification_channel, m.source_table, m.metric_name
                FROM alert_threshold a
                JOIN metric_catalog m ON m.metric_id = a.metric_id
                WHERE a.active_flag = TRUE
            """)
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]

    def _convert_to_grafana_format(self, rule: dict) -> dict:
        return {
            "uid": rule["alert_id"],
            "title": f"Alert: {rule['metric_name']} for {rule.get('application_id', 'platform')}",
            "condition": "A",
            "data": [{
                "refId": "A",
                "queryType": "range",
                "relativeTimeRange": {"from": rule["window_minutes"] * 60, "to": 0},
                "datasourceUid": "postgresql",
                "model": {
                    "rawSql": f"SELECT {rule['metric_name']} FROM {rule['source_table']} WHERE application_id = '{rule.get('application_id', '')}' ORDER BY hour_timestamp DESC LIMIT 1",
                }
            }],
            "noDataState": "NoData",
            "execErrState": "Error",
            "for": f"{rule['window_minutes']}m",
        }

    def _upsert_grafana_rule(self, rule: dict):
        resp = requests.put(
            f"{self._grafana_url}/api/v1/provisioning/alert-rules/{rule['uid']}",
            json=rule,
            headers=self._headers,
        )
        resp.raise_for_status()
```

### 9.2 CI Pipeline (`deploy.yml` steps)

```yaml
# ci/deploy.yml — runs on merge to main
steps:
  - name: Apply Elasticsearch Index Templates
    run: |
      for f in elasticsearch/index-templates/*.json; do
        curl -X PUT "${ES_URL}/_index_template/$(basename $f .json)" \
          -H "Content-Type: application/json" \
          -d @$f
      done

  - name: Apply ILM Policies
    run: |
      for f in elasticsearch/ilm-policies/*.json; do
        curl -X PUT "${ES_URL}/_ilm/policy/$(basename $f .json)" \
          -H "Content-Type: application/json" \
          -d @$f
      done

  - name: Deploy Grafana Dashboards
    run: |
      for f in grafana/dashboards/*.json; do
        curl -X POST "${GRAFANA_URL}/api/dashboards/import" \
          -H "Authorization: Bearer ${GRAFANA_TOKEN}" \
          -H "Content-Type: application/json" \
          -d "{\"dashboard\": $(cat $f), \"overwrite\": true}"
      done

  - name: Sync Alert Rules from PostgreSQL
    run: python grafana/alert-rules/sync.py
```

---

## 10. Observability Chatbot — What to Build

### 10.1 Application Structure

```text
observability-chatbot/
├── chatbot/
│   ├── __init__.py
│   ├── main.py              ← FastAPI app exposing /chat endpoint
│   ├── intent.py            ← IntentClassifier using LLM
│   ├── semantic_layer.py    ← MetricSemanticLayer reads metric_catalog
│   ├── rbac.py              ← AccessController validates user → app/LOB access
│   ├── query_planner.py     ← QueryPlanner routes to correct data source
│   ├── sources/
│   │   ├── postgres.py      ← PostgresSource: aggregate queries
│   │   ├── elasticsearch.py ← ElasticsearchSource: event/trace queries
│   │   ├── s3.py            ← S3Source: artifact retrieval with RBAC check
│   │   └── grafana.py       ← GrafanaSource: service health lookups
│   └── answer.py            ← AnswerGenerator: formats final response
├── tests/
└── Dockerfile
```

### 10.2 Intent Classifier

```python
class IntentClassifier:
    INTENT_PROMPT = """
    Classify the user's observability question into one of these intents:
    - aggregate_metric: count/trend questions answerable from PostgreSQL aggregates
    - trace_drill_down: questions about a specific trace or request by ID
    - error_rca: error spike analysis, root cause questions
    - cost_analysis: token usage, spend, budget questions
    - rag_quality: RAG hit rate, faithfulness, no-result rate questions
    - tool_health: tool failures, timeouts, SLA breaches
    - slo_status: availability, error budget, SLA breach questions
    - feedback_summary: feedback trends, negative feedback categories
    - infra_health: Kafka lag, service availability, pod health

    Question: {question}
    Intent:"""

    def __init__(self, llm_client):
        self._llm = llm_client

    def classify(self, question: str) -> str:
        response = self._llm.generate(
            self.INTENT_PROMPT.format(question=question),
            max_tokens=20,
            temperature=0.0,
        )
        return response.text.strip().lower()
```

### 10.3 Query Planner

Langfuse is added as a query source for LLM/RAG/agent quality intents. The chatbot queries Langfuse for trace-level detail and PostgreSQL/Elasticsearch for aggregates and infrastructure.

```python
class QueryPlanner:
    INTENT_TO_SOURCE = {
        "aggregate_metric":  "postgres",
        "trace_drill_down":  ["langfuse", "elasticsearch"],  # Langfuse first for LLM traces
        "error_rca":         ["elasticsearch", "langfuse", "s3"],
        "cost_analysis":     ["langfuse", "postgres"],        # Langfuse has per-call cost
        "rag_quality":       ["langfuse", "postgres"],        # Langfuse has faithfulness scores
        "tool_health":       ["postgres", "elasticsearch"],
        "slo_status":        ["postgres", "grafana"],
        "feedback_summary":  ["langfuse", "postgres"],        # Langfuse links feedback to traces
        "infra_health":      "grafana",
        "prompt_quality":    "langfuse",                      # new intent — prompt version analytics
        "llm_trace":         "langfuse",                      # new intent — specific LLM call drill-down
    }

    def plan(self, intent: str, question: str, context: dict) -> list[dict]:
        sources = self.INTENT_TO_SOURCE.get(intent, "postgres")
        if isinstance(sources, str):
            sources = [sources]
        return [{"source": s, "question": question, "context": context} for s in sources]
```

Add a `LangfuseSource` alongside the existing `ElasticsearchSource` and `PostgresSource`:

```python
# chatbot/sources/langfuse.py
from langfuse import Langfuse

class LangfuseSource:
    def __init__(self):
        self._client = Langfuse()

    def query(self, intent: str, question: str, context: dict) -> dict:
        correlation_id = context.get("correlation_id")

        if correlation_id:
            # Fetch the specific trace by correlation_id
            trace = self._client.get_trace(correlation_id)
            return {
                "source": "langfuse",
                "trace_id": trace.id,
                "total_cost_usd": sum(o.calculated_total_cost or 0 for o in trace.observations),
                "total_tokens": sum((o.usage.total or 0) for o in trace.observations if o.usage),
                "faithfulness_score": next(
                    (s.value for s in trace.scores if s.name == "faithfulness"), None
                ),
                "user_feedback": next(
                    (s.value for s in trace.scores if s.name == "user-feedback-rating"), None
                ),
                "trace_url": f"{LANGFUSE_HOST}/trace/{trace.id}",
            }

        if intent == "rag_quality":
            # Fetch aggregate RAG quality scores
            scores = self._client.get_scores(name="faithfulness", limit=1000)
            values = [s.value for s in scores if s.value is not None]
            return {
                "source": "langfuse",
                "avg_faithfulness": mean(values) if values else None,
                "sample_count": len(values),
            }

        return {"source": "langfuse", "data": None}
```

### 10.4 Answer Generator

```python
class AnswerGenerator:
    ANSWER_PROMPT = """
    Based on these data results, answer the user's question clearly.
    Include: metric value, time range, applied filters, data source used, and a recommended action if appropriate.

    Question: {question}
    Data: {data}

    Answer:"""

    def generate(self, question: str, data_results: list[dict]) -> dict:
        combined_data = json.dumps(data_results, indent=2)
        answer_text = self._llm.generate(
            self.ANSWER_PROMPT.format(question=question, data=combined_data),
            max_tokens=512,
        ).text

        return {
            "answer": answer_text,
            "sources": [r.get("source") for r in data_results],
            "time_range": data_results[0].get("time_range") if data_results else None,
            "dashboard_link": self._build_dashboard_link(data_results),
            "confidence": "high" if len(data_results) > 0 else "low",
        }
```

---

## 11. Dashboard Specifications — What to Build

### 11.1 Dashboard Build Order and Technology

All dashboards should be built as Grafana JSON (version-controlled) or Kibana saved objects (exported as NDJSON). Langfuse provides its own built-in UI for LLM/RAG/agent trace exploration — those do not need to be rebuilt in Grafana. Build in this order:

| Order | Dashboard | Technology | Primary Data Source | Note |
|---|---|---|---|---|
| 1 | Platform Overview | Grafana | PostgreSQL `agg_hourly_application_metrics` | |
| 2 | Error and Incident | Kibana | Elasticsearch `ai-obs-*-errors-*` | |
| 3 | Application / CSI | Kibana + Grafana | Elasticsearch + PostgreSQL | |
| 4 | **LLM Trace Explorer** | **Langfuse UI** (built-in) | **Langfuse DB** | **Free — no build needed** |
| 5 | **RAG Pipeline Quality** | **Langfuse UI** (built-in) | **Langfuse DB** | **Free — no build needed** |
| 6 | **Agent Step Tree** | **Langfuse UI** (built-in) | **Langfuse DB** | **Free — no build needed** |
| 7 | **Prompt Version Analytics** | **Langfuse UI** (built-in) | **Langfuse DB** | **Free — no build needed** |
| 8 | Agent Observability (aggregate) | Kibana + Grafana | Elasticsearch + PostgreSQL | For aggregate trends only; trace drill-down → Langfuse |
| 9 | Tool Health | Kibana + Grafana | Elasticsearch + PostgreSQL | |
| 10 | LLM / Token / Cost (aggregate) | Grafana | PostgreSQL `agg_hourly_llm_metrics` + nightly Langfuse sync | Aggregate view; per-call detail → Langfuse |
| 11 | RAG + Vector Health (aggregate) | Kibana + Grafana | PostgreSQL `daily_rag_quality` (synced from Langfuse nightly) | |
| 12 | Feedback + Incident | Grafana | PostgreSQL `feedback_case` + Langfuse scores | |
| 13 | SLO Error Budget | Grafana | PostgreSQL `daily_slo_compliance` | |
| 14 | Cost Governance | Grafana | PostgreSQL `budget_limits` + `agg_hourly_llm_metrics` | |
| 15 | Anomaly Detection | Grafana | Elasticsearch `ai-obs-anomalies-*` | |
| 16 | Business KPI | Grafana | PostgreSQL `agg_daily_kpi_metrics` | |

### 11.2 Required Panels per Dashboard

**Platform Overview — must include:**
- Request count (counter, 24h)
- Success rate % (gauge, target ≥ 99%)
- Error rate % (gauge, alert threshold marked)
- P95 latency (time series, 24h trend)
- Total token cost (counter, current day vs yesterday)
- Top 5 failing applications (table)
- Request volume over time (time series, grouped by `lob`)
- Positive/negative feedback ratio (bar chart)

**SLO Error Budget — must include:**
- Error budget remaining % per application (gauge, red at < 20%)
- Burn rate 1h vs 6h (stat panels, red when > 14.4×)
- Projected budget exhaustion date (stat, derived from burn rate)
- Historical SLO compliance (time series, 30d)

**Cost Governance — must include:**
- Current spend vs budget per application/model (progress bar)
- Model cost comparison (bar: actual model vs next cheaper alternative)
- Daily spend trend (time series, 30d with budget cap line)
- Top 5 most expensive agents (table)
- Budget threshold breach history (event log)

---

## 12. Offline Batch RCA Engine — What to Build

### 12.1 Job Structure

```text
rca-engine/
├── rca/
│   ├── __init__.py
│   ├── main.py              ← scheduled entry point (Airflow DAG or K8s CronJob)
│   ├── correlator.py        ← joins errors ↔ traces ↔ KPI aggregates
│   ├── hypothesis.py        ← HypothesisRanker: scores root causes
│   ├── digest.py            ← WeeklyDigestBuilder: composes Slack/email message
│   └── publisher.py         ← writes to S3 + Elasticsearch + notifies
├── tests/
└── Dockerfile
```

### 12.2 Failure Correlator

```python
class FailureCorrelator:
    def __init__(self, es_client, pg_conn):
        self._es = es_client
        self._pg = pg_conn

    def correlate(self, date_from: datetime, date_to: datetime) -> list[dict]:
        # Step 1: Get error clusters from Elasticsearch
        errors = self._es.search(index="ai-obs-*-errors-*", body={
            "query": {"range": {"timestamp": {"gte": date_from.isoformat(), "lte": date_to.isoformat()}}},
            "aggs": {
                "by_error_code": {"terms": {"field": "error_code", "size": 20}},
                "by_app": {"terms": {"field": "application_id", "size": 20}},
                "by_tool": {"terms": {"field": "tool_id", "size": 20}},
            }
        })

        # Step 2: For each error cluster, fetch correlated traces
        incidents = []
        for error_bucket in errors["aggregations"]["by_error_code"]["buckets"]:
            error_code = error_bucket["key"]
            count = error_bucket["doc_count"]
            correlated_trace_ids = self._get_correlated_traces(error_code, date_from, date_to)
            tool_latencies = self._get_tool_latency_context(correlated_trace_ids)
            kpi_impact = self._get_kpi_impact(date_from, date_to)
            incidents.append({
                "error_code": error_code,
                "error_count": count,
                "trace_ids": correlated_trace_ids[:5],
                "tool_latencies": tool_latencies,
                "kpi_impact": kpi_impact,
            })
        return incidents
```

### 12.3 Hypothesis Ranker

```python
class HypothesisRanker:
    HYPOTHESES = [
        "tool_degradation",
        "model_drift",
        "prompt_change",
        "rag_staleness",
        "kafka_lag",
        "infrastructure_degradation",
    ]

    def rank(self, incident: dict) -> list[dict]:
        scores = {}
        # tool_degradation: high tool p95 latency correlation with errors
        if incident.get("tool_latencies"):
            max_latency = max(t.get("p95_latency_ms", 0) for t in incident["tool_latencies"])
            scores["tool_degradation"] = min(1.0, max_latency / 10000)

        # model_drift: check if prompt_template_id changed recently
        scores["model_drift"] = self._score_model_drift(incident)

        # rag_staleness: check vector_health_snapshots for freshness breach
        scores["rag_staleness"] = self._score_rag_staleness(incident)

        ranked = sorted(scores.items(), key=lambda x: x[1], reverse=True)
        return [{"hypothesis": h, "score": round(s, 3)} for h, s in ranked if s > 0.1]
```

---

## 13. Development Sequence and Dependencies

### Phase 1 — Foundation (Weeks 1–3)

**Deliver first — everything else depends on these:**

1. Define Kafka topic schema (JSON envelope, mandatory fields, W3C header format)
2. Define error code catalog (PostgreSQL `error_code_catalog` seed data)
3. Define `metric_catalog` seed data (30 initial metrics with formulas and source tables)
4. Apply Elasticsearch index templates (via IaC pipeline)
5. Apply PostgreSQL DDL (all registry + governance + aggregate tables)
6. Create S3 buckets with encryption and lifecycle policies
7. **Deploy Langfuse self-hosted** (Helm chart / Docker Compose, dedicated PostgreSQL DB, internal DNS `langfuse.internal:3000`)
8. **Create Langfuse Projects** — one per application or LOB; distribute API key pairs to each service team
9. **Add `LANGFUSE_PUBLIC_KEY`, `LANGFUSE_SECRET_KEY`, `LANGFUSE_HOST` to platform secrets store**

**Team:** Platform Engineering + Data Engineering

> **Why Langfuse in Phase 1:** Deploying Langfuse takes 1 day. Every week of Phase 2 instrumentation work immediately produces visible traces in the Langfuse UI, giving the team rapid feedback and validating the instrumentation as it's built — without waiting for the full Telemetry Processor pipeline to be complete.

---

### Phase 2 — SDK + Instrumentation (Weeks 4–7)

**Deliverables:**
1. Build `ai-observability-sdk` Python package (all classes in Section 3)
2. Add SDK to: Orchestration Service, Executor Service, LLM Wrapper, Tool Executor, RAG Wrapper, Guardrails Engine, Memory Module, Feedback UI
3. W3C traceparent injection in all Kafka produces
4. Validate events flowing to Kafka topics by observing consumer output
5. **Add Langfuse `@observe` decorators to GSSP GS** — LLM generator functions (Section 3a.4)
6. **Add Langfuse `@observe` decorators to GSSP QS** — all 5 RAG pipeline stages (Section 3a.5)
7. **Add Langfuse `@observe` decorators to Agent Executor** — step execution loop (Section 3a.6)
8. **Add Langfuse `@observe` decorators to GSSP RS** — `retrieve()` and `embed()` functions
9. **Add `langfuse.score()` call to User Feedback Service** — link ratings to traces (Section 3a.7)
10. **Migrate `PromptTemplateFactory` to Langfuse Prompt Management** (Section 3a.8)

**Acceptance criteria:**
- Every request produces at minimum: `REQUEST_RECEIVED`, `AGENT_STARTED`, one `LLM_CALL_COMPLETED` or `TOOL_CALL_COMPLETED`, `AGENT_COMPLETED`, `RESPONSE_DELIVERED` — all linked by the same `correlation_id`
- **Langfuse UI shows nested trace trees for every GSSP GS, GSSP QS, and Agent Executor request**
- **Langfuse UI shows per-call token count, cost, and latency for every LLM call**
- User feedback scores are visible inside the Langfuse trace for the corresponding `correlation_id`

---

### Phase 3 — Telemetry Processor (Weeks 6–10, overlaps Phase 2)

**Deliverables:**
1. Build Telemetry Processor service with all sub-components (Section 5)
2. PII Redactor running before any writes
3. Metadata Enricher with Redis registry cache (5-min TTL)
4. Elasticsearch sink with per-LOB index routing
5. PostgreSQL rollup generator for all 5 hourly aggregate tables
6. S3 Archiver for prompts, responses, RAG contexts, full traces
7. Token Cost Calculator + Budget Accumulator
8. SLO Evaluator (burn-rate computation)
9. ~~Faithfulness Scorer for RAG events~~ → **Replaced by Langfuse LLM-as-judge evaluators** (Section 3a.9)
10. **Configure Langfuse evaluators** — faithfulness, hallucination, answer relevance (Section 3a.9)
11. **Build nightly Langfuse → PostgreSQL sync job** for `daily_rag_quality` aggregates (Section 3a.10)

**Acceptance criteria:** Elasticsearch shows indexed events; PostgreSQL `agg_hourly_application_metrics` has rows; S3 has redacted prompt objects; **Langfuse shows faithfulness scores on RAG traces; `daily_rag_quality` is populated from nightly sync**.

---

### Phase 4 — Dashboards (Weeks 10–14)

**Deliverables:** All 12 dashboards (Section 11), version-controlled as JSON in `observability-iac/`

**Build order:** Platform Overview → Error Dashboard → Application/CSI → Agent → Tool → LLM/Cost → SLO Error Budget → Cost Governance → RAG+Vector Health → Feedback+Incident → Anomaly → Business KPI

---

### Phase 5 — Chatbot (Weeks 13–17)

**Deliverables:**
1. FastAPI service with `/chat` endpoint
2. IntentClassifier (backed by LLM)
3. MetricSemanticLayer (reads `metric_catalog`)
4. AccessController (RBAC by application/LOB from `application_registry`)
5. QueryPlanner with routing to PostgreSQL, Elasticsearch, S3, Grafana
6. AnswerGenerator with source attribution and dashboard links
7. Feedback Quality Gate → `ai-obs-incidents` Kafka topic publish
8. Incident Router Service consuming `ai-obs-incidents`

---

### Phase 6 — Anomaly Detection + RCA (Weeks 17–22)

**Deliverables:**
1. Anomaly Detection Service (Isolation Forest + LSTM, Redis baselines)
2. Elasticsearch `ai-obs-anomalies-*` indexer
3. Grafana Anomaly Dashboard
4. Offline Batch RCA Engine (nightly CronJob)
5. Weekly digest (Slack webhook + SES email)

---

### Phase 7 — Observability-as-Code (Weeks 20–23)

**Deliverables:**
1. `observability-iac/` repository with all dashboard JSON, index templates, ILM policies, migrations
2. CI pipeline deploying all artifacts on merge to main
3. Alert Rule Syncer (reads `alert_threshold` → Grafana API)

---

### Phase 8 — Multi-Tenant Isolation (Weeks 22–26)

**Deliverables:**
1. Migrate Elasticsearch index routing to per-LOB pattern `ai-obs-{lob}-{type}-{date}`
2. Create per-LOB Kibana organizations with provisioned dashboards
3. Per-LOB Kafka consumer groups for independent offset management
4. Index-level RBAC configuration in Elasticsearch

---

### Dependency Graph Summary

```
Phase 1 (Schema + Infrastructure + Langfuse Deploy)
  ↓
Phase 2 (SDK + Langfuse @observe) ──────────────────┐
  ↓                                                  │
  ├── Langfuse traces visible immediately            │
  │   (no Processor needed for LLM/RAG quality)     │
  ↓                                                  │
Phase 3 (Processor + Langfuse Evals + Sync) ────┐   │
  ↓                    ↓                         │   │
Phase 4 (Dashboards)  Phase 5 (Chatbot           │   │
  (Grafana/Kibana)      + Langfuse source)        │   │
  ↓                    ↓                         │   │
Phase 7 (IaC)         Phase 5.b (Incident Router)│   │
                                                 ↓   ↓
                        Phase 6 (Anomaly Detection + RCA)
                                ↓
                        Phase 8 (Multi-Tenant Isolation)
```

**Phases 2 and 3 overlap by 2 weeks** — the processor can be built while SDK instrumentation is added service by service, as long as the Kafka schema is locked at Phase 1.

**Phases 4 and 5 can run in parallel** after Phase 3 is producing data to PostgreSQL and Elasticsearch.

**Langfuse shortcut:** After Phase 2 Langfuse instrumentation is done, the **LLM Trace Explorer, RAG Pipeline Quality, Agent Step Tree, and Prompt Version Analytics dashboards are already available in the Langfuse UI** — without building any Grafana/Kibana dashboards. This unlocks immediate value for ML engineers and application teams while Phase 3–4 builds the broader platform dashboards for SREs and business stakeholders.
