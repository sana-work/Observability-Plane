# Observability Ingestion Service — Design & Implementation Plan

> A new standalone microservice that acts as the **single intake point** for all logs, events, and metrics produced by every repo on the platform. Every service POSTs structured JSON; this service validates, normalises, enriches, and pushes to PostgreSQL.

---

## 1. Problem Statement

All eight platform services produce observability data today, but:

- **No standard schema** — each service uses different field names (`appId` vs `application_id` vs `app_name`)
- **Six of eight services emit no Kafka events** — their telemetry is trapped in local logs or PostgreSQL audit tables with no pipeline
- **Critical fields absent** — `event_id`, `environment`, `service_name`, `latency_ms` (numeric), `estimated_cost`, `span_id` are missing almost everywhere
- **No central store** — there is no single PostgreSQL table that aggregates events from all services in one uniform schema
- **No uniformity** — the Observability Plane cannot query across services without per-service custom adapters

**Solution:** Build one HTTP API — the **Observability Ingestion Service (OIS)** — that every service calls with a standardised JSON payload. OIS validates the schema, enriches fields, computes derived values (cost, latency), and writes to PostgreSQL. Services do not need Kafka to participate; they simply call an HTTP endpoint.

---

## 2. Service Design

### 2.1 Architecture Position

```
┌─────────────────────────────────────────────────────────────────────┐
│                      AI Services Platform                           │
│                                                                     │
│  Agentic Orchestration ──┐                                          │
│  Agent Executor ──────────┤                                         │
│  GSSP GS ─────────────────┤                                         │
│  GSSP QS ─────────────────┼──► POST /v1/ingest  ──► OIS API        │
│  GSSP RS ─────────────────┤         (JSON)           │              │
│  Data Ingestion ──────────┤                           ▼             │
│  Consumer Service ────────┤                    Validation &         │
│  User Feedback ───────────┘                    Enrichment           │
│                                                    │                │
│                                              ┌─────┴──────┐        │
│                                              │ PostgreSQL  │        │
│                                              │ obs_events  │        │
│                                              │ obs_metrics │        │
│                                              │ obs_errors  │        │
│                                              └────────────┘         │
└─────────────────────────────────────────────────────────────────────┘
```

### 2.2 Core Principles

1. **HTTP-first** — every service sends a single `POST /v1/ingest` call; no Kafka dependency required
2. **One schema, all services** — a single JSON envelope that every event must conform to
3. **Fire-and-forget** — services do not wait for acknowledgement; OIS responds `202 Accepted` immediately and processes async
4. **Fail-safe** — invalid events are stored in a `dead_letter` table rather than rejected, so no telemetry is silently dropped
5. **Enrichment at ingestion** — OIS adds `environment`, `service_name`, `estimated_cost`, `latency_ms` (if not supplied), normalises `user_hash`, and maps raw exceptions to error catalog codes
6. **Write to PostgreSQL** — all events land in structured, queryable PostgreSQL tables; no additional infrastructure required at this stage

---

## 3. Standard JSON Event Schema

Every service must send this JSON structure to `POST /v1/ingest`.

### 3.1 Full Schema

```json
{
  "event_id": "evt_7f1b2c90-79ec-4ed2-aee4-8dca04b734f2",
  "event_type": "LLM_CALL_COMPLETED",
  "schema_version": "1.0",
  "timestamp": "2026-05-26T10:30:00.000Z",

  "correlation_id": "CORR_abc123",
  "span_id": "SPAN_004",
  "parent_span_id": "SPAN_003",
  "request_id": "REQ_789",

  "service_name": "gssp-gs",
  "component": "vertex-generator",
  "environment": "prod",

  "application_id": "179524",
  "app_container": "gssp-gs",
  "soe_id": "PricingDomeApp",
  "lob": "Payments",
  "tenant_id": "tenant_001",
  "consumer_coin": "CitiConnect",

  "user_hash": "sha256_abc123",

  "agent_id": "payment_scrutiny_agent",
  "agent_version": "1.0.3",
  "tool_id": null,
  "rag_id": null,
  "model_name": "gemini-1.5-pro",
  "model_provider": "vertexai",
  "prompt_template_id": "pricing_summary_v2",

  "status": "success",
  "latency_ms": 1840,
  "error_code": null,
  "http_status": 200,

  "payload": {
    "input_tokens": 512,
    "output_tokens": 148,
    "total_tokens": 660,
    "estimated_cost_usd": 0.00132,
    "finish_reason": "stop",
    "rate_limit_hit": false,
    "safety_blocked": false,
    "confidence_score": 0.94,

    "step_name": null,
    "step_number": null,
    "loop_count": null,
    "handoff_count": null,
    "termination_reason": null,

    "tool_type": null,
    "retry_count": 0,
    "timeout_flag": false,
    "response_size_bytes": null,

    "rag_knowledge_base": null,
    "retrieved_chunk_count": null,
    "avg_relevance_score": null,
    "no_result_flag": null,
    "citation_coverage_pct": null,
    "context_truncation_flag": null,

    "job_id": null,
    "document_id": null,
    "job_status": null,
    "embedding_model": null,
    "embedding_latency_ms": null,

    "feedback_rating": null,
    "feedback_thumbs": null,
    "feedback_sentiment": null,
    "feedback_category": null,
    "feedback_role": null,
    "resolution_status": null,
    "linked_incident_id": null,

    "kafka_topic": null,
    "kafka_partition": null,
    "kafka_offset": null,
    "consumer_group": null,
    "kafka_lag": null,
    "dlq_flag": null,

    "s3_payload_uri": null,
    "extra": {}
  }
}
```

### 3.2 Mandatory Fields

Every event **must** include these fields. OIS will reject events missing any mandatory field into the dead-letter table.

| Field | Type | Description |
|---|---|---|
| `event_id` | `string` | UUID, unique per event. Generate with `uuid4()` if not supplied by the source. |
| `event_type` | `string` | See Section 3.3 for controlled vocabulary |
| `schema_version` | `string` | Always `"1.0"` until schema changes |
| `timestamp` | `string` | ISO 8601 UTC — `2026-05-26T10:30:00.000Z` |
| `correlation_id` | `string` | End-to-end request identifier. `CORR_<uuid>` |
| `service_name` | `string` | Emitting service identifier (see Section 3.4) |
| `environment` | `string` | `prod` \| `staging` \| `dev` \| `test` |
| `application_id` | `string` | CSI / application ID |
| `status` | `string` | `success` \| `failed` \| `partial` \| `timeout` \| `info` |

### 3.3 Controlled Event Type Vocabulary

```
# Platform / Request
REQUEST_RECEIVED
REQUEST_COMPLETED
REQUEST_FAILED
RESPONSE_DELIVERED

# Orchestration
AUTH_COMPLETED
AUTH_FAILED
CONFIG_LOADED
PLAN_CREATED
AGENT_EXECUTION_REQUEST_PRODUCED
FINAL_RESPONSE_CONSUMED
RESPONSE_BUILT
HIL_REQUEST_SENT
HIL_RESPONSE_RECEIVED

# Kafka
KAFKA_MESSAGE_PRODUCED
KAFKA_MESSAGE_CONSUMED
KAFKA_MESSAGE_DLQ
KAFKA_LAG_RECORDED

# Agent
AGENT_STARTED
AGENT_STEP_STARTED
AGENT_STEP_COMPLETED
AGENT_LOOP_ITERATION
AGENT_HANDOFF
AGENT_COMPLETED
AGENT_FAILED
AGENT_TIMEOUT

# LLM
LLM_CALL_STARTED
LLM_CALL_COMPLETED
LLM_CALL_FAILED
LLM_RATE_LIMITED
LLM_SAFETY_BLOCKED

# Tool
TOOL_CALL_STARTED
TOOL_CALL_COMPLETED
TOOL_CALL_FAILED
TOOL_CALL_TIMEOUT

# RAG
RAG_RETRIEVAL_STARTED
RAG_RETRIEVAL_COMPLETED
RAG_RETRIEVAL_FAILED
RAG_NO_RESULT
RAG_INDEX_HEALTH_CHECKED

# Document Ingestion
INGESTION_JOB_STARTED
INGESTION_JOB_COMPLETED
INGESTION_JOB_FAILED
DOCUMENT_INDEXED
DOCUMENT_EMBEDDING_CREATED
DOCUMENT_EXTRACTION_FAILED

# Guardrail
GUARDRAIL_EVALUATED
GUARDRAIL_BLOCKED
GUARDRAIL_REDACTED
GUARDRAIL_ESCALATED

# Feedback
FEEDBACK_SUBMITTED
FEEDBACK_REVIEWED
FEEDBACK_INCIDENT_TRIGGERED

# Infrastructure / Health
SERVICE_STARTED
SERVICE_STOPPED
HEALTH_CHECK
```

### 3.4 Service Name Registry

| Service | `service_name` value |
|---|---|
| Agentic Orchestration | `agentic-orchestration` |
| Agent Executor | `agent-executor` |
| GSSP Generic Generation Service | `gssp-gs` |
| GSSP Query Service | `gssp-qs` |
| GSSP Response Service | `gssp-rs` |
| Data Ingestion Service | `data-ingestion` |
| Consumer Service (Scheduler) | `consumer-service` |
| User Feedback | `user-feedback` |
| Observability Ingestion Service | `obs-ingestion` |

---

## 4. API Specification

### 4.1 Ingest Endpoint

```
POST /v1/ingest
Content-Type: application/json
Authorization: Bearer <COIN JWT>         (internal service-to-service M2M token)
X-Source-Service: <service_name>
```

**Request body:** JSON object conforming to schema in Section 3.

**Response:**
```json
HTTP 202 Accepted
{
  "accepted": true,
  "event_id": "evt_7f1b2c90-79ec-4ed2-aee4-8dca04b734f2"
}
```

**On schema validation failure (still accepted, stored to dead-letter):**
```json
HTTP 202 Accepted
{
  "accepted": true,
  "event_id": "evt_xxx",
  "warnings": ["missing required field: correlation_id", "unknown event_type: CUSTOM_FOO"]
}
```

### 4.2 Batch Ingest Endpoint

```
POST /v1/ingest/batch
Content-Type: application/json
```

**Request body:**
```json
{
  "events": [ { ... }, { ... }, { ... } ]
}
```

Maximum 500 events per batch. Response: `202 Accepted` with per-event status array.

### 4.3 Health Endpoint

```
GET /health
→ 200 { "status": "ok", "db": "connected", "version": "1.0.0" }
```

### 4.4 Metrics Endpoint

```
GET /metrics
→ Prometheus text format
```

Exposes: `ois_events_received_total`, `ois_events_written_total`, `ois_dead_letter_total`, `ois_ingestion_latency_seconds`.

---

## 5. Processing Pipeline

```
POST /v1/ingest
       │
       ▼
 ┌─────────────┐
 │  Validation  │  ← Check mandatory fields, event_type whitelist, schema_version
 │  Layer       │    → invalid events → dead_letter table (still 202)
 └──────┬──────┘
        │ valid
        ▼
 ┌─────────────┐
 │  Enrichment  │  ← Fill missing fields OIS can derive:
 │  Layer       │    • environment (from M2M token claim or env var)
 │              │    • service_name (from X-Source-Service header if missing)
 │              │    • user_hash = sha256(raw_soeid) if soeid is plain text
 │              │    • estimated_cost_usd = tokens × model_pricing lookup
 │              │    • latency_ms = (completed_at - started_at).ms if absent
 └──────┬──────┘
        │
        ▼
 ┌─────────────┐
 │  Error Code  │  ← Map raw exception strings to error_code_catalog
 │  Mapper      │    e.g. "ReadTimeout" → "TOOL_TIMEOUT"
 └──────┬──────┘
        │
        ▼
 ┌─────────────┐
 │  Router      │  ← Route to correct PostgreSQL table based on event_type
 └──────┬──────┘
        │
        ▼
 ┌─────────────┐
 │  PostgreSQL  │  ← Write to obs_events + event-type specific table
 │  Writer      │
 └─────────────┘
```

---

## 6. PostgreSQL Schema

### 6.1 Master Events Table

All events land here, regardless of type. This is the single unified table.

```sql
CREATE TABLE obs_events (
    id                    BIGSERIAL PRIMARY KEY,
    event_id              VARCHAR(128) UNIQUE NOT NULL,
    event_type            VARCHAR(64)  NOT NULL,
    schema_version        VARCHAR(16)  NOT NULL DEFAULT '1.0',
    timestamp             TIMESTAMPTZ  NOT NULL,
    received_at           TIMESTAMPTZ  NOT NULL DEFAULT NOW(),

    -- Correlation / tracing
    correlation_id        VARCHAR(256) NOT NULL,
    span_id               VARCHAR(128),
    parent_span_id        VARCHAR(128),
    request_id            VARCHAR(128),

    -- Service identity
    service_name          VARCHAR(64)  NOT NULL,
    component             VARCHAR(128),
    environment           VARCHAR(32)  NOT NULL,

    -- Application context
    application_id        VARCHAR(64)  NOT NULL,
    app_container         VARCHAR(128),
    soe_id                VARCHAR(128),
    lob                   VARCHAR(64),
    tenant_id             VARCHAR(128),
    consumer_coin         VARCHAR(128),

    -- User (hashed)
    user_hash             VARCHAR(128),

    -- Agent / LLM / Tool / RAG identifiers
    agent_id              VARCHAR(128),
    agent_version         VARCHAR(64),
    tool_id               VARCHAR(128),
    rag_id                VARCHAR(128),
    model_name            VARCHAR(128),
    model_provider        VARCHAR(64),
    prompt_template_id    VARCHAR(128),

    -- Outcome
    status                VARCHAR(32)  NOT NULL,
    latency_ms            INTEGER,
    error_code            VARCHAR(64),
    http_status           INTEGER,

    -- Payload (full JSON for ad-hoc querying)
    payload               JSONB,

    -- Enrichment metadata
    enriched_by_ois       BOOLEAN DEFAULT TRUE,
    dead_letter_reason    TEXT
);

-- Indexes for common query patterns
CREATE INDEX idx_obs_events_correlation_id ON obs_events(correlation_id);
CREATE INDEX idx_obs_events_event_type     ON obs_events(event_type);
CREATE INDEX idx_obs_events_timestamp      ON obs_events(timestamp DESC);
CREATE INDEX idx_obs_events_application_id ON obs_events(application_id);
CREATE INDEX idx_obs_events_service_name   ON obs_events(service_name);
CREATE INDEX idx_obs_events_environment    ON obs_events(environment);
CREATE INDEX idx_obs_events_agent_id       ON obs_events(agent_id);
CREATE INDEX idx_obs_events_error_code     ON obs_events(error_code);
```

### 6.2 LLM Events Table

```sql
CREATE TABLE obs_llm_events (
    id                    BIGSERIAL PRIMARY KEY,
    event_id              VARCHAR(128) UNIQUE NOT NULL REFERENCES obs_events(event_id),
    timestamp             TIMESTAMPTZ  NOT NULL,
    correlation_id        VARCHAR(256) NOT NULL,
    application_id        VARCHAR(64),
    agent_id              VARCHAR(128),
    model_name            VARCHAR(128),
    model_provider        VARCHAR(64),
    prompt_template_id    VARCHAR(128),
    input_tokens          INTEGER,
    output_tokens         INTEGER,
    total_tokens          INTEGER,
    estimated_cost_usd    NUMERIC(12, 8),
    latency_ms            INTEGER,
    finish_reason         VARCHAR(32),
    rate_limit_hit        BOOLEAN DEFAULT FALSE,
    safety_blocked        BOOLEAN DEFAULT FALSE,
    confidence_score      NUMERIC(5, 4),
    status                VARCHAR(32),
    error_code            VARCHAR(64)
);

CREATE INDEX idx_obs_llm_correlation   ON obs_llm_events(correlation_id);
CREATE INDEX idx_obs_llm_application   ON obs_llm_events(application_id);
CREATE INDEX idx_obs_llm_model         ON obs_llm_events(model_name);
CREATE INDEX idx_obs_llm_timestamp     ON obs_llm_events(timestamp DESC);
```

### 6.3 Agent Events Table

```sql
CREATE TABLE obs_agent_events (
    id                    BIGSERIAL PRIMARY KEY,
    event_id              VARCHAR(128) UNIQUE NOT NULL REFERENCES obs_events(event_id),
    timestamp             TIMESTAMPTZ  NOT NULL,
    correlation_id        VARCHAR(256) NOT NULL,
    application_id        VARCHAR(64),
    agent_id              VARCHAR(128),
    agent_version         VARCHAR(64),
    step_name             VARCHAR(128),
    step_number           INTEGER,
    loop_count            INTEGER,
    handoff_count         INTEGER,
    termination_reason    VARCHAR(128),
    latency_ms            INTEGER,
    status                VARCHAR(32),
    error_code            VARCHAR(64)
);

CREATE INDEX idx_obs_agent_correlation ON obs_agent_events(correlation_id);
CREATE INDEX idx_obs_agent_agent_id    ON obs_agent_events(agent_id);
CREATE INDEX idx_obs_agent_timestamp   ON obs_agent_events(timestamp DESC);
```

### 6.4 Tool Events Table

```sql
CREATE TABLE obs_tool_events (
    id                    BIGSERIAL PRIMARY KEY,
    event_id              VARCHAR(128) UNIQUE NOT NULL REFERENCES obs_events(event_id),
    timestamp             TIMESTAMPTZ  NOT NULL,
    correlation_id        VARCHAR(256) NOT NULL,
    application_id        VARCHAR(64),
    agent_id              VARCHAR(128),
    tool_id               VARCHAR(128),
    tool_name             VARCHAR(128),
    tool_type             VARCHAR(64),
    http_status           INTEGER,
    latency_ms            INTEGER,
    retry_count           INTEGER DEFAULT 0,
    timeout_flag          BOOLEAN DEFAULT FALSE,
    response_size_bytes   INTEGER,
    status                VARCHAR(32),
    error_code            VARCHAR(64)
);

CREATE INDEX idx_obs_tool_correlation  ON obs_tool_events(correlation_id);
CREATE INDEX idx_obs_tool_tool_id      ON obs_tool_events(tool_id);
CREATE INDEX idx_obs_tool_timestamp    ON obs_tool_events(timestamp DESC);
```

### 6.5 RAG / Ingestion Events Table

```sql
CREATE TABLE obs_rag_events (
    id                       BIGSERIAL PRIMARY KEY,
    event_id                 VARCHAR(128) UNIQUE NOT NULL REFERENCES obs_events(event_id),
    timestamp                TIMESTAMPTZ  NOT NULL,
    correlation_id           VARCHAR(256) NOT NULL,
    application_id           VARCHAR(64),
    agent_id                 VARCHAR(128),
    rag_id                   VARCHAR(128),
    knowledge_base           VARCHAR(256),
    embedding_model          VARCHAR(128),
    retrieved_chunk_count    INTEGER,
    avg_relevance_score      NUMERIC(5, 4),
    no_result_flag           BOOLEAN DEFAULT FALSE,
    citation_coverage_pct    NUMERIC(5, 2),
    context_truncation_flag  BOOLEAN DEFAULT FALSE,
    retrieval_latency_ms     INTEGER,
    embedding_latency_ms     INTEGER,
    status                   VARCHAR(32),
    error_code               VARCHAR(64),
    -- Document ingestion specific
    job_id                   VARCHAR(128),
    document_id              VARCHAR(256),
    job_status               VARCHAR(32),
    input_tokens_embed       INTEGER
);

CREATE INDEX idx_obs_rag_correlation   ON obs_rag_events(correlation_id);
CREATE INDEX idx_obs_rag_rag_id        ON obs_rag_events(rag_id);
CREATE INDEX idx_obs_rag_timestamp     ON obs_rag_events(timestamp DESC);
```

### 6.6 Feedback Events Table

```sql
CREATE TABLE obs_feedback_events (
    id                    BIGSERIAL PRIMARY KEY,
    event_id              VARCHAR(128) UNIQUE NOT NULL REFERENCES obs_events(event_id),
    timestamp             TIMESTAMPTZ  NOT NULL,
    feedback_id           VARCHAR(128),
    correlation_id        VARCHAR(256),
    application_id        VARCHAR(64),
    agent_id              VARCHAR(128),
    feedback_rating       INTEGER,      -- 1-5
    feedback_thumbs       VARCHAR(8),   -- 'up' | 'down'
    feedback_sentiment    VARCHAR(16),  -- 'positive' | 'negative' | 'neutral'
    feedback_category     VARCHAR(64),
    feedback_role         VARCHAR(64),  -- 'user' | 'cso' | 'sme' | 'admin'
    resolution_status     VARCHAR(32) DEFAULT 'open',
    linked_incident_id    VARCHAR(128)
);

CREATE INDEX idx_obs_feedback_correlation ON obs_feedback_events(correlation_id);
CREATE INDEX idx_obs_feedback_application ON obs_feedback_events(application_id);
```

### 6.7 Dead Letter Table

```sql
CREATE TABLE obs_dead_letter (
    id                    BIGSERIAL PRIMARY KEY,
    received_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    source_service        VARCHAR(64),
    raw_payload           JSONB,
    validation_errors     TEXT[],
    reprocessed           BOOLEAN DEFAULT FALSE,
    reprocessed_at        TIMESTAMPTZ
);

CREATE INDEX idx_dead_letter_received  ON obs_dead_letter(received_at DESC);
CREATE INDEX idx_dead_letter_service   ON obs_dead_letter(source_service);
```

### 6.8 Hourly Aggregates Table

OIS runs a scheduled job every hour to materialise aggregates.

```sql
CREATE TABLE obs_hourly_summary (
    hour_bucket           TIMESTAMPTZ NOT NULL,
    application_id        VARCHAR(64) NOT NULL,
    service_name          VARCHAR(64) NOT NULL,
    event_type_group      VARCHAR(32) NOT NULL, -- 'llm' | 'tool' | 'agent' | 'rag' | 'request' | 'feedback'
    event_count           BIGINT DEFAULT 0,
    success_count         BIGINT DEFAULT 0,
    failure_count         BIGINT DEFAULT 0,
    avg_latency_ms        NUMERIC(10, 2),
    p95_latency_ms        NUMERIC(10, 2),
    total_tokens          BIGINT DEFAULT 0,
    total_cost_usd        NUMERIC(12, 6) DEFAULT 0,
    PRIMARY KEY (hour_bucket, application_id, service_name, event_type_group)
);
```

---

## 7. Service Implementation Plan

### 7.1 Tech Stack

| Concern | Choice | Rationale |
|---|---|---|
| Framework | **FastAPI** | Consistent with all existing services; async support; OpenAPI docs auto-generated |
| Language | **Python 3.11+** | Consistent with platform |
| DB driver | **asyncpg + SQLAlchemy (async)** | Async write path; connection pooling |
| Validation | **Pydantic v2** | Strong typing; fast validation; clear error messages |
| Auth | **COIN JWT** (same as other services) | Consistent with platform auth; M2M token for service-to-service |
| Scheduling | **APScheduler** | Hourly aggregate rollup; consistent with Consumer Service |
| Metrics | **prometheus-fastapi-instrumentator** | `/metrics` endpoint; latency histograms |
| Deployment | **Docker + Kubernetes / Helm** | Consistent with platform |

### 7.2 Directory Structure

```
obs-ingestion-service/
├── main.py                          # FastAPI app, lifespan, middleware setup
├── api/
│   └── v1/
│       ├── api.py                   # Router registration
│       ├── ingest.py                # POST /v1/ingest, POST /v1/ingest/batch
│       └── health.py                # GET /health
├── models/
│   ├── event_schema.py              # Pydantic model: ObsEvent (the standard schema)
│   ├── event_types.py               # EventType enum (controlled vocabulary)
│   └── service_names.py             # ServiceName enum
├── processing/
│   ├── validator.py                 # Mandatory field check, event_type whitelist
│   ├── enricher.py                  # Fill environment, service_name, user_hash, cost
│   ├── error_mapper.py              # Raw exception string → error_code_catalog
│   └── router.py                    # Route event to correct DB table writer
├── writers/
│   ├── base_writer.py               # Write to obs_events (always)
│   ├── llm_writer.py                # Write to obs_llm_events
│   ├── agent_writer.py              # Write to obs_agent_events
│   ├── tool_writer.py               # Write to obs_tool_events
│   ├── rag_writer.py                # Write to obs_rag_events
│   ├── feedback_writer.py           # Write to obs_feedback_events
│   └── dead_letter_writer.py        # Write to obs_dead_letter
├── aggregation/
│   └── hourly_rollup.py             # APScheduler job: obs_hourly_summary
├── config/
│   ├── settings.py                  # Pydantic Settings (env vars)
│   ├── log_filters.py               # AppInfoFilter (service_name, environment)
│   └── pricing.py                   # Model pricing lookup table for cost calculation
├── db/
│   ├── connection.py                # asyncpg pool setup
│   └── migrations/
│       ├── 001_obs_events.sql
│       ├── 002_obs_llm_events.sql
│       ├── 003_obs_agent_events.sql
│       ├── 004_obs_tool_events.sql
│       ├── 005_obs_rag_events.sql
│       ├── 006_obs_feedback_events.sql
│       ├── 007_obs_dead_letter.sql
│       └── 008_obs_hourly_summary.sql
├── auth/
│   └── coin_jwt.py                  # COIN JWT / M2M token validation
├── logconfig.yaml                   # Structured JSON logging config
├── requirements.txt
├── Dockerfile
├── helm/                            # Helm chart for K8s deployment
└── tests/
    ├── test_validation.py
    ├── test_enrichment.py
    └── test_writers.py
```

### 7.3 Core Code Snippets

#### Pydantic Event Schema (models/event_schema.py)

```python
from pydantic import BaseModel, Field, field_validator
from typing import Optional, Any
from datetime import datetime
import uuid

class EventPayload(BaseModel):
    input_tokens: Optional[int] = None
    output_tokens: Optional[int] = None
    total_tokens: Optional[int] = None
    estimated_cost_usd: Optional[float] = None
    finish_reason: Optional[str] = None
    rate_limit_hit: Optional[bool] = None
    safety_blocked: Optional[bool] = None
    confidence_score: Optional[float] = None
    step_name: Optional[str] = None
    step_number: Optional[int] = None
    loop_count: Optional[int] = None
    handoff_count: Optional[int] = None
    termination_reason: Optional[str] = None
    tool_type: Optional[str] = None
    retry_count: Optional[int] = None
    timeout_flag: Optional[bool] = None
    response_size_bytes: Optional[int] = None
    rag_knowledge_base: Optional[str] = None
    retrieved_chunk_count: Optional[int] = None
    avg_relevance_score: Optional[float] = None
    no_result_flag: Optional[bool] = None
    citation_coverage_pct: Optional[float] = None
    context_truncation_flag: Optional[bool] = None
    job_id: Optional[str] = None
    document_id: Optional[str] = None
    job_status: Optional[str] = None
    embedding_model: Optional[str] = None
    embedding_latency_ms: Optional[int] = None
    feedback_rating: Optional[int] = None
    feedback_thumbs: Optional[str] = None
    feedback_sentiment: Optional[str] = None
    feedback_category: Optional[str] = None
    feedback_role: Optional[str] = None
    resolution_status: Optional[str] = None
    linked_incident_id: Optional[str] = None
    kafka_topic: Optional[str] = None
    kafka_partition: Optional[int] = None
    kafka_offset: Optional[int] = None
    consumer_group: Optional[str] = None
    kafka_lag: Optional[int] = None
    dlq_flag: Optional[bool] = None
    s3_payload_uri: Optional[str] = None
    extra: Optional[dict[str, Any]] = Field(default_factory=dict)

class ObsEvent(BaseModel):
    # Required
    event_id: str = Field(default_factory=lambda: f"evt_{uuid.uuid4()}")
    event_type: str
    schema_version: str = "1.0"
    timestamp: datetime
    correlation_id: str
    service_name: str
    environment: str
    application_id: str
    status: str

    # Optional — enriched by OIS if absent
    span_id: Optional[str] = None
    parent_span_id: Optional[str] = None
    request_id: Optional[str] = None
    component: Optional[str] = None
    app_container: Optional[str] = None
    soe_id: Optional[str] = None
    lob: Optional[str] = None
    tenant_id: Optional[str] = None
    consumer_coin: Optional[str] = None
    user_hash: Optional[str] = None
    agent_id: Optional[str] = None
    agent_version: Optional[str] = None
    tool_id: Optional[str] = None
    rag_id: Optional[str] = None
    model_name: Optional[str] = None
    model_provider: Optional[str] = None
    prompt_template_id: Optional[str] = None
    latency_ms: Optional[int] = None
    error_code: Optional[str] = None
    http_status: Optional[int] = None
    payload: Optional[EventPayload] = Field(default_factory=EventPayload)

    @field_validator("status")
    @classmethod
    def validate_status(cls, v: str) -> str:
        allowed = {"success", "failed", "partial", "timeout", "info"}
        if v not in allowed:
            raise ValueError(f"status must be one of {allowed}")
        return v
```

#### Enrichment (processing/enricher.py)

```python
import hashlib
from config.pricing import MODEL_PRICING

class Enricher:
    def enrich(self, event: dict, source_service_header: str) -> dict:
        # Inject service_name from header if missing
        if not event.get("service_name"):
            event["service_name"] = source_service_header or "unknown"

        # Hash SOE_ID / user identifier if plain text
        if event.get("soe_id") and not event.get("user_hash"):
            event["user_hash"] = "sha256_" + hashlib.sha256(
                event["soe_id"].encode()
            ).hexdigest()[:16]
            event["soe_id"] = None  # redact plain text

        # Calculate estimated_cost if tokens are present but cost is absent
        payload = event.get("payload") or {}
        if (
            payload.get("total_tokens")
            and event.get("model_name")
            and not payload.get("estimated_cost_usd")
        ):
            rate = MODEL_PRICING.get(event["model_name"], 0.0)
            payload["estimated_cost_usd"] = round(
                payload["total_tokens"] * rate / 1000, 8
            )

        return event
```

#### Ingest Endpoint (api/v1/ingest.py)

```python
from fastapi import APIRouter, Request, HTTPException
from models.event_schema import ObsEvent
from processing.validator import Validator
from processing.enricher import Enricher
from processing.error_mapper import ErrorMapper
from processing.router import EventRouter

router = APIRouter()
validator = Validator()
enricher = Enricher()
error_mapper = ErrorMapper()
event_router = EventRouter()

@router.post("/ingest", status_code=202)
async def ingest_event(event: ObsEvent, request: Request):
    source_service = request.headers.get("X-Source-Service", "")
    raw = event.model_dump()

    warnings = validator.validate(raw)
    if warnings:
        await event_router.write_dead_letter(raw, warnings)
        return {"accepted": True, "event_id": raw["event_id"], "warnings": warnings}

    raw = enricher.enrich(raw, source_service)
    raw = error_mapper.map(raw)
    await event_router.route(raw)

    return {"accepted": True, "event_id": raw["event_id"]}

@router.post("/ingest/batch", status_code=202)
async def ingest_batch(payload: dict, request: Request):
    events = payload.get("events", [])
    if len(events) > 500:
        raise HTTPException(status_code=400, detail="Maximum 500 events per batch")
    results = []
    for ev in events:
        try:
            obs = ObsEvent(**ev)
            raw = obs.model_dump()
            warnings = validator.validate(raw)
            if warnings:
                await event_router.write_dead_letter(raw, warnings)
                results.append({"event_id": raw["event_id"], "status": "dead_letter"})
            else:
                raw = enricher.enrich(raw, request.headers.get("X-Source-Service", ""))
                raw = error_mapper.map(raw)
                await event_router.route(raw)
                results.append({"event_id": raw["event_id"], "status": "accepted"})
        except Exception as exc:
            results.append({"status": "error", "detail": str(exc)})
    return {"results": results}
```

---

## 8. Per-Service Integration Guide

Each service needs minimal changes to start emitting to OIS.

### 8.1 Shared Emitter Utility

Create one shared function each service can call:

```python
# shared/obs_emitter.py
import httpx
import uuid
from datetime import datetime, timezone

OIS_URL = "http://obs-ingestion-service/v1/ingest"

async def emit_event(
    event_type: str,
    correlation_id: str,
    service_name: str,
    application_id: str,
    environment: str,
    status: str,
    **kwargs
):
    payload = {
        "event_id": f"evt_{uuid.uuid4()}",
        "event_type": event_type,
        "schema_version": "1.0",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "correlation_id": correlation_id,
        "service_name": service_name,
        "environment": environment,
        "application_id": application_id,
        "status": status,
        "payload": kwargs.get("payload", {}),
    }
    # Merge optional fields
    for field in ["span_id", "agent_id", "tool_id", "model_name", "latency_ms",
                  "error_code", "http_status", "lob", "user_hash", "soe_id"]:
        if field in kwargs:
            payload[field] = kwargs[field]

    async with httpx.AsyncClient(timeout=2.0) as client:
        try:
            await client.post(OIS_URL, json=payload,
                              headers={"X-Source-Service": service_name})
        except Exception:
            pass  # Fire-and-forget; never block the main request
```

### 8.2 Per-Service Changes Required

| Service | Change Required | Effort |
|---|---|---|
| **Agent Executor** | Add `emit_event()` calls in `AgentExecutionService` at start/step/complete/fail; emit LLM events with token counts already in `audit_table`; emit TOOL events from TOOL audit rows | Medium — data exists in audit_table; needs async emit calls wired in |
| **Agentic Orchestration** | Add `emit_event()` calls in `MessageProcessingService` for AGENT_EXECUTION_REQUEST_PRODUCED, PLAN_CREATED, FINAL_RESPONSE_CONSUMED; add LLM emit in planner when VertexAI called | Medium — needs LLM token capture added to planner |
| **GSSP GS** | Add `emit_event()` in HTTP interceptor (REQUEST_RECEIVED, RESPONSE_DELIVERED) and in each generator after LLM call (LLM_CALL_COMPLETED with token counts from `LLMUsageMetrics`) | Low — LLMUsageMetrics already has token counts; just emit them |
| **GSSP QS** | Add `emit_event()` in retriever (RAG_RETRIEVAL_STARTED, RAG_RETRIEVAL_COMPLETED, RAG_NO_RESULT) with chunk count and relevance score | Medium — need to extract retrieval metrics from existing retriever |
| **GSSP RS** | Add `emit_event()` for RESPONSE_DELIVERED with confidence_score, latency_ms | Low — small surface area |
| **Data Ingestion** | Add `emit_event()` in pipeline (INGESTION_JOB_STARTED, INGESTION_JOB_COMPLETED, DOCUMENT_INDEXED, DOCUMENT_EMBEDDING_CREATED) | Low — job lifecycle already tracked; just emit each transition |
| **Consumer Service** | Add `emit_event()` in `BaseTenant.ingest()` at each pipeline stage | Low — same as Data Ingestion; pipeline stages are well-defined |
| **User Feedback** | Add `emit_event(event_type="FEEDBACK_SUBMITTED")` in feedback submission handler with rating, thumbs, correlation_id, category | Low — single endpoint |

---

## 9. Model Pricing Table (for cost calculation)

```python
# config/pricing.py — price per 1,000 tokens in USD
MODEL_PRICING = {
    # Google Vertex AI
    "gemini-1.5-pro": 0.00125,
    "gemini-1.5-flash": 0.000075,
    "gemini-2.0-flash": 0.0001,
    # Anthropic Claude
    "claude-3-5-sonnet": 0.003,
    "claude-3-haiku": 0.00025,
    "claude-opus-4": 0.015,
    # Llama (internal)
    "llama-3-70b": 0.0008,
    # Default (unknown model)
    "default": 0.001,
}
```

---

## 10. Implementation Phases

### Phase 1 — Foundation (Week 1–2)

| Task | Owner | Output |
|---|---|---|
| Create OIS FastAPI project skeleton | Platform Engineering | Repo with health + ingest endpoints |
| Implement Pydantic schema (Section 3) | Platform Engineering | `ObsEvent` model with all fields |
| Run DB migrations 001–008 | Data Engineering | PostgreSQL tables created |
| Implement Validator + dead-letter writer | Platform Engineering | Invalid events stored, not dropped |
| Deploy OIS to dev environment | DevOps | OIS reachable at internal URL |

### Phase 2 — Enrichment + Writers (Week 3–4)

| Task | Owner | Output |
|---|---|---|
| Implement Enricher (environment, user_hash, cost) | Platform Engineering | Enriched events in DB |
| Implement ErrorMapper (exception → error_code) | SRE | Standard error codes in obs_events |
| Implement 6 typed writers (LLM, Agent, Tool, RAG, Feedback, Dead Letter) | Platform Engineering | Events in correct tables |
| Implement EventRouter | Platform Engineering | Event type → correct writer |
| Add `/metrics` Prometheus endpoint | Platform Engineering | Grafana can scrape OIS |

### Phase 3 — Service Integration (Week 5–7)

Integration order — lowest effort first:

| Priority | Service | Emit Events Added |
|---|---|---|
| P0 | **GSSP GS** | REQUEST_RECEIVED, LLM_CALL_COMPLETED, RESPONSE_DELIVERED |
| P0 | **User Feedback** | FEEDBACK_SUBMITTED |
| P1 | **Data Ingestion** | INGESTION_JOB_STARTED, INGESTION_JOB_COMPLETED, DOCUMENT_INDEXED |
| P1 | **Consumer Service** | INGESTION_JOB_STARTED, INGESTION_JOB_COMPLETED, DOCUMENT_INDEXED |
| P2 | **Agent Executor** | AGENT_STARTED, AGENT_STEP_COMPLETED, LLM_CALL_COMPLETED, TOOL_CALL_COMPLETED, AGENT_FAILED |
| P2 | **Agentic Orchestration** | REQUEST_RECEIVED, PLAN_CREATED, AGENT_EXECUTION_REQUEST_PRODUCED, FINAL_RESPONSE_CONSUMED, HIL_REQUEST_SENT |
| P3 | **GSSP QS** | RAG_RETRIEVAL_STARTED, RAG_RETRIEVAL_COMPLETED, RAG_NO_RESULT |
| P3 | **GSSP RS** | RESPONSE_DELIVERED |

### Phase 4 — Aggregation + Dashboards (Week 8–9)

| Task | Owner | Output |
|---|---|---|
| Implement `obs_hourly_summary` rollup job | Data Engineering | Hourly aggregates populated |
| Connect Grafana to `obs_events` and aggregates | SRE | Basic dashboards operational |
| Build error rate and latency dashboards | SRE | Alert rules can fire on real data |
| Validate all 8 services emitting events | All teams | End-to-end proof |

---

## 11. Non-Functional Requirements

| Requirement | Target |
|---|---|
| OIS response time | p95 < 50ms (fire-and-forget; async DB write) |
| Throughput | 5,000 events/second (horizontal scale via K8s replicas) |
| Availability | 99.9% — services must not fail if OIS is down (emit is fire-and-forget) |
| Data durability | Dead-letter table ensures no event is silently dropped |
| PII | SOE_ID hashed at enrichment; raw value never stored in `obs_events` |
| Authentication | COIN JWT M2M token required on all ingest calls |
| Retention | `obs_events` — 90 days; `obs_hourly_summary` — 1 year; `obs_dead_letter` — 30 days |

---

## 12. What This Service Does NOT Do

To keep OIS focused and lightweight:

- **Does not replace existing application logs** — services keep their local JSON logs; OIS is additive
- **Does not write to Elasticsearch** — OIS writes to PostgreSQL only; Elasticsearch integration is a separate Telemetry Processor concern
- **Does not compute business KPIs** — `agg_daily_kpi_metrics` is computed by a separate KPI calculation job
- **Does not route incidents** — incident routing is a separate Incident Router Service consuming from the OIS tables
- **Does not replace the audit_table in Agent Executor** — that table serves a replay/debugging purpose; OIS is the observability view
