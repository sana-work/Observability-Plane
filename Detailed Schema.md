Below is a **team-ready explanation** of how to do the foundation work and exactly what needs to be captured. This maps to the Phase 1 items in your implementation guide: define schema, finalize `correlation_id`, create catalogs, define retention/RBAC, and create PostgreSQL/Elasticsearch foundations. Your guide says Phase 1 should deliver Kafka topic schema, error catalog seed data, metric catalog seed data, Elasticsearch templates, PostgreSQL DDL, and S3 buckets before the SDK and processor work begins. 

---

# 1. Define common schema

## What it means

A **common schema** is the standard event format that every service must follow when emitting observability data.

Without this, each service may log differently:

```text
orchestration logs: appId
executor logs: application_id
LLM logs: app_name
tool logs: serviceId
```

That becomes impossible to join cleanly.

So we define one standard event envelope.

## What needs to be done

Create a standard JSON event structure for all observability events.

Example:

```json
{
  "event_id": "evt_123",
  "event_type": "TOOL_CALL_FAILED",
  "schema_version": "1.0",
  "timestamp": "2026-05-21T10:30:00Z",

  "correlation_id": "CORR_abc123",
  "request_id": "REQ_789",
  "span_id": "SPAN_001",
  "parent_span_id": "SPAN_PARENT_000",

  "application_id": "179524",
  "app_container": "gssp-gs",
  "soe_id": "PricingDomeApp",
  "lob": "Payments",
  "environment": "prod",
  "tenant_id": "tenant_001",

  "service_name": "executor-service",
  "component": "tool-executor",

  "agent_id": "payment_scrutiny_agent",
  "tool_id": "payment_details_api",
  "rag_id": null,
  "model_name": null,

  "status": "failed",
  "latency_ms": 4500,
  "error_code": "TOOL_TIMEOUT",
  "http_status": 504,

  "payload": {
    "retry_count": 2,
    "timeout_flag": true
  }
}
```

## Mandatory fields for every event

| Field            |   Required? | Why                                           |
| ---------------- | ----------: | --------------------------------------------- |
| `event_id`       |         Yes | Unique ID for this event                      |
| `event_type`     |         Yes | Tells what happened                           |
| `schema_version` |         Yes | Allows future schema changes                  |
| `timestamp`      |         Yes | Required for time-series dashboards           |
| `correlation_id` |         Yes | Joins all events for one request              |
| `application_id` |         Yes | Required for app-level dashboard              |
| `environment`    |         Yes | prod/dev/stage filtering                      |
| `service_name`   |         Yes | Identifies emitting service                   |
| `component`      |         Yes | Orchestration, executor, LLM, tool, RAG, etc. |
| `status`         |         Yes | success/failed/partial/timeout                |
| `latency_ms`     | Recommended | Required for performance dashboards           |

Your guide also defines platform request fields such as `correlation_id`, `span_id`, `parent_span_id`, `application_id`, `app_container`, `soe_id`, `lob`, `tenant_id`, `user_hash`, `channel`, `request_type`, `status`, `latency_ms`, `error_code`, token counts, and cost. 

## Event types to standardize

Create a controlled list of event types.

### Request events

```text
REQUEST_RECEIVED
REQUEST_COMPLETED
REQUEST_FAILED
RESPONSE_DELIVERED
```

### Orchestration events

```text
AUTH_COMPLETED
CONFIG_LOADED
PLAN_CREATED
AGENT_EXECUTION_REQUEST_PRODUCED
FINAL_RESPONSE_CONSUMED
RESPONSE_BUILT
```

### Kafka events

```text
KAFKA_MESSAGE_PRODUCED
KAFKA_MESSAGE_CONSUMED
KAFKA_MESSAGE_DLQ
KAFKA_LAG_RECORDED
```

### Agent events

```text
AGENT_STARTED
AGENT_STEP_STARTED
AGENT_STEP_COMPLETED
AGENT_LOOP_ITERATION
AGENT_HANDOFF
AGENT_COMPLETED
AGENT_FAILED
AGENT_TIMEOUT
```

### LLM events

```text
LLM_CALL_STARTED
LLM_CALL_COMPLETED
LLM_CALL_FAILED
LLM_RATE_LIMITED
LLM_SAFETY_BLOCKED
```

### Tool events

```text
TOOL_CALL_STARTED
TOOL_CALL_COMPLETED
TOOL_CALL_FAILED
TOOL_CALL_TIMEOUT
```

### RAG events

```text
RAG_RETRIEVAL_STARTED
RAG_RETRIEVAL_COMPLETED
RAG_RETRIEVAL_FAILED
RAG_NO_RESULT
RAG_INDEX_HEALTH_CHECKED
```

### Guardrail events

```text
GUARDRAIL_EVALUATED
GUARDRAIL_BLOCKED
GUARDRAIL_REDACTED
GUARDRAIL_ESCALATED
```

### Feedback events

```text
FEEDBACK_SUBMITTED
FEEDBACK_REVIEWED
FEEDBACK_INCIDENT_TRIGGERED
```

### Document/multimodal events

```text
DOCUMENT_UPLOADED
DOCUMENT_STORED_IN_S3
DOCUMENT_EXTRACTION_STARTED
DOCUMENT_EXTRACTION_COMPLETED
DOCUMENT_EXTRACTION_FAILED
DOCUMENT_INDEXED
DOCUMENT_EMBEDDING_CREATED
```

---

# 2. Finalize `correlation_id` rules

## What it means

`correlation_id` is the **single most important field** in the observability design.

It connects everything that happened for one request.

Example:

```text
User request
  → Orchestration
  → Kafka
  → Executor
  → Agent
  → Tool call
  → RAG retrieval
  → LLM call
  → Guardrail check
  → Final response
  → User feedback
```

All of these must have the same:

```text
correlation_id = CORR_abc123
```

Your implementation guide says every minimum request flow must produce events like `REQUEST_RECEIVED`, `AGENT_STARTED`, one `LLM_CALL_COMPLETED` or `TOOL_CALL_COMPLETED`, `AGENT_COMPLETED`, and `RESPONSE_DELIVERED`, all linked by the same `correlation_id`. 

## Rules to define

### Rule 1: Where `correlation_id` is created

Create `correlation_id` at the first entry point:

```text
API Gateway
Client UI backend
Orchestration Service entry point
Webhook endpoint
Batch ingestion job
```

If incoming request already has a valid `correlation_id`, reuse it.
If not, generate one.

Recommended format:

```text
CORR_<uuid>
```

Example:

```text
CORR_7f1b2c90-79ec-4ed2-aee4-8dca04b734f2
```

### Rule 2: It must never change during a request

For one user request, do not generate new correlation IDs in downstream services.

Bad:

```text
API Gateway: CORR_001
Executor: CORR_999
LLM: CORR_888
```

Good:

```text
API Gateway: CORR_001
Executor: CORR_001
LLM: CORR_001
Tool: CORR_001
Feedback: CORR_001
```

### Rule 3: Carry it in HTTP headers

For API calls between services:

```text
X-Correlation-ID: CORR_abc123
```

Also pass W3C tracing headers:

```text
traceparent: 00-{trace-id}-{span-id}-01
tracestate: intentiq={application_id};env={environment}
```

Your refined architecture explicitly says Kafka messages should carry W3C `traceparent` as a message header, plus `tracestate` and `correlation_id`. 

### Rule 4: Carry it in Kafka message headers and payload

For Kafka, put `correlation_id` in both:

```text
Kafka header
Kafka message body
```

Header example:

```json
{
  "correlation_id": "CORR_abc123",
  "traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
  "schema-version": "1.0"
}
```

Payload example:

```json
{
  "event_type": "AGENT_EXECUTION_REQUEST_PRODUCED",
  "correlation_id": "CORR_abc123",
  "application_id": "179524"
}
```

### Rule 5: Use `span_id` for individual steps

`correlation_id` identifies the full request.
`span_id` identifies one operation inside the request.

Example:

| Event            | `correlation_id` | `span_id` | `parent_span_id` |
| ---------------- | ---------------- | --------- | ---------------- |
| Request received | CORR_001         | SPAN_1    | null             |
| Planner executed | CORR_001         | SPAN_2    | SPAN_1           |
| Kafka produced   | CORR_001         | SPAN_3    | SPAN_2           |
| Agent started    | CORR_001         | SPAN_4    | SPAN_3           |
| LLM called       | CORR_001         | SPAN_5    | SPAN_4           |
| Tool called      | CORR_001         | SPAN_6    | SPAN_4           |

### Rule 6: Feedback must link back to `correlation_id`

When user feedback is submitted, it should include:

```text
feedback_id
correlation_id
application_id
agent_id
response_id
rating
sentiment
feedback_category
```

That lets the team connect negative feedback to the exact request, agent, tool, model, RAG chunks, and response.

---

# 3. Create error code catalog

## What it means

The **error code catalog** standardizes all platform errors.

Without this, different services will report errors differently:

```text
TimeoutException
504 Gateway Timeout
Tool timed out
TOOL_ERROR
ERR_500
```

The catalog maps these into consistent platform error codes.

## What needs to be created

Create a PostgreSQL table:

```sql
CREATE TABLE error_code_catalog (
    error_code        VARCHAR(64) PRIMARY KEY,
    error_category    VARCHAR(64),
    severity          VARCHAR(16),
    description       TEXT,
    runbook_url       VARCHAR(512),
    owner_team        VARCHAR(128),
    retryable_flag    BOOLEAN DEFAULT FALSE,
    alertable_flag    BOOLEAN DEFAULT TRUE,
    created_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

## What needs to be captured

| Field            | Meaning                                    |
| ---------------- | ------------------------------------------ |
| `error_code`     | Standard code, e.g. `TOOL_TIMEOUT`         |
| `error_category` | Platform, LLM, Tool, RAG, Guardrail, Kafka |
| `severity`       | critical/high/medium/low                   |
| `description`    | Human-readable explanation                 |
| `runbook_url`    | Link to troubleshooting steps              |
| `owner_team`     | Responsible team                           |
| `retryable_flag` | Whether retry is allowed                   |
| `alertable_flag` | Whether this should trigger alert          |
| `dashboard_link` | Optional link to related dashboard         |

## Suggested error categories and examples

| Category      | Error Code                   | Meaning                                 |
| ------------- | ---------------------------- | --------------------------------------- |
| Platform      | `REQUEST_TIMEOUT`            | End-to-end request timed out            |
| Platform      | `AUTH_FAILED`                | User/app authorization failed           |
| Orchestration | `CONFIG_LOAD_FAILED`         | Agent/tool config could not be loaded   |
| Orchestration | `PLAN_CREATION_FAILED`       | Planner failed to create execution plan |
| Kafka         | `KAFKA_PRODUCE_FAILED`       | Failed to publish Kafka message         |
| Kafka         | `KAFKA_CONSUME_FAILED`       | Failed to consume Kafka message         |
| Kafka         | `KAFKA_LAG_HIGH`             | Consumer lag crossed threshold          |
| Agent         | `AGENT_EXECUTION_FAILED`     | Agent failed during execution           |
| Agent         | `AGENT_TIMEOUT`              | Agent exceeded max execution time       |
| Agent         | `AGENT_LOOP_LIMIT_REACHED`   | Loop agent reached max iterations       |
| LLM           | `LLM_TIMEOUT`                | Model call timed out                    |
| LLM           | `LLM_RATE_LIMITED`           | Provider rate limit hit                 |
| LLM           | `LLM_SAFETY_BLOCKED`         | Model output blocked by safety          |
| LLM           | `PROMPT_TEMPLATE_ERROR`      | Prompt template missing/invalid         |
| Tool          | `TOOL_TIMEOUT`               | Tool call timed out                     |
| Tool          | `TOOL_AUTH_FAILED`           | Tool authentication failed              |
| Tool          | `TOOL_SCHEMA_INVALID`        | Tool input schema validation failed     |
| Tool          | `TOOL_5XX_ERROR`             | Tool returned server error              |
| RAG           | `RAG_NO_RESULT`              | No chunks retrieved                     |
| RAG           | `RAG_INDEX_STALE`            | Knowledge base not refreshed            |
| RAG           | `EMBEDDING_FAILED`           | Embedding generation failed             |
| Guardrail     | `GUARDRAIL_BLOCKED`          | Policy blocked input/output             |
| Guardrail     | `PII_DETECTED`               | PII detected and redacted               |
| Feedback      | `NEGATIVE_FEEDBACK_SPIKE`    | Negative feedback crossed threshold     |
| Document      | `DOCUMENT_EXTRACTION_FAILED` | PDF/image/doc processing failed         |
| Document      | `OCR_FAILED`                 | OCR failed for uploaded file            |

## How it is used

The Telemetry Processor maps raw errors to catalog errors:

```text
Raw exception: ReadTimeout
Mapped error: TOOL_TIMEOUT
Category: Tool
Severity: High
Runbook: /runbooks/tool-timeout
Owner: Tool Platform Team
```

This supports dashboards, alerts, RCA, and chatbot answers.

---

# 4. Create metric catalog

## What it means

The **metric catalog** defines approved metrics, formulas, source tables, dimensions, and aliases.

This is critical for the chatbot and dashboards because everyone should calculate metrics the same way.

Example problem without metric catalog:

```text
Team A calculates error rate as errors / requests.
Team B calculates error rate as failed responses / successful responses.
Team C excludes 4xx errors.
```

The catalog avoids this.

## What needs to be created

Create PostgreSQL table:

```sql
CREATE TABLE metric_catalog (
    metric_id         VARCHAR(64) PRIMARY KEY,
    metric_name       VARCHAR(256),
    metric_aliases    TEXT[],
    metric_category   VARCHAR(64),
    formula           TEXT,
    source_table      VARCHAR(128),
    time_grain        VARCHAR(32),
    dimensions        TEXT[],
    owner             VARCHAR(128),
    active_flag       BOOLEAN DEFAULT TRUE,
    description       TEXT,
    created_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

The guide says the chatbot semantic layer reads `metric_catalog`, and RBAC is applied by application/LOB before routing queries to PostgreSQL, Elasticsearch, S3, or Grafana. 

## What needs to be captured

| Field             | Meaning                                        |
| ----------------- | ---------------------------------------------- |
| `metric_id`       | Unique metric key                              |
| `metric_name`     | Display name                                   |
| `metric_aliases`  | Natural language names users may ask           |
| `metric_category` | Request, Agent, LLM, Tool, RAG, Feedback, Cost |
| `formula`         | Exact calculation                              |
| `source_table`    | PostgreSQL aggregate or ES index               |
| `time_grain`      | hourly/daily/request-level                     |
| `dimensions`      | application, agent, tool, model, LOB           |
| `owner`           | Responsible owner                              |
| `active_flag`     | Whether metric is available                    |
| `description`     | Business meaning                               |

## Initial metrics to create

| Metric Name            | Category | Formula                                   | Source                           |
| ---------------------- | -------- | ----------------------------------------- | -------------------------------- |
| Request Count          | Platform | `sum(request_count)`                      | `agg_hourly_application_metrics` |
| Success Rate           | Platform | `success_count / request_count * 100`     | `agg_hourly_application_metrics` |
| Error Rate             | Platform | `error_count / request_count * 100`       | `agg_hourly_application_metrics` |
| Avg Latency            | Platform | `avg(avg_latency_ms)`                     | `agg_hourly_application_metrics` |
| P95 Latency            | Platform | `p95_latency_ms`                          | PostgreSQL/Elasticsearch         |
| Agent Success Rate     | Agent    | `success_count / request_count * 100`     | `agg_hourly_agent_metrics`       |
| Agent Failure Rate     | Agent    | `error_count / request_count * 100`       | `agg_hourly_agent_metrics`       |
| Tool Failure Rate      | Tool     | `failure_count / call_count * 100`        | `agg_hourly_tool_metrics`        |
| Tool Timeout Rate      | Tool     | `timeout_count / call_count * 100`        | `agg_hourly_tool_metrics`        |
| LLM Total Tokens       | LLM      | `sum(total_tokens)`                       | `agg_hourly_llm_metrics`         |
| LLM Cost               | LLM      | `sum(estimated_cost)`                     | `agg_hourly_llm_metrics`         |
| LLM Error Rate         | LLM      | `error_count / llm_call_count * 100`      | `agg_hourly_llm_metrics`         |
| RAG No Result Rate     | RAG      | `no_result_count / retrieval_count * 100` | `agg_hourly_rag_metrics`         |
| RAG Faithfulness Score | RAG      | `avg(avg_faithfulness_score)`             | `agg_hourly_rag_metrics`         |
| Positive Feedback Rate | Feedback | `positive / total feedback * 100`         | `agg_daily_feedback_metrics`     |
| Negative Feedback Rate | Feedback | `negative / total feedback * 100`         | `agg_daily_feedback_metrics`     |
| Budget Utilization     | Cost     | `spent / budget * 100`                    | `budget_limits` + LLM aggregates |
| SLO Compliance         | SLO      | `achieved_pct >= target_pct`              | `daily_slo_compliance`           |

## Natural language aliases

For chatbot use, add aliases.

Example:

```text
metric_name: Tool Failure Rate
aliases:
- failed tool calls
- tool errors
- tool failure percentage
- tool call failures
- failed connector calls
```

So when user asks:

```text
How many failed tool calls happened yesterday?
```

The chatbot maps that to:

```text
metric: Tool Failure Count
source: agg_hourly_tool_metrics
```

---

# 5. Create KPI catalog

## What it means

The **KPI catalog** stores business KPIs, not just technical metrics.

Metrics are operational measurements.
KPIs are business outcomes.

Example:

```text
Metric: LLM token cost
KPI: FTE savings
Metric: RAG no-result rate
KPI: AI Insights Adoption Rate
Metric: Tool failure rate
KPI: Zero-Touch Search Success Rate
```

Your earlier KPI examples include IntentIQ, PegaCall, SSoT, and CoPilot KPIs like Manual Sentiment Correction Rate, Automated Urgency Accuracy, AI Insights Adoption Rate, AI Model Feedback Rate, Reduction in AHT, Average Handle Time, Call Transfer Rate, Zero-Touch Search Success Rate, and Query Resolution Rate. 

## What needs to be created

Create PostgreSQL table:

```sql
CREATE TABLE kpi_definition (
    kpi_id            VARCHAR(64) PRIMARY KEY,
    application_id    VARCHAR(64),
    agent_id          VARCHAR(64),
    kpi_name          VARCHAR(256) NOT NULL,
    kpi_category      VARCHAR(64),
    formula           TEXT,
    data_source       VARCHAR(128),
    required_attributes TEXT[],
    threshold_green   NUMERIC(12,4),
    threshold_yellow  NUMERIC(12,4),
    threshold_red     NUMERIC(12,4),
    owner             VARCHAR(128),
    business_objective TEXT,
    evidence          TEXT,
    decision_status   VARCHAR(32),
    active_flag       BOOLEAN DEFAULT TRUE,
    created_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

## KPI categories to use

Use these categories:

```text
Model Quality & Accuracy
User Adoption & Engagement
User Feedback & Continuous Improvement
Productivity & Efficiency
Risk, Compliance & Governance
Platform Performance
Guardrails & Safety
Cost Governance
RAG / Knowledge Quality
```

## Example KPI catalog entries

| KPI                                    | Category                               | Formula                                                            | Required Attributes                                     | Source                 |
| -------------------------------------- | -------------------------------------- | ------------------------------------------------------------------ | ------------------------------------------------------- | ---------------------- |
| Manual Sentiment Correction Rate       | Model Quality & Accuracy               | manual sentiment changes / total AI sentiment cases * 100          | case_id, initial_sentiment, final_sentiment, updated_by | AI insights table      |
| Automated Urgency Accuracy             | Model Quality & Accuracy               | cases with no manual urgency update / total AI urgency cases * 100 | case_id, initial_urgency, final_urgency, updated_by     | AI insights table      |
| AI Insights Adoption Rate              | User Adoption & Engagement             | users opening AI Insights / users with access * 100                | user_id, agent_id, click_timestamp                      | click events           |
| AI Model Feedback Rate                 | User Feedback & Continuous Improvement | feedback cases / total processed cases * 1000                      | feedback_id, case_id, category                          | feedback_case          |
| Case Summarization Time Saved          | Productivity & Efficiency              | manual reading time - summary tab time                             | case_id, time_spent, word_count                         | Celonis/process mining |
| Manual Effort Reduction / FTE Savings  | Productivity & Efficiency              | total cases * avg time saved / working hours per FTE               | total cases, avg time saved                             | Celonis                |
| First-Time User Attestation Compliance | Risk, Compliance & Governance          | users attested first time / users shown attestation * 100          | user_id, attestation_time                               | attestation events     |
| Guardrail Effectiveness                | Guardrails & Safety                    | blocked unsafe outputs / total risky attempts * 100                | policy_id, decision, violation_type                     | guardrail events       |
| SLO Compliance                         | Platform Performance                   | achieved availability vs target                                    | request_count, error_count                              | daily_slo_compliance   |
| Cost Budget Adherence                  | Cost Governance                        | actual spend / budget * 100                                        | model_name, tokens, budget                              | budget_limits          |

## KPI output table

Calculated KPI values should go into:

```sql
CREATE TABLE agg_daily_kpi_metrics (
    metric_date               DATE,
    kpi_id                    VARCHAR(64),
    application_id            VARCHAR(64),
    agent_id                  VARCHAR(64),
    kpi_value                 NUMERIC(16,6),
    target_value              NUMERIC(16,6),
    status                    VARCHAR(16),
    threshold_breach_flag     BOOLEAN DEFAULT FALSE,
    trend_direction           VARCHAR(8),
    PRIMARY KEY (metric_date, kpi_id, application_id, agent_id)
);
```

---

# 6. Define retention and RBAC

## A. Retention

Retention means how long each type of data is kept.

You need this because observability data can grow very fast.

Your prior architecture separates hot searchable events in Elasticsearch, structured aggregates in PostgreSQL, large artifacts in S3, and monitoring/alerts in Grafana; the storage components table also describes Elasticsearch as the hot operational event store, PostgreSQL as control-plane/aggregates, S3 as redacted payloads and full traces, Grafana as monitoring/alerting, Redis as runtime cache, and Kibana as operational search. 

## Retention policy recommendation

| Data Type                  | Store                      |                      Retention |
| -------------------------- | -------------------------- | -----------------------------: |
| Raw request events         | Elasticsearch              |                     30–90 days |
| Error events               | Elasticsearch              |                        90 days |
| Trace/span events          | Elasticsearch              |                     30–90 days |
| LLM/tool/RAG events        | Elasticsearch              |                     60–90 days |
| Guardrail events           | Elasticsearch              | 180 days or compliance-defined |
| Feedback events            | PostgreSQL + Elasticsearch |                      1–3 years |
| Hourly aggregates          | PostgreSQL                 |                         1 year |
| Daily KPI aggregates       | PostgreSQL                 |                      2–3 years |
| Redacted prompts/responses | S3                         |             compliance-defined |
| Full traces/debug bundles  | S3                         |                 90 days–1 year |
| RCA reports                | S3                         |                         1 year |
| Kafka observability topics | Kafka                      |                         7 days |
| Redis cache                | Redis                      |      minutes to days using TTL |

## What needs to be done

Create a retention matrix:

```text
data_type
store
retention_days
archive_required
delete_required
owner
compliance_reason
```

Example:

| Data Type                  | Store         | Retention | Owner           |
| -------------------------- | ------------- | --------: | --------------- |
| `ai-obs-payments-errors-*` | Elasticsearch |   90 days | SRE             |
| `redacted-prompts/`        | S3            |  180 days | AI Platform     |
| `raw-traces/`              | S3            |  365 days | Platform        |
| `agg_daily_kpi_metrics`    | PostgreSQL    |   3 years | Data/BI         |
| `feedback_case`            | PostgreSQL    |   3 years | Product/Quality |

## B. RBAC

RBAC means Role-Based Access Control.

Users should only see data for applications/LOBs they are allowed to access.

## Define roles

| Role                | Access                              |
| ------------------- | ----------------------------------- |
| Platform Admin      | All apps, all LOBs                  |
| LOB Admin           | All apps in assigned LOB            |
| Application Owner   | Own application only                |
| Agent Owner         | Assigned agents only                |
| Support Engineer    | Operational logs for assigned apps  |
| Business Viewer     | KPI dashboards only                 |
| Security/Governance | Guardrail, audit, compliance data   |
| Chatbot User        | Answers only within allowed app/LOB |

## What must be captured for RBAC

Add these fields to registry tables:

```text
application_id
lob
tenant_id
owner_team
support_contact
data_classification
allowed_roles
allowed_groups
```

Add an access mapping table:

```sql
CREATE TABLE access_policy (
    policy_id        VARCHAR(64) PRIMARY KEY,
    subject_type     VARCHAR(32), -- user, group, role
    subject_id       VARCHAR(128),
    application_id   VARCHAR(64),
    lob              VARCHAR(64),
    permission       VARCHAR(64), -- read_metrics, read_logs, read_payloads, admin
    active_flag      BOOLEAN DEFAULT TRUE
);
```

## RBAC rules

```text
1. Dashboard access must filter by application_id and LOB.
2. Chatbot must check RBAC before answering.
3. S3 artifact access requires stronger permission than dashboard access.
4. Raw prompt/response access should be admin-only or approval-based.
5. Audit all access to S3 artifacts.
```

Per-LOB Elasticsearch naming also supports RBAC. The refined architecture recommends patterns like `ai-obs-{lob}-{event_type}-*`, which enables per-LOB retention, index-level RBAC, storage quota enforcement, and Kibana per-LOB dashboards. 

---

# 7. Create PostgreSQL base tables

## What it means

PostgreSQL is the **control plane and aggregate store**.

It stores:

```text
registries
catalogs
KPI definitions
feedback cases
dashboard configs
alert thresholds
budget limits
hourly aggregates
daily aggregates
SLO compliance
RAG quality
vector health
```

The refined diagram maps PostgreSQL to registries, KPI/feedback/metric catalog, hourly/daily aggregates, budget limits, daily SLO compliance, daily RAG quality, vector health snapshots, alert thresholds, and dashboard configuration. 

## Tables to create first

### Registry tables

```text
application_registry
agent_registry
tool_registry
rag_registry
prompt_template_registry
```

These answer:

```text
Which application owns this event?
Which agent ran?
Which tool was called?
Which RAG index was used?
Which prompt template was used?
Who owns it?
```

### Governance/catalog tables

```text
error_code_catalog
metric_catalog
kpi_definition
alert_threshold
budget_limits
dashboard_config
access_policy
```

These answer:

```text
What does this error mean?
How is this metric calculated?
What KPIs exist?
When should alerts fire?
Who can see what?
```

### Feedback tables

```text
feedback_case
agg_daily_feedback_metrics
```

These answer:

```text
How much negative feedback are we getting?
Which agent has most low-rated responses?
Which feedback became an incident?
```

### Aggregate tables

```text
agg_hourly_application_metrics
agg_hourly_agent_metrics
agg_hourly_tool_metrics
agg_hourly_llm_metrics
agg_hourly_rag_metrics
agg_daily_kpi_metrics
daily_slo_compliance
daily_rag_quality
vector_health_snapshots
```

These make dashboards and chatbot fast.

## Base table responsibility map

| Table                            | Purpose                                       |
| -------------------------------- | --------------------------------------------- |
| `application_registry`           | App metadata, owner, LOB, SOE, tier           |
| `agent_registry`                 | Agent metadata, type, version, app mapping    |
| `tool_registry`                  | Tool metadata, endpoint, SLA, owner           |
| `rag_registry`                   | Knowledge base, vector index, embedding model |
| `prompt_template_registry`       | Prompt template and model mapping             |
| `error_code_catalog`             | Standard errors and runbooks                  |
| `metric_catalog`                 | Metric formulas and semantic aliases          |
| `kpi_definition`                 | Business KPI formulas and thresholds          |
| `feedback_case`                  | User feedback linked to `correlation_id`      |
| `alert_threshold`                | Source of truth for Grafana alerts            |
| `budget_limits`                  | Spend limits by app/model/period              |
| `access_policy`                  | RBAC permissions                              |
| `dashboard_config`               | Dashboard definitions and ownership           |
| `agg_hourly_application_metrics` | Hourly app metrics                            |
| `agg_hourly_agent_metrics`       | Hourly agent metrics                          |
| `agg_hourly_tool_metrics`        | Hourly tool metrics                           |
| `agg_hourly_llm_metrics`         | Hourly LLM token/cost metrics                 |
| `agg_hourly_rag_metrics`         | Hourly RAG quality metrics                    |
| `agg_daily_feedback_metrics`     | Daily feedback summary                        |
| `agg_daily_kpi_metrics`          | Daily KPI values                              |
| `daily_slo_compliance`           | SLO/error budget tracking                     |
| `daily_rag_quality`              | RAG faithfulness/precision/recall             |
| `vector_health_snapshots`        | Embedding drift/freshness snapshots           |

---

# 8. Create Elasticsearch index templates

## What it means

Elasticsearch index templates define how event fields are stored and searched.

If you do not define mappings, Elasticsearch may infer wrong types:

```text
application_id as number instead of keyword
timestamp as text instead of date
latency_ms as text instead of integer
```

That breaks filters and dashboards.

## Index naming convention

Use per-LOB index pattern:

```text
ai-obs-{lob}-{event_type}-{yyyy.MM}
```

Examples:

```text
ai-obs-payments-requests-2026.05
ai-obs-payments-errors-2026.05
ai-obs-cards-llm-calls-2026.05
ai-obs-fi-rag-events-2026.05
```

## Core indices to create

| Index Pattern                 | Purpose                          |
| ----------------------------- | -------------------------------- |
| `ai-obs-*-requests-*`         | Request lifecycle events         |
| `ai-obs-*-errors-*`           | Error events                     |
| `ai-obs-*-agent-steps-*`      | Agent execution and steps        |
| `ai-obs-*-llm-calls-*`        | LLM usage, tokens, cost, latency |
| `ai-obs-*-tool-calls-*`       | Tool usage, failures, latency    |
| `ai-obs-*-rag-events-*`       | Retrieval, no result, relevance  |
| `ai-obs-*-guardrail-events-*` | Policy decisions and blocks      |
| `ai-obs-*-feedback-*`         | User feedback events             |
| `ai-obs-*-traces-*`           | Distributed trace/span events    |
| `ai-obs-anomalies-*`          | ML anomaly events                |
| `ai-obs-quality-scores-*`     | Faithfulness, entropy, quality   |
| `ai-obs-vector-health-*`      | Vector index health              |

The refined architecture lists similar Elasticsearch indices, including request, error, agent step, LLM, tool, RAG, guardrail, feedback, trace, anomaly, quality score, and vector health indices. 

## Common fields every index should map

```json
{
  "event_id": { "type": "keyword" },
  "event_type": { "type": "keyword" },
  "timestamp": { "type": "date" },
  "correlation_id": { "type": "keyword" },
  "span_id": { "type": "keyword" },
  "parent_span_id": { "type": "keyword" },
  "application_id": { "type": "keyword" },
  "agent_id": { "type": "keyword" },
  "tool_id": { "type": "keyword" },
  "rag_id": { "type": "keyword" },
  "model_name": { "type": "keyword" },
  "lob": { "type": "keyword" },
  "environment": { "type": "keyword" },
  "status": { "type": "keyword" },
  "error_code": { "type": "keyword" },
  "latency_ms": { "type": "integer" },
  "estimated_cost": { "type": "float" },
  "total_tokens": { "type": "long" },
  "s3_payload_uri": { "type": "keyword" }
}
```

## Example request index template

```json
{
  "index_patterns": ["ai-obs-*-requests-*"],
  "template": {
    "settings": {
      "number_of_shards": 2,
      "number_of_replicas": 1,
      "index.lifecycle.name": "hot-warm-90d"
    },
    "mappings": {
      "properties": {
        "event_id": { "type": "keyword" },
        "event_type": { "type": "keyword" },
        "timestamp": { "type": "date" },
        "correlation_id": { "type": "keyword" },
        "application_id": { "type": "keyword" },
        "lob": { "type": "keyword" },
        "environment": { "type": "keyword" },
        "user_hash": { "type": "keyword" },
        "channel": { "type": "keyword" },
        "request_type": { "type": "keyword" },
        "status": { "type": "keyword" },
        "latency_ms": { "type": "integer" },
        "error_code": { "type": "keyword" },
        "http_status": { "type": "integer" },
        "total_tokens": { "type": "long" },
        "estimated_cost": { "type": "float" }
      }
    }
  }
}
```

## ILM policies to create

| Policy            | Use                                 |
| ----------------- | ----------------------------------- |
| `hot-warm-30d`    | High-volume short-lived logs        |
| `hot-warm-90d`    | Errors, traces, LLM/tool/RAG events |
| `compliance-180d` | Guardrail and audit events          |

---

# 9. How to execute this as a working plan

Use this implementation sequence.

## Step 1: Create schema working group

Participants:

```text
Platform engineering
Data engineering
SRE
Security/governance
AI/agent owners
Dashboard/reporting team
```

Output:

```text
Approved event schema
Approved event types
Approved correlation_id rules
Approved error categories
Approved metric categories
Approved KPI categories
```

## Step 2: Create foundation repositories

Create:

```text
observability-contracts/
observability-iac/
ai-observability-sdk/
telemetry-processor/
```

## Step 3: Seed PostgreSQL catalogs

Seed these first:

```text
application_registry
agent_registry
tool_registry
rag_registry
prompt_template_registry
error_code_catalog
metric_catalog
kpi_definition
alert_threshold
budget_limits
access_policy
```

## Step 4: Create Elasticsearch templates

Deploy templates for:

```text
requests
errors
agent-steps
llm-calls
tool-calls
rag-events
guardrail-events
feedback
traces
quality-scores
anomalies
vector-health
```

## Step 5: Validate with one pilot application

Pick one agent, for example IntentIQ or Payment Scrutiny.

Instrument minimum flow:

```text
REQUEST_RECEIVED
PLAN_CREATED
AGENT_STARTED
LLM_CALL_COMPLETED
TOOL_CALL_COMPLETED
RAG_RETRIEVAL_COMPLETED
AGENT_COMPLETED
RESPONSE_DELIVERED
FEEDBACK_SUBMITTED
```

Then verify:

```text
Same correlation_id exists in all events.
Events appear in Kafka.
Events are indexed in Elasticsearch.
Aggregates appear in PostgreSQL.
Large payloads appear in S3.
Basic dashboard can filter by application_id and correlation_id.
```

---

# 10. Final checklist for your team

Use this as the Phase 1 checklist.

| Work Item                       | Owner                | Output                          |
| ------------------------------- | -------------------- | ------------------------------- |
| Define common event schema      | Platform + Data      | JSON schema                     |
| Define event type catalog       | Platform             | Approved event list             |
| Finalize `correlation_id` rules | Platform             | Propagation standard            |
| Define Kafka header standard    | Platform             | `traceparent`, `correlation_id` |
| Create error code catalog       | SRE + Platform       | Seed table                      |
| Create metric catalog           | Data + Observability | Seed table                      |
| Create KPI catalog              | Business + Data      | KPI definitions                 |
| Define retention policy         | Security + Data      | Retention matrix                |
| Define RBAC model               | Security + Platform  | Role/access matrix              |
| Create PostgreSQL base tables   | Data Engineering     | DDL scripts                     |
| Create Elasticsearch templates  | Data/Observability   | Index templates                 |
| Create S3 bucket policy         | Cloud/Security       | Encrypted buckets               |
| Validate with pilot agent       | All teams            | End-to-end proof                |

The core message for your team:

> Phase 1 is about creating the contract and foundation. Before we build dashboards or chatbot, we must standardize what every service emits, how requests are correlated, what errors/metrics/KPIs mean, who can access which data, and where each type of data is stored. Once this is locked, the SDK, telemetry processor, dashboards, chatbot, alerts, and RCA can be built reliably.
