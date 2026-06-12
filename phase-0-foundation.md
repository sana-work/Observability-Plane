# Phase 0 — Foundation (Execution Document)

> **Parent plan:** `plan.md` §4 Phase 0 · **Architecture:** Kafka Direct Path (no OIS), custom AI-quality layer (Langfuse undecided)
> **Goal of Phase 0:** stand up the contract + infrastructure that *every* later phase depends on, so by the end nothing downstream is blocked on a missing topic, table, index, bucket, or pipeline.
> **Duration:** ~3 weeks · **Owners:** Platform Engineering (lead) + Data Engineering

> **⚠️ Interim storage note: Snowflake is not yet onboarded.** In Phase 0 we **do not** provision Snowflake. The event firehose/analytics store is a **PostgreSQL `obs_events` schema** (Task 0.5) that stands in for Snowflake until it's available. The `snowflake/ddl/` artifacts are written but **deferred** (applied later during the swap-back, `plan.md` §5.9/§15.6). Everywhere Snowflake is mentioned below, the Phase-0 deliverable is the Postgres `obs_events` equivalent.

## How to use this doc
Each task below is self-contained: **what to do → the actual artifact → how to apply it → how to verify**. Work top to bottom; tasks 0.1–0.3 unblock the most, so do them first. Every artifact created here lives in the new `observability-iac` repo (Task 0.0) and is applied by CI (Task 0.9), not by hand in prod.

---

## 0.0 Prerequisites & access (gather before starting)

| Need | Detail | Owner |
|---|---|---|
| Kafka cluster access | bootstrap brokers, admin ACL to create topics | Platform |
| PostgreSQL | a DB for the control plane; rights to `CREATE SCHEMA` | Data Eng |
| Elasticsearch / Kibana | cluster URL, ingest-manager / `manage_index_templates` priv | Data Eng |
| Snowflake *(deferred)* | **not needed in Phase 0** — Postgres `obs_events` stands in (Task 0.5); onboard later | Data Eng |
| AWS / S3 | account, KMS key, rights to create buckets + lifecycle | Platform |
| Redis / ElastiCache | endpoint (shared instance is fine — we only add a namespace) | Platform |
| Kubernetes | a namespace (`ai-platform`), Helm, kubectl | Platform |
| Secrets store | Vault/SealedSecrets/SM for Kafka/PG/ES/Snowflake/S3 creds | Security |
| CI runner | GitHub Actions (or equivalent) with the above creds as secrets | Platform |

**Environments:** provision `dev` first, then `staging`, then `prod`. All artifacts are parameterized by `{env}`.

---

## 0.0 Deliverable: scaffold the `observability-iac` repo

This repo is the single source of truth for all Phase 0 artifacts and is applied by CI.

```
observability-iac/
├── contracts/                      # Task 0.1 — the event contract (vendored into the SDK later)
│   ├── event_schema.py
│   ├── event_types.py
│   ├── service_names.py
│   └── w3c_headers.md
├── kafka/                          # Task 0.2
│   ├── create_topics.sh
│   └── topics.yaml
├── postgres/                       # Task 0.3
│   ├── migrations/
│   │   ├── 001_create_schema.sql
│   │   ├── 002_registries.sql
│   │   ├── 003_prompt_registry.sql
│   │   ├── 004_governance.sql
│   │   └── 005_grants.sql
│   └── seed/
│       ├── error_code_catalog.sql
│       └── metric_catalog.sql
├── elasticsearch/                  # Task 0.4
│   ├── component-templates/
│   │   ├── obs-common-settings.json
│   │   └── obs-common-mappings.json
│   ├── index-templates/
│   │   ├── ai-obs-requests.json
│   │   ├── ai-obs-errors.json
│   │   ├── ai-obs-llm-calls.json
│   │   ├── ai-obs-rag-events.json
│   │   ├── ai-obs-agent-steps.json
│   │   ├── ai-obs-tool-calls.json
│   │   ├── ai-obs-guardrail-events.json
│   │   ├── ai-obs-feedback.json
│   │   ├── ai-obs-traces.json
│   │   ├── ai-obs-quality-scores.json
│   │   └── ai-obs-anomalies.json
│   └── ilm-policies/
│       ├── hot-warm-30d.json
│       └── compliance-180d.json
├── postgres-events/                # Task 0.5 — interim event firehose (Snowflake stand-in, APPLIED)
│   └── migrations/
│       ├── 001_obs_events_schema.sql
│       ├── 002_llm_events.sql
│       ├── 003_agent_events.sql
│       ├── 004_rag_events.sql
│       ├── 005_feedback_events.sql
│       ├── 006_quality_scores.sql
│       ├── 007_slo_history.sql
│       └── 009_partitions_cron.sql
├── snowflake/                      # DEFERRED — NOT applied in Phase 0; used at swap-back when onboarded
│   └── ddl/
│       ├── 000_warehouse_role.sql
│       └── 001_sf_events.sql … 007_sf_slo.sql
├── s3/
│   ├── buckets.tf                  # or CloudFormation/cli script
│   └── lifecycle.json
├── infra/                          # Task 0.8 — supporting telemetry tools
│   ├── tempo-values.yaml
│   ├── kube-prometheus-stack-values.yaml
│   ├── kminion.yaml
│   └── fluent-bit-configmap.yaml
├── ci/
│   └── deploy.yml                  # Task 0.9 — applies everything on merge to main
└── README.md
```

```bash
gh repo create observability-iac --private --clone
cd observability-iac && mkdir -p contracts kafka postgres/migrations postgres/seed \
  elasticsearch/component-templates elasticsearch/index-templates elasticsearch/ilm-policies \
  postgres-events/migrations snowflake/ddl s3 infra ci
```

---

## 0.1 — Freeze the event contract

**Artifacts:** `contracts/event_schema.py`, `event_types.py`, `service_names.py`, `w3c_headers.md`.

### `event_types.py` — the controlled vocabulary (50 types, 10 categories)
```python
from enum import Enum

class EventType(str, Enum):
    # Request (4)
    REQUEST_RECEIVED = "REQUEST_RECEIVED"; REQUEST_COMPLETED = "REQUEST_COMPLETED"
    REQUEST_FAILED = "REQUEST_FAILED"; RESPONSE_DELIVERED = "RESPONSE_DELIVERED"
    # Orchestration (6)
    AUTH_COMPLETED = "AUTH_COMPLETED"; CONFIG_LOADED = "CONFIG_LOADED"; PLAN_CREATED = "PLAN_CREATED"
    AGENT_EXECUTION_REQUEST_PRODUCED = "AGENT_EXECUTION_REQUEST_PRODUCED"
    FINAL_RESPONSE_CONSUMED = "FINAL_RESPONSE_CONSUMED"; RESPONSE_BUILT = "RESPONSE_BUILT"
    # Kafka (4)
    KAFKA_MESSAGE_PRODUCED = "KAFKA_MESSAGE_PRODUCED"; KAFKA_MESSAGE_CONSUMED = "KAFKA_MESSAGE_CONSUMED"
    KAFKA_MESSAGE_DLQ = "KAFKA_MESSAGE_DLQ"; KAFKA_LAG_RECORDED = "KAFKA_LAG_RECORDED"
    # Agent (8)
    AGENT_STARTED = "AGENT_STARTED"; AGENT_STEP_STARTED = "AGENT_STEP_STARTED"
    AGENT_STEP_COMPLETED = "AGENT_STEP_COMPLETED"; AGENT_LOOP_ITERATION = "AGENT_LOOP_ITERATION"
    AGENT_HANDOFF = "AGENT_HANDOFF"; AGENT_COMPLETED = "AGENT_COMPLETED"
    AGENT_FAILED = "AGENT_FAILED"; AGENT_TIMEOUT = "AGENT_TIMEOUT"
    # LLM (5)
    LLM_CALL_STARTED = "LLM_CALL_STARTED"; LLM_CALL_COMPLETED = "LLM_CALL_COMPLETED"
    LLM_CALL_FAILED = "LLM_CALL_FAILED"; LLM_RATE_LIMITED = "LLM_RATE_LIMITED"
    LLM_SAFETY_BLOCKED = "LLM_SAFETY_BLOCKED"
    # Tool (4)
    TOOL_CALL_STARTED = "TOOL_CALL_STARTED"; TOOL_CALL_COMPLETED = "TOOL_CALL_COMPLETED"
    TOOL_CALL_FAILED = "TOOL_CALL_FAILED"; TOOL_CALL_TIMEOUT = "TOOL_CALL_TIMEOUT"
    # RAG (5)
    RAG_RETRIEVAL_STARTED = "RAG_RETRIEVAL_STARTED"; RAG_RETRIEVAL_COMPLETED = "RAG_RETRIEVAL_COMPLETED"
    RAG_RETRIEVAL_FAILED = "RAG_RETRIEVAL_FAILED"; RAG_NO_RESULT = "RAG_NO_RESULT"
    RAG_INDEX_HEALTH_CHECKED = "RAG_INDEX_HEALTH_CHECKED"
    # Guardrail (4)
    GUARDRAIL_EVALUATED = "GUARDRAIL_EVALUATED"; GUARDRAIL_BLOCKED = "GUARDRAIL_BLOCKED"
    GUARDRAIL_REDACTED = "GUARDRAIL_REDACTED"; GUARDRAIL_ESCALATED = "GUARDRAIL_ESCALATED"
    # Feedback (3)
    FEEDBACK_SUBMITTED = "FEEDBACK_SUBMITTED"; FEEDBACK_REVIEWED = "FEEDBACK_REVIEWED"
    FEEDBACK_INCIDENT_TRIGGERED = "FEEDBACK_INCIDENT_TRIGGERED"
    # Document / multimodal (7)
    DOCUMENT_UPLOADED = "DOCUMENT_UPLOADED"; DOCUMENT_STORED_IN_S3 = "DOCUMENT_STORED_IN_S3"
    DOCUMENT_EXTRACTION_STARTED = "DOCUMENT_EXTRACTION_STARTED"
    DOCUMENT_EXTRACTION_COMPLETED = "DOCUMENT_EXTRACTION_COMPLETED"
    DOCUMENT_EXTRACTION_FAILED = "DOCUMENT_EXTRACTION_FAILED"
    DOCUMENT_INDEXED = "DOCUMENT_INDEXED"; DOCUMENT_EMBEDDING_CREATED = "DOCUMENT_EMBEDDING_CREATED"

assert len(EventType) == 50, f"expected 50 event types, got {len(EventType)}"
```

### `service_names.py`
```python
from enum import Enum
class ServiceName(str, Enum):
    AGENTIC_ORCHESTRATION = "agentic-orchestration"; AGENT_EXECUTOR = "agent-executor"
    GSSP_GS = "gssp-gs"; GSSP_QS = "gssp-qs"; GSSP_RS = "gssp-rs"
    CONSUMER_SERVICE = "consumer-service"; DATA_INGESTION = "data-ingestion"
    USER_FEEDBACK = "user-feedback"
```

### `event_schema.py`
The full `ObsEvent` Pydantic model — copy from `plan.md` §5.2 (mandatory envelope: `event_id`, `schema_version`, `event_type`, `telemetry_type`, `timestamp`, `emitted_at`, correlation/trace ids, `service_name`, `environment`, `application_id`, `lob`, `tenant_id`, `user_hash`, `status`, `latency_ms`, `error_code`, `http_status`, `payload`) with the `event_type ∈ EventType` validator.

### `w3c_headers.md` — Kafka header contract
```
traceparent: 00-{32-hex trace-id}-{16-hex parent-id}-{flags}
tracestate:  intentiq={application_id};env={environment}
correlation_id: {correlation_id}
```

**Verify:** `python -c "from contracts.event_types import EventType; print(len(EventType))"` → `50`; `pytest` round-trips a sample event through `ObsEvent`.

---

## 0.2 — Create Kafka topics

`kafka/create_topics.sh`:
```bash
#!/usr/bin/env bash
set -euo pipefail
B="${KAFKA_BROKERS:?}"
kafka-topics.sh --bootstrap-server "$B" --create --if-not-exists --topic ai-obs-events-raw \
  --partitions 12 --replication-factor 3 \
  --config retention.ms=604800000 --config compression.type=lz4 --config min.insync.replicas=2
kafka-topics.sh --bootstrap-server "$B" --create --if-not-exists --topic ai-obs-events-processed \
  --partitions 12 --replication-factor 3 \
  --config retention.ms=259200000 --config compression.type=lz4 --config min.insync.replicas=2
kafka-topics.sh --bootstrap-server "$B" --create --if-not-exists --topic ai-obs-dead-letter \
  --partitions 3 --replication-factor 3 --config retention.ms=1209600000
```
- Partition key on produce = `correlation_id` (ordered per-request processing).
- ACLs: producers = the 8 services (write `raw`); enrichment consumer (read `raw`, write `processed`+`dead-letter`); storage + eval consumers (read `processed`).

**Verify:**
```bash
kafka-topics.sh --bootstrap-server "$KAFKA_BROKERS" --describe --topic ai-obs-events-raw
# PartitionCount: 12, ReplicationFactor: 3, retention.ms=604800000, compression.type=lz4
```

---

## 0.3 — PostgreSQL `observability` schema (control plane only)

**Migrations** (`postgres/migrations/`): the full DDL is in `plan.md` §5.7 — registries, `prompt_registry`, `metric_catalog`, `error_code_catalog`, `budget_limits`, `daily_slo_compliance`, `alert_threshold`, `dashboard_config`, `feedback_case`, plus `005_grants.sql` (dashboard_ro). Apply in order 001→005.

> **Reminder:** the `observability` schema is **control plane only**. The event firehose goes in the **separate `obs_events` schema** (Task 0.5), not here.

### Seed: `seed/error_code_catalog.sql` (starter set)
```sql
INSERT INTO observability.error_code_catalog (raw_pattern, error_code, category) VALUES
  ('%ReadTimeout%',        'UPSTREAM_TIMEOUT',   'timeout'),
  ('%ConnectionRefused%',  'UPSTREAM_UNAVAILABLE','network'),
  ('%429%',                'RATE_LIMITED',       'throttling'),
  ('%SafetyBlock%',        'LLM_SAFETY_BLOCKED', 'guardrail'),
  ('%ValidationError%',    'SCHEMA_INVALID',     'validation'),
  ('%401%','UNAUTHENTICATED','auth'), ('%403%','FORBIDDEN','auth'),
  ('%pgvector%',           'VECTOR_QUERY_ERROR', 'retrieval')
ON CONFLICT (error_code) DO NOTHING;
```

### Seed: `seed/metric_catalog.sql` — the 30 initial metrics
```sql
-- source_table points at the interim Postgres obs_events.* firehose (swap to sf_* when Snowflake onboards)
INSERT INTO observability.metric_catalog (metric_name, formula, source_table, unit) VALUES
-- golden signals
('request_count','count(*)','obs_events.events','count'),
('error_rate','errors/requests','obs_events.events','ratio'),
('latency_p50','percentile(latency_ms,50)','obs_events.events','ms'),
('latency_p95','percentile(latency_ms,95)','obs_events.events','ms'),
('latency_p99','percentile(latency_ms,99)','obs_events.events','ms'),
('throughput_rps','request_count/window_seconds','obs_events.events','rps'),
-- LLM
('llm_input_tokens','sum(input_tokens)','obs_events.llm_events','tokens'),
('llm_output_tokens','sum(output_tokens)','obs_events.llm_events','tokens'),
('llm_total_tokens','sum(total_tokens)','obs_events.llm_events','tokens'),
('llm_estimated_cost','sum(estimated_cost)','obs_events.llm_events','usd'),
('llm_latency_p95','percentile(llm_latency_ms,95)','obs_events.llm_events','ms'),
('llm_rate_limit_rate','rate_limited/llm_calls','obs_events.llm_events','ratio'),
('llm_safety_block_rate','safety_blocked/llm_calls','obs_events.llm_events','ratio'),
-- agent
('agent_success_rate','completed/started','obs_events.agent_events','ratio'),
('agent_avg_steps','avg(step_count)','obs_events.agent_events','count'),
('agent_loop_rate','loops/agent_runs','obs_events.agent_events','ratio'),
('agent_handoff_count','sum(handoff_count)','obs_events.agent_events','count'),
-- tool
('tool_success_rate','tool_completed/tool_calls','obs_events.events','ratio'),
('tool_latency_p95','percentile(tool_latency_ms,95)','obs_events.events','ms'),
('tool_timeout_rate','tool_timeouts/tool_calls','obs_events.events','ratio'),
-- RAG / quality
('rag_no_result_rate','no_result/retrievals','obs_events.rag_events','ratio'),
('rag_avg_chunks','avg(retrieved_chunk_count)','obs_events.rag_events','count'),
('rag_avg_relevance','avg(avg_relevance_score)','obs_events.rag_events','score'),
('faithfulness_score','avg(faithfulness_score)','obs_events.quality_scores','score'),
('hallucination_rate','avg(hallucination_flag)','obs_events.quality_scores','ratio'),
-- cost / budget
('budget_utilisation_pct','spend/max_spend_usd','observability.budget_limits','ratio'),
-- kafka
('kafka_consumer_lag','max(kafka_consumer_lag)','obs_events.events','count'),
-- SLO
('slo_error_budget_consumed','avg(error_budget_consumed_pct)','observability.daily_slo_compliance','ratio'),
('slo_burn_rate_1h','max(burn_rate_1h)','observability.daily_slo_compliance','ratio'),
-- feedback
('feedback_negative_rate','negative/total_feedback','obs_events.feedback_events','ratio')
ON CONFLICT (metric_name) DO NOTHING;
```

**Verify:** `\dn` shows `observability`; `\dt observability.*` lists all tables; `SELECT count(*) FROM observability.metric_catalog;` → 30; `SELECT count(*) FROM observability.error_code_catalog;` ≥ 8.

---

## 0.4 — Elasticsearch index templates + ILM

Use **composable templates**: one shared component template for settings + common mappings, then one index template per category that references it.

### `component-templates/obs-common-mappings.json`
```json
{ "template": { "mappings": { "dynamic": true, "properties": {
  "event_id":       {"type":"keyword"},
  "event_type":     {"type":"keyword"},
  "correlation_id": {"type":"keyword"},
  "trace_id":       {"type":"keyword"},
  "span_id":        {"type":"keyword"},
  "parent_span_id": {"type":"keyword"},
  "service_name":   {"type":"keyword"},
  "environment":    {"type":"keyword"},
  "application_id": {"type":"keyword"},
  "lob":            {"type":"keyword"},
  "status":         {"type":"keyword"},
  "error_code":     {"type":"keyword"},
  "error_fingerprint": {"type":"keyword"},
  "latency_ms":     {"type":"float"},
  "timestamp":      {"type":"date"}
}}}}
```

### `index-templates/ai-obs-requests.json` (pattern for all categories)
```json
{ "index_patterns": ["ai-obs-*-requests-*"],
  "composed_of": ["obs-common-settings","obs-common-mappings"],
  "template": { "settings": {
      "index.lifecycle.name": "hot-warm-30d",
      "index.lifecycle.rollover_alias": "ai-obs-requests" } },
  "priority": 200 }
```
Repeat per category (`errors`, `llm-calls`, `rag-events`, `agent-steps`, `tool-calls`, `guardrail-events`, `feedback`, `traces`, `quality-scores`, `anomalies`). Regulated LOBs point at `compliance-180d` instead of `hot-warm-30d`.

### `ilm-policies/hot-warm-30d.json`
```json
{ "policy": { "phases": {
  "hot":   {"actions":{"rollover":{"max_size":"50gb","max_age":"1d"}}},
  "warm":  {"min_age":"2d","actions":{"shrink":{"number_of_shards":1},"forcemerge":{"max_num_segments":1}}},
  "delete":{"min_age":"30d","actions":{"delete":{}}} }}}
```
(`compliance-180d.json` = same shape, delete at 180d.)

### Apply
```bash
curl -XPUT "$ES/_ilm/policy/hot-warm-30d" -H 'Content-Type: application/json' -d @ilm-policies/hot-warm-30d.json
curl -XPUT "$ES/_component_template/obs-common-mappings" -d @component-templates/obs-common-mappings.json
curl -XPUT "$ES/_index_template/ai-obs-requests" -d @index-templates/ai-obs-requests.json
# ... repeat for each template
```

**Verify:** `GET $ES/_index_template/ai-obs-requests` returns the template; `GET $ES/_ilm/policy/hot-warm-30d` exists; a test write `POST ai-obs-shared-requests-2026.06.12/_doc` lands and is searchable.

---

## 0.5 — Event firehose: PostgreSQL `obs_events` schema (interim; Snowflake deferred)

**Snowflake is not onboarded**, so the firehose store the architecture labels "Snowflake" is, for Phase 0, a **separate PostgreSQL schema `obs_events`** (kept apart from the `observability` control plane so the eventual swap is clean). Tables mirror the future `sf_*` set: `events`, `llm_events`, `agent_events`, `rag_events`, `feedback_events`, `quality_scores`, `slo_history`.

`postgres-events/migrations/001_obs_events_schema.sql`:
```sql
CREATE SCHEMA IF NOT EXISTS obs_events;

CREATE TABLE IF NOT EXISTS obs_events.events (
  event_id       VARCHAR(64),
  event_type     VARCHAR(64), telemetry_type VARCHAR(16),
  correlation_id VARCHAR(64), trace_id VARCHAR(64), span_id VARCHAR(64), parent_span_id VARCHAR(64),
  service_name   VARCHAR(64), environment VARCHAR(16), application_id VARCHAR(64), lob VARCHAR(32),
  status VARCHAR(16), latency_ms DOUBLE PRECISION, error_code VARCHAR(64),
  payload        JSONB,                          -- the VARIANT stand-in
  event_ts       TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (event_id, event_ts)
) PARTITION BY RANGE (event_ts);                 -- monthly partitions

CREATE INDEX IF NOT EXISTS ix_events_app_ts  ON obs_events.events (application_id, event_ts);
CREATE INDEX IF NOT EXISTS ix_events_corr    ON obs_events.events (correlation_id);
CREATE INDEX IF NOT EXISTS ix_events_type_ts ON obs_events.events (event_type, event_ts);
CREATE INDEX IF NOT EXISTS ix_events_payload ON obs_events.events USING GIN (payload);

-- create the next few monthly partitions up front (or use pg_partman — see 009)
CREATE TABLE IF NOT EXISTS obs_events.events_2026_06 PARTITION OF obs_events.events
  FOR VALUES FROM ('2026-06-01') TO ('2026-07-01');
CREATE TABLE IF NOT EXISTS obs_events.events_2026_07 PARTITION OF obs_events.events
  FOR VALUES FROM ('2026-07-01') TO ('2026-08-01');

GRANT USAGE ON SCHEMA obs_events TO dashboard_ro;
GRANT SELECT ON ALL TABLES IN SCHEMA obs_events TO dashboard_ro;
ALTER DEFAULT PRIVILEGES IN SCHEMA obs_events GRANT SELECT ON TABLES TO dashboard_ro;
```
The per-domain tables (`llm_events`, `agent_events`, `rag_events`, `feedback_events`, `quality_scores`, `slo_history`) follow the same shape with domain columns promoted out of `payload` (e.g. `llm_events.input_tokens`, `estimated_cost`, `model_name`). `009_partitions_cron.sql` installs `pg_partman` (or a cron) to roll new monthly partitions and detach/drop old ones (>~90 days → archived to S3 `raw-traces/`).

> **Interim differences vs Snowflake:** **not "forever"** (bounded ~90-day window), JSONB instead of VARIANT, partitions instead of clustering keys. The `snowflake/ddl/*` files are written but **deferred** — applied during the swap-back when Snowflake onboards (`plan.md` §5.9).

**Verify:** `\dn` shows `obs_events`; `\dt obs_events.*` lists all 7 tables; partitions exist (`\d+ obs_events.events`); an `INSERT` into `obs_events.events` routes to the right monthly partition and `SELECT count(*)` works.

---

## 0.6 — S3 buckets + lifecycle

`s3/lifecycle.json`:
```json
{ "Rules": [
  {"ID":"ia-30","Status":"Enabled","Filter":{"Prefix":""},
   "Transitions":[{"Days":30,"StorageClass":"STANDARD_IA"},{"Days":180,"StorageClass":"GLACIER"}]} ]}
```
```bash
aws s3api create-bucket --bucket ai-obs-payloads-$ENV --region us-east-1
aws s3api put-bucket-encryption --bucket ai-obs-payloads-$ENV \
  --server-side-encryption-configuration '{"Rules":[{"ApplyServerSideEncryptionByDefault":{"SSEAlgorithm":"aws:kms","KMSMasterKeyID":"'$KMS_KEY'"}}]}'
aws s3api put-bucket-lifecycle-configuration --bucket ai-obs-payloads-$ENV --lifecycle-configuration file://s3/lifecycle.json
aws s3api put-public-access-block --bucket ai-obs-payloads-$ENV \
  --public-access-block-configuration BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true
# create the prefix layout
for p in redacted-prompts redacted-responses raw-traces rag-contexts audit-evidence debug-bundles rca-reports iac-dashboards; do
  aws s3api put-object --bucket ai-obs-payloads-$ENV --key "$p/"; done
```

**Verify:** `get-bucket-encryption` shows KMS; `get-bucket-lifecycle-configuration` shows the rule; public-access-block all true.

---

## 0.7 — Redis namespace/keys

No new infra (shared instance). Document the key conventions so all components agree:
| Key | Type | TTL | Purpose |
|---|---|---|---|
| `obs:corr:{id}` | hash | 1h | active correlation context |
| `obs:budget:{app}:{model}:{date}` | float | end of day | live spend accumulator |
| `obs:dedup:{event_id}` | string (SETNX) | 24h | at-least-once dedup guard |
| `obs:registry:{type}:{id}` | json | 5m | registry cache |

**Verify:** `redis-cli SET obs:dedup:test 1 EX 60 NX` returns OK; key visible; no collision with existing namespaces.

---

## 0.8 — Provision supporting telemetry infra

`infra/tempo-values.yaml` (S3 backend, 30d):
```yaml
tempo:
  storage: { trace: { backend: s3, s3: { bucket: ai-obs-traces-$ENV, endpoint: s3.amazonaws.com, region: us-east-1 } } }
  retention: 720h
```
`infra/kube-prometheus-stack-values.yaml`:
```yaml
grafana: { enabled: false }   # Custom Dashboard Service replaces Grafana
prometheus: { enabled: true }
```
`infra/kminion.yaml` — deploy the container, `KAFKA_BROKERS` set, exposes `:8080/metrics`.
`infra/fluent-bit-configmap.yaml` — tail `/var/log/containers/*.log`, JSON parser, OUTPUT → ES `ai-obs-${service_name}-logs-%Y.%m` (full config in `2026-05-28_tool-recommendations-by-signal.md` §4).

```bash
helm repo add grafana https://grafana.github.io/helm-charts && helm install tempo grafana/tempo -f infra/tempo-values.yaml -n ai-platform
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm install kps prometheus-community/kube-prometheus-stack -f infra/kube-prometheus-stack-values.yaml -n monitoring
kubectl apply -f infra/kminion.yaml -n ai-platform
kubectl apply -f infra/fluent-bit-configmap.yaml -n ai-platform
```

**Verify:** Tempo + Prometheus pods Ready; `curl kminion:8080/metrics` returns `kminion_*`; Fluent Bit DaemonSet running on all nodes; a test log line appears in an `ai-obs-*-logs-*` index.

---

## 0.9 — CI/CD pipeline

`ci/deploy.yml` (GitHub Actions) — on merge to `main`, apply every artifact idempotently:
```yaml
name: deploy-observability-iac
on: { push: { branches: [main] } }
jobs:
  deploy:
    runs-on: ubuntu-latest
    environment: ${{ github.ref_name }}
    steps:
      - uses: actions/checkout@v4
      - name: Contract test
        run: pip install pydantic pytest && pytest contracts/
      - name: Kafka topics
        run: bash kafka/create_topics.sh
        env: { KAFKA_BROKERS: ${{ secrets.KAFKA_BROKERS }} }
      - name: Postgres migrations + seed
        run: |
          for f in postgres/migrations/*.sql postgres/seed/*.sql; do psql "$PG_DSN" -f "$f"; done
        env: { PG_DSN: ${{ secrets.PG_DSN }} }
      - name: Elasticsearch templates + ILM
        run: bash elasticsearch/apply.sh
        env: { ES: ${{ secrets.ES_URL }} }
      - name: Postgres obs_events firehose (interim; Snowflake deferred)
        run: for f in postgres-events/migrations/*.sql; do psql "$PG_DSN" -f "$f"; done
        env: { PG_DSN: ${{ secrets.PG_DSN }} }
        # NOTE: snowflake/ddl/* is intentionally NOT applied until Snowflake is onboarded (swap-back)
      - name: S3 + lifecycle
        run: bash s3/apply.sh
        env: { ENV: ${{ github.ref_name }}, KMS_KEY: ${{ secrets.KMS_KEY }} }
```
All steps are **idempotent** (`--if-not-exists`, `IF NOT EXISTS`, `ON CONFLICT`, `PUT` template) so re-runs are safe.

**Verify:** open a PR adding a metric to `metric_catalog.sql`, merge, confirm the Action goes green and the row appears in PostgreSQL.

---

## Acceptance — Phase 0 Definition of Done
Phase 0 is complete when **all** pass (this is the gate to Phase 1):

- [ ] `contracts/` frozen: `EventType` has exactly **50** members; `ObsEvent` round-trips a sample; W3C header format documented.
- [ ] 3 Kafka topics exist with correct partitions/retention/compression; ACLs set.
- [ ] `observability` schema present with all control-plane tables; `metric_catalog`=30 rows, `error_code_catalog`≥8; **no** `obs_events` table.
- [ ] ES: all index templates + 2 ILM policies applied; a test doc indexes and is searchable.
- [ ] PostgreSQL `obs_events` schema: all 7 tables created, partitioned, and writable (Snowflake deferred).
- [ ] S3 bucket per env: KMS encryption on, lifecycle set, public access blocked, 8 prefixes present.
- [ ] Redis key conventions documented; SETNX dedup test passes.
- [ ] Tempo + Prometheus + kminion + Fluent Bit running; a test log lands in ES; kminion exposes metrics.
- [ ] CI applies every artifact idempotently from a merge to `main` (green run proven).

---

## Hand-off to Phase 1
Phase 1 (Shared SDK) consumes directly from Phase 0:
- `contracts/` → vendored into `ai-observability-sdk`.
- Kafka `ai-obs-events-raw` → the SDK emitter's target.
- `observability.prompt_registry` → backs the SDK `get_prompt()`.
- `observability.application_registry` → enrichment registry lookups (Phase 3).
- Tempo OTLP endpoint → the SDK `init_tracing()` exporter.

## Owners & effort (per task)
| Task | Owner | Est. |
|---|---|---|
| 0.0 repo scaffold | Platform | 0.5d |
| 0.1 contract | Platform | 1.5d |
| 0.2 Kafka topics | Platform | 0.5d |
| 0.3 Postgres schema + seed | Data Eng | 1.5d |
| 0.4 ES templates + ILM | Data Eng | 1.5d |
| 0.5 obs_events event store (Postgres) | Data Eng | 1d |
| 0.6 S3 + lifecycle | Platform | 0.5d |
| 0.7 Redis conventions | Platform | 0.25d |
| 0.8 supporting infra | Platform | 2d |
| 0.9 CI/CD | Platform | 1.5d |

## Notes / risks specific to Phase 0
- **50 vs 38:** the contract uses **50** (the enumerated catalog). Reconcile the diagram/docs label before sign-off (`plan.md` §15.2).
- **`min.insync.replicas=2`** assumes rf3 with ≥2 in-sync — confirm broker count.
- **JSONB `payload`** holds the flexible fields; promote hot query fields (tokens, cost, model, latency) to real columns for index/partition pruning. (Becomes Snowflake VARIANT at swap-back.)
- **Interim Postgres firehose** is bounded (~90-day window via monthly partitions) — not "forever". Onboarding Snowflake removes that limit (`plan.md` §15.6).
- **Per-LOB ES RBAC** roles can be deferred to Phase 4, but create the index *templates* now so naming is fixed.
