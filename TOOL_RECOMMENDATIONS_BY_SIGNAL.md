# Tool Recommendations by Signal Type — AI Services Platform Observability

> For every category of observability data the platform captures, this document recommends the best tool,
> explains why it fits, shows how to integrate it, and lists alternatives.
> Langfuse was the first recommendation — this document covers every remaining signal.

---

## Signal Map Overview

| # | Signal Category | Recommended Tool | Effort | Replaces Custom Build? |
|---|---|---|---|---|
| 1 | LLM call traces, tokens, cost | **Langfuse** | Low | Yes — 12–18 weeks |
| 2 | RAG pipeline spans, faithfulness | **Langfuse** | Low | Yes — 8–10 weeks |
| 3 | Agent step trace trees | **Langfuse** | Medium | Yes — 6–8 weeks |
| 4 | Structured application logs | **structlog + Fluent Bit + Elasticsearch** | Low | No — enhance existing |
| 5 | Distributed traces (cross-service) | **OpenTelemetry + Grafana Tempo** | Medium | Yes — 4–6 weeks |
| 6 | HTTP latency / error rate metrics | **prometheus-fastapi-instrumentator** | Very Low | Yes — 2–3 weeks |
| 7 | Kafka consumer lag & health | **kminion → Prometheus → Custom Dashboard Service** | Low | Yes — 2 weeks |
| 8 | PII detection and redaction | **GLiNER** (in-process NER, no sidecar) | Low | Yes — 1 day |
| 9 | Error aggregation and grouping | **Sentry (self-hosted)** | Low | Yes — 4 weeks |
| 10 | Cost and budget governance | **Langfuse + Redis + PostgreSQL + Custom Dashboard** | Low | Yes — 4 weeks |
| 11 | Kubernetes / infra metrics | **kube-state-metrics + Prometheus** | Very Low | Yes — 1 week |
| 12 | Document ingestion pipeline events | **OpenTelemetry custom spans → OIS** | Low | No — new signals |
| 13 | Guardrail decisions | **OIS custom events** | Low | No — new signals |
| 14 | Vector / embedding health | **pgvector health queries → Prometheus Pushgateway** | Medium | No — new signals |
| 15 | Platform dashboards & visualization | **Custom Dashboard Service (FastAPI + React + Tremor)** | Medium | Yes — replaces Grafana |
| 16 | Anomaly detection | **Custom Isolation Forest (Kibana ML for log anomalies)** | Medium | Partial |

---

## 1. LLM Call Traces — Tokens, Cost, Latency, Model

**Tool: Langfuse (self-hosted)**
> Already covered in detail in `OBSERVABILITY_INGESTION_SERVICE_PLAN.md` Section 13 and `Developer_Implementation_Guide.md` Section 3a. Summarised here for completeness.

**What it captures:**
- `input_tokens`, `output_tokens`, `total_tokens` — auto from SDK
- `estimated_cost_usd` — auto from built-in pricing table
- `latency_ms` — auto (ms precision)
- `model_name`, `model_provider`, `finish_reason`
- `rate_limit_hit`, `safety_blocked`
- `prompt_hash` for drift detection

**One-line integration:**
```python
from langfuse.decorators import observe
@observe(as_type="generation")
async def generate(self, prompt, ctx): ...
```

---

## 2. RAG Pipeline Quality — Faithfulness, Chunk Count, Relevance

**Tool: Langfuse (self-hosted)**
> Already covered. Summarised for completeness.

**What it captures:** `retrieved_chunk_count`, `avg_relevance_score`, `no_result_flag`, `faithfulness_score` (LLM-as-judge), `context_truncation_flag`, retrieval latency per stage.

---

## 3. Agent Step Trees

**Tool: Langfuse (self-hosted)**
> Already covered.

---

## 4. Structured Application Logs

**Problem today:** 8 services all use `JSONFormatter` / `logconfig.yaml` but each has different field names, no `service_name`, no `environment`, SOE_ID in plain text, `latency_ms` as a string.

**Tool: `structlog` + `Fluent Bit` + `Elasticsearch`**

### Why structlog?

| Concern | Current (`logging` + `JSONFormatter`) | `structlog` |
|---|---|---|
| Consistent JSON output | ⚠️ Manually configured per service | ✅ Built-in JSON renderer |
| Context binding (`correlation_id`, `service_name`) | ⚠️ Custom `AppInfoFilter` per service | ✅ `structlog.contextvars.bind_contextvars()` |
| Async-safe context | ❌ Thread-local leaks in async | ✅ Uses `contextvars` natively |
| Typed log levels as structured field | ⚠️ String only | ✅ `level` as structured key |
| Dev mode (pretty print) vs prod (JSON) | ❌ Different config per env | ✅ `configure()` switch |

### Installation
```bash
pip install structlog
```

### Shared log configuration (replace per-service `logconfig.yaml`)

```python
# shared/logging_config.py — used by all 8 services
import structlog
import logging
from config.settings import get_settings

def configure_logging():
    settings = get_settings()

    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,          # injects correlation_id, service_name etc.
            structlog.stdlib.add_log_level,
            structlog.stdlib.add_logger_name,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.UnicodeDecoder(),
            structlog.processors.JSONRenderer()               # prod: JSON; dev: ConsoleRenderer
            if settings.ENVIRONMENT != "dev"
            else structlog.dev.ConsoleRenderer(),
        ],
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )
```

### Context injection in FastAPI middleware (once per service)

```python
# middleware/correlation.py
import structlog
from structlog.contextvars import clear_contextvars, bind_contextvars

class ObservabilityMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        clear_contextvars()
        bind_contextvars(
            correlation_id=request.headers.get("X-Correlation-ID", str(uuid4())),
            service_name=settings.SERVICE_NAME,        # "gssp-gs", "agent-executor" etc.
            environment=settings.ENVIRONMENT,          # "prod" | "staging" | "dev"
            application_id=request.headers.get("X-Application-ID", ""),
        )
        response = await call_next(request)
        return response
```

After this, **every log line in every service automatically includes** `correlation_id`, `service_name`, `environment`, `application_id` — without changing any individual log statement.

### Log emission (same API everywhere)

```python
import structlog

log = structlog.get_logger()

# Anywhere in any service — context fields injected automatically
log.info("llm_call_completed",
    model="gemini-1.5-pro",
    latency_ms=1840,
    input_tokens=512,
    output_tokens=148,
    status="success",
)

log.error("tool_call_failed",
    tool_id="servicenow-create",
    error_code="TOOL_TIMEOUT",
    http_status=504,
    retry_count=3,
)
```

**Output (prod JSON):**
```json
{
  "event": "llm_call_completed",
  "level": "info",
  "timestamp": "2026-05-28T10:30:00.000Z",
  "service_name": "gssp-gs",
  "environment": "prod",
  "correlation_id": "CORR_abc123",
  "application_id": "179524",
  "model": "gemini-1.5-pro",
  "latency_ms": 1840,
  "input_tokens": 512,
  "output_tokens": 148,
  "status": "success"
}
```

### Why Fluent Bit (not Fluentd)?

| | Fluent Bit | Fluentd |
|---|---|---|
| Memory footprint | ~1 MB | ~40 MB |
| CPU | Very low | Medium |
| K8s sidecar | ✅ Ideal | ⚠️ Too heavy |
| Plugins | Sufficient | More extensive |
| Verdict | Use for log shipping | Use for complex transforms |

### Fluent Bit DaemonSet config (ships logs → Elasticsearch)

```yaml
# k8s/fluent-bit-configmap.yaml
[INPUT]
    Name              tail
    Path              /var/log/containers/*.log
    Parser            docker
    Tag               kube.*
    Mem_Buf_Limit     5MB

[FILTER]
    Name              kubernetes
    Match             kube.*
    Merge_Log         On
    Keep_Log          Off
    K8S-Logging.Parser On

[FILTER]
    Name              parser
    Match             kube.*
    Key_Name          log
    Parser            json             # parse structured JSON logs from structlog

[OUTPUT]
    Name              es
    Match             kube.*
    Host              ${ELASTICSEARCH_HOST}
    Port              9200
    Index             ai-obs-${service_name}-logs-%Y.%m
    tls               On
    tls.verify        Off
    Retry_Limit       5
```

**Result:** Every `structlog` JSON line is automatically picked up, parsed, and indexed in Elasticsearch under a per-service, per-month index — queryable in Kibana.

---

## 5. Distributed Traces (Cross-Service Spans)

**Problem today:** No `span_id`, no `parent_span_id`, no W3C `traceparent` propagation across the 8 services. A request touches Orchestration → Agent Executor → GSSP QS → GSSP RS → GSSP GS and there is no single trace linking all hops.

**Tool: OpenTelemetry Python SDK + Grafana Tempo**

### Why Grafana Tempo over Jaeger?

| | Grafana Tempo | Jaeger |
|---|---|---|
| Storage backend | Object storage (S3, GCS) — very cheap | Cassandra or Elasticsearch — expensive at scale |
| Grafana integration | ✅ Native (same vendor) | ⚠️ Plugin needed |
| Scalability | Horizontally scalable | Complex at scale |
| TraceQL query language | ✅ Powerful | ❌ None |
| Cost at 1M spans/day | ~$5/month (S3) | Much higher (ES storage) |

### Installation
```bash
pip install opentelemetry-sdk opentelemetry-exporter-otlp opentelemetry-instrumentation-fastapi opentelemetry-instrumentation-httpx opentelemetry-instrumentation-asyncpg
```

### Shared tracer setup (one file, imported by all services)

```python
# shared/telemetry.py
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor
from opentelemetry.instrumentation.asyncpg import AsyncPGInstrumentor

def init_tracing(app, service_name: str, environment: str):
    resource = Resource.create({
        "service.name": service_name,
        "deployment.environment": environment,
    })

    provider = TracerProvider(resource=resource)
    exporter = OTLPSpanExporter(endpoint="http://tempo.internal:4317")  # Grafana Tempo OTLP endpoint
    provider.add_span_processor(BatchSpanProcessor(exporter))
    trace.set_tracer_provider(provider)

    # Auto-instrument FastAPI (HTTP server spans)
    FastAPIInstrumentor.instrument_app(app)

    # Auto-instrument outbound HTTP (httpx calls between services)
    HTTPXClientInstrumentor().instrument()

    # Auto-instrument PostgreSQL queries
    AsyncPGInstrumentor().instrument()

    return trace.get_tracer(service_name)
```

### In each service's `main.py`

```python
# main.py
from shared.telemetry import init_tracing
from fastapi import FastAPI

app = FastAPI()
tracer = init_tracing(app, service_name="gssp-qs", environment=settings.ENVIRONMENT)
```

**What you get automatically with zero further code:**
- Span for every incoming HTTP request to any FastAPI endpoint
- Span for every outgoing `httpx` call (GSSP QS → GSSP RS, GSSP QS → GSSP GS)
- Span for every `asyncpg` DB query
- `traceparent` header automatically injected on all outbound calls and extracted from all inbound calls
- Full trace tree in Grafana Tempo for every cross-service request

### Custom spans for business events

```python
# For events that need a custom span (not auto-instrumented)
tracer = trace.get_tracer("gssp-rs")

with tracer.start_as_current_span("mmr-reranking") as span:
    span.set_attribute("retrieved_chunk_count", len(chunks))
    span.set_attribute("top_k", top_k)
    result = mmr_reranker.rerank(chunks)
    span.set_attribute("reranked_count", len(result))
```

### Grafana Tempo deployment (Helm)

```yaml
# helm/tempo-values.yaml
tempo:
  storage:
    trace:
      backend: s3
      s3:
        bucket: ai-obs-traces-prod
        endpoint: s3.amazonaws.com
        region: us-east-1
  retention: 720h   # 30 days
```

**Grafana data source:** Add Tempo as a Grafana data source and enable **trace-to-logs correlation** — clicking a span opens the matching Elasticsearch log lines for that `correlation_id`. This is the trace drill-down the Observability Chatbot needs.

---

## 6. HTTP Endpoint Metrics — Latency, Error Rate, Throughput

**Problem today:** `latency_ms` is a string in log messages, not a numeric metric. No `/metrics` endpoint. No p95/p99 latency. No per-endpoint error rate.

**Tool: `prometheus-fastapi-instrumentator`**

This is a one-line addition per service. It automatically exposes a `/metrics` endpoint in Prometheus format with:
- `http_request_duration_seconds` histogram (p50/p95/p99 latency per endpoint)
- `http_requests_total` counter (grouped by method, endpoint, status code)
- `http_requests_in_progress` gauge

### Installation
```bash
pip install prometheus-fastapi-instrumentator
```

### Integration (add to each service's `main.py`)

```python
# main.py — add 3 lines
from prometheus_fastapi_instrumentator import Instrumentator

app = FastAPI()

# Add after app creation — auto-instruments all routes
Instrumentator(
    should_group_status_codes=False,
    should_respect_env_var=True,
    excluded_handlers=["/health", "/metrics"],
).instrument(app).expose(app)
```

**That is all.** Every FastAPI service now has `/metrics` with full latency histograms. Prometheus scrapes it. The Custom Dashboard Service proxies the data and plots it.

### Prometheus scrape config

```yaml
# prometheus.yml — add per service
scrape_configs:
  - job_name: gssp-gs
    static_configs:
      - targets: ["gssp-gs.internal:8000"]
    metrics_path: /metrics

  - job_name: gssp-qs
    static_configs:
      - targets: ["gssp-qs.internal:8000"]
```

### Custom Dashboard Service — PromQL query for p95 latency per endpoint

```promql
histogram_quantile(0.95,
  sum(rate(http_request_duration_seconds_bucket{job="gssp-gs"}[5m])) by (le, handler)
)
```

### Custom business metrics (beyond HTTP)

For domain-specific counters (token count, queue depth, document count), use the Prometheus Python client:

```python
from prometheus_client import Counter, Histogram, Gauge

# Define once at module level
LLM_TOKENS_TOTAL = Counter(
    "llm_tokens_total",
    "Total LLM tokens consumed",
    ["service", "model", "application_id"]
)
KAFKA_LAG = Gauge(
    "kafka_consumer_lag",
    "Current consumer lag",
    ["topic", "partition", "consumer_group"]
)
INGESTION_JOB_DURATION = Histogram(
    "ingestion_job_duration_seconds",
    "Document ingestion job duration",
    ["status", "document_format"],
    buckets=[1, 5, 15, 30, 60, 120, 300]
)

# Use in code
LLM_TOKENS_TOTAL.labels(
    service="gssp-gs",
    model="gemini-1.5-pro",
    application_id=ctx.application_id
).inc(result.usage.total_tokens)
```

---

## 7. Kafka Consumer Lag & Health

**Problem today:** `kafka_lag`, `kafka_partition`, `kafka_offset`, `consumer_group` all ❌ Missing. No visibility into whether the Agent Executor or Orchestration consumer is falling behind.

**Tool: kminion → Prometheus → Custom Dashboard Service (Kafka Health page)**

### Why kminion over alternatives?

| Tool | What it does | Verdict |
|---|---|---|
| **kminion** | Modern Kafka monitoring, Prometheus-native, Kafka 2.4+ | ✅ Best choice today |
| Kafka JMX Exporter | JMX → Prometheus, official but complex | ⚠️ Complex config |
| Burrow (LinkedIn) | Consumer lag only, HTTP API | ⚠️ No Prometheus native, abandoned |
| Confluent Control Center | Full monitoring UI | ❌ Requires Confluent Platform |

### Deploy kminion (single Docker container)

```yaml
# docker-compose or Helm
kminion:
  image: redpandadata/kminion:latest
  environment:
    KAFKA_BROKERS: "kafka-broker-1:9092,kafka-broker-2:9092"
  ports:
    - "8080:8080"    # Prometheus metrics endpoint
```

### What kminion exposes automatically

```promql
# Consumer lag per topic/partition/consumer_group
kminion_consumer_group_topic_partition_lag

# Messages produced per second per topic
rate(kminion_topic_log_end_offset[1m])

# Number of partitions with lag > 0
count(kminion_consumer_group_topic_partition_lag > 0)

# Time to drain current lag at current consumption rate
kminion_consumer_group_topic_partition_lag /
  rate(kminion_consumer_group_topic_partition_messages_fetched_total[5m])
```

### Custom Dashboard Kafka Health page

The Custom Dashboard Service Kafka Health page reads Prometheus metrics via a FastAPI backend query:

```python
# dashboard-service/api/v1/kafka_health.py
@router.get("/kafka-health")
async def kafka_health(conn=Depends(get_pg_conn), user=Depends(require_coin_token)):
    # Prometheus remote-read or kminion Prometheus endpoint proxy
    rows = await conn.fetch("""
        SELECT metric_name, metric_value, metric_tags, timestamp
        FROM obs_metrics
        WHERE metric_name = 'kafka_consumer_lag'
        ORDER BY timestamp DESC LIMIT 100
    """)
    return [dict(r) for r in rows]
```

The React + Tremor Kafka Health page shows consumer lag per topic/partition as a `BarChart` with a red threshold line at 1000 messages.

### Emit lag into OIS (for correlation with agent latency)

```python
# In Agentic Orchestration KafkaConsumerService — after each poll
from prometheus_client import Gauge

lag = consumer.metrics()["consumer-fetch-manager-metrics"]["records-lag-max"]
KAFKA_LAG.labels(
    topic="AGENT_EXECUTION_REQUEST",
    consumer_group="agent-executor-group"
).set(lag)

# Also emit to OIS for cross-correlation
await emit_event(
    telemetry_type="metric",
    event_type="KAFKA_LAG_RECORDED",
    metric_name="kafka_consumer_lag",
    metric_value=lag,
    payload={"kafka_topic": "AGENT_EXECUTION_REQUEST", "consumer_group": "agent-executor-group"},
    ...
)
```

---

## 8. PII Detection and Redaction

**Problem today:** `user_id` / SOE_ID logged as plain text in all 8 services. Full HTTP bodies logged in Consumer Service, Data Ingestion, GSSP GS, User Feedback — all flagged 🔴 PII Risk.

**Tool: GLiNER (in-process NER model — no sidecar service)**

### Why GLiNER over Presidio?

| | Custom regex (current plan) | Presidio | GLiNER |
|---|---|---|---|
| Entity types covered | 5 patterns | 50+ fixed types | Unlimited — defined as plain English strings |
| ML-based detection | ❌ Regex only | ✅ spaCy NER | ✅ Zero-shot transformer NER |
| Add custom entity type | Hard (regex) | New recogniser class | Add a string to a list |
| Infrastructure | None | 2 sidecar services (analyzer + anonymizer) | None — runs in-process inside OIS |
| Network hop | None | HTTP call per event | None |
| Failure mode | Never fails | HTTP timeout = event blocked | Never fails |
| Multi-language | ❌ | ✅ | ✅ (zero-shot) |
| Accuracy | Low | High | High — matches or exceeds Presidio |
| Self-hosted | ✅ | ✅ | ✅ |

**Key advantage:** GLiNER runs inside the OIS process. No new K8s deployment, no HTTP call per event, no network failure mode. Model loads once at startup and is shared across all events.

### Installation

```bash
pip install gliner
```

Model is downloaded once on first startup (~300MB, cached locally):

```python
from gliner import GLiNER
model = GLiNER.from_pretrained("urchade/gliner_medium-v2.1")
```

### Integration in OIS Enrichment pipeline

```python
# processing/pii_redactor.py
import re
import hashlib
from gliner import GLiNER


class PiiRedactor:
    """
    GLiNER-based PII redactor — runs in-process inside OIS.
    No sidecar services, no HTTP calls, no new K8s deployments.
    Model loads once at startup (~300MB).
    """

    # Zero-shot entity labels — add new types here as plain English strings
    PII_LABELS = [
        "person name",
        "email address",
        "phone number",
        "credit card number",
        "social security number",
        "home address",
        "date of birth",
        "organization name",
        "employee ID",
        "case ID",
        "account number",
        "COIN token",
        "passport number",
        "IP address",
    ]

    # Regex fallback for structured patterns GLiNER may miss in short strings
    REGEX_PATTERNS = {
        "SSN":         re.compile(r"\b\d{3}-\d{2}-\d{4}\b"),
        "CREDIT_CARD": re.compile(r"\b(?:4[0-9]{12}(?:[0-9]{3})?|5[1-5][0-9]{14}|3[47][0-9]{13})\b"),
        "IP_ADDRESS":  re.compile(r"\b(?:\d{1,3}\.){3}\d{1,3}\b"),
        "COIN_TOKEN":  re.compile(r"eyJ[A-Za-z0-9+/=]{20,}"),   # JWT / COIN token
    }

    # Fields that contain free text — run full NER + regex
    TEXT_FIELDS = {"message", "free_text_comment", "error_description", "raw_prompt", "payload"}

    # Fields that are always hashed (known structured user identifiers)
    HASH_FIELDS = {"soe_id", "user_id", "soeid"}

    def __init__(self):
        self.model = GLiNER.from_pretrained("urchade/gliner_medium-v2.1")

    def redact_event(self, event: dict) -> dict:
        # Hash known user identifier fields
        for field in self.HASH_FIELDS:
            if event.get(field):
                event["user_hash"] = "sha256_" + hashlib.sha256(
                    str(event[field]).encode()
                ).hexdigest()[:16]
                event[field] = None     # remove plain text identifier

        # Redact free-text fields with GLiNER + regex
        for field in self.TEXT_FIELDS:
            if event.get(field) and isinstance(event[field], str):
                event[field] = self._redact_text(event[field])

        return event

    def _redact_text(self, text: str) -> str:
        if not text or len(text) > 10_000:
            return text

        # GLiNER NER pass — catches names, addresses, context-aware PII
        entities = self.model.predict_entities(
            text, self.PII_LABELS, threshold=0.5
        )
        for ent in sorted(entities, key=lambda x: x["start"], reverse=True):
            tag = ent["label"].upper().replace(" ", "_")
            text = text[:ent["start"]] + f"[{tag}]" + text[ent["end"]:]

        # Regex pass — catches structured patterns (SSN format, card numbers, JWT)
        for label, pattern in self.REGEX_PATTERNS.items():
            text = pattern.sub(f"[{label}]", text)

        return text
```

### Adding a new custom entity type

No code changes to the model. Add a string to `PII_LABELS`:

```python
# Before: detect COIN tokens
PII_LABELS = [..., "COIN token"]

# After: also detect employee badge numbers
PII_LABELS = [..., "COIN token", "employee badge number", "internal case reference"]
```

The zero-shot model handles it immediately — no retraining, no redeployment.

### Wire into OIS at startup

```python
# ois/main.py
from processing.pii_redactor import PiiRedactor

# Load model once at startup — shared across all requests
pii_redactor = PiiRedactor()

@app.post("/v1/ingest")
async def ingest(event: ObsEvent):
    clean_event = pii_redactor.redact_event(event.dict())
    await writer.write(clean_event)
    return {"status": "accepted"}
```

---

## 9. Error Aggregation and Grouping

**Problem today:** Errors are in Elasticsearch but not grouped, de-duplicated, or tracked by occurrence count. Same `ReadTimeoutError` appears 1,000 times with no way to see "this error spiked 3x today."

**Tool: Sentry (self-hosted)**

### Why Sentry?

| Capability | Elasticsearch errors index | Sentry |
|---|---|---|
| Error deduplication (same error grouped) | ❌ Each occurrence is separate | ✅ Fingerprinting groups identical errors |
| Occurrence count and trend | ❌ Aggregation query only | ✅ Built-in "seen 847 times, up 40% today" |
| First seen / last seen | ❌ Manual query | ✅ Automatic |
| Breadcrumbs (events before the error) | ❌ | ✅ Last 20 log lines before crash |
| Assigned user / resolution status | ❌ | ✅ "Assigned to DevOps, resolved in v1.2.3" |
| Release tracking | ❌ | ✅ "This error first appeared in deploy abc123" |
| Performance monitoring | ❌ | ✅ Integrated with traces |
| Self-hosted | N/A | ✅ `sentry.io` or self-hosted via `getsentry/self-hosted` |

### Deploy Sentry self-hosted

```bash
git clone https://github.com/getsentry/self-hosted.git
cd self-hosted
./install.sh
docker-compose up -d
```

### SDK integration (add to each service)

```bash
pip install sentry-sdk[fastapi]
```

```python
# main.py — add before FastAPI app creation
import sentry_sdk
from sentry_sdk.integrations.fastapi import FastApiIntegration
from sentry_sdk.integrations.asyncpg import AsyncPGIntegration
from sentry_sdk.integrations.httpx import HttpxIntegration

sentry_sdk.init(
    dsn=settings.SENTRY_DSN,                       # from self-hosted Sentry
    environment=settings.ENVIRONMENT,
    release=settings.APP_VERSION,
    integrations=[
        FastApiIntegration(transaction_style="url"),
        AsyncPGIntegration(),
        HttpxIntegration(),
    ],
    traces_sample_rate=0.1,                        # 10% of requests traced in Sentry
    before_send=scrub_pii_from_sentry_event,       # PII scrubber hook (see below)
)
```

### PII scrubber hook (prevent PII reaching Sentry)

```python
def scrub_pii_from_sentry_event(event, hint):
    # Remove SOE_ID / user identifiers from Sentry events
    if "request" in event:
        event["request"].pop("data", None)          # never send request body
        event["request"].pop("cookies", None)
    if "user" in event:
        event["user"].pop("email", None)
        event["user"].pop("username", None)
    return event
```

### Add correlation_id and application_id to Sentry scope

```python
# In ObservabilityMiddleware — runs on every request
import sentry_sdk

with sentry_sdk.configure_scope() as scope:
    scope.set_tag("correlation_id", correlation_id)
    scope.set_tag("application_id", application_id)
    scope.set_tag("service_name", settings.SERVICE_NAME)
```

**Result:** Every unhandled exception in Sentry shows `correlation_id` as a searchable tag — you can jump from a Sentry error directly to the Grafana Tempo trace and Kibana logs for that exact request.

---

## 10. Cost and Budget Governance

**Tool: Langfuse (per-call) + Redis (real-time accumulator) + PostgreSQL (budget caps) + Custom Dashboard Service**
> This combination is already detailed in `Developer_Implementation_Guide.md` Section 3a and the `OBSERVABILITY_INGESTION_SERVICE_PLAN.md`. Summary here:

| Layer | Tool | What it does |
|---|---|---|
| Per-call cost | Langfuse | `estimated_cost_usd` auto-calculated per LLM call from built-in pricing table |
| Real-time daily accumulator | Redis `INCRBYFLOAT` | Running total per `application_id:model:date` key; sub-millisecond |
| Budget caps | PostgreSQL `budget_limits` table | Threshold checked on each accumulator update |
| Budget breach event | OIS → Kafka `ai-obs-events` `BUDGET_THRESHOLD_EXCEEDED` | Visible on Custom Dashboard Cost Governance page |
| Aggregate dashboard | **Custom Dashboard Service** — Cost Governance page | Queries PostgreSQL `agg_hourly_llm_metrics` + `budget_limits`; AreaChart actual vs cap |

```tsx
// src/pages/CostGovernance.tsx — React + Tremor
import { Card, Title, AreaChart, Table, Badge } from "@tremor/react";

// AreaChart: actual_spend vs budget_cap over 30 days
// Table with utilisation_pct Badge (red if > 80%)
```

---

## 11. Kubernetes / Infrastructure Metrics

**Problem today:** No pod CPU/memory, no DB connection pool health, no K8s event monitoring.

**Tool: kube-state-metrics + Prometheus Node Exporter → Prometheus → Custom Dashboard Service**

### Install (standard K8s observability stack — Grafana disabled)

```bash
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm install kube-prometheus-stack prometheus-community/kube-prometheus-stack \
  --namespace monitoring \
  --set grafana.enabled=false \   # ← Grafana disabled; Custom Dashboard Service replaces it
  --set prometheus.enabled=true
```

This installs:
- **kube-state-metrics** — K8s object metrics (pod restarts, deployment replicas, job status)
- **node-exporter** — Node CPU, memory, disk, network
- **Prometheus** — Scrapes everything

The Custom Dashboard Service Platform Overview page exposes K8s health via a `/api/v1/infra-health` endpoint that proxies Prometheus queries:

```python
# dashboard-service/api/v1/infra_health.py
import httpx

PROMETHEUS_URL = "http://prometheus.monitoring.svc:9090"

@router.get("/infra-health")
async def infra_health(user=Depends(require_coin_token)):
    async with httpx.AsyncClient() as client:
        pod_restarts = await client.get(f"{PROMETHEUS_URL}/api/v1/query", params={
            "query": "sum by (pod) (kube_pod_container_status_restarts_total)"
        })
        cpu_usage = await client.get(f"{PROMETHEUS_URL}/api/v1/query", params={
            "query": "sum by (container) (rate(container_cpu_usage_seconds_total[5m]))"
        })
    return {"pod_restarts": pod_restarts.json(), "cpu_usage": cpu_usage.json()}
```

**What you get immediately without any code:**
- Pod restart count per service (detects crash loops)
- Container CPU and memory usage vs limits
- K8s deployment rollout status
- Node disk pressure and memory pressure
- Persistent volume usage

### PostgreSQL connection pool metrics (asyncpg)

```python
# Add to each service — exposes DB pool health to Prometheus
from prometheus_client import Gauge

PG_POOL_SIZE = Gauge("asyncpg_pool_size", "Current pool size", ["service"])
PG_POOL_IDLE = Gauge("asyncpg_pool_idle", "Idle connections", ["service"])

# In DB startup / health check
async def update_pool_metrics(pool, service_name: str):
    PG_POOL_SIZE.labels(service=service_name).set(pool.get_size())
    PG_POOL_IDLE.labels(service=service_name).set(pool.get_idle_size())
```

---

## 12. Document Ingestion Pipeline Events

**Problem today:** `DOCUMENT_PARSE_STARTED`, `DOCUMENT_PARSE_COMPLETED`, `DOCUMENT_PARSE_FAILED`, `chunk_count`, `page_count`, `extraction_status`, `parser_used` — all ❌ Missing.

**Tool: OpenTelemetry custom spans → OIS**

Custom spans from the OpenTelemetry SDK (already installed for distributed tracing) are the right primitive here. The ingestion pipeline becomes a traceable span tree, and the key fields are emitted as span attributes and also forwarded to OIS as structured events.

```python
# consumer_service/ingestion/base_tenant.py
from opentelemetry import trace
from shared.obs_emitter import emit_event

tracer = trace.get_tracer("consumer-service")

async def ingest(self, job: IngestionJob):
    with tracer.start_as_current_span("ingestion-job") as job_span:
        job_span.set_attribute("job_id", job.job_id)
        job_span.set_attribute("document_id", job.document_id)
        job_span.set_attribute("application_id", job.application_id)

        # Download from S3
        with tracer.start_as_current_span("s3-download") as dl_span:
            content, size_bytes = await self.download_from_s3(job.s3_key)
            dl_span.set_attribute("document_size_bytes", size_bytes)

        # Parse
        with tracer.start_as_current_span("document-parse") as parse_span:
            await emit_event(
                telemetry_type="event",
                event_type="DOCUMENT_PARSE_STARTED",
                payload={"document_format": job.format, "document_size_bytes": size_bytes},
                **ctx_fields
            )
            try:
                chunks, page_count = await self.parser.parse(content, job.format)
                parse_span.set_attribute("chunk_count", len(chunks))
                parse_span.set_attribute("page_count", page_count)
                parse_span.set_attribute("parser_used", self.parser.name)
                parse_span.set_attribute("extraction_status", "success")

                await emit_event(
                    telemetry_type="event",
                    event_type="DOCUMENT_PARSE_COMPLETED",
                    payload={
                        "chunk_count": len(chunks),
                        "page_count": page_count,
                        "parser_used": self.parser.name,
                        "extraction_status": "success",
                        "document_format": job.format,
                    },
                    **ctx_fields
                )
            except ParseError as e:
                parse_span.record_exception(e)
                await emit_event(
                    telemetry_type="event",
                    event_type="DOCUMENT_PARSE_FAILED",
                    status="failed",
                    error_code="PARSE_ERROR",
                    payload={"parser_used": self.parser.name, "document_format": job.format},
                    **ctx_fields
                )
                raise

        # Embed + store (similar pattern)
        with tracer.start_as_current_span("embedding") as emb_span:
            embeddings, embed_tokens = await self.embedder.embed(chunks)
            emb_span.set_attribute("embedding_model", self.embedder.model_name)
            emb_span.set_attribute("input_tokens", embed_tokens)
            emb_span.set_attribute("chunk_count", len(chunks))
```

This produces:
1. A **Grafana Tempo** trace showing the full ingestion pipeline with timing per stage (Tempo = trace backend, not dashboard tool)
2. OIS events (`DOCUMENT_PARSE_STARTED/COMPLETED/FAILED`) written to `obs_events` for Custom Dashboard aggregate panels
3. Prometheus metrics (via `prometheus_client`) for queue depth and job duration

---

## 13. Guardrail Decisions

**Problem today:** `GUARDRAIL_EVALUATED`, `GUARDRAIL_BLOCKED`, `risk_score`, `violation_type`, `policy_version` — all ❌ Missing.

**Tool: OIS custom events (no external tool needed)**

Guardrail data is domain-specific and already has the right home in OIS. The integration is one `emit_event()` call in the guardrail client:

```python
# gssp-qs/query/guardrail_client.py
from shared.obs_emitter import emit_event

async def evaluate(self, query: str, ctx: ObsContext) -> GuardrailResult:
    result = await self.lakera_client.evaluate(query)

    event_type = "GUARDRAIL_BLOCKED" if result.blocked else "GUARDRAIL_EVALUATED"
    await emit_event(
        telemetry_type="event",
        event_type=event_type,
        status="success" if not result.blocked else "failed",
        correlation_id=ctx.correlation_id,
        application_id=ctx.application_id,
        service_name="gssp-qs",
        environment=ctx.environment,
        payload={
            "policy_id": result.policy_id,
            "policy_version": result.policy_version,
            "decision": "block" if result.blocked else "allow",
            "risk_score": result.risk_score,
            "violation_type": result.violation_type,     # "toxicity" | "pii" | "off_topic"
            "blocked_stage": "input",
            "guardrail_latency_ms": result.latency_ms,
        },
    )
    return result
```

**Custom Dashboard indicator on guardrail spike:**

The Custom Dashboard Platform Overview page shows a guardrail block rate KPI card. When `block_rate > 15%` over 1h the card turns red. Query:
```python
# dashboard-service/api/v1/overview.py — guardrail section
SELECT
    COUNT(*) FILTER (WHERE event_type = 'GUARDRAIL_BLOCKED') AS blocked,
    COUNT(*) FILTER (WHERE event_type = 'GUARDRAIL_EVALUATED') AS total,
    COUNT(*) FILTER (WHERE event_type = 'GUARDRAIL_BLOCKED')::float /
        NULLIF(COUNT(*) FILTER (WHERE event_type = 'GUARDRAIL_EVALUATED'), 0) AS block_rate
FROM obs_events
WHERE timestamp >= NOW() - INTERVAL '1 hour'
```

---

## 14. Vector / Embedding Health

**Problem today:** Embedding drift, index freshness, retrieval recall@k — ❌ not monitored anywhere. Silent RAG quality degradation is undetectable.

**Tool: Custom pgvector health queries → Prometheus Pushgateway → Custom Dashboard RAG Quality page**

There is no off-the-shelf tool for pgvector health. This is a custom monitoring job that runs on a schedule and pushes metrics to Prometheus.

```python
# aggregation/vector_health_monitor.py — K8s CronJob, runs every hour
import asyncpg
from prometheus_client import CollectorRegistry, Gauge, push_to_gateway
from datetime import datetime, timezone

async def check_vector_health():
    registry = CollectorRegistry()

    freshness_hours = Gauge(
        "pgvector_index_freshness_hours",
        "Hours since last embedding update for a knowledge base",
        ["rag_id", "knowledge_base"],
        registry=registry,
    )
    chunk_count = Gauge(
        "pgvector_chunk_count",
        "Total number of chunks in the vector index",
        ["rag_id"],
        registry=registry,
    )
    avg_embedding_norm = Gauge(
        "pgvector_avg_embedding_norm",
        "Average L2 norm of embeddings — drift proxy metric",
        ["rag_id"],
        registry=registry,
    )

    conn = await asyncpg.connect(settings.PG_RS_DSN)

    # Freshness: when was the last embedding inserted?
    rows = await conn.fetch("""
        SELECT
            retrieval_config_id AS rag_id,
            MAX(created_at) AS last_indexed_at,
            COUNT(*) AS chunk_count,
            AVG(vector_norm(embedding)) AS avg_norm
        FROM document_chunks
        GROUP BY retrieval_config_id
    """)

    now = datetime.now(timezone.utc)
    for row in rows:
        hours_since = (now - row["last_indexed_at"]).total_seconds() / 3600
        freshness_hours.labels(
            rag_id=row["rag_id"],
            knowledge_base=row["rag_id"]
        ).set(hours_since)
        chunk_count.labels(rag_id=row["rag_id"]).set(row["chunk_count"])
        avg_embedding_norm.labels(rag_id=row["rag_id"]).set(float(row["avg_norm"] or 0))

    # Push to Prometheus Pushgateway (for batch jobs that don't run a server)
    push_to_gateway("http://pushgateway.internal:9091", job="vector-health", registry=registry)

    # Also write snapshot to PostgreSQL for historical trend
    await conn.executemany("""
        INSERT INTO vector_health_snapshots
            (snapshot_date, rag_id, knowledge_base, last_indexed_at,
             hours_since_indexed, freshness_breach_flag)
        VALUES ($1, $2, $3, $4, $5, $6)
        ON CONFLICT (snapshot_date, rag_id) DO UPDATE SET
            hours_since_indexed = EXCLUDED.hours_since_indexed,
            freshness_breach_flag = EXCLUDED.freshness_breach_flag
    """, [
        (datetime.today().date(), r["rag_id"], r["rag_id"],
         r["last_indexed_at"], (now - r["last_indexed_at"]).total_seconds() / 3600,
         (now - r["last_indexed_at"]).total_seconds() / 3600 > 24)
        for r in rows
    ])
```

**Custom Dashboard RAG Quality page shows stale index indicator:**

Snapshot rows from `vector_health_snapshots` (written by the CronJob) are displayed in the Custom Dashboard RAG Quality page. Any `hours_since_indexed > 24` row renders with a red `Badge` in the Tremor `Table`:
```tsx
<Badge color={row.hours_since_indexed > 24 ? "red" : "green"}>
  {row.hours_since_indexed > 24 ? "STALE" : "FRESH"}
</Badge>
```

**Embedding drift proxy:** If `avg_embedding_norm` shifts significantly over time (rolling std > 2×), it indicates the embedding model may have changed or the document distribution has shifted — a signal to re-embed the knowledge base.

---

## 15. Platform Dashboards and Visualization

**Tool: Custom Dashboard Service (FastAPI + React + Tremor)**

This is Option 4 — the platform's own dashboard service instead of Grafana.

### Architecture

```
PostgreSQL agg_* tables ──┐
Elasticsearch anomalies ──┤──► FastAPI /api/v1/* ──► React + Tremor UI
Langfuse SDK              ──┘                         COIN JWT auth
```

### Backend (FastAPI)

```python
# dashboard-service/api/v1/overview.py
from fastapi import APIRouter, Depends
from auth.coin_jwt import require_coin_token
from db.connection import get_pg_conn

router = APIRouter()

@router.get("/overview")
async def platform_overview(
    hours: int = 24,
    application_id: str = None,
    conn=Depends(get_pg_conn),
    user=Depends(require_coin_token),
):
    query = """
        SELECT service_name,
               SUM(request_count) AS total_requests,
               SUM(success_count)::float / NULLIF(SUM(request_count), 0) AS success_rate,
               SUM(error_count)::float   / NULLIF(SUM(request_count), 0) AS error_rate,
               AVG(avg_latency_ms) AS avg_latency,
               MAX(p95_latency_ms) AS p95_latency,
               SUM(total_tokens) AS total_tokens,
               SUM(estimated_cost) AS total_cost
        FROM agg_hourly_application_metrics
        WHERE hour_timestamp >= NOW() - ($1 || ' hours')::INTERVAL
          AND ($2::text IS NULL OR application_id = $2)
        GROUP BY service_name
    """
    rows = await conn.fetch(query, hours, application_id)
    return [dict(r) for r in rows]
```

### Frontend (React + Tremor)

```tsx
// src/pages/PlatformOverview.tsx
import { Card, Title, AreaChart, BarChart, Grid } from "@tremor/react";

// KPI cards: total requests, error rate, total LLM cost
// BarChart: error rate by service
// BarChart: P95 latency by service
// AreaChart: request volume over 24h
```

### Dashboard Pages

| Page | Data Source | Key Components |
|---|---|---|
| Platform Overview | `agg_hourly_application_metrics` | KPI cards, BarChart (errors/latency), AreaChart (volume) |
| Cost Governance | `budget_limits` + `agg_hourly_llm_metrics` | AreaChart (actual vs cap), Table with utilisation Badge |
| Business KPIs | `agg_daily_kpi_metrics` | Table + Trend sparklines |
| Kafka Health | `obs_metrics` (kafka_consumer_lag) | BarChart (lag by topic) |
| RAG Quality | `daily_rag_quality` + `vector_health_snapshots` | Table with freshness Badge, faithfulness score KPIs |
| Anomaly View | Elasticsearch `ai-obs-anomalies-*` | Timeline, anomaly score sparklines |
| Feedback Trends | `agg_daily_feedback_metrics` + `feedback_case` | BarChart (pos/neg ratio), categories Table |

---

## 16. Anomaly Detection

**Tool: Custom Isolation Forest (already designed in `Developer_Implementation_Guide.md` Section 7)**

The Custom Anomaly Detection Service uses scikit-learn Isolation Forest + LSTM for temporal anomalies. Results flow:

```
Anomaly Detection Service → ai-obs-anomalies (Kafka) → Elasticsearch → Custom Dashboard Anomaly View
```

For log-level anomaly detection (unusual error patterns in free-text logs), **Kibana Machine Learning** is used — it is already included in the Elasticsearch cluster and requires no additional infrastructure:

```
Kibana → Machine Learning → Anomaly Explorer
→ Detects unusual error frequency patterns per service
→ No separate Grafana plugin needed
```

---

## Full Tool Stack Summary

```
┌─────────────────────────────────────────────────────────────────────────┐
│  SIGNAL → TOOL MAPPING                                                  │
│                                                                         │
│  LLM/RAG/Agent traces    ──► Langfuse (self-hosted)                    │
│  Prompt management       ──► Langfuse Prompt Management                │
│  Evaluations             ──► Langfuse LLM-as-judge                     │
│                                                                         │
│  Structured app logs     ──► structlog → Fluent Bit → Elasticsearch    │
│  Distributed traces      ──► OpenTelemetry SDK → Grafana Tempo         │
│                               (Tempo = trace backend only, no UI)      │
│  HTTP latency metrics    ──► prometheus-fastapi-instrumentator          │
│                               → Prometheus → Custom Dashboard Service  │
│                                                                         │
│  Kafka consumer lag      ──► kminion → Prometheus                      │
│                               → Custom Dashboard Kafka Health page     │
│  K8s / infra metrics     ──► kube-prometheus-stack                     │
│                               (grafana.enabled=false; metrics scraped  │
│                                by Custom Dashboard Service)            │
│  Vector / embed health   ──► Custom CronJob → Pushgateway              │
│                               → Custom Dashboard RAG Quality page      │
│                                                                         │
│  PII redaction           ──► GLiNER (in-process, OIS pipeline)         │
│  Error aggregation       ──► Sentry (self-hosted)                      │
│  Cost / budget caps      ──► Langfuse + Redis + PostgreSQL             │
│                               → Custom Dashboard Cost Governance page  │
│                                                                         │
│  Guardrail events        ──► OIS custom events                         │
│  Document ingestion      ──► OpenTelemetry custom spans → OIS          │
│  Anomaly detection       ──► Custom Isolation Forest → Elasticsearch   │
│                               → Custom Dashboard Anomaly View          │
│  Log anomaly detection   ──► Kibana ML (built into Elasticsearch)      │
│                                                                         │
│  Platform dashboards     ──► Custom Dashboard Service                  │
│                               (FastAPI + React + Tremor, COIN JWT)     │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## What Each Tool Replaces (Custom Build Avoided)

| Custom Component in Docs | Replaced By | Weeks Saved |
|---|---|---|
| Custom `JSONFormatter` + `AppInfoFilter` | `structlog` shared config | 2 weeks |
| Custom `PiiRedactor` (regex) | GLiNER in-process NER | 1 day |
| Custom `FaithfulnessScorer` | Langfuse LLM-as-judge | 3–4 weeks |
| Custom Anomaly Detection service (Phase 1) | Kibana ML (built into Elasticsearch) | 6–8 weeks |
| Custom distributed trace correlation | OpenTelemetry → Grafana Tempo | 4–6 weeks |
| Custom `/metrics` endpoint per service | prometheus-fastapi-instrumentator | 2–3 weeks per service |
| Custom Kafka lag monitoring | kminion | 2 weeks |
| Custom K8s metrics | kube-prometheus-stack | 1 week |
| Error grouping in Elasticsearch | Sentry | 4 weeks |
| **Total custom engineering replaced** | | **~30–45 weeks** |

---

## Phased Adoption Order

| Phase | Tools to Add | Time | Immediate Value |
|---|---|---|---|
| **Week 1** | Langfuse deploy + GSSP GS `@observe` | 2 days | LLM traces visible immediately |
| **Week 1** | `prometheus-fastapi-instrumentator` on all 8 services | 1 day | HTTP p95 latency for all services |
| **Week 1** | kminion deploy | 0.5 day | Kafka lag metrics collecting |
| **Week 2** | kube-prometheus-stack Helm install (`grafana.enabled=false`) | 0.5 day | K8s + infra metrics collecting |
| **Week 2** | `structlog` migration (start with 1 service) | 3 days | Consistent log schema |
| **Week 3** | OpenTelemetry + Grafana Tempo (trace backend) | 3 days | Cross-service trace tree in Tempo |
| **Week 3** | Langfuse on GSSP QS + Agent Executor | 3 days | RAG + agent traces |
| **Week 4** | `pip install gliner` + wire `PiiRedactor` into OIS enrichment pipeline | 1 day | PII redaction for all events — no new service |
| **Week 4** | Sentry self-hosted + SDK in all services | 2 days | Error grouping and trends |
| **Week 5** | Custom Dashboard Service — backend FastAPI endpoints | 3 days | Platform Overview + Kafka Health live |
| **Week 5** | Custom Dashboard Service — React + Tremor frontend | 2 days | Cost Governance + RAG Quality pages |
| **Week 6** | Kibana ML anomaly detection jobs | 1 day | Log anomaly detection with no code |
| **Week 7** | Custom Isolation Forest service | 3 days | Metric-level anomaly detection |
| **Week 8** | Vector health CronJob + Pushgateway | 2 days | Embedding freshness on RAG Quality page |
