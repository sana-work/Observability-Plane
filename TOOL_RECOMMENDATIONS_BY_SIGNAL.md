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
| 7 | Kafka consumer lag & health | **kminion → Prometheus → Grafana** | Low | Yes — 2 weeks |
| 8 | PII detection and redaction | **Microsoft Presidio** | Low | Yes — 3–4 weeks |
| 9 | Error aggregation and grouping | **Sentry (self-hosted)** | Low | Yes — 4 weeks |
| 10 | SLO / error budget burn rate | **Pyrra → Prometheus → Grafana** | Low | Yes — 3–4 weeks |
| 11 | Cost and budget governance | **Langfuse + Redis + PostgreSQL** | Low | Yes — 4 weeks |
| 12 | Kubernetes / infra metrics | **kube-state-metrics + Prometheus** | Very Low | Yes — 1 week |
| 13 | Document ingestion pipeline events | **OpenTelemetry custom spans → OIS** | Low | No — new signals |
| 14 | Guardrail decisions | **OIS custom events** | Low | No — new signals |
| 15 | Vector / embedding health | **pgvector health queries → Prometheus** | Medium | No — new signals |
| 16 | Alert routing | **Grafana Alerting + PagerDuty** | Low | Partial |
| 17 | Anomaly detection | **Grafana ML plugin** | Medium | Partial |

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

**That is all.** Every FastAPI service now has `/metrics` with full latency histograms. Prometheus scrapes it. Grafana plots it.

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

### Grafana dashboard query (p95 latency per endpoint)

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

**Tool: kminion → Prometheus → Grafana**

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

### Grafana alert — lag threshold breach

```yaml
# Alert: AGENT_EXECUTION_REQUEST topic consumer lag > 1000
- alert: KafkaConsumerLagHigh
  expr: kminion_consumer_group_topic_partition_lag{topic="AGENT_EXECUTION_REQUEST"} > 1000
  for: 5m
  annotations:
    summary: "Agent Executor consumer is falling behind — {{ $value }} messages behind"
```

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

**Tool: Microsoft Presidio (self-hosted)**

### Why Presidio?

| | Custom regex redactor (current plan) | Presidio |
|---|---|---|
| Entity types covered | 5 patterns (email, phone, card, SSN, passport) | 50+ entity types including names, addresses, medical IDs, bank account numbers |
| ML-based detection | ❌ Regex only | ✅ spaCy NER models — detects context-aware PII |
| Multi-language | ❌ English only | ✅ Multi-language |
| Custom entities | Hard to add | ✅ Easy to add (e.g. COIN token patterns, account IDs) |
| False positive rate | High (regex over-matches) | Low (NER + pattern) |
| Self-hosted | ✅ | ✅ |

### Deploy Presidio (two services)

```yaml
# docker-compose
presidio-analyzer:
  image: mcr.microsoft.com/presidio-analyzer:latest
  ports:
    - "5001:3000"

presidio-anonymizer:
  image: mcr.microsoft.com/presidio-anonymizer:latest
  ports:
    - "5002:3000"
```

### Integration in OIS Enrichment pipeline

```python
# processing/pii_redactor.py — replaces the custom PiiRedactor in Telemetry Processor
import httpx

class PresidioPiiRedactor:
    ANALYZER_URL = "http://presidio-analyzer.internal:5001/analyze"
    ANONYMIZER_URL = "http://presidio-anonymizer.internal:5002/anonymize"

    # Fields that contain free text and need full NER analysis
    TEXT_FIELDS = {"message", "free_text_comment", "error_description", "raw_prompt"}

    # Fields that are always hashed (known user identifiers)
    HASH_FIELDS = {"soe_id", "user_id", "soeid"}

    async def redact(self, event: dict) -> dict:
        import hashlib

        # Hash known user identifier fields
        for field in self.HASH_FIELDS:
            if event.get(field):
                event["user_hash"] = "sha256_" + hashlib.sha256(
                    event[field].encode()
                ).hexdigest()[:16]
                event[field] = None     # remove plain text

        # Run Presidio NER on free-text fields
        for field in self.TEXT_FIELDS:
            if event.get(field):
                event[field] = await self._anonymize(event[field])

        return event

    async def _anonymize(self, text: str) -> str:
        async with httpx.AsyncClient() as client:
            # Step 1: Analyze — find PII entities
            analyze_resp = await client.post(self.ANALYZER_URL, json={
                "text": text,
                "language": "en",
                "entities": ["PERSON", "EMAIL_ADDRESS", "PHONE_NUMBER",
                             "CREDIT_CARD", "US_SSN", "IBAN_CODE", "IP_ADDRESS"],
            })
            entities = analyze_resp.json()

            if not entities:
                return text

            # Step 2: Anonymize — replace with type tags
            anon_resp = await client.post(self.ANONYMIZER_URL, json={
                "text": text,
                "analyzer_results": entities,
                "anonymizers": {
                    "DEFAULT": {"type": "replace", "new_value": "<REDACTED>"},
                    "PERSON":  {"type": "replace", "new_value": "<NAME>"},
                    "EMAIL_ADDRESS": {"type": "replace", "new_value": "<EMAIL>"},
                },
            })
            return anon_resp.json()["text"]
```

### Custom Presidio recogniser for COIN / internal IDs

```python
from presidio_analyzer import PatternRecognizer, Pattern

# Add to Presidio analyzer on startup
coin_token_recognizer = PatternRecognizer(
    supported_entity="COIN_TOKEN",
    patterns=[Pattern("COIN JWT", r"eyJ[A-Za-z0-9+/=]{20,}", 0.9)],
)
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

## 10. SLO / Error Budget Burn Rate

**Problem today:** SLO calculation is planned as a custom `SloEvaluator` class using Redis counters. This works but requires maintaining custom burn-rate logic.

**Tool: Pyrra → Prometheus → Grafana**

### Why Pyrra?

Pyrra is an open-source SLO management tool. You define SLOs as Kubernetes custom resources. Pyrra automatically generates the Prometheus recording rules, alerting rules, and Grafana dashboards — no custom Python code.

| | Custom SloEvaluator (current plan) | Pyrra |
|---|---|---|
| Burn rate formula | Custom Python | ✅ Auto-generated Prometheus rules |
| Multi-window alerts (1h + 6h) | Custom code | ✅ Automatic (Google SRE standard) |
| Error budget remaining | Custom Redis counter | ✅ Prometheus metric |
| Grafana dashboard | Custom build | ✅ Auto-generated |
| SLO definition format | PostgreSQL table | ✅ Kubernetes CRD (YAML, version-controlled) |

### Install Pyrra

```bash
helm repo add pyrra https://pyrra-dev.github.io/pyrra
helm install pyrra pyrra/pyrra -n monitoring
```

### Define SLOs as Kubernetes CRDs (version-controlled in `observability-iac/`)

```yaml
# observability-iac/slos/gssp-qs-availability.yaml
apiVersion: pyrra.dev/v1alpha1
kind: ServiceLevelObjective
metadata:
  name: gssp-qs-availability
  namespace: monitoring
spec:
  target: "99.9"                              # 99.9% availability SLO
  window: 30d                                 # 30-day rolling window
  description: "GSSP Query Service availability"
  indicator:
    ratio:
      errors:
        metric: http_requests_total{job="gssp-qs", status=~"5.."}
      total:
        metric: http_requests_total{job="gssp-qs"}
```

```yaml
# observability-iac/slos/gssp-qs-latency.yaml
apiVersion: pyrra.dev/v1alpha1
kind: ServiceLevelObjective
metadata:
  name: gssp-qs-p95-latency
  namespace: monitoring
spec:
  target: "99"                               # 99% of requests under 2s
  window: 30d
  indicator:
    latency:
      success:
        metric: http_request_duration_seconds_bucket{job="gssp-qs", le="2"}
      total:
        metric: http_request_duration_seconds_count{job="gssp-qs"}
```

**Pyrra auto-generates from these CRDs:**
- Prometheus recording rules for error budget consumption at 1h, 6h, 24h, 3d windows
- Multi-window burn-rate alert rules (Google SRE standard)
- Grafana SLO dashboard with error budget remaining gauge

**This replaces the entire custom `SloEvaluator` class, Redis counters, and `daily_slo_compliance` write logic.** The data flows: FastAPI `/metrics` → Prometheus → Pyrra rules → Grafana SLO dashboard.

---

## 11. Cost and Budget Governance

**Tool: Langfuse (per-call) + Redis (real-time accumulator) + PostgreSQL (budget caps)**
> This combination is already detailed in `Developer_Implementation_Guide.md` Section 3a and the `OBSERVABILITY_INGESTION_SERVICE_PLAN.md`. Summary here:

| Layer | Tool | What it does |
|---|---|---|
| Per-call cost | Langfuse | `estimated_cost_usd` auto-calculated per LLM call from built-in pricing table |
| Real-time daily accumulator | Redis `INCRBYFLOAT` | Running total per `application_id:model:date` key; sub-millisecond |
| Budget caps | PostgreSQL `budget_limits` table | Threshold checked on each accumulator update |
| Alert on breach | OIS → Kafka `ai-obs-events` `BUDGET_THRESHOLD_EXCEEDED` | Routed to Grafana alert + PagerDuty |
| Aggregate dashboard | Grafana | Queries PostgreSQL `agg_hourly_llm_metrics` + `budget_limits` |

---

## 12. Kubernetes / Infrastructure Metrics

**Problem today:** No pod CPU/memory, no DB connection pool health, no K8s event monitoring.

**Tool: kube-state-metrics + Prometheus Node Exporter → Prometheus → Grafana**

### Install (standard K8s observability stack)

```bash
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm install kube-prometheus-stack prometheus-community/kube-prometheus-stack \
  --namespace monitoring \
  --set grafana.enabled=true \
  --set prometheus.enabled=true
```

This single Helm chart deploys:
- **kube-state-metrics** — K8s object metrics (pod restarts, deployment replicas, job status)
- **node-exporter** — Node CPU, memory, disk, network
- **Prometheus** — Scrapes everything
- **Grafana** — Pre-built K8s dashboards (Kubernetes / Compute Resources / Workload)

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

## 13. Document Ingestion Pipeline Events

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
1. A Grafana Tempo trace showing the full ingestion pipeline with timing per stage
2. OIS events (`DOCUMENT_PARSE_STARTED/COMPLETED/FAILED`) written to `obs_events` for aggregate dashboards
3. Prometheus metrics (via `prometheus_client`) for queue depth and job duration

---

## 14. Guardrail Decisions

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

**Grafana alert on guardrail spike:**
```promql
# Alert when guardrail block rate exceeds 5x baseline
increase(obs_events_total{event_type="GUARDRAIL_BLOCKED"}[1h]) /
increase(obs_events_total{event_type="GUARDRAIL_EVALUATED"}[1h]) > 0.15
```

---

## 15. Vector / Embedding Health

**Problem today:** Embedding drift, index freshness, retrieval recall@k — ❌ not monitored anywhere. Silent RAG quality degradation is undetectable.

**Tool: Custom pgvector health queries → Prometheus Pushgateway → Grafana**

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

**Grafana alert on stale index:**
```promql
pgvector_index_freshness_hours{rag_id=~".*"} > 24
```

**Embedding drift proxy:** If `avg_embedding_norm` shifts significantly over time (rolling std > 2×), it indicates the embedding model may have changed or the document distribution has shifted — a signal to re-embed the knowledge base.

---

## 16. Alert Routing

**Tool: Grafana Alerting + Alertmanager + PagerDuty**

### Why this combination?

- **Grafana Alerting** evaluates rules on any data source (Prometheus, PostgreSQL, Elasticsearch, Loki)
- **Alertmanager** handles grouping, silencing, routing, and deduplication
- **PagerDuty** provides on-call scheduling, escalation, and mobile push

### Alertmanager routing config

```yaml
# alertmanager.yml
global:
  pagerduty_url: "https://events.pagerduty.com/v2/enqueue"

route:
  receiver: "default"
  group_by: ["alertname", "service_name", "application_id"]
  group_wait: 30s
  group_interval: 5m
  repeat_interval: 4h

  routes:
    # Critical LLM / agent failures → immediate page
    - match:
        severity: critical
      receiver: pagerduty-critical
      continue: false

    # SLO burn rate alerts → high-priority page
    - match_re:
        alertname: "SLO.*BurnRate.*"
      receiver: pagerduty-slo
      continue: false

    # Budget threshold → Slack notification
    - match:
        alertname: BudgetThresholdExceeded
      receiver: slack-finance
      continue: false

receivers:
  - name: pagerduty-critical
    pagerduty_configs:
      - routing_key: "${PAGERDUTY_INTEGRATION_KEY}"
        severity: critical
        description: "{{ .GroupLabels.alertname }}: {{ .Annotations.summary }}"

  - name: slack-finance
    slack_configs:
      - api_url: "${SLACK_WEBHOOK_URL}"
        channel: "#platform-costs"
        text: "💰 Budget alert: {{ .Annotations.summary }}"
```

### Key alert rules (Prometheus)

```yaml
# observability-iac/grafana/alert-rules/platform.yml
groups:
  - name: platform-availability
    rules:
      - alert: ServiceErrorRateHigh
        expr: |
          rate(http_requests_total{status=~"5.."}[5m]) /
          rate(http_requests_total[5m]) > 0.05
        for: 2m
        labels: { severity: critical }
        annotations:
          summary: "{{ $labels.job }} error rate > 5% for 2 minutes"

      - alert: LLMCostBudgetWarning
        expr: |
          (SELECT budget_utilization_pct FROM obs_events
           WHERE event_type = 'BUDGET_THRESHOLD_EXCEEDED'
           ORDER BY timestamp DESC LIMIT 1) > 80
        for: 0m
        labels: { severity: warning }

      - alert: KafkaLagCritical
        expr: kminion_consumer_group_topic_partition_lag > 5000
        for: 3m
        labels: { severity: critical }
        annotations:
          summary: "Kafka consumer {{ $labels.consumer_group }} is {{ $value }} messages behind on {{ $labels.topic }}"

      - alert: RAGNoResultRateHigh
        expr: |
          increase(obs_events_total{event_type="RAG_NO_RESULT"}[1h]) /
          increase(obs_events_total{event_type="RAG_RETRIEVAL_COMPLETED"}[1h]) > 0.2
        for: 10m
        labels: { severity: warning }
        annotations:
          summary: "RAG returning no results on >20% of queries in the last hour"
```

---

## 17. Anomaly Detection

**Tool: Grafana Machine Learning plugin (Phase 1) → Custom Isolation Forest (Phase 2)**

### Phase 1 — Grafana ML Plugin (zero code, ships in Grafana Enterprise / OSS with plugin)

Grafana's ML plugin runs forecasting and anomaly detection directly on any Prometheus or Elasticsearch metric — no separate service needed.

```
Grafana ML Plugin setup:
1. Select metric: http_request_duration_seconds{job="gssp-qs", quantile="0.95"}
2. Select algorithm: "Outlier Detection (DBSCAN)"
3. Set sensitivity: 0.7
4. Enable: "Alert when anomaly detected"
→ Grafana automatically alerts when p95 latency deviates from its learned baseline
```

This requires zero infrastructure and gives you anomaly detection on any Prometheus metric in minutes.

### Phase 2 — Custom Isolation Forest (for LLM/agent-specific signals)

When Grafana ML is insufficient (e.g., detecting correlated anomalies across `latency_ms + error_rate + token_cost` for the same `application_id`), use the custom Anomaly Detection Service already designed in `Developer_Implementation_Guide.md` Section 7.

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
│  Distributed traces      ──► OpenTelemetry SDK → Grafana Tempo          │
│  HTTP latency metrics    ──► prometheus-fastapi-instrumentator          │
│                               → Prometheus → Grafana                   │
│                                                                         │
│  Kafka consumer lag      ──► kminion → Prometheus → Grafana             │
│  K8s / infra metrics     ──► kube-prometheus-stack (1 Helm install)    │
│  Vector / embed health   ──► Custom CronJob → Pushgateway → Grafana    │
│                                                                         │
│  PII redaction           ──► Microsoft Presidio (self-hosted)          │
│  Error aggregation       ──► Sentry (self-hosted)                      │
│  SLO / error budgets     ──► Pyrra → Prometheus → Grafana              │
│  Cost / budget caps      ──► Langfuse + Redis + PostgreSQL             │
│                                                                         │
│  Guardrail events        ──► OIS custom events                         │
│  Document ingestion      ──► OpenTelemetry custom spans → OIS          │
│  Alert routing           ──► Grafana Alerting → Alertmanager           │
│                               → PagerDuty                              │
│  Anomaly detection       ──► Grafana ML plugin (Phase 1)               │
│                               Custom Isolation Forest (Phase 2)        │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## What Each Tool Replaces (Custom Build Avoided)

| Custom Component in Docs | Replaced By | Weeks Saved |
|---|---|---|
| Custom `JSONFormatter` + `AppInfoFilter` | `structlog` shared config | 2 weeks |
| Custom `PiiRedactor` (regex) | Microsoft Presidio | 3–4 weeks |
| Custom `FaithfulnessScorer` | Langfuse LLM-as-judge | 3–4 weeks |
| Custom `SloEvaluator` + Redis burn rate | Pyrra → Prometheus | 3–4 weeks |
| Custom Anomaly Detection (Phase 1) | Grafana ML plugin | 6–8 weeks |
| Custom distributed trace correlation | OpenTelemetry → Tempo | 4–6 weeks |
| Custom `/metrics` endpoint per service | prometheus-fastapi-instrumentator | 2–3 weeks per service |
| Custom Kafka lag monitoring | kminion | 2 weeks |
| Custom K8s metrics | kube-prometheus-stack | 1 week |
| Error grouping in Elasticsearch | Sentry | 4 weeks |
| **Total custom engineering replaced** | | **~35–50 weeks** |

---

## Phased Adoption Order

| Phase | Tools to Add | Time | Immediate Value |
|---|---|---|---|
| **Week 1** | Langfuse deploy + GSSP GS `@observe` | 2 days | LLM traces visible immediately |
| **Week 1** | `prometheus-fastapi-instrumentator` on all 8 services | 1 day | HTTP p95 latency for all services |
| **Week 1** | kminion deploy | 0.5 day | Kafka lag dashboard live |
| **Week 2** | kube-prometheus-stack Helm install | 0.5 day | K8s + infra dashboards live |
| **Week 2** | `structlog` migration (start with 1 service) | 3 days | Consistent log schema |
| **Week 3** | OpenTelemetry + Grafana Tempo | 3 days | Cross-service trace tree |
| **Week 3** | Langfuse on GSSP QS + Agent Executor | 3 days | RAG + agent traces |
| **Week 4** | Presidio deploy + wire into OIS redactor | 2 days | PII redaction for all events |
| **Week 4** | Sentry self-hosted + SDK in all services | 2 days | Error grouping and trends |
| **Week 5** | Pyrra SLO definitions | 1 day | SLO dashboards + burn alerts |
| **Week 6** | Grafana ML anomaly detection | 1 day | Anomaly alerts with no code |
| **Week 8** | Vector health CronJob + Pushgateway | 2 days | Embedding freshness monitoring |
