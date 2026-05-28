# Kafka Direct Path — Observability Architecture

## Overview

This document describes the alternative observability ingestion architecture where all 8 backend services produce events **directly to Kafka topics** instead of calling the OIS HTTP endpoint. The Observability Ingestion Service (OIS) is replaced by a Kafka-native pipeline consisting of a shared producer utility, an Enrichment Consumer, and a Storage Consumer.

---

## Context: Why Consider Kafka Direct?

The OIS + Kafka hybrid architecture (documented in `INGESTION_ARCHITECTURE_DECISION.md`) routes events through an HTTP endpoint first, then to Kafka. This means:

- If the OIS pod is down, events are lost at the HTTP layer before reaching Kafka
- Every event adds an async HTTP call to the emitting service
- OIS is a potential single point of failure despite Kafka's durability downstream

The Kafka direct path eliminates the HTTP gap entirely:

```
OIS path:   Service → HTTP → OIS → Kafka → Consumer → Storage
                               ↑
                          SPOF here

Kafka path: Service → Kafka → Enrichment Consumer → Storage
                         ↑
                    No SPOF — events durable from produce time
```

---

## Architecture Pipeline

```
┌─────────────────────────────────────────────────────────────┐
│                    8 Backend Services                       │
│                                                             │
│  Agentic Orchestration │ Agent Executor │ GSSP GS          │
│  GSSP QS │ GSSP RS │ Data Ingestion                        │
│  Consumer Service │ User Feedback                          │
│                                                             │
│  shared emit_event() → confluent-kafka producer            │
│  (same function interface — only transport changes)        │
└─────────────────────┬───────────────────────────────────────┘
                      │ produce (async, fire-and-forget)
                      ▼
┌─────────────────────────────────────────────────────────────┐
│  Kafka Topic: ai-obs-events-raw                             │
│  Retention: 7 days │ Partitions: 12 │ Replication: 3       │
│  Durable from the moment of produce                        │
└─────────────────────┬───────────────────────────────────────┘
                      │ consume
                      ▼
┌─────────────────────────────────────────────────────────────┐
│  Enrichment Consumer Service                                │
│                                                             │
│  Step 1 — Schema validation (Pydantic)                     │
│  Step 2 — GLiNER PII redaction (in-process)                │
│  Step 3 — Metadata enrichment (app/agent/tool registry)    │
│  Step 4 — Error code mapping (error_code_catalog)          │
│  Step 5 — Token cost calculation                           │
│  Step 6 — Aggregate rollup generation                      │
│                                                             │
│  Valid events   → ai-obs-events-processed                  │
│  Invalid events → ai-obs-dead-letter                       │
└──────────┬──────────────────────────┬───────────────────────┘
           │                          │
           ▼                          ▼
┌─────────────────────┐   ┌──────────────────────────────────┐
│  ai-obs-events-     │   │  ai-obs-dead-letter              │
│  processed          │   │  Retention: 14 days              │
│  Retention: 3 days  │   │  Replay after producer fix       │
└──────────┬──────────┘   └──────────────────────────────────┘
           │ consume
           ▼
┌─────────────────────────────────────────────────────────────┐
│  Storage Consumer Service                                   │
│                                                             │
│  → PostgreSQL   obs_events, agg_* tables                   │
│  → Elasticsearch  ai-observability-* indices               │
│  → Amazon S3    redacted payloads, trace archives          │
└─────────────────────────────────────────────────────────────┘
```

---

## Kafka Topics

| Topic | Purpose | Retention | Partitions | Replication |
|---|---|---|---|---|
| `ai-obs-events-raw` | Raw events from all 8 services — unvalidated, unredacted | 7 days | 12 | 3 |
| `ai-obs-events-processed` | Enriched, redacted, validated events ready for storage | 3 days | 12 | 3 |
| `ai-obs-dead-letter` | Failed validation — stored for debugging and replay after fix | 14 days | 3 | 3 |
| `ai-obs-anomalies` | Anomaly detection results | 7 days | 6 | 3 |

### Create topics

```bash
# ai-obs-events-raw
kafka-topics.sh --bootstrap-server kafka:9092 \
  --create \
  --topic ai-obs-events-raw \
  --partitions 12 \
  --replication-factor 3 \
  --config retention.ms=604800000 \
  --config compression.type=lz4

# ai-obs-events-processed
kafka-topics.sh --bootstrap-server kafka:9092 \
  --create \
  --topic ai-obs-events-processed \
  --partitions 12 \
  --replication-factor 3 \
  --config retention.ms=259200000

# ai-obs-dead-letter
kafka-topics.sh --bootstrap-server kafka:9092 \
  --create \
  --topic ai-obs-dead-letter \
  --partitions 3 \
  --replication-factor 3 \
  --config retention.ms=1209600000
```

---

## Component 1 — Shared Kafka Producer (in every service)

### Installation

```bash
pip install confluent-kafka
```

Add to each service's `requirements.txt`:

```
confluent-kafka==2.4.0
```

### Shared emitter utility

This replaces the OIS HTTP emitter. The interface is identical — only the transport changes. All 8 services call `emit_event()` exactly as before.

```python
# shared/obs_emitter.py
import json
import hashlib
import structlog
from datetime import datetime, timezone
from functools import lru_cache
from confluent_kafka import Producer

log = structlog.get_logger()


@lru_cache(maxsize=1)
def _get_producer() -> Producer:
    return Producer({
        "bootstrap.servers": settings.KAFKA_BROKERS,
        "acks": "1",                # leader ack — balance of speed and durability
        "retries": 3,
        "retry.backoff.ms": 100,
        "compression.type": "lz4", # ~60% size reduction
        "linger.ms": 5,            # micro-batch for throughput
        "batch.size": 65536,
        "delivery.timeout.ms": 5000,
    })


async def emit_event(
    telemetry_type: str,
    event_type: str,
    status: str,
    service_name: str,
    environment: str,
    correlation_id: str | None = None,
    application_id: str | None = None,
    agent_id: str | None = None,
    tool_id: str | None = None,
    payload: dict | None = None,
    **kwargs,
) -> None:
    """
    Emit an observability event directly to Kafka.
    Fire-and-forget — never blocks the calling service.
    Falls back to structured log if Kafka is unreachable.
    """
    event = {
        "telemetry_type":  telemetry_type,
        "event_type":      event_type,
        "status":          status,
        "service_name":    service_name,
        "environment":     environment,
        "correlation_id":  correlation_id,
        "application_id":  application_id,
        "agent_id":        agent_id,
        "tool_id":         tool_id,
        "payload":         payload or {},
        "emitted_at":      datetime.now(timezone.utc).isoformat(),
        **kwargs,
    }

    try:
        producer = _get_producer()
        producer.produce(
            topic="ai-obs-events-raw",
            key=(correlation_id or "no-correlation").encode(),
            value=json.dumps(event).encode(),
            on_delivery=_on_delivery,
        )
        producer.poll(0)    # non-blocking — triggers delivery callbacks

    except Exception as exc:
        # Kafka unreachable — write to structured log as fallback
        # Fluent Bit picks this up and routes to Elasticsearch
        log.warning(
            "obs_emit_kafka_failed",
            error=str(exc),
            event_type=event_type,
            correlation_id=correlation_id,
            obs_fallback_event=json.dumps(event),   # recoverable from ES logs
        )


def _on_delivery(err, msg):
    if err:
        log.error(
            "kafka_delivery_failed",
            topic=msg.topic(),
            partition=msg.partition(),
            error=str(err),
        )
```

### Usage in a service — unchanged from OIS path

```python
# gssp-gs/rag/retriever.py
from shared.obs_emitter import emit_event

async def retrieve(query: str, ctx: ObsContext) -> list[Chunk]:
    chunks = await self.vector_search(query)

    await emit_event(
        telemetry_type="event",
        event_type="RAG_RETRIEVAL_COMPLETED",
        status="success",
        service_name="gssp-gs",
        environment=ctx.environment,
        correlation_id=ctx.correlation_id,
        application_id=ctx.application_id,
        payload={
            "rag_id": self.rag_id,
            "chunk_count": len(chunks),
            "avg_relevance_score": avg_score,
            "latency_ms": latency,
        },
    )
    return chunks
```

---

## Component 2 — Enrichment Consumer Service

This is a new FastAPI/Python service that replaces OIS as the enrichment layer. It consumes from `ai-obs-events-raw`, applies the full enrichment pipeline, and produces to `ai-obs-events-processed`.

### Installation

```bash
pip install confluent-kafka gliner pydantic structlog asyncpg
```

### Schema validation (Pydantic)

```python
# enrichment_consumer/validator.py
from pydantic import BaseModel, ValidationError
from typing import Any

class ObsEvent(BaseModel):
    telemetry_type:  str
    event_type:      str
    status:          str
    service_name:    str
    environment:     str
    correlation_id:  str | None = None
    application_id:  str | None = None
    agent_id:        str | None = None
    tool_id:         str | None = None
    payload:         dict[str, Any] = {}
    emitted_at:      str


def validate(raw: dict) -> tuple[ObsEvent | None, str | None]:
    try:
        return ObsEvent(**raw), None
    except ValidationError as e:
        return None, str(e)
```

### PII Redaction (GLiNER — same code as OIS path)

```python
# enrichment_consumer/pii_redactor.py
import re
import hashlib
from gliner import GLiNER


class PiiRedactor:
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

    REGEX_PATTERNS = {
        "SSN":         re.compile(r"\b\d{3}-\d{2}-\d{4}\b"),
        "CREDIT_CARD": re.compile(r"\b(?:4[0-9]{12}(?:[0-9]{3})?|5[1-5][0-9]{14}|3[47][0-9]{13})\b"),
        "IP_ADDRESS":  re.compile(r"\b(?:\d{1,3}\.){3}\d{1,3}\b"),
        "COIN_TOKEN":  re.compile(r"eyJ[A-Za-z0-9+/=]{20,}"),
    }

    TEXT_FIELDS  = {"message", "free_text_comment", "error_description", "raw_prompt", "payload"}
    HASH_FIELDS  = {"soe_id", "user_id", "soeid"}

    def __init__(self):
        # Model loads once at consumer startup — shared across all messages
        self.model = GLiNER.from_pretrained("urchade/gliner_medium-v2.1")

    def redact_event(self, event: dict) -> dict:
        for field in self.HASH_FIELDS:
            if event.get(field):
                event["user_hash"] = "sha256_" + hashlib.sha256(
                    str(event[field]).encode()
                ).hexdigest()[:16]
                event[field] = None

        for field in self.TEXT_FIELDS:
            if event.get(field) and isinstance(event[field], str):
                event[field] = self._redact_text(event[field])

        return event

    def _redact_text(self, text: str) -> str:
        if not text or len(text) > 10_000:
            return text

        entities = self.model.predict_entities(text, self.PII_LABELS, threshold=0.5)
        for ent in sorted(entities, key=lambda x: x["start"], reverse=True):
            tag = ent["label"].upper().replace(" ", "_")
            text = text[:ent["start"]] + f"[{tag}]" + text[ent["end"]:]

        for label, pattern in self.REGEX_PATTERNS.items():
            text = pattern.sub(f"[{label}]", text)

        return text
```

### Enricher (metadata + cost calculation)

```python
# enrichment_consumer/enricher.py
import asyncpg
from datetime import datetime, timezone

MODEL_COST_PER_1K_TOKENS = {
    "gpt-4":        {"input": 0.03,  "output": 0.06},
    "gpt-4-turbo":  {"input": 0.01,  "output": 0.03},
    "gpt-3.5-turbo":{"input": 0.0005,"output": 0.0015},
}

class Enricher:
    def __init__(self, pg_pool: asyncpg.Pool):
        self.pg_pool = pg_pool

    async def enrich(self, event: dict) -> dict:
        # Add enrichment timestamp
        event["enriched_at"] = datetime.now(timezone.utc).isoformat()

        # Enrich from application registry
        if event.get("application_id"):
            app = await self._get_application(event["application_id"])
            if app:
                event["lob"]       = app["lob"]
                event["soe_id"]    = app["soe_id"]
                event["app_owner"] = app["owner_team"]

        # Token cost calculation for LLM events
        if event.get("event_type") == "LLM_CALL_COMPLETED":
            payload = event.get("payload", {})
            model   = payload.get("model_name", "")
            costs   = MODEL_COST_PER_1K_TOKENS.get(model, {})
            if costs:
                input_tokens  = payload.get("input_tokens", 0)
                output_tokens = payload.get("output_tokens", 0)
                event["payload"]["estimated_cost"] = round(
                    (input_tokens / 1000 * costs["input"]) +
                    (output_tokens / 1000 * costs["output"]), 6
                )

        return event

    async def _get_application(self, application_id: str) -> dict | None:
        async with self.pg_pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT lob, soe_id, owner_team FROM application_registry "
                "WHERE application_id = $1", application_id
            )
            return dict(row) if row else None
```

### Main consumer loop

```python
# enrichment_consumer/main.py
import json
import asyncio
import asyncpg
import structlog
from confluent_kafka import Consumer, Producer

from validator import validate
from pii_redactor import PiiRedactor
from enricher import Enricher

log = structlog.get_logger()

RAW_TOPIC       = "ai-obs-events-raw"
PROCESSED_TOPIC = "ai-obs-events-processed"
DLQ_TOPIC       = "ai-obs-dead-letter"


async def run():
    pg_pool = await asyncpg.create_pool(settings.PG_DSN)

    pii_redactor = PiiRedactor()        # GLiNER model loads here — once
    enricher     = Enricher(pg_pool)

    consumer = Consumer({
        "bootstrap.servers":  settings.KAFKA_BROKERS,
        "group.id":           "obs-enrichment-consumer",
        "auto.offset.reset":  "earliest",
        "enable.auto.commit": False,    # manual commit — only after success
        "max.poll.interval.ms": 300000,
    })

    processed_producer = Producer({"bootstrap.servers": settings.KAFKA_BROKERS})
    dlq_producer       = Producer({"bootstrap.servers": settings.KAFKA_BROKERS})

    consumer.subscribe([RAW_TOPIC])
    log.info("enrichment_consumer_started", topic=RAW_TOPIC)

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                log.error("consumer_error", error=str(msg.error()))
                continue

            raw = json.loads(msg.value().decode())

            # Step 1 — Validate schema
            event, validation_error = validate(raw)
            if not event:
                dlq_producer.produce(
                    DLQ_TOPIC,
                    value=json.dumps({
                        "raw_event":        raw,
                        "validation_error": validation_error,
                        "source_topic":     RAW_TOPIC,
                        "source_partition": msg.partition(),
                        "source_offset":    msg.offset(),
                        "failed_at":        datetime.now(timezone.utc).isoformat(),
                    }).encode(),
                )
                dlq_producer.poll(0)
                consumer.commit(msg)
                log.warning("event_validation_failed", error=validation_error)
                continue

            event = event.dict()

            # Step 2 — PII redaction (GLiNER)
            event = pii_redactor.redact_event(event)

            # Step 3 — Metadata enrichment + cost calculation
            event = await enricher.enrich(event)

            # Produce to processed topic
            processed_producer.produce(
                PROCESSED_TOPIC,
                key=event.get("correlation_id", "").encode(),
                value=json.dumps(event).encode(),
            )
            processed_producer.poll(0)

            # Commit only after successful processing
            consumer.commit(msg)

    finally:
        consumer.close()
        await pg_pool.close()


if __name__ == "__main__":
    asyncio.run(run())
```

---

## Component 3 — Storage Consumer Service

Reads from `ai-obs-events-processed` and writes to PostgreSQL, Elasticsearch, and S3.

```python
# storage_consumer/main.py
import json
import asyncio
import asyncpg
from confluent_kafka import Consumer
from elasticsearch import AsyncElasticsearch

PROCESSED_TOPIC = "ai-obs-events-processed"

async def run():
    pg_pool = await asyncpg.create_pool(settings.PG_DSN)
    es      = AsyncElasticsearch([settings.ES_HOST])

    consumer = Consumer({
        "bootstrap.servers":  settings.KAFKA_BROKERS,
        "group.id":           "obs-storage-consumer",
        "auto.offset.reset":  "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([PROCESSED_TOPIC])

    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            continue

        event = json.loads(msg.value().decode())

        # Route to correct Elasticsearch index by event type
        index = _resolve_index(event["event_type"])
        await es.index(index=index, document=event)

        # Write to PostgreSQL obs_events table
        async with pg_pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO obs_events
                    (correlation_id, event_type, service_name, status,
                     application_id, environment, payload, emitted_at)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
                ON CONFLICT DO NOTHING
            """,
                event.get("correlation_id"),
                event["event_type"],
                event["service_name"],
                event["status"],
                event.get("application_id"),
                event["environment"],
                json.dumps(event.get("payload", {})),
                event["emitted_at"],
            )

        consumer.commit(msg)


def _resolve_index(event_type: str) -> str:
    mapping = {
        "LLM_CALL":      "ai-observability-llm-calls",
        "RAG_RETRIEVAL": "ai-observability-rag-events",
        "TOOL_CALL":     "ai-observability-tool-calls",
        "AGENT":         "ai-observability-agent-steps",
        "GUARDRAIL":     "ai-observability-guardrail-events",
        "FEEDBACK":      "ai-observability-feedback",
        "ERROR":         "ai-observability-errors",
    }
    for prefix, index in mapping.items():
        if event_type.startswith(prefix):
            return f"{index}-{datetime.today().strftime('%Y.%m.%d')}"
    return f"ai-observability-requests-{datetime.today().strftime('%Y.%m.%d')}"


if __name__ == "__main__":
    asyncio.run(run())
```

---

## Dead Letter — Replay after fix

One of the key benefits of Kafka over OIS. After fixing a producer bug, replay all failed events:

```python
# scripts/replay_dead_letter.py
from confluent_kafka import Consumer, Producer
import json

consumer = Consumer({
    "bootstrap.servers": settings.KAFKA_BROKERS,
    "group.id":          "dlq-replay-group",
    "auto.offset.reset": "earliest",
})
consumer.subscribe(["ai-obs-dead-letter"])

producer = Producer({"bootstrap.servers": settings.KAFKA_BROKERS})

replayed = 0
while True:
    msg = consumer.poll(1.0)
    if msg is None:
        break

    dlq_entry = json.loads(msg.value())
    raw_event = dlq_entry["raw_event"]

    # Re-produce to raw topic — will go through enrichment pipeline again
    producer.produce("ai-obs-events-raw", value=json.dumps(raw_event).encode())
    producer.poll(0)
    replayed += 1

producer.flush()
print(f"Replayed {replayed} events from dead letter topic")
```

---

## Kubernetes Deployments

### Enrichment Consumer

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: obs-enrichment-consumer
  namespace: ai-platform
spec:
  replicas: 3    # multiple replicas = parallel partition consumption
  selector:
    matchLabels:
      app: obs-enrichment-consumer
  template:
    metadata:
      labels:
        app: obs-enrichment-consumer
    spec:
      containers:
        - name: enrichment-consumer
          image: obs-enrichment-consumer:latest
          env:
            - name: KAFKA_BROKERS
              value: "kafka.kafka.svc.cluster.local:9092"
            - name: PG_DSN
              valueFrom:
                secretKeyRef:
                  name: pg-credentials
                  key: dsn
          resources:
            requests:
              cpu: 500m
              memory: 1Gi     # GLiNER model needs ~512Mi
            limits:
              cpu: 2000m
              memory: 2Gi
          readinessProbe:
            exec:
              command: ["python", "-c", "import confluent_kafka; print('ok')"]
            initialDelaySeconds: 30   # wait for GLiNER model to load
```

### Storage Consumer

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: obs-storage-consumer
  namespace: ai-platform
spec:
  replicas: 3
  selector:
    matchLabels:
      app: obs-storage-consumer
  template:
    spec:
      containers:
        - name: storage-consumer
          image: obs-storage-consumer:latest
          env:
            - name: KAFKA_BROKERS
              value: "kafka.kafka.svc.cluster.local:9092"
            - name: PG_DSN
              valueFrom:
                secretKeyRef:
                  name: pg-credentials
                  key: dsn
            - name: ES_HOST
              value: "http://elasticsearch.logging.svc.cluster.local:9200"
          resources:
            requests:
              cpu: 200m
              memory: 256Mi
            limits:
              cpu: 1000m
              memory: 512Mi
```

---

## Failure Scenarios

| Failure | What happens | Recovery |
|---|---|---|
| Enrichment Consumer crashes | Events stay in `ai-obs-events-raw` — Kafka retains them | Consumer restarts, resumes from last committed offset — zero data loss |
| Storage Consumer crashes | Events stay in `ai-obs-events-processed` — Kafka retains them | Consumer restarts, replays — zero data loss |
| PostgreSQL slow / down | Storage Consumer retries — events safe in Kafka | PG recovers, consumer drains backlog |
| Elasticsearch down | Storage Consumer retries — events safe in Kafka | ES recovers, consumer drains backlog |
| Schema bug in consumer | Bad events go to DLQ | Fix consumer, replay DLQ to raw topic — full recovery |
| Burst traffic | Kafka absorbs burst — consumers drain at their own pace | No events lost — visible as consumer lag in kminion |
| Kafka broker loses a node | Replicated partitions take over automatically | No data loss if replication-factor ≥ 3 |
| Service emitter fails to produce | Fallback logger writes to stdout | Fluent Bit ships to Elasticsearch — recoverable |

---

## Tool Stack Changes vs OIS Path

### Tools that change

| Tool | OIS Path | Kafka Direct Path |
|---|---|---|
| **OIS HTTP Service** | FastAPI service, `POST /v1/ingest` | ❌ Removed |
| **Emitter transport** | `httpx.AsyncClient.post()` | `confluent_kafka.Producer.produce()` |
| **Schema validation** | FastAPI Pydantic at HTTP layer | Pydantic inside Enrichment Consumer |
| **GLiNER PII redaction** | Runs inside OIS process | Runs inside Enrichment Consumer |
| **Dead letter** | PostgreSQL `dead_letter` table | `ai-obs-dead-letter` Kafka topic |
| **New: confluent-kafka** | Not needed in services | `pip install confluent-kafka` in all 8 services |
| **New: Enrichment Consumer** | Part of OIS | Standalone consumer service |
| **New: Storage Consumer** | Part of OIS | Standalone consumer service |

### Tools that do not change

| Tool | Status | Notes |
|---|---|---|
| **Langfuse** | ✅ Unchanged | Independent LLM/RAG/agent tracing layer |
| **structlog** | ✅ Unchanged | Application logs → Fluent Bit → Elasticsearch |
| **Fluent Bit** | ✅ Unchanged | Log collection DaemonSet — no change |
| **OpenTelemetry + Grafana Tempo** | ✅ Unchanged | Distributed tracing layer |
| **prometheus-fastapi-instrumentator** | ✅ Unchanged | HTTP metrics per service |
| **kminion** | ✅ Unchanged | Now also monitors enrichment + storage consumer lag |
| **kube-prometheus-stack** | ✅ Unchanged | K8s infra metrics |
| **Custom Dashboard Service** | ✅ Unchanged | Reads PostgreSQL/Elasticsearch downstream |
| **Sentry / GlitchTip / ES fingerprinting** | ✅ Unchanged | Error grouping layer |

---

## Full Updated Tool Stack Summary

```
┌──────────────────────────────────────────────────────────────────────┐
│  SIGNAL → TOOL MAPPING (Kafka Direct Path)                           │
│                                                                      │
│  LLM/RAG/Agent traces  ──► Langfuse (self-hosted)                   │
│  Prompt management     ──► Langfuse Prompt Management               │
│  Evaluations           ──► Langfuse LLM-as-judge                    │
│                                                                      │
│  Obs events (all 8 svcs) ──► confluent-kafka producer               │
│                              → ai-obs-events-raw (7 days)           │
│                              → Enrichment Consumer                  │
│                                 GLiNER PII redaction                │
│                                 Pydantic schema validation          │
│                                 metadata enrichment + cost calc     │
│                              → ai-obs-events-processed (3 days)     │
│                              → Storage Consumer                     │
│                                 PostgreSQL + Elasticsearch + S3     │
│                              → ai-obs-dead-letter (invalid events)  │
│                                                                      │
│  Structured app logs   ──► structlog → Fluent Bit → Elasticsearch   │
│  Distributed traces    ──► OpenTelemetry SDK → Grafana Tempo         │
│  HTTP metrics          ──► prometheus-fastapi-instrumentator         │
│  Kafka health          ──► kminion → Prometheus                     │
│  K8s infra metrics     ──► kube-prometheus-stack (grafana.enabled=false) │
│                                                                      │
│  Error aggregation     ──► Elasticsearch fingerprinting             │
│  PII redaction         ──► GLiNER (in Enrichment Consumer)          │
│  Platform dashboards   ──► Custom Dashboard Service                 │
│                              FastAPI + React + Tremor + COIN JWT    │
└──────────────────────────────────────────────────────────────────────┘
```

---

## OIS Path vs Kafka Direct Path — Decision Matrix

| Criterion | OIS Path | Kafka Direct Path |
|---|---|---|
| Developer simplicity | High — one HTTP call | Medium — Kafka producer setup per service |
| Integration effort for 6 non-Kafka services | Hours | Days |
| Data durability | Low (HTTP gap before Kafka) | High (durable from produce time) |
| Single point of failure | OIS pod | None |
| Replay on schema bug | ❌ Not possible | ✅ Full replay from any offset |
| Consumer lag visibility | Via OIS metrics | Via kminion per consumer group |
| Schema enforcement | At HTTP layer (immediate rejection) | In Enrichment Consumer (post-ingest) |
| Dead letter handling | PostgreSQL table | Kafka topic (replayable) |
| Burst traffic handling | OIS pod saturated | Kafka absorbs — consumers drain at pace |
| New infrastructure | OIS deployment | Enrichment Consumer + Storage Consumer |
| Recommended phase | Phase 1 (simpler start) | Phase 2 (production hardening) |

---

## Phased Adoption

| Phase | Action | Outcome |
|---|---|---|
| **Week 1** | Deploy Kafka topics (`ai-obs-events-raw`, `processed`, `dead-letter`) | Topics ready |
| **Week 1** | Deploy Enrichment Consumer with Pydantic validation only (GLiNER disabled) | Validation pipeline live |
| **Week 2** | Enable GLiNER in Enrichment Consumer | PII redaction live |
| **Week 2** | Deploy Storage Consumer | Events flowing to PostgreSQL + Elasticsearch |
| **Week 3** | Migrate shared emitter utility from HTTP to Kafka producer | All 8 services producing to Kafka |
| **Week 3** | Enable kminion consumer lag monitoring for enrichment + storage groups | Lag visibility live |
| **Week 4** | Wire dead letter replay script | Full recovery capability |
| **Week 5** | Decommission OIS HTTP service | Single clean Kafka-native pipeline |
