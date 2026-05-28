# Observability Ingestion — Architecture Decision

## Platform Context

- 8 services: Agentic Orchestration, Agent Executor, GSSP GS, GSSP QS, GSSP RS, Data Ingestion, Consumer Service, User Feedback
- Only 2 of 8 services (Orchestration, Agent Executor) already use Kafka
- All 8 use FastAPI + Python + PostgreSQL
- Platform already has Kafka infrastructure running for agent execution
- Current OIS emitter uses `except: pass` (fire-and-forget) — OIS downtime = silent data loss

---

## The Three Options

### Option 1 — Observability Ingestion Service (OIS)

```
Service → POST /v1/ingest → OIS → Validate/Enrich → PostgreSQL
```

#### Pros

| Strength | Why it matters |
|---|---|
| Single unified interface | All 8 services call one endpoint; schema enforced centrally |
| Zero Kafka dependency for producers | 6 services with no Kafka today can integrate in hours, not weeks |
| Schema validation + enrichment in one place | PII hashing, cost calc, error code mapping not duplicated across 8 codebases |
| Dead-letter table | Invalid events stored, not silently dropped |
| Additive | Does not change existing flows |
| Consistent correlation / tracing | `correlation_id`, `span_id` enrichment happens once |

#### Cons

| Weakness | Real impact |
|---|---|
| **Single point of failure** | If OIS is down, all 8 services lose telemetry silently |
| **No buffering** | If OIS is slow, events back up in the calling service |
| **No replay** | Once dropped (OIS down or emitter exception), the event is gone forever |
| HTTP overhead | Every request adds an async HTTP call |
| Vertical scaling ceiling | A single OIS pod becomes bottleneck at high throughput |

#### Backup Scenario if OIS is Down

The current plan (`except: pass`) has **no backup**. Options to fix:
- Local structured log fallback: emitter catches exception → writes to local JSON log → sidecar drains to OIS on recovery
- Redis local queue: push to in-process Redis list when OIS unreachable; drain on recovery
- Kafka fallback topic: emit to `ai-obs-events` Kafka topic when OIS HTTP fails

None of these are in the current plan.

---

### Option 2 — Raw Events in Kafka Topic

```
Service → produce to ai-obs-events → Kafka (durable buffer) → Consumer/Processor → Storage
```

#### Pros

| Strength | Why it matters |
|---|---|
| **True durability** | Kafka retains events for configurable days even if consumers are down; no data loss |
| **Replay** | Rewind consumer offset and reprocess all events — critical for schema migrations |
| **Decoupled producers and consumers** | A slow PostgreSQL write does not stall the producing service at all |
| High throughput | Kafka handles millions of events/sec; no bottleneck at ingestion |
| Multiple independent consumers | Same topic → write to PostgreSQL, Elasticsearch, S3 independently |
| Natural backpressure | Consumer lag is visible; no data is lost, it just queues |
| Consistent with existing architecture | Platform already uses Kafka; same operational model |

#### Cons

| Weakness | Real impact |
|---|---|
| **6 of 8 services have no Kafka producers** | High integration effort; each needs Kafka client, serialization, retry, error handling |
| Schema enforcement is harder | No built-in request validation; need Schema Registry or a consumer that handles malformed messages |
| Enrichment must happen in the consumer | PII, cost calc, error mapping must live in the consumer — you still need an OIS-equivalent consumer service |
| Operational complexity | More moving parts: topic management, partition sizing, consumer group lag monitoring |
| Debugging is harder | Tracing an event through Kafka topics adds cognitive overhead |

#### Backup Scenario if Consumer is Down

Best of the three options — Kafka retains all messages until consumer recovers. Consumer replays from last committed offset when it comes back. This is Kafka's core value proposition.

---

### Option 3 — Direct Write to Individual PostgreSQL Tables per Service

```
Service → INSERT directly into its own obs_* table in PostgreSQL
```

#### Pros

| Strength | Why it matters |
|---|---|
| Simplest path | No new service; just add DB writes in existing code |
| Immediate consistency | Event is in the DB the moment the service writes it |
| No single point of failure | Each service has its own DB connection |

#### Cons

| Weakness | Real impact |
|---|---|
| **No schema standardization** | This is the exact problem the platform has today |
| **No cross-service correlation** | `correlation_id` joins across 8 different tables with different schemas are painful |
| No enrichment | PII hashing, cost calc, error mapping duplicated or absent across 8 codebases |
| Schema changes touch 8 services | Any schema evolution requires coordinated deploys across the whole platform |
| No dead-letter | Malformed data written directly with no validation |
| Tight coupling | Business logic services must manage observability DB connections and migrations |
| No unified query surface | The Observability Plane cannot query across services without custom adapters per service |

#### Backup Scenario if a Service's DB is Down

Worst scenario: the service's own DB is what's down, meaning the service itself may be degraded. No buffering; events are either written synchronously or lost. A failed obs write can surface as an error in the main request path if not handled carefully.

---

## The Strong Argument: What to Build

### Option 3 is eliminated immediately.

It recreates the exact problem the Observability Plane is trying to solve: no standard schema, no central correlation, no enrichment, high coupling.

### Between OIS and Kafka — they solve different layers, not the same problem.

| Layer | What solves it |
|---|---|
| **Developer interface** (how services emit) | OIS HTTP — simple, no Kafka producer needed in 6 services |
| **Durability** (what if OIS/consumer is down) | Kafka — persistent buffer, replay |
| **Schema enforcement** | OIS validation layer |
| **Enrichment** | OIS enrichment layer |
| **Fan-out to multiple stores** | Kafka consumer can write to PostgreSQL, ES, S3 independently |

### Recommended Architecture: OIS + Kafka + Langfuse

```
                        ┌─────────────────────────────────────────┐
                        │           8 Backend Services            │
                        └──┬──────────────────────────────────────┘
                           │  emit to both layers in parallel
          ┌────────────────┴─────────────────────┐
          ▼                                       ▼
  ┌──────────────────┐                  ┌─────────────────────┐
  │  OIS HTTP        │                  │  Langfuse SDK       │
  │  POST /v1/ingest │                  │  @observe()         │
  └────────┬─────────┘                  └──────────┬──────────┘
           │ Validate + Enrich                      │ LLM/RAG/Agent
           │ Map errors                             │ trace trees
           ▼                                        ▼
  ┌──────────────────┐                  ┌─────────────────────┐
  │  Kafka           │                  │  Langfuse DB        │
  │  ai-obs-events   │                  │  Traces + Scores    │
  │  (durable buffer)│                  │  Prompts + Datasets │
  └────────┬─────────┘                  └──────────┬──────────┘
           │                                        │ nightly sync
           ▼                                        ▼
  ┌──────────────────┐                  ┌─────────────────────┐
  │  PostgreSQL      │◄─────────────────│  daily_rag_quality  │
  │  obs_events      │                  │  agg_hourly_llm     │
  │  Elasticsearch   │                  └─────────────────────┘
  │  S3              │
  └──────────────────┘
```

**What each layer handles:**

| Signal | Layer | Why |
|---|---|---|
| LLM call traces, token counts, cost | Langfuse | Native LLM trace model; auto cost calc |
| RAG pipeline spans, chunk count, faithfulness | Langfuse | Native retrieval span type; LLM-as-judge evals |
| Agent step trees, tool/LLM nesting | Langfuse | Native agent trace hierarchy |
| Prompt versions, A/B tests | Langfuse | Built-in prompt management |
| User feedback linked to exact trace | Langfuse | `langfuse.score(trace_id=...)` |
| Kafka lag, consumer offsets | OIS → PostgreSQL | Infrastructure metric |
| Document ingestion pipeline events | OIS → PostgreSQL | Non-LLM pipeline |
| Service health, auth events | OIS → PostgreSQL | Platform-level signals |
| SLO compliance, error budgets | OIS → PostgreSQL → Grafana | SRE operational concern |
| Business KPIs, budget governance | OIS → PostgreSQL → Grafana | Domain aggregates |

**Why this is correct:**

1. **Services stay simple** — 8 services call one HTTP endpoint. No Kafka client, no topic management in any of the 8 repos. The current OIS emitter utility is unchanged.
2. **Kafka handles the "consumer is down" problem** — Once OIS produces to Kafka, the event is durable. PostgreSQL writes can fail and retry without losing the event.
3. **You already have Kafka** — Same cluster, same ops team, same runbooks. Not new infrastructure.
4. **Replay is free** — When schema changes, replay the Kafka topic through the new consumer. Without Kafka, any schema bug = permanent data loss for that window.
5. **Multiple consumers become possible** — Today PostgreSQL. Tomorrow Elasticsearch. The Kafka topic is the single source of truth.

### Failure Matrix

| Failure | OIS-only plan | OIS + Kafka |
|---|---|---|
| OIS pod crashes | All events lost during downtime | Events fail at HTTP layer — mitigate with client-side local log buffer |
| PostgreSQL slow/down | OIS blocks, then drops | Events safely in Kafka; consumer retries; no data loss |
| Consumer service crashes | N/A (OIS writes directly) | Consumer recovers, replays from last offset; no data loss |
| Schema bug in consumer | Data written incorrectly; unrecoverable | Replay Kafka topic with fixed consumer; events reprocessed correctly |
| Burst traffic | OIS pod saturated, HTTP timeouts | Kafka absorbs burst; consumer drains at its own pace |

### Summary Comparison

| Criterion | OIS only | Kafka raw | Direct Postgres | OIS + Kafka |
|---|---|---|---|---|
| Developer simplicity | High | Low | Medium | High |
| Data durability | Low | High | Medium | High |
| Replay on failure | None | Full replay | None | Full replay |
| Schema enforcement | Centralized | Requires Schema Registry | None | Centralized |
| Enrichment location | OIS | Consumer service | Each service | OIS |
| Cross-service correlation | Strong | Strong (via consumer) | Weak | Strong |
| Infrastructure cost | Low | Medium (existing Kafka) | Low | Medium |
| Backup if down | Events lost | Events buffered | Events lost | Events buffered |
| **Recommended** | Phase 1 only | No (too much producer work) | Never | **Target state** |

**Recommendation in one sentence:** Build OIS (with Kafka backing for durability) for platform/infrastructure signals, and deploy Langfuse self-hosted for the LLM/RAG/agent trace layer — the two are complementary, joined by `correlation_id`, and together cover every observability gap in the current platform without duplicating any effort.

---

## Q: If OIS + Kafka is Chosen, What Happens When the OIS Pod is Down?

### Direct Answer: The Data is Gone at the HTTP Layer

```
Service → POST /v1/ingest → [OIS POD DOWN] ✗
                                    ↑
                              HTTP fails here
                              Event never reaches Kafka
```

**Kafka only protects the segment AFTER OIS.** It has zero visibility into events that never arrived at OIS.

### Actual Failure Map

| Segment | Who buffers it | If it fails |
|---|---|---|
| Service → OIS (HTTP) | **Nobody** | Event is lost |
| OIS → Kafka (produce) | Kafka durability | Event safe once produced |
| Kafka → Consumer | Kafka offset tracking | Event replayed on recovery |
| Consumer → PostgreSQL | Kafka offset tracking | Event replayed on recovery |

OIS + Kafka solves PostgreSQL/consumer downtime. It does **not** solve OIS pod downtime.

---

### The Three Solutions to the HTTP Gap

#### Solution 1 — High-Availability OIS (Mitigate, Not Eliminate)

Run 3+ OIS replicas in Kubernetes behind a load balancer. K8s health checks restart crashed pods in seconds. Rolling deploys mean zero downtime deployments.

```
Service → LB → [OIS pod 1]
               [OIS pod 2]  → Kafka → Consumer → PostgreSQL
               [OIS pod 3]
```

- Pod crash = other pods absorb traffic in ~5 seconds. Data loss window shrinks dramatically but is not zero.
- Does not protect against full cluster outage, network partition, or a bad deploy that takes all pods down.

#### Solution 2 — Client-Side Buffer in the Emitter SDK (Near Zero-Loss)

The emitter in every service catches HTTP failure and writes to a **local fallback log**:

```python
async def emit_event(...):
    try:
        await client.post(OIS_URL, json=payload, timeout=2.0)
    except Exception:
        # OIS is down — write to local structured log
        # Existing log shipper (Fluentd/Filebeat) picks this up
        # and routes it to Elasticsearch or a recovery Kafka topic
        fallback_logger.warning("obs_fallback_event", extra={"payload": payload})
```

Every service already has a log shipper running as a sidecar. Those logs go to Elasticsearch. **The infrastructure already exists** — the fallback write is the only addition needed. Events are recoverable from Elasticsearch logs during total OIS downtime.

#### Solution 3 — Services Write Directly to Kafka (Architecturally Correct, High Effort)

```
Service → Kafka topic (ai-obs-raw-events)
               ↓
          OIS Consumer (validates + enriches + routes)
               ↓
          PostgreSQL obs_* tables
```

Eliminates the HTTP single point of failure entirely. Kafka is the entry point, not OIS. OIS becomes a consumer, not a server.

**The tradeoff:** 6 of 8 services have no Kafka producer today. Each needs a Kafka client, topic config, serialization, and error handling — weeks of integration work vs. hours for HTTP.

---

### Practical Recommendation for This Platform

**Phase 1:** OIS + HA (3 replicas) + client-side fallback log in the emitter.
- OIS downtime window shrinks to seconds
- Events during that window land in Elasticsearch via fallback log
- Near-zero loss with minimal added complexity

**Phase 2 (future):** Migrate the emitter to produce directly to Kafka behind the scenes.
- The 8 services never change their call — the shared emitter utility changes its transport from HTTP to Kafka
- OIS becomes the consumer
- Zero data loss achieved

### The Key Insight

The emitter utility is the seam. All 8 services call `emit_event()`. Change what that function does underneath — they never know. That is why the shared emitter was the right design call. You can swap the transport from HTTP to Kafka in one file when the time comes.

```python
# Today
async def emit_event(...):
    await http_client.post(OIS_URL, json=payload)

# Phase 2 — services unchanged, transport swapped
async def emit_event(...):
    await kafka_producer.produce("ai-obs-raw-events", value=payload)
```
