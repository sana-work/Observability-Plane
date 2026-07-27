# ai-observability-sdk — Implementation Guide

*How every module of this package works, why it is written the way it is, and
— marked with 🔧 — exactly where you must change things to fit the real
environment and the real services.*

Audience: the developers building/maintaining the SDK (Dev B track in
[TASKS-2dev-phase0-phase1.md](../TASKS-2dev-phase0-phase1.md)) and the eight
service teams integrating it.

---

## 0. The package at a glance

```
ai-observability-sdk/
├── pyproject.toml            packaging + dependencies
├── .env.example              every AI_OBS_* variable, documented
├── README.md                 quick start + per-signal integration examples
├── IMPLEMENTATION.md         ← this file
├── ci/sdk-ci.yml             lint → contract gate → tests → drift check
├── ai_obs_sdk/
│   ├── __init__.py           the public API surface (what services import)
│   ├── config.py             ObsSettings — all AI_OBS_* env vars
│   ├── contracts/            VENDORED frozen contract (do not edit here)
│   │   ├── event_schema.py   ObsEvent envelope, schema_version 1.0
│   │   ├── event_types.py    the 50 EventType values
│   │   └── service_names.py  the 8 ServiceName values
│   ├── context.py            ObsContext + contextvar plumbing
│   ├── hashing.py            prompt_hash / query_hash (16-hex sha256)
│   ├── kafka_headers.py      W3C traceparent inject/extract for Kafka
│   ├── emitter.py            KafkaEmitter + emit_event()  ← the core
│   ├── tracing.py            init_tracing() → OTEL → Tempo (OTLP gRPC)
│   ├── log_config.py         configure_logging() → structlog JSON
│   ├── middleware.py         ObservabilityMiddleware + init_observability()
│   ├── decorators.py         @trace_llm/_rag/_tool/_agent
│   ├── cost.py               producer-side token-cost estimate
│   └── prompts.py            get_prompt() + in-process TTL cache
└── tests/                    17 tests, all broker-free (FakeEmitter)
```

Layering rule: each module imports only from modules above it in this list.
`contracts/` and `config.py` are at the bottom; `middleware.py` and
`decorators.py` are the top — they are the only files service developers
interact with.

Three platform decisions this code embodies (do not "fix" them):

1. **`user_id` is the raw SOE ID, unhashed** (owner decision 2026-07-15).
   Access control lives at the stores, not in the SDK.
2. **No Redis** — the prompt cache is an in-process TTL cache.
3. **Fire-and-forget** — emitting telemetry may never block or crash a
   service. Every code path in `emitter.py` upholds this.

---

## 1. `config.py` — configuration

### How it works

One `pydantic-settings` class. Every field is an environment variable with
the `AI_OBS_` prefix (`kafka_linger_ms` ⇒ `AI_OBS_KAFKA_LINGER_MS`).
`get_settings()` is wrapped in `@lru_cache` — the environment is read once
per process, and every module calls `get_settings()` instead of passing
config around.

```python
class ObsSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="AI_OBS_", env_file=".env", extra="ignore")
    service_name: str          # required — no default
    lob: str                   # required
    application_id: str        # required
    enabled: bool = True       # master kill-switch: False = every SDK call no-ops
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_topic_raw: str = "ai-obs-events-raw"     # pinned by IaC policy test
    ...
```

Only three variables have no default on purpose: a service that forgets to
say who it is must fail at startup, not emit anonymous events.

### 🔧 Where to make real changes

| What | Where | Change |
|---|---|---|
| **Real Kafka brokers + SASL** | each service's Helm values | set `AI_OBS_KAFKA_BOOTSTRAP_SERVERS`, `AI_OBS_KAFKA_SECURITY_PROTOCOL=SASL_SSL`, `AI_OBS_KAFKA_SASL_MECHANISM=SCRAM-SHA-512`, username/password from a K8s Secret. **No code change** — the emitter already forwards these. |
| **Real Tempo endpoint** | Helm values | `AI_OBS_OTLP_ENDPOINT=http://tempo-distributor.observability.svc:4317`. If prod Tempo uses TLS, change `insecure=True` in `tracing.py` (see §6). |
| **Real prompt-registry URL** | Helm values, after Phase Q ships the API | `AI_OBS_PROMPT_REGISTRY_URL=http://obs-dashboard-svc/api/v1/prompts` |
| New config knob | `config.py` | add the field here, document it in `.env.example`, and reference it via `get_settings()`. Never read `os.environ` anywhere else in the package. |
| Topic rename (don't) | — | `kafka_topic_raw`'s default is asserted equal to `observability-iac/kafka/topics.yaml` by the IaC policy test. Renaming means changing both + the consumers. |

---

## 2. `contracts/` — the vendored wire contract

### How it works

Byte-for-byte copies of `observability-iac/contracts/*.py`. `ObsEvent` is a
Pydantic model whose validators **reject** unknown `event_type`,
`service_name`, `telemetry_type`. Because the emitter constructs an
`ObsEvent` for every emission, a typo'd event type raises during unit tests
— it can never reach the topic.

The envelope fields (memorise these — every store mirrors them):

```
identity     event_id, schema_version="1.0", event_type, telemetry_type
time         timestamp, emitted_at                     (UTC ISO-8601)
correlation  correlation_id, request_id, trace_id, span_id, parent_span_id
ownership    service_name, component, environment, application_id,
             lob, tenant_id, user_id                   (user_id = RAW SOE ID)
outcome      status, latency_ms, error_code, http_status
domain       payload: dict                             (everything else)
```

### 🔧 Where to make real changes

- **Never edit files in `ai_obs_sdk/contracts/` directly.** The change goes
  into `observability-iac/contracts/` first (bump `schema_version` for any
  envelope change), then is re-copied here **in the same PR**:
  ```bash
  cp observability-iac/contracts/*.py ai-observability-sdk/ai_obs_sdk/contracts/
  ```
  Two CI checks fail if you skip this: the SDK drift-diff (`ci/sdk-ci.yml`)
  and the IaC policy test (byte-identity).
- **Adding a new event type** (the most common change): append to the enum in
  the IaC copy, update its `assert len(EventType) == 50` count, update the
  IaC policy test's expected count, re-vendor. Downstream consumers accept it
  automatically (they validate against the same file); dashboards pick it up
  when you map it.
- **Adding a 9th service**: same flow via `service_names.py` + the
  `service_registry` seed in `observability-iac/postgres/seed/001_registries.sql`
  (a policy test keeps enum and seed in sync).

---

## 3. `context.py` — request-scoped identity

### How it works

`ObsContext` is a dataclass holding the per-request IDs. It lives in a
**contextvar** — Python's async-safe equivalent of a thread-local: whatever
the middleware binds is visible to every function called during that request,
with no argument-passing, and concurrent asyncio requests can't see each
other's context.

Key methods:

```python
ctx.child()        # copy with parent_span_id = old span_id, fresh span_id
                   # → how decorators build the span tree
get_context()      # current context, or a NEW detached one if none bound
                   # → background jobs never crash, they just start a fresh trace
bind_context(ctx)  # returns a token; reset_context(token) restores — ALWAYS reset
```

`user_id` here is the raw SOE ID (see decision #1).

### 🔧 Where to make real changes

- If the platform later adds a request-scoped field (e.g. `session_id`):
  add it to `ObsContext`, populate it in `middleware.py`, map it into the
  event in `emitter.py` — either into the envelope (contract change, §2
  process) or into `payload` (no contract change).
- Nothing here is environment-specific.

---

## 4. `emitter.py` — the core (how data actually reaches Kafka)

### How it works, step by step

**`emit_event(event_type, *, status, latency_ms, error_code, http_status,
payload, component)`** is the single funnel for all emissions:

1. If `settings.enabled` is false → return immediately (kill switch).
2. Assemble the `ObsEvent` from three sources:
   - *settings* → `service_name`, `environment`, `application_id`, `lob`;
   - *the contextvar* → `correlation_id`, `request_id`, span ids,
     `tenant_id`, `user_id`;
   - *the live OTEL span* (via `kafka_headers.current_trace_ids()`) →
     `trace_id`/`span_id` hex, so events join to Tempo traces.
3. Constructing `ObsEvent(...)` **validates** the event (see §2).
4. Hand to the singleton `KafkaEmitter`.
5. The entire body is inside `try/except Exception: log; return` — the
   **never-raises** guarantee.

**`KafkaEmitter.emit(event)`** does the Kafka handoff:

```python
self._producer.produce(
    topic   = settings.kafka_topic_raw,
    key     = (event.correlation_id or event.event_id).encode(),  # partition key!
    value   = event.model_dump_json().encode(),
    headers = inject_trace_headers(...),          # traceparent + correlation_id
    on_delivery = self._on_delivery,
)
self._producer.poll(0)    # 0 ms: serve delivery callbacks, never wait
```

Crucial mechanics — this is why it never blocks:

- `produce()` only appends to **librdkafka's in-memory queue** (a C library
  with its own background threads) and returns in microseconds. No network
  I/O happens on the request path, ever.
- The background thread batches messages for up to `linger.ms=50`, picks the
  partition as `hash(correlation_id) % partitions` (→ per-request ordering),
  compresses with lz4, sends, and waits for the broker ack
  (`enable.idempotence=True`, so retries can't duplicate).
- The ack (or failure) fires `_on_delivery` on a later `poll(0)`:
  success → `delivered += 1`; failure → `dropped += 1` + a log warning.
  Nobody is waiting on it.
- **Queue full** (`BufferError`, e.g. long broker outage): drop the event
  with a warning. Blocking is the only alternative, and blocking is
  forbidden. Telemetry is allowed to be lossy; the product is not allowed
  to be slow.
- `atexit`-registered `flush()` drains the queue on clean shutdown.

The producer config worth knowing (all from `ObsSettings`):
`enable.idempotence=True`, `linger.ms=50`, `compression.type=lz4`,
`queue.buffering.max.messages=100_000`, `delivery.timeout.ms=10_000`,
`client.id=ai-obs-sdk.<service_name>`.

### 🔧 Where to make real changes

| What | Change |
|---|---|
| mTLS instead of SASL | add `ssl.ca.location` / `ssl.certificate.location` / `ssl.key.location` fields to `ObsSettings` and forward them into the `conf` dict in `KafkaEmitter.__init__` — same pattern the SASL block already uses. |
| Drop-rate visibility | the counters `self.delivered` / `self.dropped` exist; wire them into Prometheus by exposing a small gauge in `middleware.py`'s init (recommended follow-up). |
| Tuning for very hot services | raise `AI_OBS_KAFKA_LINGER_MS` (bigger batches) or `AI_OBS_KAFKA_QUEUE_MAX_MESSAGES`. Env-only; no code. |
| Do NOT | add `producer.flush()` on the request path, or re-raise from `emit()`. Both violate the design contract that services depend on. |

---

## 5. `kafka_headers.py` + `hashing.py` — small rails

**`kafka_headers.py`**: wraps OTEL's W3C `TraceContextTextMapPropagator`.
`inject_trace_headers(correlation_id)` produces the Kafka message headers
(`traceparent`, optional `tracestate`, plus `correlation_id`) so any consumer
can continue the same distributed trace. `extract_trace_context(headers)` is
the consumer-side inverse — the Enrichment Consumer uses it, and so does any
of our own services that consumes Kafka (Consumer Service pattern, §10).

**`hashing.py`**: `prompt_hash()` / `query_hash()` — 16-hex sha256 prefixes
used to group identical prompts/queries across events without shipping the
full text. **These are grouping keys, not privacy controls** — user identity
is deliberately not hashed anywhere (decision #1).

🔧 Real change: none expected. If a store needs longer hashes, change the
slice length here once — every emitter of that hash picks it up.

---

## 6. `tracing.py` — OTEL spans → Grafana Tempo

### How it works

`init_tracing(app)` (called for you by `init_observability`):

- builds a `TracerProvider` with resource attributes
  (`service.name`, `service.namespace=ai-services-platform`,
  `deployment.environment`, `lob`);
- `ParentBasedTraceIdRatio(settings.trace_sample_ratio)` sampling — respects
  the caller's sampling decision, applies the ratio at the root;
- `BatchSpanProcessor` → OTLP **gRPC** exporter at `settings.otlp_endpoint`
  (Tempo's distributor, port 4317);
- if given the FastAPI `app` → server spans per route (excluding
  /metrics,/health,/ready);
- httpx + asyncpg auto-instrumentation wrapped in try/except — services
  that don't use them lose nothing;
- `_initialized` guard makes repeat calls no-ops.

This is the *infrastructure* trace layer (timing tree in Tempo). The
*AI-quality* trace view is reconstructed from Kafka events. Both carry the
same `correlation_id`.

### 🔧 Where to make real changes

- **TLS to Tempo in prod**: the exporter is constructed with
  `insecure=True`. Change to read a new `AI_OBS_OTLP_INSECURE` setting
  (add to `config.py`) and pass credentials when false.
- **Hot-path sampling**: set `AI_OBS_TRACE_SAMPLE_RATIO=0.1` per service via
  env — no code.
- **A service that also uses OTEL already** (check GSSP GS): make sure it
  doesn't call `trace.set_tracer_provider` twice — integrate by letting
  `init_tracing` win and removing the service's own setup.

---

## 7. `log_config.py` — structlog JSON with automatic correlation

### How it works

`configure_logging()` builds one processor chain used by **both** structlog
and stdlib logging (uvicorn, kafka lib, everything), so all lines come out
in the same JSON shape and Fluent Bit parses exactly one format:

- `merge_contextvars` + a custom `_add_obs_context` processor that reads the
  current `ObsContext` and stamps `correlation_id` / `span_id` /
  `request_id` onto **every** log line;
- ISO UTC timestamps, level, logger name, exception rendering;
- JSON renderer — or pretty console when `AI_OBS_LOG_JSON=false` (laptops).

Result: `log.info("plan created", step_count=3)` anywhere in a request emits
`{"event": "plan created", "step_count": 3, "correlation_id": "d4f7...", ...}`
with zero effort at the call site — which is what makes logs joinable to
events and traces.

### 🔧 Where to make real changes

- **Services with existing logging config** (all 8 have some): delete their
  `logging.basicConfig`/dictConfig/custom formatters. Two configs fighting
  over the root logger produce duplicate or misformatted lines. The SDK's
  config must be the only one.
- Extra permanent fields (e.g. pod name): add
  `structlog.contextvars.bind_contextvars(pod=...)` once at startup, or a
  processor here.

---

## 8. `middleware.py` — where a request's identity is born

### How it works

`ObservabilityMiddleware.dispatch()` per request:

1. Skip `/metrics`, `/health`, `/ready`, `/livez` (no event spam).
2. Build the `ObsContext`:
   - `correlation_id` = `X-Correlation-ID` header **or a new uuid4** — this
     line is where the platform-wide ID is minted;
   - `request_id`, `usecase_id`, `tenant_id` from their headers;
   - **`user_id` = `X-User-ID` or `X-SOE-ID`, copied verbatim** (raw by
     decision #1).
3. `bind_context(ctx)` → everything downstream (decorators, logs, manual
   emits) now sees these IDs.
4. Emit `REQUEST_RECEIVED` (method + path in payload).
5. Run the handler:
   - exception → emit `REQUEST_FAILED` (`error_code` = exception class
     name), reset context, **re-raise** (the service's own error handling
     still runs);
   - response → `REQUEST_COMPLETED` (or FAILED when status ≥ 500) with
     latency and `http_status`.
6. **Echo `X-Correlation-ID` on the response** — a user's bug report can
   quote the exact trace.
7. `reset_context(token)` — always, or contexts leak across asyncio requests.

`init_observability(app)` = `configure_logging()` + `init_tracing(app)` +
`add_middleware(ObservabilityMiddleware)` + Prometheus instrumentator
exposing `/metrics`.

### 🔧 Where to make real changes (this is the main adaptation point)

| Real-world fact | Change here |
|---|---|
| **COIN JWT carries the user identity**, not a plain header | in `dispatch()`, replace the header lookup: decode the already-verified JWT claims (services verify COIN JWTs today) and set `user_id=claims["soeid"]`, `tenant_id=claims.get("tenant")`. Keep the header fallback for service-to-service calls. |
| Different header names in your gateway (e.g. `X-Request-Id` vs `X-Correlation-ID`) | adjust the `request.headers.get(...)` names once, here — never per service. |
| `usecase_id` known only after routing | leave it None here; the handler can set `get_context().usecase_id = ...` before its emits. |
| Extra paths to exclude (e.g. `/docs`, `/openapi.json`) | add to `_SKIP_PATHS`. |
| Non-FastAPI service (none today — all 8 are FastAPI) | the middleware is pure ASGI (Starlette `BaseHTTPMiddleware`); it works on Starlette/Quart-ASGI as-is. |

---

## 9. `decorators.py` + `cost.py` + `prompts.py` — the instrumentation API

### How it works

All four decorators come from one factory (`_make_decorator`). Each wrapper
(sync **and** async via `inspect.iscoroutinefunction`):

1. `get_context().child()` + bind → nested span for the trace tree;
2. emit `*_STARTED` with the decorator's static kwargs as payload;
3. run the function inside an OTEL span;
4. success → emit `*_COMPLETED` with `latency_ms` + merged payload;
   exception → emit `*_FAILED` — or the timeout event
   (`TOOL_CALL_TIMEOUT`/`AGENT_TIMEOUT`) when it's a `TimeoutError` — with
   `error_code` + truncated message, then **re-raise**;
5. reset the context token.

Payload merging, three sources (later wins):
**static decorator kwargs** ∪ **`result.obs_payload`** (a dict the wrapped
function attaches to its return value — how runtime facts like token counts
get in) ∪ **`obs_extra=` kwarg** passed by the caller.

The LLM finalizer additionally: computes `total_tokens` and
`estimated_cost_usd` (from `cost.py`) when token counts are present, and
**replaces any `prompt_text` key with its `prompt_hash`** — full prompts
never ride in events.

`cost.py` is the *estimate* (per-1k pricing dict + DEFAULT fallback); the
Enrichment Consumer recomputes authoritatively from the
`observability.model_pricing` table.

`prompts.py` — `get_prompt(template_id, version="active")`: httpx GET to the
control-plane API (3 s timeout) → frozen `Prompt` dataclass
(`.format(**vars)`), behind a thread-safe in-process TTL cache
(300 s default — the Redis stand-in; a future shared cache changes only
this module).

### 🔧 Where to make real changes

| What | Change |
|---|---|
| **Pricing rows** | `cost.py` `PRICING` **and** `observability-iac/postgres/seed/003_metric_catalog.sql` together — the IaC policy test fails if they diverge. Review against real R2D2/vendor billing before go-live. |
| New model onboarded | same two files, one row each. |
| A service's LLM client returns usage differently | no SDK change — the *service* maps its client's response into `result.obs_payload = {"input_tokens": ..., "output_tokens": ..., "finish_reason": ...}` (see §10 examples). |
| Prompt API response shape | must stay `{template_id, version, text, prompt_hash, ab_bucket}` — documented in `observability-iac/postgres/migrations/003_prompt_registry.sql`. If Phase Q changes it, change `prompts.py::get_prompt` to match in the same PR. |
| New signal type (e.g. `@trace_guardrail`) | add the event types to the contract (§2 process), then add one `_make_decorator(...)` block — ~15 lines. |

---

## 10. Integrating the eight real services (what each team edits)

Universal steps for every service:
1. `ai-observability-sdk` in requirements; the `AI_OBS_*` block in Helm values.
2. `init_observability(app)` right after `app = FastAPI()`.
3. **Delete** the service's own logging config (§7) and any ad-hoc
   request-logging middleware — the SDK replaces both.
4. Decorate the hot paths (below).
5. Verify: one real request → the full ordered event chain on
   `ai-obs-events-raw` under one correlation_id (J-2 smoke recipe in the
   task board).

Service-specific edits (file names are indicative — find the equivalent):

| Service | Where to edit | What to add |
|---|---|---|
| **Agentic Orchestration** | its existing Kafka-emission module | *delete* the bespoke producer; `emit_event(PLAN_CREATED / AGENT_EXECUTION_REQUEST_PRODUCED / FINAL_RESPONSE_CONSUMED, payload=...)` at the same points. It already propagates correlation ids — map its field into `X-Correlation-ID` when calling other services. |
| **Agent Executor** | agent run loop + tool dispatcher | `@trace_agent(agent_id=..., agent_type=...)` on the run entrypoint; `@trace_tool(tool_id=..., tool_type=...)` on the dispatcher; `emit_event(AGENT_STEP_COMPLETED, payload={"step_count": n})` inside the loop; put real numeric `latency_ms` (it logs strings today). |
| **GSSP GS** (LLM gateway) | the provider-call wrapper that already fills `LLMUsageMetrics` | `@trace_llm(model_provider=..., model_name=...)` on that wrapper; copy `LLMUsageMetrics` fields into `result.obs_payload` — this service needs the least new logic, its data is already collected. Document events (`DOCUMENT_*`) on the file/attachment paths. |
| **GSSP QS** | RAG orchestration + guardrail hooks + semantic cache | `@trace_rag(vector_db_index=...)` on retrieval; `@trace_llm` on generation; `emit_event(GUARDRAIL_EVALUATED/BLOCKED, payload={"policy_id": ..., "decision": ...})` in the guardrail hook; cache hit/miss into `obs_extra`. |
| **GSSP RS** | embedding + retrieval functions | `@trace_rag`; **stop discarding embedding token usage** — put it in `obs_payload` so embedding cost becomes visible. |
| **Consumer Service** (no HTTP) | the APScheduler job functions + its Kafka consumer | no middleware — bind context manually per unit of work (pattern in §3/README): `ObsContext(correlation_id=from message header or new)`; `emit_event(DOCUMENT_EXTRACTION_STARTED/COMPLETED, DOCUMENT_EMBEDDING_CREATED, ...)` along the pipeline. |
| **Data Ingestion** | its ingest endpoint + processing steps | middleware covers the endpoint; add `emit_event(DOCUMENT_UPLOADED / DOCUMENT_STORED_IN_S3 / DOCUMENT_INDEXED, ...)` on the **success** paths (today only errors are logged) and fix timestamps to UTC by simply using the SDK. |
| **User Feedback** | the feedback-submit handler | `emit_event(FEEDBACK_SUBMITTED, payload={"feedback_id": ..., "rating": ..., "thumbs": ..., "category": ...})` — **crucially** the request must carry the `X-Correlation-ID` of the answer being rated, so feedback joins to the trace that produced it. Frontend change: pass the correlation id it received with the answer. |

---

## 11. End-to-end: what lands on Kafka

One `POST /generate` with `X-User-ID: SOE12345` produces, in order, on one
partition (because all four share the correlation_id key):

`REQUEST_RECEIVED` → `LLM_CALL_STARTED` → `LLM_CALL_COMPLETED` →
`REQUEST_COMPLETED`

Each message: **key** = correlation_id · **headers** = `traceparent`,
`correlation_id` · **value** = the ObsEvent JSON, e.g.:

```json
{
  "event_id": "9b2e...", "schema_version": "1.0",
  "event_type": "LLM_CALL_COMPLETED", "telemetry_type": "event",
  "timestamp": "2026-07-27T09:14:03.201+00:00",
  "correlation_id": "d4f7c2e8-...", "trace_id": "4bf92f35...",
  "span_id": "a1b2c3d4e5f6a7b8", "parent_span_id": "00f067aa...",
  "service_name": "gssp-gs", "environment": "prod",
  "application_id": "app-1234", "lob": "wealth", "user_id": "SOE12345",
  "status": "success", "latency_ms": 842.6,
  "payload": {"model_name": "gemini-1.5-pro", "input_tokens": 1200,
              "output_tokens": 240, "total_tokens": 1440,
              "estimated_cost_usd": 0.0027, "finish_reason": "stop"}
}
```

Local proof (5 minutes):

```bash
cd observability-iac && docker compose -f docker-compose.dev.yml up -d
KAFKA_ENV=dev BOOTSTRAP=localhost:9092 ./kafka/create_topics.sh
# run any FastAPI app with the .env.example values, hit an endpoint, then:
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic ai-obs-events-raw --from-beginning \
  --property print.key=true --property print.headers=true
```

---

## 12. Testing, CI, and release

- **Tests are broker-free**: `tests/conftest.py` sets deterministic
  `AI_OBS_*` env and monkeypatches a `FakeEmitter` (captures `ObsEvent`s in
  a list) in place of the singleton. 17 tests cover the contract gate, the
  never-raises emitter behavior, all four decorators (sync + async +
  timeout mapping + payload merging), and the middleware (event pairs,
  correlation echo, raw `user_id` capture, health-path skip).
- **CI** (`ci/sdk-ci.yml`): ruff → contract tests → full pytest → **drift
  diff** of `ai_obs_sdk/contracts/` vs `observability-iac/contracts/`.
- **Release**: bump `pyproject.toml` version, build the wheel, publish to
  the internal index. Services pin the minor (`~=0.1`). Envelope changes
  additionally bump the contract's `schema_version` (§2 process).

## 13. Consolidated 🔧 checklist before production

- [ ] Helm values per service: SASL creds, real brokers, Tempo endpoint (§1)
- [ ] COIN-JWT claim extraction in `middleware.py` replacing/augmenting header lookup (§8)
- [ ] Pricing table reviewed vs real billing — `cost.py` + IaC seed together (§9)
- [ ] TLS for the OTLP exporter if prod Tempo requires it (§6)
- [ ] Each service's legacy logging config deleted (§7)
- [ ] `AI_OBS_PROMPT_REGISTRY_URL` set once Phase Q's API exists; until then services keep baked-in prompt fallbacks (§9)
- [ ] Emitter `delivered`/`dropped` counters exposed to Prometheus (§4, follow-up)
- [ ] User-Feedback frontend passes the answer's correlation id back on submit (§10)
