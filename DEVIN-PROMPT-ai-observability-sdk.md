# Devin Prompt — Build `ai-observability-sdk` from scratch

*Copy everything below the line into Devin as the task. Attach/grant read
access to the 8 AI service repos and (if available) the `observability-iac`
repo.*

---

## Mission

Build a production-ready Python package called **`ai-observability-sdk`** from
scratch. It is the shared observability library that 8 existing AI platform
services will install with `pip`. Its single job: capture what each service
does (requests, LLM calls, tool calls, RAG retrievals, agent runs) as
structured events and publish them to a Kafka topic, plus set up distributed
tracing and JSON logging — all through one line of integration code per
service.

Deliver a complete, tested, documented, installable package. Do not modify any
of the 8 service repos.

## Context you need

Eight Python/FastAPI services form an AI platform (agent orchestration, agent
execution, an LLM gateway, RAG query/retrieval services, document ingestion,
and user feedback). Today each logs in its own format and nothing connects
across services. This SDK is the producer side of a central observability
pipeline:

```
8 services + this SDK  →  Kafka topic `ai-obs-events-raw`
                       →  (an enrichment consumer, built separately — not your task)
                       →  Elasticsearch + PostgreSQL + S3
                       →  dashboards
```

Every event carries a `correlation_id` that is constant for one user request
across all services — that is the join key that makes the whole platform work.

## Access you have — and what to do with it

You have **read-only** access to the 8 service repos. Before writing SDK code,
spend time mining them and write your findings to `INTEGRATION-NOTES.md` in the
new SDK repo. Specifically find:

1. **App startup shape** — how each service constructs its FastAPI app, and
   whether any already configures logging, OpenTelemetry, or middleware that
   would conflict with the SDK's setup.
2. **LLM call sites** — which client libraries are used (e.g. VertexAI/Gemini,
   Claude, Llama via an internal gateway), and what the response objects
   expose for token usage, finish reason, and latency. One service is an LLM
   gateway with an existing usage-metrics model — the SDK's LLM decorator must
   map onto it cleanly with no restructuring of their code.
3. **RAG/retrieval and tool call sites** — function signatures, whether they
   are sync or async, and what result objects expose (chunk counts, scores,
   HTTP status).
4. **Incoming request headers** — what the API gateway / auth layer actually
   sends: is there an existing correlation/request ID header, and does user
   identity arrive as a header or as a claim inside a JWT? Note exact header
   names.
5. **Non-HTTP services** — at least one service is a scheduler/Kafka consumer
   with no HTTP requests. Note how its work units start, because it will need
   a manual context-binding pattern instead of middleware.

These notes shape the decorator ergonomics and the README examples. **Do not
open pull requests against those repos.**

## Non-negotiable invariants

These are platform decisions already made. Do not "improve" them; implement
them exactly, and add a code comment explaining each where it appears.

1. **`emit_event()` must never raise.** Wrap the entire body so that a
   validation error, a broker outage, or any bug results in a log line and a
   dropped event — never a propagated exception. This code runs inside live
   customer request paths.
2. **`emit_event()` must never block.** Use an asynchronous producer: hand the
   message to the client's in-memory queue and return. If the local queue is
   full, drop the event with a warning. Never wait for broker acknowledgement
   on the request path. Losing telemetry is acceptable; adding latency to a
   customer request is not.
3. **User identity is carried RAW.** The field is `user_id` and it holds the
   user's corporate ID (SOE ID) exactly as received, unhashed. Audit trails and
   "by user" dashboards require it; protection is handled by access control on
   the storage layer. Do **not** add any user-hashing helper, salt setting, or
   redaction of this field. (Hashing large *text* like prompts is separate and
   required — see below.)
4. **No Redis.** Any cache you need must be an in-process TTL cache. Isolate it
   so a future swap touches one module.
5. **The event contract is frozen.** Do not add, rename, or remove envelope
   fields or event types. If something seems missing, put it in the free-form
   `payload` dictionary.
6. **Kafka message key = `correlation_id`**, so all events of one request land
   on one partition in order.
7. **Full prompt text must never appear in an event.** Replace it with a
   16-character hash.

## The frozen contract — implement exactly

Create these three modules under `ai_obs_sdk/contracts/`. If an
`observability-iac` repo is provided, copy its `contracts/*.py` files
byte-for-byte instead of writing your own, and add a CI step that diffs them
and fails on any difference.

**`event_schema.py`** — a Pydantic v2 model named `ObsEvent`:

| Group | Fields |
|---|---|
| identity | `event_id` (str, default: new uuid4), `schema_version` (str, `"1.0"`), `event_type` (str, validated), `telemetry_type` (str, default `"event"`, one of event/log/metric) |
| time | `timestamp`, `emitted_at` (str, default: current UTC ISO-8601) |
| correlation | `correlation_id`, `request_id`, `trace_id`, `span_id`, `parent_span_id` (all optional str) |
| ownership | `service_name` (str, validated), `component`, `environment` (str), `application_id`, `lob`, `tenant_id`, `user_id` (optional str — RAW, see invariant 3) |
| outcome | `status` (str), `latency_ms` (optional float), `error_code` (optional str), `http_status` (optional int) |
| domain | `payload` (dict, default empty) |

Field validators must **reject** any `event_type` not in the enum below, any
`service_name` not in the service enum, and any `telemetry_type` outside
event/log/metric.

**`event_types.py`** — a `str, Enum` named `EventType` with exactly these 50
members, then `assert len(EventType) == 50`:

```
Request (4):        REQUEST_RECEIVED, REQUEST_COMPLETED, REQUEST_FAILED, RESPONSE_DELIVERED
Orchestration (6):  AUTH_COMPLETED, CONFIG_LOADED, PLAN_CREATED,
                    AGENT_EXECUTION_REQUEST_PRODUCED, FINAL_RESPONSE_CONSUMED, RESPONSE_BUILT
Kafka (4):          KAFKA_MESSAGE_PRODUCED, KAFKA_MESSAGE_CONSUMED, KAFKA_MESSAGE_DLQ, KAFKA_LAG_RECORDED
Agent (8):          AGENT_STARTED, AGENT_STEP_STARTED, AGENT_STEP_COMPLETED, AGENT_LOOP_ITERATION,
                    AGENT_HANDOFF, AGENT_COMPLETED, AGENT_FAILED, AGENT_TIMEOUT
LLM (5):            LLM_CALL_STARTED, LLM_CALL_COMPLETED, LLM_CALL_FAILED,
                    LLM_RATE_LIMITED, LLM_SAFETY_BLOCKED
Tool (4):           TOOL_CALL_STARTED, TOOL_CALL_COMPLETED, TOOL_CALL_FAILED, TOOL_CALL_TIMEOUT
RAG (5):            RAG_RETRIEVAL_STARTED, RAG_RETRIEVAL_COMPLETED, RAG_RETRIEVAL_FAILED,
                    RAG_NO_RESULT, RAG_INDEX_HEALTH_CHECKED
Guardrail (4):      GUARDRAIL_EVALUATED, GUARDRAIL_BLOCKED, GUARDRAIL_REDACTED, GUARDRAIL_ESCALATED
Feedback (3):       FEEDBACK_SUBMITTED, FEEDBACK_REVIEWED, FEEDBACK_CLOSED
Document (7):       DOCUMENT_UPLOADED, DOCUMENT_STORED_IN_S3, DOCUMENT_EXTRACTION_STARTED,
                    DOCUMENT_EXTRACTION_COMPLETED, DOCUMENT_EXTRACTION_FAILED,
                    DOCUMENT_INDEXED, DOCUMENT_EMBEDDING_CREATED
```

**`service_names.py`** — a `str, Enum` named `ServiceName` with exactly these 8
values, then `assert len(ServiceName) == 8`:
`agentic-orchestration`, `agent-executor`, `gssp-gs`, `gssp-qs`, `gssp-rs`,
`consumer-service`, `data-ingestion`, `user-feedback`.

## Modules to build

Build in this order; each depends only on those above it.

**`config.py`** — `ObsSettings` using `pydantic-settings` with env prefix
`AI_OBS_`. Required with no defaults: `service_name`, `lob`, `application_id`
(a service that cannot identify itself must fail at startup). Also:
`environment`; an `enabled` master switch that turns every SDK call into a
no-op; a Kafka block (bootstrap servers, topic defaulting to
`ai-obs-events-raw`, optional SASL mechanism/username/password, security
protocol, linger ~50ms, lz4 compression, bounded queue ~100k, delivery timeout
~10s); tracing (OTLP endpoint, sample ratio, enable flag); logging (level,
JSON on/off); a metrics flag; prompt registry URL and cache TTL (~300s).
Expose an `lru_cache`d accessor. **No other module may read `os.environ`.**

**`context.py`** — an `ObsContext` dataclass holding `correlation_id`
(defaulting to a new uuid4), `request_id`, `trace_id`, `span_id` (16 hex
chars), `parent_span_id`, `usecase_id`, `agent_id`, `tenant_id`, `user_id`.
Store it in a `contextvars.ContextVar` so it is visible to all code handling
the current request and isolated between concurrent async requests. Provide
`bind_context()` (returns a reset token), `reset_context(token)`, and
`get_context()` — which, when nothing is bound (background jobs, schedulers),
creates and binds a fresh detached context rather than failing. Provide a
`.child()` method returning a copy whose `parent_span_id` is the current
`span_id` with a newly generated `span_id`; this is how span trees are built.

**`hashing.py`** — `prompt_hash(text)` and `query_hash(text)`: sha256, first 16
hex characters. Document that these are grouping keys for large text, not
privacy controls, and that user identity is deliberately not hashed.

**`kafka_headers.py`** — using OpenTelemetry's W3C trace-context propagator:
`inject_trace_headers(correlation_id)` returning Kafka-shaped
`list[tuple[str, bytes]]` containing `traceparent` (plus `tracestate` when
present) and a `correlation_id` header; `extract_trace_context(headers)` as the
inverse for consumers; and `current_trace_ids()` returning the active span's
trace and span IDs as hex strings (or `None, None` when no valid span).

**`emitter.py`** — the core. A `KafkaEmitter` class wrapping a
`confluent_kafka.Producer` configured from settings (idempotence on, lz4,
linger, bounded queue, SASL when configured, a `client.id` identifying the
service). Include a delivery callback that counts delivered/dropped messages
and logs failures; register a `flush()` at process exit; expose it as a
lazily-created singleton guarded by a lock. Then the public function:

```python
emit_event(event_type, *, status="success", latency_ms=None, error_code=None,
           http_status=None, payload=None, component=None) -> None
```

It returns immediately when disabled; otherwise it builds an `ObsEvent` by
combining settings (service identity), the current `ObsContext` (request
identity including raw `user_id`), and `current_trace_ids()`; serialises to
JSON; and produces with key = `correlation_id`, the trace headers, and a
non-blocking `poll(0)`. Honour invariants 1, 2, 6.

**`tracing.py`** — `init_tracing(app=None)`: build an OpenTelemetry
`TracerProvider` with resource attributes (service name, namespace,
environment, lob), a parent-based ratio sampler from settings, and a batch span
processor exporting OTLP over gRPC to the configured endpoint (Grafana Tempo).
When given a FastAPI app, instrument it, excluding `/metrics`, `/health`,
`/ready`. Attempt httpx and asyncpg auto-instrumentation inside try/except —
not every service uses both. Make repeat calls no-ops. Expose `get_tracer()`.

**`log_config.py`** — `configure_logging()` using `structlog`: a processor
pipeline producing JSON with ISO UTC timestamps, level, logger name, and
exception formatting, **plus a custom processor that stamps `correlation_id`,
`span_id`, and `request_id` from the current `ObsContext` onto every log
line**. Route standard-library logging (uvicorn, third-party libraries) through
the same formatter so downstream log shipping sees exactly one format. Support
a pretty console renderer when JSON is disabled.

**`middleware.py`** — an ASGI middleware (Starlette `BaseHTTPMiddleware`) that,
per request: skips `/metrics`, `/health`, `/ready`, `/livez`; builds an
`ObsContext` from headers — `X-Correlation-ID` or a newly minted uuid4,
`X-Request-ID`, `X-Usecase-ID`, `X-Tenant-ID`, and `X-User-ID` or `X-SOE-ID`
copied **verbatim** into `user_id`; binds it; emits `REQUEST_RECEIVED` with
method and path; runs the handler; on exception emits `REQUEST_FAILED` with
`error_code` set to the exception class name and **re-raises**; otherwise emits
`REQUEST_COMPLETED` (or `REQUEST_FAILED` when the status is ≥ 500) with latency
and HTTP status; echoes `X-Correlation-ID` on the response; and always resets
the context token. Use the exact header names you found in the service repos —
if they differ, implement those and document the mapping in
`INTEGRATION-NOTES.md`.

Also provide `init_observability(app)` — the single line services adopt —
which calls `configure_logging()`, `init_tracing(app)`, adds the middleware,
and (when metrics are enabled) exposes a Prometheus `/metrics` endpoint via
`prometheus-fastapi-instrumentator`.

**`decorators.py`** — four decorators produced by one shared factory so their
behaviour cannot drift: `trace_llm`, `trace_tool`, `trace_rag`, `trace_agent`.
Each must support both sync and async functions. Per call: bind
`get_context().child()`; emit the `*_STARTED` event carrying the decorator's
static keyword arguments as payload; run the function inside an OTEL span; on
success emit `*_COMPLETED` with measured `latency_ms` and a merged payload; on
exception emit `*_FAILED` — or `TOOL_CALL_TIMEOUT` / `AGENT_TIMEOUT` when the
exception is a `TimeoutError` — with the exception class as `error_code` and a
truncated message, then **re-raise**; always reset the context token.

Payload merging has three sources, later winning: the decorator's static
kwargs; an `obs_payload` dict attached by the wrapped function to its return
value (this is how runtime facts like token counts arrive without changing
function signatures); and an `obs_extra=` keyword argument passed by the
caller. For LLM calls additionally compute `total_tokens` and
`estimated_cost_usd` when token counts are present, and replace any
`prompt_text` key with its `prompt_hash` (invariant 7).

Static kwargs to support, based on what you find in the service repos, at
minimum: LLM — provider, model name, model version, prompt template id/version,
temperature; tool — tool id, name, version, type
(`REST|DB|ServiceNow|RAG|InternalAPI`), calling agent id; RAG — vector index,
embedding model, top-k, knowledge base; agent — agent id, version, type,
execution mode.

**`cost.py`** — a per-1000-token pricing dictionary keyed by model name holding
(input price, output price) in USD, a default for unknown models, and
`estimate_cost_usd(model, input_tokens, output_tokens)`. Document that this is
a producer-side estimate that a downstream service recomputes authoritatively.
Populate it with the models you find in use across the 8 repos.

**`prompts.py`** — `get_prompt(template_id, version="active")` performing an
HTTP GET against the configured prompt-registry URL with a short timeout,
returning a frozen `Prompt` dataclass (`template_id`, `version`, `text`,
`prompt_hash`, `ab_bucket`) with a `.format(**kwargs)` helper, behind a
thread-safe in-process TTL cache (invariant 4 — isolate it here). Raise a
clear error when no registry URL is configured, so services know to keep a
local fallback.

**`__init__.py`** — export exactly the public surface: `init_observability`,
`ObservabilityMiddleware`, `emit_event`, the four decorators, `get_prompt`,
`configure_logging`, `init_tracing`, the contract types, and the context
helpers. Include a docstring with the two-line quick start. Add a `py.typed`
marker.

## Tests (required — the suite must run with no infrastructure)

Use pytest. Create a `FakeEmitter` fixture that captures `ObsEvent` objects in
a list and is monkeypatched over the emitter singleton, so **no Kafka, no
network, and no database is needed to run the suite.** Cover at minimum:

- Contract: the enums are exactly 50 and 8; a minimal event round-trips
  through JSON; unknown event type and unknown service name are rejected;
  every event type is constructible.
- Emitter: the envelope is fully populated from settings + context (assert
  `user_id` is present and raw); an invalid event type is swallowed with no
  exception and nothing emitted; an unbound context still produces a
  correlation ID; a delivery failure increments the dropped counter.
- Decorators: a sync LLM call emits the STARTED/COMPLETED pair with computed
  tokens and cost and correct span parentage; an async retrieval failure emits
  the FAILED event and re-raises; a `TimeoutError` in a tool maps to
  `TOOL_CALL_TIMEOUT`; `obs_extra` is merged into the terminal payload.
- Middleware (FastAPI `TestClient`): a request emits the RECEIVED/COMPLETED
  pair sharing one correlation ID; a missing correlation header is minted and
  echoed on the response; a handler exception produces `REQUEST_FAILED` with
  the exception class name; the inbound user header lands verbatim in
  `user_id`; requests to `/health` emit nothing.
- Prompts: a fetch hits the network once and then serves from cache until the
  TTL expires (monkeypatch the clock).

## Verify your own work before finishing

Run these and confirm each passes; paste the output in your final summary.

```bash
pip install -e ".[dev]"
pytest -v                 # all tests green
ruff check ai_obs_sdk tests
python -c "import ai_obs_sdk; print(ai_obs_sdk.__version__)"
```

Then prove it end-to-end against a real broker. Start a single-broker Kafka
(e.g. `docker run -d -p 9092:9092 apache/kafka:3.7.0`), create the topic
`ai-obs-events-raw`, write a ~15-line FastAPI demo app that calls
`init_observability(app)` and has one `@trace_llm`-decorated function, run it
with the required `AI_OBS_*` variables and tracing disabled, send one request
carrying `X-Correlation-ID: demo-123` and `X-User-ID: SOE12345`, and consume
the topic. Confirm in the output that:

- four events appear in order: `REQUEST_RECEIVED`, `LLM_CALL_STARTED`,
  `LLM_CALL_COMPLETED`, `REQUEST_COMPLETED`;
- all four carry `correlation_id: "demo-123"` and `user_id: "SOE12345"`;
- the LLM completion carries `total_tokens` and `estimated_cost_usd`;
- the LLM events' `parent_span_id` links them under the request span;
- the message key equals the correlation ID and a `traceparent` header is
  present.

Finally, stop the broker while the demo app is still receiving requests and
confirm the app's responses stay fast and successful — proving invariants 1
and 2. Include this in your summary.

## Also deliver

- `README.md`: quick start (the one-line integration), a copy-paste example for
  each of the four decorators using the **real call patterns you found in the
  8 repos**, the pattern for non-HTTP services that must bind context manually
  from a message, the hard invariants, and the local smoke recipe above.
- `.env.example`: every `AI_OBS_*` variable with a short comment; mark the
  three required ones.
- `INTEGRATION-NOTES.md`: your findings from the 8 repos (see "Access you
  have"), plus a short per-service note on what each team will have to change
  to adopt the SDK.
- A CI workflow file running lint, tests, and — if `observability-iac` is
  available — a diff of the vendored contract files against it.
- `pyproject.toml` targeting Python ≥3.11 with version `0.1.0`.

## Out of scope — do not build

The enrichment consumer, the storage consumer, any dashboard, any
infrastructure-as-code, and any changes to the 8 service repos. If you believe
something outside this package is broken or missing, write it in your summary
rather than fixing it.

## Definition of done

The package installs cleanly, the full test suite passes with no
infrastructure, lint is clean, the live-broker smoke test shows the four events
with correct correlation and cost, the broker-outage test shows no impact on
the demo app, and a reader can integrate the SDK into a FastAPI service using
the README alone.
