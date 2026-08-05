# Sync/Streaming API — Implementation Plan (rev. 4, 2026-08-04)

Based on `DISCOVERY-v2.md` plus direct reads of `excutor/models/agui_events.py` and `excutor/service/agui_kafka_stream_service.py`.

---

## Three delivery modes, chosen by the caller

| | **Async (existing, unchanged)** | **Sync/streaming — `/stream`** | **Sync/blocking — `/sync`** |
|---|---|---|---|
| Caller has Kafka | Yes | **No — that is the point** | **No** |
| Endpoint | `POST .../task-executor` etc. | `POST .../task-executor/stream` | `POST .../task-executor/sync` |
| Immediate response | `{"x_correlation_id": ..., "message": "Execution Initiated Successfully"}` | `text/event-stream`, held open | nothing — the connection just waits |
| Step visibility | None | Every AG-UI event live | None — caller doesn't want it |
| Final result delivered via | Caller's Kafka response topic, or webhook | **The SSE stream itself** | **The single JSON response body** |
| If it runs long | N/A, caller polls | `stream.timeout` frame at 870s, connection closes, execution continues | `202` + a status URL at ~20-30s, execution continues |
| Depends on the executor's AG-UI change | No | **Yes** | **No** |
| Caller infrastructure needed | Broker credentials, a topic, a consumer | An HTTP client | An HTTP client |

Teams with Kafka keep using the existing architecture untouched. Teams without Kafka use `/stream` or `/sync` and need nothing but HTTP — `/stream` for step-by-step visibility, `/sync` for a plain "give me the answer" call. Nothing about the async path changes.

**`/sync` is the cheaper of the two new modes, and doesn't need Phase 0 at all.** It never touches AG-UI events or the platform AG-UI topic — it only needs to know when `AGENT_EXECUTION_FINAL_RESPONSE` lands on the *existing* internal topic, which already happens today for every execution, async or not. See "The `/sync` endpoint" below.

**Two consequences that drive the whole design:**

1. **The event source cannot live on caller infrastructure.** AG-UI events must be published to a platform-owned topic, or a Kafka-less caller produces no events at all. This is why rev. 3 was wrong — see below.
2. **The final result must travel on the SSE stream.** Today the result reaches the caller through `ResponseService` — webhook or Kafka. A caller with neither would receive every step and then a closed connection with no answer. The stream must emit the final response payload as its last frame, and must not depend on `ResponseService` succeeding.

### Why rev. 3 failed

`AGUIKafkaStreamService.create()` resolves its topic from the caller's own response config:

```python
# agui_kafka_stream_service.py:96-109
if not (usecase_config.metadata and usecase_config.metadata.ag_ui_events_streaming):
    return None
kafka_env = build_kafka_environment(usecase_config.response_config.kafka)
if kafka_env is None:
    return None
```

A caller without Kafka has no `response_config.kafka`, so `create()` returns `None` and **no AG-UI events are produced at all**. Bridging the caller's response topic to SSE would therefore have worked only for callers who already consume Kafka — precisely the ones who do not need this API.

**AG-UI events must be published to a platform-owned topic.** That is a change to the executor, and it is unavoidable: the event source cannot depend on caller infrastructure.

---

## Architecture

```
Caller (no Kafka)
  │  POST /api/v1/agentic-orchestration/task-executor/stream
  │  X-Correlation-ID, X-Authorization-Coin, Config-ID, X-Application-ID
  ▼
Orchestration ──dispatch──► internal topic ──► Executor
  │                                              │
  │                                    AGUIKafkaStreamService
  │                                              │
  │         ┌────────────────────────────────────┴───────────────┐
  │         ▼                                                    ▼
  │  agui topic (PLATFORM-OWNED, new)          caller response topic (optional,
  │         │                                   unchanged, only if configured)
  │         ▼
  └── assign() at OFFSET_END, no group
      → registry[x_correlation_id] → SSE frames ──► Caller
```

The caller touches Kafka nowhere. Existing Kafka-consuming callers keep their response topic exactly as today.

---

## Executor change — small, and currently inert

One file: `excutor/service/agui_kafka_stream_service.py`.

**0. Make streaming request-scoped, not use-case-scoped.** Add an optional defaulted field to the internal envelope:

```python
# excutor/models/task_payload.py
ag_ui_streaming: bool = False
```

The `/stream` endpoints set it `True`; the async endpoints leave it alone. `create()` then enables on `task_payload.ag_ui_streaming or usecase_config.metadata.ag_ui_events_streaming`.

Calling `/stream` becomes the opt-in, so no per-use-case config can be forgotten — which matters because the failure mode of forgetting is a stream that connects successfully and emits nothing but keepalives for 870 seconds. Async callers generate zero AG-UI traffic. The existing use-case flag still works for anyone wanting always-on.

**Deploy the executor before orchestration** so the new field is understood before it is sent. Check `TaskPayloadModel.model_config` first: if it sets `extra='forbid'`, an un-upgraded executor rejects the whole message rather than ignoring the field.

**1. Decouple the flag from the caller's Kafka.** Never require `response_config.kafka`. Resolve a platform Kafka environment from Helm config instead.

**2. Publish to the platform topic.** Decide the destination based on the Phase 0 check:

- **If no use case currently has `ag_ui_events_streaming` enabled** (the expected finding): publish to the platform topic **only**. There is no existing behaviour to preserve, and this is the smaller change.
- **If some use case does have it enabled**: dual-publish — platform topic always, plus the caller's response topic when `response_config.kafka` is configured, so that consumer sees no change.

`_do_publish` already swallows all exceptions (`:141-147`), so a failure on any destination cannot affect execution.

**3. Everything else stays.** Same `to_dict()` payloads, same key (`self._thread_id` = `x_correlation_id`), same fire-and-forget `_schedule_publish`.

**Why the risk is low:** this class returns `None` and does nothing unless `ag_ui_events_streaming` is enabled on a use case — which today is presumably no use case at all. You are modifying code that does not currently execute. Verify that assumption first (see Phase 0).

**New config**, both repos' Helm values: `internal_kafka_agui_events_topic: 181229_agui_events_NAM_001_{env}`. Reuse the existing `internal_kafka_bootstrap_servers` and credentials.

### Why a dedicated topic rather than reusing the internal one

The internal topic carries agent **dispatch** — `AGENT_EXECUTION_REQUEST` and `AGENT_EXECUTION_FINAL_RESPONSE`. AG-UI emits `TEXT_MESSAGE_CONTENT` at roughly token granularity. Putting that volume onto the control-plane topic risks adding lag to execution dispatch itself, and forces one retention policy on two very different kinds of data. A separate topic also lets the consumer read without discriminating AG-UI events from task payloads.

---

## The event source

`AGUIKafkaStreamService` publishes AG-UI protocol events during execution, emitted from `excutor/service/agent_execution_service.py`:

| Event | Site |
|---|---|
| `RunStartedEvent` | `:94` |
| `ToolCallStartEvent`, `ToolCallArgsEvent`, `ToolCallEndEvent` | `:111-114` |
| `TextMessageStartEvent`, `TextMessageContentEvent`, `TextMessageEndEvent` | `:139` |
| `StateSnapshotEvent`, `RunFinishedEvent` | `:162-163` |
| `RunErrorEvent` | `:154` |

`excutor/models/agui_events.py` is a faithful implementation of the AG-UI spec (14 event types). AG-UI's canonical transport is SSE, so the bridge forwards payloads unchanged and standard AG-UI clients work against this endpoint unmodified.

> **Unverified: whether text is streamed incrementally.** All three text events sit at one site (`agent_execution_service.py:139`), which matches the `emit_text_message()` convenience helper (`agui_kafka_stream_service.py:207-212`) — it generates a `message_id` and fires start → content → end for one *complete* message. If so, you get one `TEXT_MESSAGE_CONTENT` per message, **not** per token. The models and `emit_text_message_content()` support incremental deltas, but the call site may not use them. Check line 139 before promising token-level streaming to anyone; it materially changes how the sync API feels.

### Four facts that decide the implementation

**1. Route by the Kafka message key.**

```python
# :69, :136-139
self._thread_id = task_payload.x_correlation_id
await self._producer.push_message_async(topic=self._topic, key=self._thread_id, message=event_dict)
```

Only `RunStartedEvent` and `RunFinishedEvent` carry any run identifier. `TextMessageContentEvent` has only `message_id` and `delta`; `ToolCallArgsEvent` only `tool_call_id` and `delta`; `StateDeltaEvent` only `delta`. There is **no correlation field on most event types** — filtering on the payload silently drops everything after `RUN_STARTED`.

**2. `run_id` is NOT the correlation id.** `self._run_id = str(uuid.uuid4())` (`:70`), fresh per agent execution. The docstring at `agui_events.py:91` calling it a "correlation ID" is wrong. On the wire, `threadId` is the correlation id.

**3. One stream carries MULTIPLE `RUN_STARTED`/`RUN_FINISHED` pairs.** `create()` is called per agent execution (`agent_execution_service.py:42`); a multi-agent plan runs several hops under one `x_correlation_id`. **Never terminate on `RUN_FINISHED`** — it would truncate the stream after the first agent and present a partial result as complete. Forward those frames as hop boundaries.

**4. Forward payloads verbatim; there is no sequence number.** `to_dict()` is `model_dump(mode="json", by_alias=True)` with `alias_generator=to_camel` (`agui_events.py:66-79`), so the JSON is already correct AG-UI camelCase — `threadId`, `runId`, `messageId`, `toolCallId`. Do not deserialise, rename, or wrap. `BaseEvent` has only `type` and `timestamp`; ordering comes from Kafka partition order, guaranteed because messages are keyed by correlation id.

### Terminating the stream — and delivering the answer on it

Run a **second** consumer on the fixed internal topic (`internal_kafka_agentic_events_topic`), also `assign()` at `OFFSET_END` with no group, watching for `event_type == AGENT_EXECUTION_FINAL_RESPONSE` with a matching `x_correlation_id`.

**Emit that payload to the client as the final SSE frame, then close.** For a Kafka-less caller this frame *is* the answer — the same body that `ResponseService` would have posted to a webhook or produced to a response topic. Suggested framing: `event: execution.completed`, `data: <the final response payload>`. On the failure path the same applies: orchestration assembles an error response at `message_processing_service.py:90-99` (`x_correlation_id`, `status: FAILED`, `response`, `event_type`, `state`) — emit it as `event: execution.failed` and close.

The stream must **not** depend on `ResponseService` succeeding. A sync caller may have neither a webhook nor a response topic configured; whatever that path does or fails to do is irrelevant to the SSE response.

This is additive: it does not touch the existing `agentic_internal_planner_group_{topic}` consumer or `process_message`.

---

## The `/sync` endpoint — same answer, no streaming

A caller who doesn't want step-by-step detail — just "run this and give me the result" — gets a plain `POST .../task-executor/sync` that holds the HTTP connection open and returns **one** `application/json` body when the execution finishes.

### Why it's cheaper than `/stream`

`/stream` needs two consumers: **Consumer A** (AG-UI topic, forwards every step) and **Consumer B** (internal topic, detects the terminal signal). `/sync` needs only **Consumer B** — it never surfaces intermediate steps, so there's nothing for Consumer A to do.

Consequences:

- **No dependency on Phase 0.** `/sync` doesn't touch the AG-UI topic, doesn't need `ag_ui_streaming` set on the payload, and doesn't care whether the executor's AG-UI change has shipped. It only needs `AGENT_EXECUTION_FINAL_RESPONSE` on the *existing* internal topic — which already happens today, unchanged, for every execution.
- **`/sync` can ship before Phase 0.** It's the fastest path to "a Kafka-less caller gets an answer from one HTTP call." Move it earlier in the phasing (see below).
- Same registry, same register-before-produce ordering, same "no DB session held across the wait" as `/stream`.

### Registry: extend, don't duplicate

`agui_stream_registry.py` already maps `x_correlation_id → asyncio.Queue` for `/stream`. Add a second registration kind for `/sync`: `x_correlation_id → asyncio.Future`, resolved once by Consumer B and never touched by Consumer A. Both live in the same registry module; `/sync` requests simply never register with Consumer A.

### Response shapes

Success (`200`):

```json
{ "x_correlation_id": "3f9c1a...", "status": "COMPLETED", "response": {...}, "state": {...} }
```

Failure (`200`, `status: FAILED` — same error assembly as `message_processing_service.py:90-99`, used consistently with how `/stream`'s `execution.failed` frame is built):

```json
{ "x_correlation_id": "3f9c1a...", "status": "FAILED", "response": "...", "error": {...} }
```

Still running when the wait budget elapses (`202` — the long-running-operation pattern, reusing the existing status endpoint rather than inventing a new one):

```json
{ "x_correlation_id": "3f9c1a...", "status": "IN_PROGRESS",
  "status_url": "/api/v1/agentic-orchestration/execution-status?x_correlation_id=3f9c1a..." }
```

### Why the wait budget must be short — shorter than `/stream`'s 870s

SSE sends `: keepalive` bytes every 15s specifically so idle-timeout proxies don't kill the connection. A blocking JSON call sends **nothing** until the very end — to any intermediary doing idle-timeout detection (corporate proxies, API gateways, some HTTP client defaults), that is indistinguishable from a hung connection, and many such intermediaries default to well under 870s.

Do not give `/sync` the same 870s budget as `/stream`. Default the wait to something short — **20-30s**, configurable via `SYNC_WAIT_SECONDS`, with a hard cap around 60s — and degrade to the `202` response above rather than holding longer. Document `/sync` as the right choice for fast, simple executions; point anything that might run long at `/stream` instead.

### What to build

| File | Change |
|---|---|
| `orchestration/api/sync_routes.py` | **New.** Four routes (`/task-executor/sync` and its three siblings), sharing dispatch logic with `stream_routes.py` where sensible — both are new code, so sharing between them is fine (unlike sharing with the frozen four). |
| `orchestration/service/agui_stream_registry.py` | **Extend.** Add the future-based registration kind alongside the existing queue-based one. |
| `orchestration/config/environment.py` | New keys: `SYNC_ENABLED` (default `False`), `SYNC_WAIT_SECONDS` (default 25, cap 60). |

Same zero-impact properties as `/stream`: no database change, gated behind its own flag, guarded by the same OpenAPI snapshot test, `Depends(JWTBearer())` matching the four existing POSTs.

---

## What to build in orchestration

| Existing (frozen) | New — streaming | New — blocking |
|---|---|---|
| `POST .../task-executor` (`orchestration/api/api.py:55`) | `.../task-executor/stream` | `.../task-executor/sync` |
| `POST .../conversational-task-executor` (`:107`) | `.../conversational-task-executor/stream` | `.../conversational-task-executor/sync` |
| `POST .../native-conversational-task-executor` (`:161`) | `.../native-conversational-task-executor/stream` | `.../native-conversational-task-executor/sync` |
| `POST .../agent-testing` (`:223`) | `.../agent-testing/stream` | `.../agent-testing/sync` |

| File | Change |
|---|---|
| `orchestration/api/stream_routes.py` | **New.** Four routes + one shared SSE generator. |
| `orchestration/service/agui_stream_registry.py` | **New.** Per-process `dict[x_correlation_id, asyncio.Queue]`, bounded (maxsize 100), capped registration count. |
| `orchestration/service/agui_consumer_service.py` | **New.** Two `AIOKafkaConsumer`s, both `assign()` at `OFFSET_END`, **no `group_id`**: the platform AG-UI topic (forward events) and the internal topic (detect terminal). Both topics are fixed and known at startup. |
| `orchestration/main.py` | Consumer start/stop inside the **existing** `lifespan`, plus one `include_router`. |
| `orchestration/config/environment.py` | New keys only: `SSE_ENABLED` (default `False`), `SSE_MAX_DURATION_SECONDS` (870), the AG-UI topic name. |

### Fan-in

Orchestration runs `--workers 1`, so one process per pod, but `replicaCount` can exceed 1 and an HPA exists. The pod holding the SSE connection is not necessarily the one a consumer group would assign the partition to.

Use `assign()` at `OFFSET_END` with **no consumer group**. Every pod tails every partition and filters locally: no coordinator state, no rebalances, no offset commits, no dead groups accumulating as pods restart. Durability is not needed — a dropped frame is a dropped frame, and the authoritative result still reaches the caller by the existing response path.

Do not reuse `agentic_internal_planner_group_{topic}`; a shared group would shard events across pods so each sees only a fraction.

### Stream mechanics

- **Auth**: `Depends(JWTBearer())`, matching the four existing POSTs. Do **not** copy `GET /execution-status`, which has no authentication (`api.py:292-294`) — a pre-existing gap; do not widen it.
- Register the stream **before** producing to Kafka, or events in the gap are lost.
- SSE frame: `event: <payload's `type` field>`, `data: <AG-UI JSON verbatim>`.
- Emit `run.accepted` immediately after dispatch, before the first `RUN_STARTED`.
- `: keepalive` every 15s idle, via `asyncio.sleep` — never a blocking sleep.
- **Terminate on `AGENT_EXECUTION_FINAL_RESPONSE`, never on `RUN_FINISHED`** — and emit that payload as the final frame. It is the caller's answer.
- Cap at `SSE_MAX_DURATION_SECONDS` = 870, just under the 900s HAProxy Route timeout (`helm/values.yaml:8`). Emit `stream.timeout` and close cleanly rather than letting HAProxy sever it.
- If the resolved use case has `ag_ui_events_streaming` disabled, fail fast at registration with a clear 4xx rather than streaming keepalives for 870 seconds.
- **Open no database session in the streaming path.** The pool is `pool_size=5` + default `max_overflow=10` = 15 per pod, shared with the existing endpoints.
- `unregister()` in a `finally`. On disconnect the execution continues and its result still reaches the caller by the existing path.

### Zero-impact properties

- Executor: one file, in a class that does nothing unless `ag_ui_events_streaming` is on. Dual-publish preserves existing caller response topics byte-for-byte.
- Database: **no change** — no table, no index, no migration, no query.
- `main.py`: one `include_router` plus consumer start/stop in the existing `lifespan`.
- Everything behind `SSE_ENABLED=False`; routes not registered when off.
- Gate: an OpenAPI snapshot test over the seven existing paths, written first.

---

## Phasing

**Phase A — `/sync`, ships first.** No executor dependency at all. Registry (future-based), Consumer B, the four `/sync` routes, the short wait budget with `202` fallback. This alone gives every Kafka-less caller a working "get me the answer" path.

**Phase 0.** Confirm no use case currently has `ag_ui_events_streaming` enabled (this is what makes the executor change near-zero-risk). Create the platform AG-UI topic. Add the Helm value to both repos. Make the executor change. Enable the flag on one use case. Confirm events land with `kafka-console-consumer`. Confirm SSE flows through the OpenShift Route — `curl -N`, not just against the pod.

**Phase 1.** The four `/stream` endpoints — registry extended with the queue-based kind, Consumer A, the SSE generator. Builds on Phase A's registry and Consumer B.

**Phase 2.** Reconnect, only if clients need it. Hard: AG-UI has no sequence number and events live only in Kafka. Either seek by timestamp within retention, or accept resume-from-now. Do not add a database table to solve this without asking.

**Phase 3.** Cancellation. Nothing exists today; the only phase touching the executor's execution path.

---

## Risks

**Pod restart orphans in-flight executions.** Both existing consumers use `auto_offset_reset='latest'` with auto-commit, so an execution in flight when a pod dies is never reprocessed and sits as `IN_PROGRESS` indefinitely. Pre-existing; the duration cap is what stops a client hanging on it.

**AG-UI publishes are fire-and-forget** (`_schedule_publish`, and `drain()` is deliberately not called during execution — `:162-176`). The stream is best-effort; the authoritative result remains the existing response path. Say so in the API contract.

**No cancellation, no DLQ, no retry cap** anywhere in either repo. Unchanged by this work.
