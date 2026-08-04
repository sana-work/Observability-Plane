# GitHub Copilot Prompts — Sync/Streaming API (rev. 3, 2026-08-04)

Companion to `SYNC-API-IMPLEMENTATION-PLAN.md`. Run in **Copilot agent mode** in the workspace holding both repos.

Rewritten against `DISCOVERY-v2.md`. Revisions 1 and 2 were built on a fabricated report and contained wrong paths, the wrong Kafka client, the wrong table name, and a design (polling the audit table) that cannot work.

**One prompt per session.** Review, test, and commit between each.

---

## Pre-flight

**1. Confirm no use case currently has `ag_ui_events_streaming` enabled.** This is what makes the executor change near-zero-risk — you would be modifying a class that does not currently execute. It also decides whether the executor publishes to the platform topic only (expected) or dual-publishes.

**2. Create the platform AG-UI topic** and add `internal_kafka_agui_events_topic` to both repos' Helm values. Reuse the existing `internal_kafka_bootstrap_servers` and credentials.

**3. Make the executor change** (Phase 0 prompt below), enable the flag on one use case, and confirm events land:

```bash
kafka-console-consumer --bootstrap-server <broker> --topic <agui-topic> --from-beginning --max-messages 20
```

**4. Verify SSE flows through the OpenShift Route** with `curl -N`, not just against the pod. The HAProxy Route timeout is 900s (`helm/values.yaml:8`).

---

## Phase 0 — Executor: publish AG-UI events to a platform-owned topic

> ## Why this change is necessary
>
> The sync API exists to serve calling teams that have **no Kafka at all**. Today `AGUIKafkaStreamService.create()` resolves its topic from the caller's own response config:
>
>     # excutor/service/agui_kafka_stream_service.py:96-109
>     if not (usecase_config.metadata and usecase_config.metadata.ag_ui_events_streaming):
>         return None
>     kafka_env = build_kafka_environment(usecase_config.response_config.kafka)
>     if kafka_env is None:
>         return None
>
> A caller without Kafka has no `response_config.kafka`, so `create()` returns `None` and **no AG-UI events are produced at all**. The event source must not depend on caller infrastructure.
>
> ## The change — two files
>
> **A. `excutor/models/task_payload.py`** — add one optional, defaulted field:
>
>     ag_ui_streaming: bool = False
>
> This makes streaming **request-scoped**: the orchestration `/stream` endpoints set it, the async endpoints do not. Calling `/stream` becomes the opt-in, so no per-use-case config can be forgotten, and async callers generate zero AG-UI traffic.
>
> Report `TaskPayloadModel.model_config` before changing it. If it sets `extra='forbid'`, note that in your summary — the executor must then be deployed before orchestration starts sending the field, or messages are rejected outright.
>
> **B. `excutor/service/agui_kafka_stream_service.py`:**
>
> 1. Enable publishing on `task_payload.ag_ui_streaming or usecase_config.metadata.ag_ui_events_streaming`. Never require `response_config.kafka`.
> 2. Resolve the Kafka environment from a new platform Helm config value `internal_kafka_agui_events_topic`, using the existing `internal_kafka_bootstrap_servers` and credentials — the same way the internal agentic-events environment is already built.
> 3. Publish to that platform topic. **If** any use case already has `ag_ui_events_streaming` enabled today, also keep publishing to `response_config.kafka` when it is configured, so that existing consumer sees no change. If none does, publish to the platform topic only.
>
> Change nothing else: same `to_dict()` payloads, same key (`self._thread_id` = `x_correlation_id`), same fire-and-forget `_schedule_publish`, same swallowed exceptions in `_do_publish`.
>
> ## Constraints
> - This is the only executor file that changes. Do not touch `agent_execution_service.py`, `runner`, the audit table, or any consumer.
> - Publishing must never affect execution. Keep every publish inside the existing try/except that logs and continues.
> - Add the Helm value to `values.yaml` and all env value files in both repos.
>
> ## Done when
> - A payload with `ag_ui_streaming=True` produces AG-UI events on the platform topic **even when the use case has no `response_config.kafka` at all** — this is the acceptance test that matters, because it is the Kafka-less caller's case.
> - A payload with `ag_ui_streaming=False` and the use-case flag off produces nothing; `create()` returns `None`.
> - The use-case flag alone still enables publishing, unchanged.
> - A broker outage on the AG-UI topic does not fail or slow an execution.
> - Also report: does `agent_execution_service.py:139` call `emit_text_message()` (one content event per whole message) or `emit_text_message_content()` incrementally (per-chunk)? Do not change it — just say which, since it determines whether this API can offer token-level streaming.

**2. Enable `ag_ui_events_streaming`** on one use case, run an execution, and confirm events land:

```bash
kafka-console-consumer --bootstrap-server <broker> --topic <agui-topic> --from-beginning --max-messages 20
```

**3. Decide the 900s question.** `haproxy.router.openshift.io/timeout` defaults to `900s` (`helm/values.yaml:8`). Either keep streams under it (recommended: cap at 870s), raise it, or require client reconnect.

**4. Verify SSE actually flows through the Route**, not just against the pod — `curl -N` through the OpenShift Route. No buffering annotations exist either way, so this is empirical.

---

## Step 0 — Repo instructions file (do this once)

> Create `.github/copilot-instructions.md` at the workspace root with exactly the content below. Change no other file.
>
> ```markdown
> # Agentic Workflow — Copilot instructions
>
> Two Python FastAPI services sharing one PostgreSQL database (`gssp_agentic` schema) and one Kafka cluster.
> Both run `uvicorn --workers 1` and use a `lifespan` context manager (NOT `@app.on_event`).
> - Orchestration: source under `orchestration/`. App: `FastAPI(title="Agentic-Planner", lifespan=lifespan)`.
> - Executor: source under `excutor/` (note the spelling — it is not `executor` or `app`).
>
> ## Facts that are commonly guessed wrong
> - Kafka client is **aiokafka** (`AIOKafkaConsumer`, `AIOKafkaProducer`). NOT confluent_kafka. Do not add another.
> - The executor processes messages **concurrently** — `asyncio.create_task(process_message(...))` at
>   `excutor/service/kafka_consumer_service.py:67`, not awaited.
> - The audit table is `gssp_agentic.audit_table` (class `AuditLogPGStore`). Its writes are **fire-and-forget**
>   and its rows are **UPDATEd in place**. It is a compliance record, NOT an event stream. Never poll it.
> - `x_correlation_id` is a caller-supplied required HTTP header `X-Correlation-ID`, propagated to the executor
>   in the Kafka payload and used as the Kafka message key. It groups an execution including all sub-agents.
> - Auth is `JWTBearer` reading the `X-Authorization-Coin` header via `COINAuthorizer` — not a plain Bearer token.
> - No Redis exists in either repo. Do not add or suggest it.
> - No Alembic or any migration tooling exists. Tables pre-exist or come from `Base.metadata.create_all`.
>
> ## The event source for streaming
> `excutor/service/agui_kafka_stream_service.py` (`AGUIKafkaStreamService`) already publishes AG-UI protocol
> events to Kafka during execution, keyed by `x_correlation_id`, when `ag_ui_events_streaming` is enabled on the
> use-case config: RunStartedEvent, ToolCallStart/Args/End, TextMessageStart/Content/End, StateSnapshotEvent,
> RunFinishedEvent, RunErrorEvent. Consume these. Never re-instrument the executor.
>
> ## Hard rules — this work is PURELY ADDITIVE
> Zero impact on currently working code outranks elegance and outranks avoiding duplication.
>
> Frozen — do not change paths, models, status codes, handler logic, or tags:
> - POST /api/v1/agentic-orchestration/task-executor            (orchestration/api/api.py:55)
> - POST /api/v1/agentic-orchestration/conversational-task-executor        (api.py:107)
> - POST /api/v1/agentic-orchestration/native-conversational-task-executor (api.py:161)
> - POST /api/v1/agentic-orchestration/agent-testing                       (api.py:223)
> - GET  /api/v1/agentic-orchestration/execution-status                    (api.py:292)
> - GET  /api/v1/agentic-orchestration/registered-agents                   (api.py:297)
> - POST /api/v1/agentic-orchestration/reload-configs
>
> - The executor repository does not change. If a task seems to need an executor edit, stop and say so.
> - No database schema changes, no new tables, no indexes, no migrations.
> - Do not refactor to share code with existing handlers. Duplicate the logic into the new module instead.
> - Do not add or modify fields on existing Pydantic models. Subclass instead.
> - Do not add dependencies without saying so and pinning them in `requirements.txt`.
> - Do not reformat or reorder imports in files you are not otherwise changing.
> ```

---

## Phase 1 — The four streaming endpoints

This is the whole build.

> ## Goal
>
> The orchestration service has four task-execution endpoints. Each POSTs, dispatches over Kafka, returns `{"x_correlation_id": ..., "message": "Execution Initiated Successfully"}`, and leaves the caller polling `GET /execution-status`.
>
> Add a **streaming twin for each**, so a caller POSTs once and receives every step of the execution as Server-Sent Events on that same response.
>
> | Existing (frozen) | New |
> |---|---|
> | `POST .../task-executor` | `POST .../task-executor/stream` |
> | `POST .../conversational-task-executor` | `POST .../conversational-task-executor/stream` |
> | `POST .../native-conversational-task-executor` | `POST .../native-conversational-task-executor/stream` |
> | `POST .../agent-testing` | `POST .../agent-testing/stream` |
>
> ## Where the events come from
>
> **Do not instrument the executor and do not touch the audit table.** `AGUIKafkaStreamService` already publishes AG-UI protocol events to Kafka during execution, keyed by `x_correlation_id`. Consume that topic and forward to the HTTP client.
>
> The event models are in `excutor/models/agui_events.py` — a complete AG-UI implementation with 14 event types: `RUN_STARTED`, `RUN_FINISHED`, `RUN_ERROR`, `TEXT_MESSAGE_START`, `TEXT_MESSAGE_CONTENT`, `TEXT_MESSAGE_END`, `TOOL_CALL_START`, `TOOL_CALL_ARGS`, `TOOL_CALL_END`, `STATE_SNAPSHOT`, `STATE_DELTA`, `MESSAGES_SNAPSHOT`, `RAW`, `CUSTOM`.
>
> **Five properties decide the implementation. Read all of them before writing the consumer.**
>
> **(a) Consume the platform AG-UI topic** (`internal_kafka_agui_events_topic`), created in Phase 0. It is fixed and known at startup — assign it directly. Never consume a caller's `response_config.kafka` topic: the whole point of this API is serving callers who have no Kafka.
>
> **(b) Route by the Kafka message key, NOT by anything in the payload.**
>
>     # :69, :136-139
>     self._thread_id = task_payload.x_correlation_id
>     await self._producer.push_message_async(topic=self._topic, key=self._thread_id, message=event_dict)
>
> Only `RunStartedEvent` and `RunFinishedEvent` carry any run identifier. `TextMessageContentEvent` has only `message_id` and `delta`; `ToolCallArgsEvent` only `tool_call_id` and `delta`; `StateDeltaEvent` only `delta`. Looking for a correlation field in the JSON silently drops every mid-stream event, and the stream appears to hang after `RUN_STARTED`. Use `msg.key`.
>
> **(c) `run_id` is NOT the correlation id.** `self._run_id = str(uuid.uuid4())` at `:70` — a fresh UUID per agent execution. The docstring in `agui_events.py:91` calls it a "correlation ID" and is wrong. On the wire, `threadId` is the correlation id; `runId` is not.
>
> **(d) One stream carries MULTIPLE `RUN_STARTED`/`RUN_FINISHED` pairs.** `AGUIKafkaStreamService.create()` is called per agent execution (`agent_execution_service.py:42`), and a multi-agent plan runs several hops under one `x_correlation_id`. **Never terminate the stream on `RUN_FINISHED`** — that cuts it off after the first agent and reports a partial result as complete. Forward those frames; they mark hop boundaries.
>
> The terminal signal comes from a **second consumer** on the fixed Helm-configured internal topic (`internal_kafka_agentic_events_topic`), also `assign()` at `OFFSET_END` with no group, watching for a payload with `event_type == AGENT_EXECUTION_FINAL_RESPONSE` whose `x_correlation_id` matches a registered stream. Do not modify the existing `agentic_internal_planner_group_{topic}` consumer or `process_message` to achieve this.
>
> **Emit that payload to the client as the final SSE frame, then close** — `event: execution.completed`, `data: <the final response payload>`. For a caller with no Kafka this frame *is* the answer; it is the same body `ResponseService` would have sent to a webhook or response topic. On failure, orchestration assembles an error response at `message_processing_service.py:90-99` — emit it as `event: execution.failed` and close.
>
> The stream must **not** depend on `ResponseService` succeeding. A sync caller may have no webhook and no response topic; what that path does is irrelevant to the SSE response.
>
> **(e) Forward the payload verbatim; there is no sequence number.** `BaseEvent.to_dict()` is `model_dump(mode="json", by_alias=True)` with `alias_generator=to_camel` (`agui_events.py:66-79`), so the JSON is already correct AG-UI camelCase — `threadId`, `runId`, `messageId`, `toolCallId`. Do not deserialise into your own model, rename fields, or wrap it. The SSE `event:` name is the payload's `type` field. `BaseEvent` has only `type` and `timestamp`; ordering comes from Kafka partition order. Do not invent a sequence number or reorder by timestamp.
>
> ## What to build
>
> **1. `orchestration/service/agui_stream_registry.py` (new)**
>
> Per-process `dict[str, asyncio.Queue]` keyed by `x_correlation_id`. `register()` / `unregister()` / `publish()`. Queues bounded at 100 — on overflow drop the event and mark the stream lagged rather than blocking or growing without limit. Cap total concurrent registrations (default 200, configurable); reject beyond that with HTTP 503.
>
> **2. `orchestration/service/agui_consumer_service.py` (new)**
>
> An `AIOKafkaConsumer` on the AG-UI topic that dispatches each message into the registry by `x_correlation_id`, dropping unknown ids silently.
>
> **Use `assign()` with every partition at `OFFSET_END` — not `subscribe()`, and no `group_id` at all.** The service can run multiple pods, and the pod holding the SSE connection is not necessarily the one that would be assigned that partition. Manual assignment means every pod tails everything and filters locally: no group coordinator state, no rebalances, no offset commits, no dead groups accumulating as pods restart.
>
> Do **not** reuse `agentic_internal_planner_group_{topic}` — a shared group would shard events across pods so each sees only a fraction.
>
> This is a live tail; never commit offsets.
>
> **3. `orchestration/main.py`**
>
> Start and stop the consumer inside the **existing** `lifespan` context manager, and add one `include_router` with a new tag `"Task Execution (Streaming)"`, registered only when `SSE_ENABLED`. Nothing else in this file changes.
>
> If the consumer fails to start (broker unreachable), log and let the app start anyway. The seven existing routes must not depend on the streaming consumer being healthy.
>
> **4. `orchestration/api/stream_routes.py` (new) — the four routes**
>
> Each mirrors its async twin: same request model imported unchanged, same headers (`X-Correlation-ID`, `Config-ID`, `X-Application-ID`, `x_soeid`; plus `Session-ID` for the native variant), same `Depends(JWTBearer())`. Response: `StreamingResponse`, `media_type="text/event-stream"`, headers `Cache-Control: no-cache`, `Connection: keep-alive`, `X-Accel-Buffering: no`.
>
> One shared SSE generator in this module, called by all four. That is internal to new code and fine. What is not fine is reaching into `orchestration/api/api.py` or the planner modules to share their dispatch logic — **copy** it.
>
> Handler order — load-bearing:
> 1. authenticate
> 2. `registry.register(x_correlation_id)` — **BEFORE** the Kafka produce. If you produce first, events emitted in the gap are lost and the stream misses its first frames.
> 3. dispatch with a **copy** of the async twin's logic (`planner.plan(...)` → `send_to_kafka(...)`), producing a byte-identical payload. The executor must not be able to tell the difference.
> 4. return the streaming generator
>
> ## Stream mechanics — none optional
>
> - Emit a `run.accepted` event immediately after dispatch, carrying `x_correlation_id`, before any executor event arrives.
> - `: keepalive` every 15 seconds of idle. Use `asyncio.sleep` — a blocking sleep would stall the single worker process and every request it is serving.
> - Cap the stream at `SSE_MAX_DURATION_SECONDS` (default **870**). The OpenShift Route's HAProxy timeout is 900s (`helm/values.yaml:8`); close cleanly just under it with a `stream.timeout` event rather than letting HAProxy sever the connection. The execution continues server-side.
> - `registry.unregister()` in a `finally`. On disconnect the execution keeps running and its result still reaches the caller by the existing webhook/Kafka response path.
> - **Do not open a database session anywhere in the streaming path.** This design never needs one; keep it that way. The pool is 5 + 10 overflow = 15 per pod and is shared with the existing endpoints.
> - Auth: use `Depends(JWTBearer())` like the four existing POSTs. **Do not copy `GET /execution-status`** — it has no authentication (`api.py:292-294`), which is a pre-existing gap. Do not widen it.
>
> ## Constraints
> - The executor repository does not change.
> - No database access at all — no tables, no indexes, no migrations, no queries.
> - No new dependencies. `aiokafka` and Starlette's `StreamingResponse` are already present. Do not add `sse-starlette` or `confluent_kafka`.
> - Write the OpenAPI snapshot test **first**: capture `/openapi.json` filtered to the seven existing paths and assert this work leaves it unchanged. That is the acceptance gate.
>
> ## Done when
> - `pytest` passes: OpenAPI snapshot, register-before-produce ordering, terminal event closes the stream, unknown `x_correlation_id` dropped, duration cap fires, disconnect unregisters.
> - With `SSE_ENABLED=false` the new routes are absent from `/openapi.json` and the app behaves exactly as before.
> - With `SSE_ENABLED=true`, `curl -N -X POST .../task-executor/stream -H "X-Correlation-ID: <uuid>" -H "X-Authorization-Coin: <jwt>" -d '{...}'` streams AG-UI events live and terminates on its own — and the same request to `/task-executor` still returns its `x_correlation_id` payload unchanged.
> - Verified **through the OpenShift Route**, not only against the pod.
> - An execution with sub-agents streams their events too (they share the `x_correlation_id`).

---

## Phase 2 — Reconnect (only if clients need it)

> Support `Last-Event-ID` on the four `.../stream` routes.
>
> This is harder than with a database-backed log: AG-UI events live only in Kafka and there is no durable per-run sequence to resume from. Two options, pick one deliberately:
> (a) assign the AG-UI topic and seek by timestamp, filtering by `x_correlation_id` — replays within the topic's retention;
> (b) accept that reconnect resumes from now and document it.
>
> Do not add a database table to make this easier. Ask first.

---

## Phase 3 — Cancellation

> Nothing supports cancellation today — no endpoint, no flag, no interrupt mechanism in either repo. This is the only phase that touches the executor's execution path. Keep it to a single flag check in the loop, gate it behind a config flag defaulting to off, and enforce the same `JWTBearer` auth as the POST endpoints.

---

## If Copilot goes off the rails

| Symptom | Correction |
|---|---|
| Polls `audit_table`, or reads it at all | "Revert. Audit writes are fire-and-forget and rows are UPDATEd in place, so a cursor poll emits every start and no completion. It is a compliance record, not an event stream. Consume the AG-UI Kafka events." |
| Instruments the executor step loop | "Revert. `AGUIKafkaStreamService` already publishes AG-UI events keyed by `x_correlation_id`. The executor repo does not change." |
| Adds `confluent_kafka` or `sse-starlette` | "Revert. This stack uses `aiokafka`, already a dependency, and Starlette's built-in `StreamingResponse`." |
| Uses `subscribe()` with a `group_id` | "Use `assign()` at `OFFSET_END` with no group. A consumer group shards partitions across pods, so the pod holding the SSE connection would see only a fraction of events." |
| Converts `main.py` away from `lifespan`, or adds `@app.on_event` | "The app already uses a `lifespan` context manager. Add the consumer's start/stop inside the existing one." |
| Creates a table, index, or migration | "Revert. This design touches no database at all, and there is no migration tooling in either repo." |
| Opens a DB session in the streaming path | "Nothing in this design needs one. The pool is 15 per pod and shared with the existing endpoints; a session held for a 15-minute stream would exhaust it and break them." |
| Produces to Kafka before registering the queue | "Reorder. `registry.register()` must precede the produce or events in the gap are lost." |
| Copies the auth (or lack of it) from `/execution-status` | "That endpoint has no authentication — a pre-existing gap. Use `Depends(JWTBearer())` like the four POST endpoints." |
| Blocking `sleep()` in the generator | "Use `asyncio.sleep`. The service runs a single worker process; a blocking sleep stalls every request it is serving." |
| Filters events by a correlation field in the JSON payload | "Only RUN_STARTED and RUN_FINISHED carry `run_id`. Every other event type has none, so this drops the whole stream after the first frame. Route by the Kafka message key." |
| Consumes a caller's `response_config.kafka` topic | "Never. This API exists for callers who have no Kafka — depending on their topic makes the feature impossible for its intended users. Consume the platform `internal_kafka_agui_events_topic`." |
| Closes the stream without sending the final result | "The last frame must carry the final response payload. For a Kafka-less caller with no webhook, that frame is the only place the answer can arrive." |
| Terminates the stream on `RUN_FINISHED` | "A multi-agent plan emits one RUN_FINISHED per hop, so this truncates the stream after the first agent. Terminate on `AGENT_EXECUTION_FINAL_RESPONSE` from the internal topic instead." |
| Treats `runId` as the correlation id | "`run_id` is `str(uuid.uuid4())`, fresh per agent execution (`agui_kafka_stream_service.py:70`). The `agui_events.py` docstring saying otherwise is wrong. `threadId` is the correlation id." |
| Deserialises AG-UI events into a new Pydantic model, or converts keys to snake_case | "Forward the payload verbatim. It is already correct AG-UI camelCase wire format; re-serialising breaks standard AG-UI clients." |
| Adds a sequence number to events | "There is no sequence number in AG-UI and none is needed — Kafka partition order is the ordering guarantee, since messages are keyed by correlation id." |
| Modifies an existing handler or extracts a shared helper from one | "Revert. Copy the dispatch logic into the new module. The seven existing routes must stay byte-identical, including their OpenAPI schema." |
