# GitHub Copilot Prompts — Sync/Streaming API

Companion to `SYNC-API-IMPLEMENTATION-PLAN.md`. Run these in **Copilot agent mode** in the workspace containing both repos (`c:\Citidev\AI Services Repositories New CSI 181229\Agentic Workflow`).

**Use one prompt per session.** Do not paste them all at once — Copilot degrades badly on multi-repo changes and will silently skip requirements. Review, test, and commit after each phase before moving on.

Set up Step 0 first: it's a persistent instructions file Copilot reads automatically on every request, so the later prompts stay short.

---

> ### ⚠️ Verify the discovery report's line references first
>
> The live Swagger (`Agentic-Planner`, `localhost:8080/docs`) exposes:
>
> - `POST /api/v1/agentic-orchestration/task-executor`
> - `POST /api/v1/agentic-orchestration/conversational-task-executor`
> - `POST /api/v1/agentic-orchestration/native-conversational-task-executor`
> - `POST /api/v1/agentic-orchestration/agent-testing` (tagged under both *Task Execution* and *Testing* — **one route**, listed twice)
> - `GET  /api/v1/agentic-orchestration/execution-status`
>
> `SYNC-API-DISCOVERY-REPORT.md` instead documents `POST /api/v1/agent/run` and `GET /api/v1/agent/run/{run_id}/status`. Those paths do not exist in this service, so every `file:line` in that report is unconfirmed.
>
> Before Phase 1, run this one-liner in the workspace and fix the references in these prompts to match:
>
> > Find and list, with exact `file:line`: (1) every handler behind the five routes listed above; (2) the request and response Pydantic model for each; (3) the agent step loop in the executor and its max-steps constant; (4) every Kafka topic name in either repo and the produce/consume site for each. Report only what you find in the code — do not infer, and say "not found" where it is absent.

---

## Pre-flight — things that break the running system if done in the wrong order

None of these are code Copilot writes. They are the ways this rollout breaks production anyway.

**1. Create `agent.step.events` on the broker before Phase 1 deploys.** The discovery report found no Terraform, admin client, or Helm hook that applies `config/kafka_topics.json` — the topic is declared in config and may not exist on the cluster. If it does not and `auto.create.topics.enable` is false, every step-event produce fails. Runs survive (the produce is wrapped), but you get zero events and a log line per step. Verify first:

```bash
kafka-topics --bootstrap-server <broker> --describe --topic agent.step.events
```

**2. Run the Alembic migration before deploying the executor code that writes to it.** The executor bootstraps with `Base.metadata.create_all`, which creates missing tables but **never alters existing ones**. If both services start with a model the database has not caught up to, the executor writes fail. Order: migration → executor → orchestration.

**3. Check `max_connections` before adding executor replicas.** Per the report, orchestration is 30 connections per worker × 4 workers = **120 per pod**, and each executor replica adds up to 15. PostgreSQL defaults to `max_connections = 100`, so a single orchestration pod can already exhaust a default-configured instance.

```sql
SHOW max_connections;
SELECT count(*) FROM pg_stat_activity;
```

**4. Adding executor replicas triggers a consumer-group rebalance.** Joining `agent-executor-consumer` pauses processing across the group for a few seconds. Harmless, but do it in a low-traffic window rather than during a demo.

**5. Confirm the ingress idle timeout and response buffering before enabling `SSE_ENABLED` in any shared environment.** No K8s or ingress manifests exist in either repo, so this chain is unverified. A 60-second idle timeout silently kills every stream mid-run.

---

## Step 0 — Repo instructions file (do this once)

> Create a file at `.github/copilot-instructions.md` at the workspace root with exactly the content below. Do not change any other file.
>
> ```markdown
> # Agentic Workflow — Copilot instructions
>
> Two Python 3.11 services sharing one PostgreSQL 16 database (`agentdb`) and one Kafka cluster:
> - `181229.genaiservices.agentic-orchestration` — FastAPI 0.110 + Uvicorn 0.29, async throughout, 4 uvicorn worker processes.
> - `181229.genaiservices.agentic-agent-executor` — no web framework; a plain `python -m app.main` Kafka consumer process.
>
> ## Conventions to follow
> - `structlog` for logging, always with `run_id=` bound. Never `print()`.
> - `confluent_kafka` for all Kafka I/O. Do not introduce `aiokafka`, `kafka-python`, or a second Kafka client.
> - Producers keep the existing config: `acks=all`, `linger.ms=5`, `retries=5`, `enable.idempotence=True`.
> - All Kafka messages are keyed by `run_id` so a run stays on one partition and keeps its order. Never change the key.
> - SQLAlchemy async (`AsyncSession`), Pydantic v2 models in `app/schemas/`.
> - Schema changes go through Alembic in `agentic-orchestration/db_migrations/versions/`. The executor has no migrations directory and relies on `Base.metadata.create_all`, so any new ORM model must be added to BOTH services' `app/models/`.
> - No Redis exists in this stack. Do not add it or suggest it.
>
> ## Hard rules — this work is PURELY ADDITIVE
>
> The overriding requirement is **zero impact on currently working code**. New capability is added alongside the existing endpoints; nothing existing changes behaviour.
>
> These five routes are frozen. Do not change their paths, request models, response models, status codes, handler logic, or tags:
> - `POST /api/v1/agentic-orchestration/task-executor`
> - `POST /api/v1/agentic-orchestration/conversational-task-executor`
> - `POST /api/v1/agentic-orchestration/native-conversational-task-executor`
> - `POST /api/v1/agentic-orchestration/agent-testing`
> - `GET  /api/v1/agentic-orchestration/execution-status`
>
> - **Do not refactor to share code with existing handlers.** If new code needs logic that an existing handler has, duplicate it into the new module. Extracting a "shared helper" out of a working handler is a behaviour change and is forbidden here, even when it looks like obvious cleanup.
> - **Do not add or modify fields on existing Pydantic models.** That would change the published OpenAPI contract. Subclass instead.
> - New routes live in their own router module and their own OpenAPI tag.
> - Never remove the `callback_url` webhook in the executor.
> - Do not add dependencies without saying so explicitly and adding them to `requirements.txt` with a pinned version.
> - Do not reformat, reorder imports, or "tidy" files you are not otherwise changing.
> ```

---

## Phase 1 — Executor emits step events

Ships on its own. Verify with a console consumer before starting Phase 2.

> ## Goal
>
> Make `181229.genaiservices.agentic-agent-executor` publish a structured event at every step boundary of an agent run, to Kafka and to a new Postgres table. This is the event source for a streaming API built later — but it is independently useful, so build it as a standalone feature.
>
> The topic **already exists** and is currently unused: `agent.step.events`, declared in `config/kafka_topics.json` and as `KAFKA_STEP_EVENTS_TOPIC` in `app/core/config.py:11`. Use it. Do not create a new topic or rename it.
>
> ## What to build
>
> **1. `app/services/step_publisher.py` (new)**
>
> A function `publish_step_event(run_id, seq, event_type, data, tenant_id, user_id, trace_id=None)` that produces one JSON message to `settings.KAFKA_STEP_EVENTS_TOPIC`, keyed by `run_id`. Reuse the module-level producer pattern and config already in `app/services/kafka_producer.py:10-16`. Envelope:
>
>     {
>       "run_id": "uuid",
>       "seq": 3,
>       "event_type": "tool.started",
>       "ts": "2026-08-03T18:52:00.123Z",   // ISO-8601 UTC
>       "tenant_id": "...",
>       "user_id": "...",
>       "trace_id": "...",                   // may be null for now
>       "data": { }                          // event-specific, see below
>     }
>
> Publishing must never break a run: wrap the produce in try/except, log the failure with `structlog`, and continue. A dropped step event is acceptable; a crashed agent run is not.
>
> **Do not call `producer.flush()` per event.** `flush()` blocks until the broker acknowledges. Calling it on every step event turns each one into a synchronous round-trip and adds that latency to every agent run — including the async runs that work fine today. Rely on `linger.ms=5` and call `flush()` only after the terminal event and on process shutdown.
>
> **Cap the serialized message size.** Truncate every string field in `data` (not just tool results) so the encoded message stays under 512 KB; Kafka's default `max.message.bytes` is 1 MB and a large LLM message or tool output would otherwise be rejected. Truncate with an explicit `"...[truncated]"` marker so consumers can tell.
>
> **2. Wire it into `app/agent/runner.py`**
>
> These structlog call sites already mark exactly the right boundaries. Emit an event alongside each existing log line — keep the log lines, do not replace them:
>
> | Line | `event_type` | `data` |
> |---|---|---|
> | `runner.py:26` (status=RUNNING) | `run.started` | `agent_id`, `input_text` |
> | `runner.py:31` ("Agent step start") | `step.started` | `step` |
> | `runner.py:46` ("Executing tool") | `tool.started` | `step`, `tool`, `args` |
> | `runner.py:48` ("Tool execution complete") | `tool.completed` | `step`, `tool`, `result` (truncate to 4000 chars) |
> | `runner.py:38` ("Agent reached stop condition") | `step.completed` | `step`, `finish_reason` |
> | `runner.py:55` (max steps) | `run.max_steps_exceeded` | `step` |
> | `runner.py:62-64` (finally block) | `run.completed` / `run.failed` | `status`, `result`, `error` |
>
> Also in `app/agent/llm_client.py`, around the existing log at line 12 and the call at lines 15-19: emit `llm.started` (`model`, `message_count`) and `llm.completed` (`finish_reason`, `usage` if the SDK returns it, latency in ms). Pass `run_id` and the seq counter down from the runner — do not use a global.
>
> **3. Sequence numbers**
>
> `seq` is a per-run monotonic integer starting at 1, incremented once per emitted event. Keep the counter on a small per-run context object created in `run_agent()` and pass it down explicitly.
>
> The executor today processes **one run at a time per process** (`asyncio.run()` inside the poll loop), so a module-level counter would appear to work. Do not rely on that: concurrency arrives the moment a second replica is deployed, and a shared counter would then interleave sequence numbers across runs and corrupt every stream. Do not use a module-level variable, a class attribute, or a `contextvar` shared across tasks. Add a test that runs two agents concurrently and asserts each gets an independent `1..N`.
>
> The `confluent_kafka.Producer` is thread-safe and is designed to be shared — use one module-level producer for all runs. Do not create a producer per run or per event.
>
> **4. Terminal event guarantee — critical**
>
> `run.completed` or `run.failed` must be emitted **in the `finally` block** at `runner.py:55-65`, on every exit path including exceptions. A future streaming client hangs forever without it. Emit it after the existing `update_run_status` and `publish_result` calls so ordering matches the DB state.
>
> **5. New table `agent_run_steps`**
>
>     CREATE TABLE agent_run_steps (
>       run_id     VARCHAR     NOT NULL REFERENCES agent_runs(run_id) ON DELETE CASCADE,
>       seq        INTEGER     NOT NULL,
>       event_type VARCHAR     NOT NULL,
>       payload    JSONB,
>       created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
>       PRIMARY KEY (run_id, seq)
>     );
>
> `ON DELETE CASCADE` is required: without it, any existing job that deletes or archives `agent_runs` rows starts failing on a foreign-key violation the day this table ships. No separate index — the primary key already covers `(run_id, seq)`.
>
> **Create only this one new table. Do not add, alter, or drop any column on `agent_runs`.** That table is written by live code in both services, and the executor bootstraps via `Base.metadata.create_all`, which creates missing tables but never alters existing ones — so an ORM model that expects a new column would break the executor against an unmigrated database. If you need to distinguish sync from async executions, record it in the `run.started` event payload, not as a column.
>
> - Alembic migration `0002_create_agent_run_steps.py` in `agentic-orchestration/db_migrations/versions/`, revising down to `0001_create_agent_runs`. Include a working `downgrade()`.
> - ORM model in **both** `agentic-orchestration/app/models/` and `agentic-agent-executor/app/models/` (the executor has no migrations and uses `create_all`).
> - Write each step event to this table from the executor, same `seq` as the Kafka message. Use the existing `AsyncSessionLocal` pattern from `result_service.py`. Same rule as above: a DB write failure logs and continues, it never fails the run.
>
> **The insert must be `ON CONFLICT (run_id, seq) DO NOTHING`** (`postgresql.insert(...).on_conflict_do_nothing()`). A run that exceeds `max.poll.interval.ms` is evicted and its message redelivered, so the executor re-runs it and re-emits `seq` 1..N for the same `run_id`. A plain `INSERT` would raise a primary-key violation on every event of that retry and you would lose the entire step history for exactly the runs you most need to debug.
>
> ## Constraints
> - Do not add a streaming API, an HTTP endpoint, or any orchestration-side code in this phase.
> - Do not change the existing `agent.run.requests` or `agent.run.results` envelopes.
> - Do not add dependencies.
>
> ## Done when
> - `pytest` passes (add unit tests for `step_publisher` with a mocked producer, and for seq monotonicity across a full run).
> - A local run produces an ordered `seq` 1..N on `agent.step.events` ending in exactly one terminal event, and the same rows land in `agent_run_steps`.
> - Killing the LLM call mid-run still produces a `run.failed` terminal event.

**Verify before Phase 2:**

```bash
kafka-console-consumer --bootstrap-server localhost:9092 --topic agent.step.events --from-beginning
```

---

## Phase 2 — Four sync/SSE endpoints, mirroring the four async ones

> ## Goal
>
> The service today has four async task-execution endpoints. Each accepts a POST, dispatches the work over Kafka, returns immediately, and leaves the caller to poll `GET /api/v1/agentic-orchestration/execution-status`.
>
> Add a **streaming twin for each one**, so a caller can POST once and receive every step of the execution as Server-Sent Events on that same response, ending with the final result. Same request body, same auth, same Kafka dispatch — the only difference is that the response streams instead of returning a job id.
>
> | Existing (async, unchanged) | New (sync/SSE) |
> |---|---|
> | `POST .../task-executor` | `POST .../task-executor/stream` |
> | `POST .../conversational-task-executor` | `POST .../conversational-task-executor/stream` |
> | `POST .../native-conversational-task-executor` | `POST .../native-conversational-task-executor/stream` |
> | `POST .../agent-testing` | `POST .../agent-testing/stream` |
>
> Note `agent-testing` is tagged under both *Task Execution* and *Testing* in the current OpenAPI — it is **one route**, listed twice. Create **four** new routes, not five.
>
> ## This must be purely additive
>
> Zero impact on the working endpoints is the top priority, above elegance and above avoiding duplication.
>
> - All four new routes go in **one new module**, `app/api/stream_routes.py`, registered with a single new `app.include_router(...)` line and a new OpenAPI tag (`"Task Execution (Streaming)"`). That line plus the added startup/shutdown handlers below are the only permitted edits to `main.py`.
> - **Do not touch the existing handlers or their modules.** Where a new handler needs the same dispatch logic, **copy it** into the new module. Do not extract shared helpers out of working code — that is a behaviour change to a frozen endpoint.
> - **Do not modify existing request/response models.** Import and reuse the request models as-is. If a streaming variant needs an extra field, subclass: `class TaskExecutorStreamRequest(TaskExecutorRequest): ...`.
> - Gate every new route behind a config flag `SSE_ENABLED` (default `False`). When false the routes are not registered at all, so the feature can ship dark and be switched off without a rollback.
> - Add a test that snapshots `/openapi.json` filtered to the five existing paths and asserts it is byte-identical before and after your change. This is the acceptance gate for "zero impact" — write it first.
> - Leave `KAFKA_CONSUMER_GROUP_ID` and `KAFKA_AGENT_RESULT_TOPIC` in `app/core/config.py` untouched even though they appear unused. Add new config keys for the streaming consumer instead of repurposing them.
>
> The executor needs **no changes at all** in this phase, and no knowledge of whether a run is sync or async. It emits step events for every run (Phase 1); the streaming endpoints simply choose to listen. That is what keeps the blast radius at zero.
>
> Phase 1 is already done: the executor publishes ordered step events to the Kafka topic `agent.step.events`, keyed by `run_id`, always ending in a `run.completed` or `run.failed` event.
>
> ## Critical context — read before designing
>
> The service runs **4 uvicorn worker processes** (`Dockerfile:11`). These are separate OS processes. The worker holding an SSE connection is not necessarily the one whose Kafka consumer receives that run's events. Therefore:
>
> - Every worker process must receive **all** events and filter for the runs it is locally holding. Use `consumer.assign()` with every partition of the topic at `OFFSET_END` — **not** `subscribe()`. Manual assignment uses no consumer group at all: no group coordinator state, no rebalances, no offset commits, and no accumulation of dead groups on the cluster as workers restart. Durability is not needed here; a missed live event is recovered from `agent_run_steps`.
> - If you use `subscribe()` instead, the `group.id` **must** be unique per process (`f"orch-sse-{uuid4().hex}"`). A shared group would shard partitions across the 4 workers so each sees only a fraction of events — and every restart would leave another dead group behind on the cluster.
> - **`auto.offset.reset` must be `"latest"`.** The executor's consumer uses `"earliest"` — do not copy that. Starting from `earliest` would replay a full day of step events into the registry on every worker restart.
> - Set `enable.auto.commit=False` and never commit offsets. This consumer is a live tail; committing would create group state you do not want and do not use.
> - Do NOT use the static `KAFKA_CONSUMER_GROUP_ID` currently sitting unused in `app/core/config.py`. A shared static group would shard events across the 4 workers so each sees only a fraction. **Leave that config key exactly as it is** — something outside these repos may read it — and add separate new keys for the streaming consumer.
>
> ## What to build
>
> **1. `app/services/stream_registry.py` (new)**
>
> A per-process registry: `dict[str, asyncio.Queue]` keyed by `run_id`, with `register(run_id) -> Queue`, `unregister(run_id)`, `publish(run_id, event) -> bool`. Queues are **bounded** (maxsize 100). On a full queue, drop the event and set a lagged flag rather than blocking or growing without limit. Cap total concurrent registrations (default 200, configurable) and reject beyond that with HTTP 503.
>
> **2. `app/services/step_consumer.py` (new)**
>
> A background consumer subscribing to `agent.step.events` and `agent.run.results`, dispatching each message into the registry by `run_id`; messages for unknown run_ids are dropped silently (they belong to another worker).
>
> `confluent_kafka.Consumer` is **blocking** — never call `poll()` on the event loop, it would stall every request that worker is serving. Run the poll loop in a `threading.Thread` and hand messages to the loop with `loop.call_soon_threadsafe(...)`. Do not add `aiokafka`.
>
> **3. `app/main.py`**
>
> Start and stop the consumer thread with **an additional `@app.on_event("startup")` and `@app.on_event("shutdown")` pair**. Multiple handlers of the same event are all executed, so this is strictly additive and the existing `create_all` hook keeps working untouched.
>
> **Do NOT convert the app to a `lifespan` context manager.** Passing `lifespan=` to the `FastAPI()` constructor makes Starlette **ignore every `@app.on_event` handler silently** — no warning, no error. The existing `create_all` startup hook would simply stop running, and you would not find out until a deploy against a fresh database came up with no tables. `on_event` being deprecated is not a reason to touch working startup code in this change.
>
> Both handlers must be defensive: if the consumer thread fails to start (Kafka unreachable), log the error and let the app start anyway. The four existing endpoints must not depend on the streaming consumer being healthy.
>
> **4. `app/api/stream_routes.py` (new) — the four streaming routes**
>
> Each mirrors its async twin: same request model (imported unchanged), same auth dependency, same Kafka dispatch. Response: `StreamingResponse` with `media_type="text/event-stream"` and headers `Cache-Control: no-cache`, `Connection: keep-alive`, `X-Accel-Buffering: no`.
>
> Factor the shared streaming machinery into **one** generator helper in this new module that all four routes call — that is internal to new code and fine. What is not fine is reaching into the existing handlers' modules to share their dispatch logic; copy that instead.
>
> Order of operations in each handler is load-bearing:
>
> 1. authenticate
> 2. create the run row exactly as the async twin does — same columns, same values. Do not add a column to mark it as sync.
> 3. **`registry.register(run_id)` — BEFORE the Kafka produce.** If you produce first, events emitted before registration are lost and the stream misses its first steps. This ordering is the whole reason for choosing SSE over WebSocket; do not reorder it.
> 4. produce to Kafka using a **copy** of the existing dispatch logic, with the envelope byte-identical to what the async endpoint produces. The executor must not be able to tell the difference — that is what guarantees it needs no changes and cannot regress.
> 5. return the streaming generator
>
> **Do not hold a database session open across the stream.** Do not put `db: AsyncSession = Depends(get_db)` on these handlers. A FastAPI-injected session lives for the whole request, and these requests last up to 15 minutes — roughly 30 concurrent streams would exhaust the connection pool and take down the existing endpoints with it. Open a short-lived session with `async with AsyncSessionLocal()` for the initial insert, close it before yielding the first event, and open another only if a later query is genuinely needed. The stream itself reads from the in-memory queue and touches no database.
>
> The generator must:
> - emit a `run.accepted` event **immediately** after the Kafka produce, carrying the `run_id`, before any executor event arrives. The executor is serial and this run may sit queued for minutes; the client needs to see something at once, then heartbeats, then `run.started` when execution actually begins.
> - emit each event as SSE: `id: <seq>\nevent: <event_type>\ndata: <json>\n\n`
> - emit `: keepalive\n\n` every 15 seconds of idle — without this a proxy will kill the connection during a slow LLM call
> - **verify `event["user_id"] == current_user["sub"]` before emitting any event** and drop mismatches; the existing status endpoint enforces ownership the same way (`run_service.py:28`)
> - close after a `run.completed` / `run.failed` terminal event
> - enforce a max stream duration (config `SSE_MAX_DURATION_SECONDS`, default 900); on expiry emit a `stream.timeout` event and close — the run continues server-side
> - `registry.unregister(run_id)` in a `finally`, so a client disconnect leaks nothing. The run must keep running and stay retrievable via the existing status endpoint.
>
> ## Constraints
> - All five existing routes must keep working byte-identically, including their OpenAPI schema. The snapshot test is the gate.
> - No new dependencies. `StreamingResponse` from Starlette is enough — do not add `sse-starlette`.
> - No Redis, no sticky routing, no changes to the executor in this phase.
>
> ## Done when
> - `pytest` passes, including: the OpenAPI snapshot for the five frozen paths, registration-before-produce ordering, terminal event closes the stream, disconnect unregisters, and ownership mismatch is filtered.
> - With `SSE_ENABLED=false` the new routes are absent from `/openapi.json` and the app behaves exactly as before.
> - With `SSE_ENABLED=true`, `curl -N -X POST .../task-executor/stream -H "Authorization: Bearer <jwt>" -d '{...}'` streams step events live and terminates on its own — and the same request to `/task-executor` still returns its job id as before.
> - All four streaming routes work, and two concurrent streams served by different workers each receive only their own execution's events.

---

## Phase 3 — Hardening (separate sessions, in this order)

### 3a — Reliability of the executor consumer

> In `181229.genaiservices.agentic-agent-executor/app/services/kafka_consumer.py:31-35`, the current failure path does not commit the offset, has no retry cap, and no DLQ (the code comment states this). One persistently failing message therefore blocks its partition forever.
>
> Add a retry counter carried in a Kafka message header, a configurable max (default 3), and a `agent.run.requests.dlq` topic. On exceeding the max: produce the original message plus the error to the DLQ, commit the offset, emit a `run.failed` step event so any attached stream terminates, and set `agent_runs.status='FAILED'` with the error. Add the DLQ topic to `config/kafka_topics.json`.
>
> **Messages already on the topic when this deploys have no retry header.** Treat a missing header as count 0 — do not raise, and do not skip the message. Add a test with a headerless message.
>
> Create the DLQ topic on the broker **before** deploying this, and confirm the producer can reach it. If the DLQ topic does not exist, the error path fails inside the error handler and the message is neither dead-lettered nor committed — the exact partition-blocking failure this change is meant to remove.
>
> Do not change the success path or the offset-commit-on-success behaviour.

### 3b — Reconnect and replay

> Support the SSE `Last-Event-ID` header on the four `.../stream` endpoints, and add a new `GET /api/v1/agentic-orchestration/execution-stream` (attach to an already-running execution by id) alongside — not replacing — the existing `GET .../execution-status`.
>
> On reconnect with `Last-Event-ID: <seq>`: **register the live queue first**, then backfill from `agent_run_steps WHERE run_id = ? AND seq > ? ORDER BY seq`, then drain the live queue — de-duplicating by `seq` so events arriving during backfill are not emitted twice. Registering after the backfill would lose events in the gap.
>
> If the run already reached a terminal state, replay from the table and close immediately without registering.

### 3c — Cancellation

> There is no cancellation anywhere today. Add `DELETE /api/v1/agentic-orchestration/execution/{run_id}` in orchestration, producing to a new `agent.run.commands` topic keyed by `run_id`. The executor consumes it on a separate thread, sets a per-run `is_cancelled` flag, and the step loop checks the flag at the top of each iteration, exiting via the normal `finally` path so a `run.failed` (reason `cancelled`) terminal event is emitted and any attached stream closes. Enforce the same ownership check as the existing status endpoint.
>
> This is the one phase that modifies the executor's running step loop. Keep the change to a single flag check at the top of the loop — no restructuring — and gate the whole feature behind a config flag defaulting to off, so the command consumer is not even started until you turn it on.

### 3d — Token-level streaming (optional, last)

> In `agentic-agent-executor/app/agent/llm_client.py:15-19`, `client.chat.completions.create` is called without `stream=True`. Add an opt-in streaming mode that emits `llm.delta` step events as tokens arrive, gated behind a config flag defaulting to off. Batch deltas (~50ms or 20 tokens) before publishing — do not emit one Kafka message per token.

---

## If Copilot goes off the rails

Common failure modes with these prompts, and the correction to paste back:

| Symptom | Correction |
|---|---|
| Adds `aiokafka` or `sse-starlette` | "Revert that dependency. Use `confluent_kafka` in a background thread with `loop.call_soon_threadsafe`, and Starlette's built-in `StreamingResponse`." |
| Uses a static consumer group id | "That shards events across the 4 uvicorn workers, so each worker sees only some events. Use a unique `group.id` per process: `f'orch-sse-{uuid4().hex}'`, with `auto.offset.reset='latest'`." |
| Produces to Kafka before registering the queue | "Reorder: `registry.register()` must happen before `publish_run_request()`, or events emitted in the gap are lost." |
| Suggests Redis for fan-out | "There is no Redis in this stack and none will be added. Use the per-process broadcast consumer group." |
| Modifies the existing run/status endpoints | "Revert those. Both must stay byte-identical — existing async callers depend on them." |
| Terminal event only on the success path | "Move it into the `finally` block. Every exit path, including exceptions, must emit a terminal event or the stream hangs." |
| `Depends(get_db)` on a streaming handler | "That holds a pooled DB connection for the entire 15-minute stream and will exhaust the pool. Use a short-lived `async with AsyncSessionLocal()` for the insert, closed before the first yield." |
| Refactors the executor consumer loop for concurrency | "Out of scope. Executor concurrency is a separate change with its own rollout — offset commits under out-of-order completion are not a side quest. Revert it." |
| Converts `main.py` to a `lifespan` context manager | "Revert. Passing `lifespan=` makes Starlette silently ignore every `@app.on_event` handler, which disables the existing `create_all` startup hook with no warning. Add a second `on_event('startup')` pair instead." |
| Adds or alters a column on `agent_runs` | "Revert. Only the new `agent_run_steps` table may be created. The executor uses `create_all`, which never alters existing tables, so a model expecting a new column breaks it against an unmigrated database." |
| Plain `INSERT` into `agent_run_steps` | "Use `ON CONFLICT (run_id, seq) DO NOTHING`. Redelivered messages re-emit the same seq values and a plain insert raises a primary-key violation on every event of the retry." |
| `producer.flush()` after each step event | "Remove it. `flush()` blocks on broker acknowledgement, so per-event flushing adds a network round-trip to every step of every run — including runs that work fine today. Flush only after the terminal event and on shutdown." |
| Adds `sleep()` or a blocking call inside the SSE generator | "Use `asyncio.wait_for` on the queue with a timeout for the heartbeat. A blocking sleep stalls the whole worker's event loop and every request it is serving." |
