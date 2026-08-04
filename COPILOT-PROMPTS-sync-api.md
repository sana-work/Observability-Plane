# GitHub Copilot Prompts — Sync/Streaming API (rev. 2026-08-04)

Companion to `SYNC-API-IMPLEMENTATION-PLAN.md`. Run in **Copilot agent mode** in the workspace holding both repos.

Rewritten after finding `gssp_agentic.audit_log`. The build is much smaller than the previous revision: **no new table, no migration, no executor changes, no Kafka work.** Everything is orchestration-side.

**One prompt per session.** Review, test, and commit between each.

---

## Pre-flight — three things to verify before writing code

These are not code. They are how this rollout fails anyway.

**1. Correlation-id propagation — the load-bearing assumption.**

> Trace `x_correlation_id` end to end with exact `file:line`: where the value originates in the orchestration service (middleware, header, generated?); how it is propagated to the executor; and where the executor sets `x_correlation_id` on the rows it writes to `gssp_agentic.audit_log`. Confirm whether the orchestration service can know this exact value at the moment it dispatches an execution, before any audit row exists. Report only what is in the code.

If orchestration cannot predict the value the executor will stamp, stop — the whole design rests on it.

**2. Index on `x_correlation_id`.** The table shows a primary key on `id` and no other index. Polling an unindexed column twice a second per stream means a sequential scan on a growing audit table — that degrades the database for everything, including the endpoints you are protecting.

```sql
\d gssp_agentic.audit_log
```

If missing, add it (purely additive, no write lock, and it also speeds up the existing `get_sub_executions_by_root_agent()` reader):

```sql
CREATE INDEX CONCURRENTLY idx_audit_log_correlation ON gssp_agentic.audit_log (x_correlation_id, sequence_id);
```

**3. Grant.** Confirm the orchestration DB user has `SELECT` on `gssp_agentic.audit_log` — different schema, possibly a different role.

Also outstanding, unchanged: **executor replicas** (it runs one execution at a time), and the **ingress idle-timeout and buffering chain** before `SSE_ENABLED` goes on in any shared environment.

---

## Step 0 — Repo instructions file (do this once)

> Create `.github/copilot-instructions.md` at the workspace root with exactly the content below. Change no other file.
>
> ```markdown
> # Agentic Workflow — Copilot instructions
>
> Two Python services sharing one PostgreSQL database and one Kafka cluster:
> - `181229.genaiservices.agentic-orchestration` — FastAPI + Uvicorn, async, multiple worker processes.
> - `181229.genaiservices.agentic-agent-executor` — Kafka consumer process. Source lives under `excutor/core/…`.
>
> ## The audit log is the event source
> `gssp_agentic.audit_log` (defined in `excutor/core/db/audit_table_pg_store.py`) records every step of every
> execution synchronously, in order, written from `excutor/core/agent/runner.py`. Event types: INVOCATION,
> LLM_REQUEST, LLM_RESPONSE, TOOL_CALL, TOOL_RESULT, ERROR. `x_correlation_id` groups one execution;
> `sequence_id` orders events within it; the terminal row is INVOCATION with agent_status COMPLETED or FAILED,
> written in a `finally` block. Never re-instrument the step loop — read this table.
>
> ## Conventions
> - `confluent_kafka` for Kafka. Never introduce `aiokafka` or `kafka-python`.
> - SQLAlchemy async (`AsyncSession`), Pydantic v2.
> - No Redis exists in this stack. Do not add or suggest it.
>
> ## Hard rules — this work is PURELY ADDITIVE
> Zero impact on currently working code outranks elegance and outranks avoiding duplication.
>
> Frozen — do not change paths, request/response models, status codes, handler logic, or tags:
> - POST /api/v1/agentic-orchestration/task-executor
> - POST /api/v1/agentic-orchestration/conversational-task-executor
> - POST /api/v1/agentic-orchestration/native-conversational-task-executor
> - POST /api/v1/agentic-orchestration/agent-testing
> - GET  /api/v1/agentic-orchestration/execution-status
>
> - The executor repository does not change. If a task seems to need an executor edit, stop and say so.
> - No database schema changes. `audit_log` is read-only from orchestration — never write to it.
> - Do not refactor to share code with existing handlers. Duplicate the logic into the new module instead.
>   Extracting a shared helper out of a working handler is a behaviour change and is forbidden here.
> - Do not add or modify fields on existing Pydantic models. Subclass instead.
> - Do not convert `main.py` to a `lifespan` context manager. Passing `lifespan=` makes Starlette silently
>   ignore every existing `@app.on_event` handler.
> - Do not add dependencies without saying so and pinning them in `requirements.txt`.
> - Do not reformat or reorder imports in files you are not otherwise changing.
> ```

---

## Phase 1 — The four streaming endpoints

This is the whole build.

> ## Goal
>
> `181229.genaiservices.agentic-orchestration` has four task-execution endpoints. Each POSTs, dispatches work to the executor, returns immediately, and leaves the caller polling `GET /api/v1/agentic-orchestration/execution-status`.
>
> Add a **streaming twin for each**, so a caller POSTs once and receives every step of the execution as Server-Sent Events on that same response, ending with the final result.
>
> | Existing (frozen) | New |
> |---|---|
> | `POST .../task-executor` | `POST .../task-executor/stream` |
> | `POST .../conversational-task-executor` | `POST .../conversational-task-executor/stream` |
> | `POST .../native-conversational-task-executor` | `POST .../native-conversational-task-executor/stream` |
> | `POST .../agent-testing` | `POST .../agent-testing/stream` |
>
> `agent-testing` is tagged under both *Task Execution* and *Testing* — one route, listed twice. Create **four** routes, not five.
>
> ## Where the events come from
>
> **Do not instrument anything.** The executor already writes every step to `gssp_agentic.audit_log`, synchronously and in order, from `excutor/core/agent/runner.py`. The stream reads that table:
>
>     SELECT * FROM gssp_agentic.audit_log
>     WHERE x_correlation_id = :cid AND sequence_id > :last
>     ORDER BY sequence_id, id
>
> polled every `SSE_POLL_INTERVAL_MS` (default 500), advancing `:last` past the highest row returned.
>
> Terminal condition: a row with `event_type = 'INVOCATION'` and `agent_status` in (`COMPLETED`, `FAILED`). Verify those exact literals in `excutor/core/agent/runner.py` before relying on them.
>
> Rows for sub-agents (`root_agent_name`, `agent_name`) share the same `x_correlation_id`. **Emit them too** — do not filter. They are part of what the caller wants to see.
>
> Because Postgres is the shared source, any worker process can serve any execution's stream. There is **no** in-memory registry, **no** Kafka consumer, **no** background thread, and **no** startup hook to add.
>
> ## What to build
>
> **1. `app/services/audit_stream_reader.py` (new)**
>
> Read-only. One cursor query as above, and a mapper turning a row into an SSE event payload: `event_type`, `agent_status`/`tool_status`, `agent_name`, `tool_name`, `model_name`, token counts, timestamps, and the relevant text field (`agent_response`, `input_args`, `error_message`). Truncate long text fields with an explicit `…[truncated]` marker.
>
> **2. `app/api/stream_routes.py` (new) — the four routes**
>
> Each mirrors its async twin: same request model (imported unchanged), same auth dependency. Response: `StreamingResponse`, `media_type="text/event-stream"`, headers `Cache-Control: no-cache`, `Connection: keep-alive`, `X-Accel-Buffering: no`.
>
> One shared SSE generator in this module, called by all four routes. That is internal to new code and fine. What is not fine is reaching into the existing handlers' modules to share their dispatch logic — **copy** it.
>
> Handler order:
> 1. authenticate
> 2. resolve `x_correlation_id` — from the incoming `X-Correlation-ID` header if present, otherwise generate one — using the **same mechanism the existing endpoints already use**, so the executor stamps the identical value onto its audit rows
> 3. dispatch to the executor with a **copy** of the async twin's dispatch logic, byte-identical payload. The executor must not be able to tell the difference; that is what guarantees it needs no changes
> 4. return the streaming generator
>
> **3. `app/main.py`** — one `include_router` line with a new tag `"Task Execution (Streaming)"`, registered only when `SSE_ENABLED`. Nothing else in this file changes.
>
> **4. `app/core/config.py`** — new keys only: `SSE_ENABLED` (default `False`), `SSE_POLL_INTERVAL_MS` (500), `SSE_MAX_DURATION_SECONDS` (900).
>
> ## Stream mechanics — none of these are optional
>
> - **Never hold a database session across the stream.** Do not put `db: AsyncSession = Depends(get_db)` on these handlers — a FastAPI-injected session lives for the whole request, and these run up to 15 minutes. Acquire a session per poll with `async with AsyncSessionLocal()` and release it immediately. Holding one would exhaust the pool and take the existing endpoints down with it; that is the one way this feature can break them without touching their code.
> - Emit a `run.accepted` event **immediately** after dispatch, before any audit row exists. The executor processes one execution at a time, so this may sit queued for minutes; a client that connects to silence assumes the endpoint is broken.
> - SSE frame: `id: <sequence_id>`, `event: <event_type>`, `data: <json>`.
> - Emit `: keepalive` every 15 seconds of idle. Use `asyncio.sleep` between polls — never a blocking `sleep`, which would stall the worker's event loop and every request it is serving.
> - Enforce `SSE_MAX_DURATION_SECONDS`; on expiry emit `stream.timeout` and close. The execution continues server-side and stays retrievable via the existing status endpoint.
> - Ownership: check `audit_log.user_id` against the authenticated caller the same way the existing status endpoint does, and drop rows that do not match.
> - On client disconnect, stop polling. There is nothing to unregister.
>
> ## Constraints
> - The executor repository does not change.
> - No schema changes, no migration. `audit_log` is read-only here.
> - No new dependencies — Starlette's `StreamingResponse` is enough. Do not add `sse-starlette`.
> - Write the OpenAPI snapshot test **first**: capture `/openapi.json` filtered to the five frozen paths and assert it is unchanged by this work. That test is the acceptance gate.
>
> ## Done when
> - `pytest` passes, including the OpenAPI snapshot, cursor advancement without duplicates or gaps, terminal-row detection closing the stream, ownership filtering, and max-duration expiry.
> - With `SSE_ENABLED=false`, the new routes are absent from `/openapi.json` and the app behaves exactly as before.
> - With `SSE_ENABLED=true`, `curl -N -X POST .../task-executor/stream -H "Authorization: Bearer <jwt>" -d '{...}'` streams steps live and terminates on its own — and the same request to `/task-executor` still returns its job id unchanged.
> - A run with sub-agent executions streams those events too.

---

## Phase 2 — Reconnect and replay

> Support the SSE `Last-Event-ID` header on the four `.../stream` routes, and add `GET /api/v1/agentic-orchestration/execution-stream` to attach to an execution already in flight — **alongside**, not replacing, the existing `GET .../execution-status`.
>
> The query is unchanged; the cursor simply starts at the supplied `sequence_id` instead of 0. This is why the audit table is the right source — replay needs no extra storage.
>
> If the execution already reached its terminal row, replay everything after the cursor and close immediately without polling.

---

## Phase 3 — Cancellation

> Nothing supports cancellation today. Add `DELETE /api/v1/agentic-orchestration/execution/{x_correlation_id}` in orchestration, signalling the executor to stop.
>
> **This is the only phase that modifies the executor's running step loop.** Keep the change to a single flag check at the top of the loop — no restructuring — exiting via the existing `finally` path so the terminal `INVOCATION` row is still written and any attached stream closes. Gate the whole feature behind a config flag defaulting to off, so nothing new even starts until it is turned on. Enforce the same ownership check as the status endpoint.

---

## Optional later — replace polling with Kafka

Only when concurrent streams outgrow polling. Measure first.

> Publish each audit row to a Kafka topic from inside `AuditTablePGStore.insert_audit_log()` in `excutor/core/db/audit_table_pg_store.py`, after the commit succeeds — one method, one insertion point, wrapped in try/except so a broker problem can never fail an execution. Never call `producer.flush()` per event; it blocks on broker acknowledgement and would add a round-trip to every step of every execution, including ones that work fine today.
>
> Orchestration then consumes into a per-process registry keyed by `x_correlation_id`, and the SSE generator reads from a queue instead of polling. Use `assign()` at `OFFSET_END` rather than `subscribe()` — no consumer group, no rebalances, no dead groups accumulating as workers restart. Keep `audit_log` as the replay source; the reconnect query does not change.

This reintroduces the fan-in machinery Phase 1 avoids — registry, consumer thread, startup hooks, and the `lifespan` hazard. Take it on only when measurement justifies it.

---

## If Copilot goes off the rails

| Symptom | Correction |
|---|---|
| Instruments the executor step loop, or adds step-event publishing | "Revert. `gssp_agentic.audit_log` already records every step synchronously and in order. Read that table; the executor repo does not change." |
| Creates a table or an Alembic migration | "Revert. No schema changes. The audit log is the event source and is read-only from orchestration." |
| `Depends(get_db)` on a streaming handler | "That holds a pooled connection for the whole 15-minute stream and will exhaust the pool, breaking the existing endpoints. Acquire per poll with `async with AsyncSessionLocal()` and release immediately." |
| Converts `main.py` to `lifespan` | "Revert. Passing `lifespan=` makes Starlette silently ignore every `@app.on_event` handler, disabling existing startup code with no warning. Polling needs no startup hook at all." |
| Builds a registry, Kafka consumer, or background thread | "Not needed. Postgres is the shared source, so any worker can serve any stream. Poll the audit table." |
| Blocking `sleep()` in the generator | "Use `asyncio.sleep`. A blocking sleep stalls the worker's event loop and every request it is serving." |
| Filters out sub-agent rows | "Keep them. Sub-agent events share the `x_correlation_id` and are part of what the caller wants to see." |
| Modifies an existing handler or extracts a shared helper from one | "Revert. Copy the dispatch logic into the new module. The five existing routes must stay byte-identical, including their OpenAPI schema." |
| Adds `sse-starlette` or another dependency | "Revert. Starlette's built-in `StreamingResponse` is sufficient." |
