# Sync/Streaming API — Implementation Plan (rev. 2026-08-04)

Supersedes the previous revision. The earlier plan was built on `SYNC-API-DISCOVERY-REPORT.md`, which described code that does not exist in these repos. This revision is based on the `audit_log` table and executor source read directly.

---

## What changed, and why this got much smaller

`gssp_agentic.audit_log` already records **every step of every execution, synchronously, in order**. It is the event source. There is nothing to instrument and no table to create.

Everything below follows from that:

| Previously planned | Now |
|---|---|
| New `agent_run_steps` table + Alembic migration + ORM models in two services | **Deleted.** No schema change. |
| Instrument 7 sites in the executor step loop | **Deleted.** Already instrumented. |
| Per-run `seq` counter, terminal-event guarantee, `ON CONFLICT` handling | **Deleted.** `sequence_id` and the `finally`-written terminal row already exist. |
| Kafka `agent.step.events` producer | **Deferred** to an optional upgrade (see the last section). |
| Per-worker Kafka consumer, in-memory registry, background thread, startup hooks | **Deleted.** Postgres is the fan-in point; see below. |
| Key everything on `run_id` | Key everything on **`x_correlation_id`**. |

**The executor repository does not change at all.** The entire feature is orchestration-side.

### ⚠️ Report references are withdrawn

The previous revision cited `app/agent/runner.py`, `app/services/kafka_consumer.py`, an `agent_runs` table, and an `AgentRunRequest` model. The real executor lives under `excutor/core/…` (`excutor/core/agent/runner.py`, `excutor/core/db/audit_table_pg_store.py`). Treat every `file:line` from the discovery report as unverified, including the Kafka topic names, partition counts, and `max.poll.interval.ms` value quoted in the blockers below.

---

## The event source

`gssp_agentic.audit_log`, defined in `excutor/core/db/audit_table_pg_store.py` (`AuditTablePGStore._create_audit_log_table`).

Written by `excutor/core/agent/runner.py:24-120` via `await audit_store.insert_audit_log(...)` at each boundary, blocking until the insert commits (`audit_table_pg_store.py:56-59`).

| `event_type` | `agent_status` / `tool_status` | When |
|---|---|---|
| `INVOCATION` | `STARTED` | before the step loop |
| `LLM_REQUEST` | `RUNNING` | before each LLM call |
| `LLM_RESPONSE` | `RUNNING` | after each LLM call (carries token counts) |
| `TOOL_CALL` | `STARTED` | before each tool execution |
| `TOOL_RESULT` | `COMPLETED` | after each tool returns |
| `ERROR` | `FAILED` | on any unhandled exception |
| `INVOCATION` | `COMPLETED` / `FAILED` | in `finally` — always |

Columns relevant here: `id` (autoincrement PK), `x_correlation_id` (groups one execution), `sequence_id` (monotonic within an execution), `event_type`, `agent_status`, `tool_name`, `tool_status`, `agent_name`, `root_agent_name`, `model_name`, `input_args`, `agent_response`, `error_type`, `error_message`, `input_token_count` / `output_token_count` / `total_token_count`, `start_timestamp`, `end_timestamp`, `created_at`, `user_id`, `session_id`, `invocation_id`, `usecase_name`, `function_call_id`.

`root_agent_name` and `get_sub_executions_by_root_agent()` (`audit_table_pg_store.py:68-75`) indicate **sub-agent executions**. Nested agents share the `x_correlation_id`, so their events stream naturally with no extra work — do not filter them out.

---

## Design: SSE, sourced by polling `audit_log`

```
POST /api/v1/agentic-orchestration/task-executor/stream
  │
  ├─ auth (existing dependency, unchanged)
  ├─ resolve x_correlation_id  (incoming X-Correlation-ID header, else generate)
  ├─ dispatch to the executor  (byte-identical copy of the async twin's dispatch)
  └─ return StreamingResponse(text/event-stream)
          │
          └─ every ~500ms:
             SELECT … FROM gssp_agentic.audit_log
             WHERE x_correlation_id = :cid AND sequence_id > :last
             ORDER BY sequence_id, id
             → emit each row as an SSE event, advance :last
             → stop on INVOCATION + (COMPLETED|FAILED)
```

### Why polling, and what it removes

Postgres is already the shared fan-in point, so the hard part of the original design evaporates:

- **No in-memory registry.** Any worker can serve any execution's stream.
- **No Kafka consumer, no background thread, no consumer groups, no offset handling.**
- **No startup/shutdown hooks** — which removes the `lifespan` trap entirely (passing `lifespan=` to `FastAPI()` silently disables existing `@app.on_event` handlers). Nothing in `main.py` changes except one `include_router` line.
- **No 4-uvicorn-worker fan-in problem.** It was only a problem because events arrived on a per-process Kafka consumer.
- **No new Kafka topic** to create on the cluster.

Latency cost is ≤ one poll interval — invisible against steps that take seconds. At the executor's current concurrency ceiling this is a handful of indexed lookups per second.

---

## Prerequisites — verify all three before building

**1. Can orchestration know the `x_correlation_id` before the executor writes it?** This is the load-bearing assumption. The stream must query on the same value the executor stamps onto its audit rows. Your logs show `X-Correlation-ID` already flowing as a request header, so a middleware almost certainly sets or generates it — confirm it is propagated into the executor and lands in `audit_log.x_correlation_id` unchanged. If orchestration cannot predict this value, nothing else in this plan works.

**2. Is there an index on `x_correlation_id`?** The table definition shows a primary key on `id` and no other index. Polling an unindexed column on a growing audit table means a sequential scan twice a second per stream — that would degrade the database for everything, including the endpoints you are protecting. If it is missing:

```sql
CREATE INDEX CONCURRENTLY idx_audit_log_correlation
  ON gssp_agentic.audit_log (x_correlation_id, sequence_id);
```

`CONCURRENTLY` avoids taking a write lock. This is the one schema change that may be required, it is purely additive, and it also speeds up the existing `get_sub_executions_by_root_agent()` reader.

**3. Does the orchestration DB user have `SELECT` on `gssp_agentic.audit_log`?** Different schema, possibly a different role. Confirm the grant rather than discovering it at runtime.

---

## What to build

All in `agentic-orchestration`. Four new routes, one new module.

| Existing (frozen) | New |
|---|---|
| `POST .../task-executor` | `POST .../task-executor/stream` |
| `POST .../conversational-task-executor` | `POST .../conversational-task-executor/stream` |
| `POST .../native-conversational-task-executor` | `POST .../native-conversational-task-executor/stream` |
| `POST .../agent-testing` | `POST .../agent-testing/stream` |

`agent-testing` is tagged under both *Task Execution* and *Testing* in the current OpenAPI — one route, listed twice. Four new routes, not five.

| File | Change |
|---|---|
| `app/api/stream_routes.py` | **New.** All four routes plus one shared SSE generator. |
| `app/services/audit_stream_reader.py` | **New.** The cursor query and row → SSE-event mapping. Read-only. |
| `app/main.py` | **One** `include_router` line. Nothing else. |
| `app/core/config.py` | New keys only: `SSE_ENABLED` (default `False`), `SSE_POLL_INTERVAL_MS` (500), `SSE_MAX_DURATION_SECONDS` (900). |

### Stream mechanics

- **Never hold a DB session across the stream.** Acquire from the pool per poll and release immediately. A session held for a 15-minute request would exhaust the pool and take the existing endpoints down with it — the one way this feature can break them without editing their code.
- Emit `run.accepted` immediately on dispatch, before any audit row exists. The executor is serial and an execution may sit queued; a client that connects to silence assumes the endpoint is broken.
- SSE frame: `id: <sequence_id>`, `event: <event_type>`, `data: <json row>`.
- Heartbeat `: keepalive` every 15s of idle, or a proxy will drop the connection during a slow LLM call.
- Terminate on `event_type='INVOCATION'` with `agent_status` in (`COMPLETED`, `FAILED`). Confirm those literals against the code.
- Cap the stream at `SSE_MAX_DURATION_SECONDS`; emit `stream.timeout` and close. The execution continues.
- On client disconnect, just stop polling. Nothing to clean up — another advantage of having no registry.
- Enforce the same ownership check the existing status endpoint uses, against `audit_log.user_id`.
- Order by `sequence_id, id` and track the last `sequence_id`. (If executor concurrency is added later, revisit: with concurrent inserts a lower `id` can commit after a higher one, so a strict `id >` cursor could skip rows.)

### Zero-impact properties

- Executor repo: **no change**.
- Database: **no change**, except possibly one `CREATE INDEX CONCURRENTLY`.
- `main.py`: one line.
- Existing handlers, models, routes: untouched. Duplicate dispatch logic into the new module rather than extracting shared helpers out of working code.
- Everything gated behind `SSE_ENABLED=False` — routes are not registered at all when off.
- Acceptance gate: a test that snapshots `/openapi.json` filtered to the five existing paths and asserts it is unchanged. Write it first.

---

## Blockers that remain

**Executor concurrency.** Confirmed: one execution at a time per process — `asyncio.run()` inside a synchronous poll loop. SSE does not make this worse, it makes it visible; async callers already queue, they just can't see it. Fix by running more executor replicas (no code change), not by reworking the consumer loop — offset commits under out-of-order completion are a separate project. Check `max_connections` before scaling; the report's pool figures are unverified but orchestration is multi-worker and each executor replica adds connections.

**Ingress timeouts and buffering.** No K8s or ingress manifests in either repo, so this chain is unknown. Before enabling `SSE_ENABLED` anywhere shared, confirm: idle/read timeout ≥ 900s, response buffering off (`proxy_buffering off`), no compression on `text/event-stream`, HTTP/1.1 not downgraded.

---

## Upgrade path: Kafka, when polling stops paying

Polling is right while concurrent streams are in the tens. If that changes, publish each audit row to Kafka instead of polling for it — **without** re-instrumenting the step loop:

Add the publish inside `AuditTablePGStore.insert_audit_log()`, after the commit succeeds. One method, one insertion point, wrapped in try/except so a broker problem can never fail an execution. Orchestration then consumes into a per-process registry and the stream reads from a queue.

That reintroduces the fan-in machinery this revision deleted (registry, consumer thread, startup hooks, the `lifespan` trap), so it is worth doing only when measurement says polling costs more than that complexity. Keep `audit_log` as the replay store either way — the reconnect query is the same.

---

## Phasing

**Phase 0 — verify.** The three prerequisites above, plus executor replicas and the ingress chain.

**Phase 1 — the four streaming endpoints.** This is now the whole build.

**Phase 2 — reconnect.** Support `Last-Event-ID: <sequence_id>` on the four routes and add `GET .../execution-stream` to attach to an execution already in flight. The query is unchanged — the cursor simply starts at the supplied value instead of 0. If the execution already terminated, replay from the table and close.

**Phase 3 — cancellation.** Nothing supports it today. This is the only phase that touches the executor's running step loop; keep it to a single flag check at the top of the loop, behind a config flag defaulting to off.
