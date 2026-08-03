# Sync/Streaming API — Implementation Plan

Based on `SYNC-API-DISCOVERY-REPORT.md` (2026-08-03).
Services: `181229.genaiservices.agentic-orchestration`, `181229.genaiservices.agentic-agent-executor`.

---

## Decision: SSE

Confirmed after reading the report. The evidence is stronger than it was on general principles:

- **No WebSocket anywhere** — no handler, no upgrade config, no ingress manifest. WS is not "already working," it's a from-scratch build with an unknown gateway story.
- **FastAPI + Uvicorn async** — `StreamingResponse` is native, no new server, no new dependency.
- **Auth just works.** `Depends(get_current_user)` (`app/core/auth.py:10-22`) applies unchanged to an SSE POST. WS handshake auth would need a separate mechanism.
- **The correlation race is real here.** Today's POST returns 202 and the client polls. If you bolt on a WS subscribe-after-POST, every event between `producer.flush()` and subscribe is lost. With SSE the same request starts the run and streams it — register the queue *before* producing, and the gap closes.
- **No ingress config exists yet**, so the SSE timeout/buffering chain gets configured correctly from day one rather than retrofitted. (Still must be confirmed with whoever owns infra — see Blocker 3.)

Fan-in mechanism: **ephemeral broadcast consumer group per worker process** (no Redis needed — the report confirms none exists), plus a new `agent_run_steps` table for replay.

---

## Three blockers to settle before writing SSE code

### Blocker 1 — the executor runs exactly one agent at a time (highest priority)

`app/services/kafka_consumer.py:31-35`:

```python
try:
    asyncio.run(run_agent(payload))   # blocks the entire process
    consumer.commit(message=msg)
```

A synchronous `while True: consumer.poll()` loop calling `asyncio.run()` per message, single process, single thread. Run #2 waits for run #1 — up to 10 minutes (`MAX_STEPS=10`, no per-step timeout on `call_llm`).

**Confirmed** against the live repos on 2026-08-03 — this is not an inference from the report.

Two things follow, and they pull in opposite directions:

- **SSE does not make this worse; it makes it visible.** The executor is serial in production today. Async callers simply cannot see it — they POST, get an id, and poll until whenever. This is therefore not a reason to hold the sync API back, only a reason to know the ceiling before publishing one.
- **The fix does not belong in this change.** Reworking the consumer loop is the most invasive edit available in either repo, and it conflicts directly with the zero-impact requirement.

So:

- **Now:** run N executor replicas (topic has 6 partitions, so up to 6 concurrent runs). No code change at all. Enough for a pilot. Watch consumer lag on `agent.run.requests` as the queue-depth signal, and check `max_connections` first (see Pre-flight).
- **Later, as its own project:** bounded `asyncio.Semaphore` task pool with `pause()`/`resume()` on the partitions under pressure. The hard part is not the semaphore — it is offset commits once completions finish out of order, which needs a lowest-contiguous-completed watermark per partition. Commit offset 3 while 1 and 2 are still running and you lose them on restart.

Because a run may sit queued for minutes, the SSE stream must emit a `run.accepted` event immediately on dispatch, then heartbeats, then `run.started` when execution actually begins. A client that connects to silence assumes the endpoint is broken.

Related: `max.poll.interval.ms = 600000` means a run exceeding 10 min gets the consumer evicted and the message **redelivered — the agent runs twice**. Under a sync API the caller watches that happen live.

### Blocker 2 — poison messages loop forever

`kafka_consumer.py:31-35` — on exception the offset is not committed, there is no DLQ, and no retry counter. One bad message blocks its entire partition indefinitely. Add a retry count + DLQ topic before putting user-visible latency on this path.

### Blocker 3 — the production network path is unknown

No K8s manifests, Helm charts, or ingress config in either repo (§6). The single biggest SSE risk — an upstream proxy idle-timeout or `proxy_buffering on` — cannot be assessed from the code. **Get this confirmed by whoever owns the infra repo before Phase 2.** Required settings:

- ingress read/idle timeout ≥ intended max stream duration (suggest 900s)
- `proxy_buffering off` (nginx) / response buffering disabled at the gateway
- HTTP/1.1 (not downgraded), no response compression on `text/event-stream`

---

## Architecture

```
POST /api/v1/agent/run/stream
  │
  ├─ auth (existing get_current_user)
  ├─ INSERT agent_runs (status=PENDING, delivery_mode='sync')
  ├─ registry.register(run_id) → asyncio.Queue      ◄── BEFORE the produce. closes the race.
  ├─ produce → agent.run.requests                        (unchanged)
  └─ return StreamingResponse(text/event-stream)
            ▲
            │ queue.get()
     ┌──────┴───────────────────────────────┐
     │ per-worker-process broadcast consumer│
     │ group.id = orch-sse-<uuid4>          │
     │ auto.offset.reset = latest           │
     │ topics: agent.step.events,           │
     │         agent.run.results            │
     └──────▲───────────────────────────────┘
            │
   agent.step.events  ◄── NEW producer in executor, wired to the
                          structlog sites that already exist
```

The executor keeps producing to Kafka exactly as it does now. **The sync path does not bypass Kafka** — it is a live tap on the same stream, so async callers, sync callers, and the observability plane all see identical events.

### Why the fan-in is needed even in a single pod

`Dockerfile:11` — `uvicorn --workers 4`. That's 4 **separate OS processes**. The worker holding the SSE connection is not necessarily the worker whose consumer received the event. The registry is per-process, so the consumer must be per-process too, with a unique `group.id` per process (not per pod).

⚠️ `auto.offset.reset` must be **`latest`** for these groups. The executor's existing consumer uses `earliest` (`kafka_consumer.py:14`); copying that would make every new SSE consumer replay a full day of `agent.step.events` on startup.

⚠️ Ephemeral group IDs accumulate in Kafka. Set `offsets.retention.minutes` low on the cluster, or reap them.

---

## Change list

### `agentic-agent-executor` — emit the step events

The topic **already exists**: `agent.step.events` in `config/kafka_topics.json` (6 partitions, rf 3, 1 day) and `KAFKA_STEP_EVENTS_TOPIC` in `app/core/config.py:11`. Declared, never produced to. This was clearly the intended design — build it.

| File | Change |
|---|---|
| `app/services/step_publisher.py` | **New.** `publish_step_event(run_id, seq, event_type, data)` → produce to `agent.step.events`, `key=run_id`, reuse the existing producer config from `kafka_producer.py:10-16` (acks=all, idempotence on — keep it). Never `flush()` per event: it blocks on broker ack and would add a round-trip to every step of every run, including the async runs that work fine today. Flush after the terminal event and on shutdown. Truncate payload strings so encoded messages stay under 512 KB (default `max.message.bytes` is 1 MB). |
| `app/agent/runner.py` | Wire the publisher into the step boundaries that **already exist as structlog calls** — lines 26, 31, 46, 48, 55, 62-64. Add a per-run monotonic `seq` counter. |
| `app/agent/llm_client.py:12` | Emit `llm.started` / `llm.completed` around the existing log call. |
| `app/agent/runner.py:55-65` | The `finally` block must **always** emit a terminal event (`run.completed` / `run.failed`). A stream with no terminal event hangs the client until timeout. |
| `app/services/result_service.py` | Also persist each step event to the new `agent_run_steps` table, same `seq`. This is what makes replay possible. |

**Event envelope** — align with the observability plane so these feed it directly:

```json
{
  "run_id": "uuid",
  "seq": 3,
  "event_type": "tool.started",
  "ts": "2026-08-03T18:52:00Z",
  "tenant_id": "...",
  "user_id": "...",
  "trace_id": "...",
  "data": { "tool": "search", "args": {...} }
}
```

`trace_id` does not exist today (§9: no W3C context in the envelope, and OTel is installed but never initialized). Add the field now while you're touching the schema — it's free here and expensive to retrofit.

**Taxonomy:** `run.started`, `step.started`, `llm.started`, `llm.completed`, `tool.started`, `tool.completed`, `step.completed`, `run.max_steps_exceeded`, `run.completed`, `run.failed`.

### `agentic-orchestration` — the SSE endpoint

| File | Change |
|---|---|
| `app/api/stream_routes.py` | **New module** holding all four `.../stream` routes — one per existing task-execution endpoint. Reuses the existing request models and auth dependency unchanged. Registers the queue **before** producing to Kafka, returns `StreamingResponse(media_type="text/event-stream")`. |
| `app/services/stream_registry.py` | **New.** Module-level `dict[run_id, asyncio.Queue]`. `register()` / `unregister()` / `publish()`. Per-process. Bounded queues, cap concurrent registrations. |
| `app/services/step_consumer.py` | **New.** Live tail of `agent.step.events` + `agent.run.results` via `assign()` at `OFFSET_END` — **no consumer group**, no offset commits, no rebalances, no dead-group accumulation. On message: look up `run_id` in the registry, push if present, drop otherwise. |
| `app/main.py` | One `include_router` line, plus an **additional** `@app.on_event("startup")`/`("shutdown")` pair for the consumer thread. **Do not convert to `lifespan`** — see the trap below. |
| `app/core/config.py` | Add new keys only (`SSE_ENABLED`, `SSE_MAX_DURATION_SECONDS`, …). Leave `KAFKA_AGENT_RESULT_TOPIC` and `KAFKA_CONSUMER_GROUP_ID` exactly as they are even though nothing reads them here — something outside these repos might. |
| `db_migrations/versions/0002_*.py` | **New** Alembic migration creating `agent_run_steps` and nothing else. |

> ### ⚠️ The `lifespan` trap
>
> Passing `lifespan=` to the `FastAPI()` constructor makes Starlette **silently ignore every `@app.on_event` handler** — no warning, no error. The existing `create_all` startup hook would simply stop running, and nobody would notice until a deploy against a fresh database came up with no tables.
>
> Multiple `@app.on_event("startup")` handlers are all executed, so adding a second one is strictly additive and leaves the existing hook untouched. `on_event` being deprecated is not a reason to touch working startup code in this change.

**Threading note.** The existing code uses `confluent_kafka.Consumer`, which is blocking. Two options in an async worker:

1. Run `consumer.poll(0.1)` in a background thread; hand off with `loop.call_soon_threadsafe(queue.put_nowait, evt)`. ~40 lines, **no new dependency**. Recommended.
2. Switch the orchestration-side consumer to `aiokafka`. Cleaner async, but adds a second Kafka client library to the stack.

Do not call blocking `poll()` on the event loop — it stalls every request that worker is serving.

### New table

```sql
CREATE TABLE agent_run_steps (
  run_id     VARCHAR     NOT NULL REFERENCES agent_runs(run_id) ON DELETE CASCADE,
  seq        INTEGER     NOT NULL,
  event_type VARCHAR     NOT NULL,
  payload    JSONB,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (run_id, seq)
);
```

> ### ⚠️ Open decision: an existing audit table may already cover this
>
> The discovery report claimed no per-step record exists anywhere (§5), but that report also documented the wrong endpoint paths, so treat the claim as unverified. An `audit_table` reportedly exists in these services. **Settle this before writing the migration.**
>
> Reuse it only if all four hold:
>
> 1. It stores a row **per agent step** (tool call, LLM call) — not per API call or per run. If it is run-level, it cannot drive a step stream at all and the question is moot.
> 2. It has a **monotonically increasing column that orders rows within a run**. A global `BIGSERIAL` is fine — `Last-Event-ID` only needs comparability, queried as `WHERE run_id = ? AND id > ?`. A timestamp alone is not sufficient: ties and clock skew cause duplicate or dropped events on reconnect.
> 3. Writes are **synchronous in the executor's run path**, not batched or emitted by API-boundary middleware.
> 4. Its **retention** suits operational telemetry (days), and its existing readers — compliance exports, dashboards, anything outside these repos — tolerate 10–40 additional rows per agent run.
>
> If any fail, keep the dedicated table. Under a zero-impact constraint, a new table nothing currently reads is the lowest-risk object that can be added to this database; writing high-volume telemetry into an audit trail with existing consumers has a materially larger blast radius than creating one. Failing (4) in a regulated environment is a compliance question, not an engineering one.

`agent_runs` has no per-step record and no sequence column today (§5, unverified). Both services share `agentdb` and the executor writes to it directly, so the executor can write this table without an API boundary — consistent with the existing (if questionable) pattern.

Three things this DDL is deliberately doing:

- **`ON DELETE CASCADE`** — without it, any existing archival or cleanup job that deletes `agent_runs` rows starts failing on a foreign-key violation the day this ships.
- **No separate index** — the primary key already covers `(run_id, seq)`.
- **One new table, no changes to `agent_runs`.** Earlier drafts of this plan added a `delivery_mode` column; that is now dropped deliberately. The executor bootstraps via `Base.metadata.create_all`, which creates missing tables but **never alters existing ones**, so an ORM model expecting a new column breaks the executor against an unmigrated database. Record the delivery mode in the `run.started` event payload instead.

Inserts must be **`ON CONFLICT (run_id, seq) DO NOTHING`**. A run that exceeds `max.poll.interval.ms` is evicted and redelivered, so the executor re-runs it and re-emits `seq` 1..N for the same `run_id` — a plain insert raises a primary-key violation on every event of the retry, losing the step history for exactly the runs worth debugging.

The executor has no migrations directory and relies on `Base.metadata.create_all`. Add the ORM model to **both** services' `app/models/` so either bootstrap path produces the table.

**Deploy order: migration → executor → orchestration.** Executor code that writes to this table, shipped before the migration runs, fails on every write.

### Connection-pool safety — the way this breaks the existing endpoints

The new endpoints share a database pool with the four frozen ones, so exhausting it is a regression even though no existing file changed.

**Never put `db: AsyncSession = Depends(get_db)` on a streaming handler.** A FastAPI-injected session lives for the whole request, and these requests run up to 15 minutes. At 30 connections per worker, roughly 30 concurrent streams would drain the pool and start failing the existing endpoints. Use a short-lived `async with AsyncSessionLocal()` for the initial insert, closed before the first yield; the stream itself reads from the in-memory queue and touches no database.

Check `max_connections` before scaling anything: orchestration is already 30 × 4 workers = 120 per pod against a PostgreSQL default of 100, and each executor replica adds up to 15.

### Stream mechanics — don't skip these

- **Heartbeat:** `: keepalive\n\n` every 15s. Without it, an idle proxy kills a stream during a slow LLM call.
- **Max duration:** cap at ~900s, emit a `stream.timeout` terminal event, then close. Run continues server-side.
- **Client disconnect:** `unregister()` in a `finally`. The run keeps going; result stays available via the existing `GET /agent/run/{run_id}/status`.
- **Ownership check:** verify `event.user_id == current_user["sub"]` before emitting. §7 notes there is no tenant-level isolation beyond the `user_id` check on the poll endpoint — don't widen that gap on a new endpoint.
- **Backpressure:** bounded queue (e.g. 100). On overflow, drop and emit a `stream.lagged` marker rather than growing unbounded.
- **`Cache-Control: no-cache`, `X-Accel-Buffering: no`** on the response — the second one disables nginx buffering per-response, useful insurance given Blocker 3.

---

## Phasing

**Phase 0 — unblock.** Executor concurrency (replicas), DLQ + retry cap, confirm the ingress timeout chain.

**Phase 1 — step events.** Executor produces to `agent.step.events` + writes `agent_run_steps`. Ships independently: it feeds the observability plane whether or not SSE exists, and it's the piece with real design content. Verify with `kafka-console-consumer` before touching orchestration.

**Phase 2 — SSE.** Registry + per-process consumer + `POST /agent/run/stream`.

**Phase 3 — hardening.** `Last-Event-ID` replay from `agent_run_steps`; cancellation (`DELETE /agent/run/{run_id}` → `agent.run.commands` topic → `is_cancelled` check in the step loop — none of this exists today, §10); token-level streaming (`stream=True` on `client.chat.completions.create`, `llm_client.py:15-19`).

Keep `POST /agent/run` and the polling endpoint exactly as they are. The `callback_url` webhook (`runner.py:67-78`) also stays — three delivery modes: poll, webhook, stream.

---

## Smaller findings worth fixing while you're in here

- **`AgentRunStatusResponse` is defined with an `error` field but never used as a response model** (`agent_schemas.py:17-22`). The status endpoint returns `AgentRunResponse`, so `agent_runs.error` is silently dropped — a failed run reports `status=FAILED` with no reason. Looks like a straight bug; fix it, and make sure the SSE `run.failed` event carries the error.
- **OTel is installed but never initialized** (§9) — four packages, no `TracerProvider`. Either wire it up or drop the dependencies. If you add `trace_id` to the step envelope, wiring it up is the natural moment.
- **`config/kafka_topics.json` is applied by nothing** — no Terraform, no admin client, no Helm hook (§3). Before Phase 1 you need a real answer for how `agent.step.events` comes to exist in production.
