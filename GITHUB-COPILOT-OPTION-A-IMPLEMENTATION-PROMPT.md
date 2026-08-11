# GitHub Copilot Prompt — Implement Option A Fully Synchronous Path

Both repositories are open in this VS Code workspace:

- Agentic Orchestration
- Agent Executor

The workspace also contains:

- `OPTION-A-PHASE-0-DISCOVERY.md`
- `OPTION-A-SYNC-BASE-IMPLEMENTATION-PLAN.md`

Use those two files as the source of truth.

## Goal

Implement the short-term **Option A fully synchronous path** while keeping the existing async Kafka path functionally unchanged.

Do not build a second agent runtime.

Refactor shared logic so Kafka and HTTP paths reuse the same execution and finalization code.

## Target flow

```text
Caller
  -> Orchestration /sync endpoint
  -> existing planner
  -> Orchestration sync coordinator
  -> direct internal HTTP call to Executor for ONE step
  -> Executor runs existing agent/tool runtime
  -> returns step result
  -> Orchestration selects next step
  -> repeat
  -> shared Orchestration finalization
  -> HTTP final response
```

Existing async must remain:

```text
Caller -> Orchestration -> Kafka -> Executor -> Kafka
       -> shared finalization -> ResponseService -> Kafka/webhook
```

## Confirmed behaviors you must preserve

1. Orchestration creates `agent_execution` rows as `NOT_STARTED`.
2. Executor writes `IN_PROGRESS`.
3. Executor writes `COMPLETED` on success.
4. Executor failure handling currently does not reliably persist `FAILED`.
5. Async final response is assembled in Orchestration after `AGENT_EXECUTION_FINAL_RESPONSE`.
6. Multi-agent output-generator logic must be reused by sync.
7. Conversational chat-history finalization must be reused by sync.
8. Caller supplies `X-Correlation-ID`.
9. Sync routes must validate it as a UUID.
10. Correlation ID is NOT an idempotency key.
11. Do not automatically retry after Executor may have started.
12. HIL and AG-UI streaming are out of scope for initial sync.
13. Sync and async Executor work require separate concurrency budgets.

## Work phase by phase

Do not make one giant change.

### Phase 1 — Extract shared Orchestration finalization

Inspect:

- `orchestration/service/message_processing_service.py`
- `process_message()`
- `prepare_response()`
- `_prepare_chat_history()`
- current inline success/error response construction

Extract reusable business finalization into a service, preferably:

```text
orchestration/service/finalization_service.py
```

The async Kafka path must then:

```text
process_message()
  -> finalization_service.finalize(...)
  -> ResponseService.respond(...)
```

Preserve current async behavior exactly.

Run existing async tests before continuing.

Do not implement `/sync` yet.

### Phase 2 — Extract Executor one-step execution core

Inspect:

- `executor/service/agent_orchestrator.py`
- `executor/service/agent_execution_service.py`
- `executor/dependencies.py`
- step status and `agent_execution` store/model code

Refactor so one shared function executes exactly one already-planned step.

Kafka path must reuse it and keep the existing routing-slip behavior.

Add:

```text
execution_mode = ASYNC_KAFKA | SYNC_DIRECT
```

Add separate concurrency budgets:

```python
async_step_semaphore
sync_step_semaphore
```

Async Kafka work must retain guaranteed capacity.

Also fix terminal failure persistence:

```text
agent_execution.status = FAILED
```

Do not remove or redesign the existing Kafka path.

Run async regression tests again.

### Phase 3 — Add private Executor HTTP one-step API

Locate the actual Executor FastAPI app and route registration.

Do not invent a new entrypoint if one already exists.

Add an internal-only route conceptually like:

```text
POST /internal/v1/agent-step
```

It must:

- execute exactly one plan step;
- use `SYNC_DIRECT` capacity;
- call the shared one-step execution core;
- return the updated task/step payload;
- not publish the next step to Kafka;
- not publish a final Kafka response;
- return normalized failures;
- be feature/config controlled.

Add integration tests.

### Phase 4 — Add Orchestration sync coordinator

Create a reusable sync coordinator/service.

It must:

1. receive the existing plan;
2. establish an absolute overall deadline;
3. call the Executor internal step route sequentially;
4. merge returned step status/output into the plan;
5. select the next step;
6. stop on failure;
7. stop if the overall deadline is exhausted;
8. stop dispatching future steps if the client disconnects;
9. call shared finalization after the terminal step;
10. return the final business response.

For each step:

```text
effective_step_timeout =
  min(
    configured_step_timeout,
    remaining_overall_time - safety_margin
  )
```

Use one shared async HTTP client per Orchestration process with explicit connection limits.

Do not automatically retry uncertain Executor calls.

### Phase 5 — Add first public sync route

Start only with:

```text
/task-executor/sync
```

Reuse the same:

- JWT authentication;
- guardrails;
- config loading;
- planner;
- `agent_execution` seeding.

Add UUID validation for `X-Correlation-ID`.

Contract:

- caller sends a valid UUID;
- caller sends a new UUID for every new execution;
- UUID is for tracing/correlation;
- UUID is not an idempotency guarantee.

Reject:
- HIL-enabled workflows;
- AG-UI streaming requests;
- use cases failing explicit sync eligibility checks.

Keep the route behind a feature flag.

## Concurrency protection

The Executor will serve Kafka and sync HTTP in the same process.

Do NOT use one shared execution semaphore.

Use separate capacity reservations.

When sync capacity is full:
- fail fast with 429/503;
- do not allow sync work to starve Kafka.

Add/retain metrics such as:

```text
executor_active_steps{execution_mode}
executor_step_duration_seconds{execution_mode}
executor_sync_rejected_total
kafka_consumer_lag
db_pool_checked_out
db_pool_wait_seconds
```

Do not simply increase the DB pool.

## Client disconnect

While Orchestration awaits Executor, monitor the caller concurrently.

Race:

```text
Executor HTTP task
vs
request.is_disconnected() watcher
```

Use `asyncio.wait(..., FIRST_COMPLETED)` or equivalent.

If caller disconnects:
- stop future steps;
- cancel/close the Orchestration-side wait;
- log correlation ID/current step;
- increment disconnect metrics.

Do not assume this cancels an already-running LLM/tool inside Executor.

## Response equivalence

The sync business response must reuse the same finalization rules as async.

Must match:
- `x_correlation_id`
- status
- response
- state
- canonical error semantics
- chat history where applicable

Transport metadata may differ.

For deterministic/mocked tests, use exact structural comparison.

Do not require exact text equality for live LLM responses.

## Scope restrictions

Do not silently expand scope.

- HIL: unsupported for initial sync; reject.
- AG-UI/SSE: out of scope.
- conversational sync routes: only after session-state persistence is verified.
- multimodal parts: preserve current behavior or keep parts-enabled use cases out of sync until offload/reattach semantics are confirmed.
- global idempotency registry: do not add.
- durable result store: do not add.
- `202 + result_url`: do not add.
- WebSocket: do not add.

## Kubernetes / draining

Check existing Helm/OpenShift conventions.

Readiness must become false when draining starts.

Do not admit new sync work during drain.

Ensure:

```text
terminationGracePeriodSeconds
>
maximum sync deadline
+ preStop/drain allowance
+ shutdown cleanup allowance
```

Do not hard-code a final value without checking current deployment conventions.

## Testing required

Before calling the implementation complete, run/create tests for:

1. existing async regression;
2. single-agent sync success;
3. multi-agent sync success;
4. output-generator sync success;
5. tool success;
6. tool failure;
7. runtime failure;
8. explicit `FAILED` DB persistence;
9. UUID validation;
10. sync capacity saturation while Kafka traffic is active;
11. DB pool pressure;
12. overall timeout;
13. step timeout;
14. client disconnect;
15. response equivalence against async golden fixtures.

## Working rules

Before each phase:

1. inspect the real code;
2. list the files/functions you will modify;
3. make the smallest safe refactor;
4. run relevant tests;
5. summarize the result;
6. then proceed.

If code contradicts `OPTION-A-PHASE-0-DISCOVERY.md`, stop and report the contradiction instead of guessing.

Do not redesign beyond Option A.

Do not change existing async API contracts.

Do not duplicate agent execution or final-response logic.
