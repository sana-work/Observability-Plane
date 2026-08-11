# Option A — Base Fully Synchronous Implementation Plan

## 1. Objective

Implement a new fully synchronous execution path while keeping the existing asynchronous Kafka path unchanged.

The synchronous path must:
- expose separate `/sync` endpoints;
- keep planning in Orchestration;
- let Orchestration drive multi-step plans sequentially;
- call Executor directly over internal HTTP for one step at a time;
- reuse the same Executor agent/tool execution core as Kafka;
- reuse the same Orchestration final-response assembly used by async;
- return the final caller-facing result directly over HTTP;
- protect async Kafka workloads from sync bursts;
- avoid automatic retries after an Executor request may have started.

This is the short-term Option A implementation, not the future durable orchestration design.

## 2. Scope

### In scope
- New synchronous routes.
- Multi-agent sync execution controlled by Orchestration.
- Private Executor one-step HTTP route.
- Shared Executor execution core.
- Shared Orchestration finalization logic.
- Explicit `FAILED` persistence in `agent_execution`.
- UUID validation for sync `X-Correlation-ID`.
- Separate sync/async Executor concurrency budgets.
- Overall and per-step deadlines.
- Client-disconnect detection.
- Metrics, feature flags, graceful draining.
- Golden async-vs-sync regression tests.

### Out of scope
- SSE / AG-UI streaming.
- WebSocket.
- Kafka-backed sync response.
- Durable final-result store.
- `202 + result_url`.
- Global idempotency/deduplication registry.
- Automatic retries after uncertain execution.
- HIL-enabled sync workflows.
- Replacing the existing async Kafka routing-slip path.
- Workflow-state/sharding redesign.

## 3. Confirmed current behavior

### `agent_execution`
- Orchestration seeds plan steps as `NOT_STARTED`.
- Executor writes `IN_PROGRESS` when a step begins.
- Executor writes `COMPLETED` on success.
- Executor failure handling puts `FAILED` into the Kafka payload but does not reliably persist `FAILED`.
- `task_id` is the correlation ID and there are multiple step rows per execution.

### Final response
The current async Orchestration consumer performs business finalization after `AGENT_EXECUTION_FINAL_RESPONSE`:
- success/failure detection;
- single-agent response selection;
- multi-agent final-step selection;
- output-generator LLM synthesis when enabled;
- canonical error construction;
- state propagation;
- chat-history update;
- delivery via `ResponseService`.

The sync path must reuse this business finalization logic, but return via FastAPI instead of `ResponseService`.

### Correlation ID
- Caller supplies `X-Correlation-ID`.
- It is currently a plain string.
- It is used as `agent_execution.task_id`, Kafka key, log correlation value, and outbound header.
- It is not an idempotency key.
- Duplicate correlation IDs can overwrite/update previous `(task_id, step_id)` rows.

Sync contract:

> Caller must provide a valid, unique UUID for every new execution.  
> `X-Correlation-ID` is for tracing/correlation only and is not an idempotency guarantee.

## 4. Target architecture

```text
Caller
  |
  v
Orchestration /sync API
  | auth / guardrail / UUID validation
  | load config / build plan / seed agent_execution
  v
Sync Execution Coordinator
  |
  | direct HTTP: one step
  v
Executor internal step API
  | acquire SYNC_DIRECT capacity
  | mark IN_PROGRESS
  | run existing agent/tool runtime
  | mark COMPLETED or FAILED
  | return step result
  v
Sync Coordinator
  | merge result / choose next step / repeat
  v
Shared Finalization Service
  | output generator if configured
  | canonical success/error
  | state
  | chat history
  v
HTTP final response
```

Existing async remains:

```text
Caller -> Orchestration -> Kafka -> Executor -> Kafka
       -> shared finalization -> ResponseService -> Kafka/webhook
```

## 5. API rollout

Recommended order:
1. `/task-executor/sync`
2. `/agent-testing/sync`
3. `/conversational-task-executor/sync` after session-state verification
4. `/native-conversational-task-executor/sync` after session-state verification

Rules:
- existing authentication unchanged;
- sync `X-Correlation-ID` must parse as UUID;
- caller uses a new UUID for every new execution;
- reject HIL-enabled use cases;
- AG-UI streaming is unsupported on Option A sync routes;
- routes are feature-flagged.

Expected business response should reuse the existing async final shape, for example:

```json
{
  "x_correlation_id": "<uuid>",
  "status": "SUCCESS",
  "response": {},
  "event_type": "EXECUTION_FINAL_RESPONSE",
  "state": {}
}
```

Transport-only headers may differ.

## 6. Orchestration changes

### 6.1 Extract shared finalization

Refactor `orchestration/service/message_processing_service.py`.

Move reusable business logic from:
- `process_message()`;
- `prepare_response()`;
- `_prepare_chat_history()`;
- inline success/failure dict construction.

Create a shared service such as:

```text
orchestration/service/finalization_service.py
```

Conceptual API:

```python
async def build_final_response(task_payload, orchestrator_config) -> dict:
    ...

async def finalize(task_payload, usecase_config, orchestrator_config) -> dict:
    ...
```

It owns:
- failed-step detection;
- canonical error payload;
- single-agent final response;
- multi-agent last-step response;
- output-generator synthesis;
- state propagation;
- chat-history preparation;
- final business-response dict.

Async after refactor:

```text
process_message()
  -> finalization_service.finalize(...)
  -> ResponseService.respond(...)
```

Sync:

```text
sync coordinator
  -> finalization_service.finalize(...)
  -> FastAPI response
```

Exit gate: async behavior/tests remain unchanged.

### 6.2 Add Sync Execution Coordinator

Create a dedicated Orchestration service such as:

```text
orchestration/service/sync_execution_service.py
```

Responsibilities:
1. receive planned `TaskPayloadModel`;
2. establish absolute overall deadline;
3. iterate through plan steps;
4. call Executor internal HTTP route;
5. merge returned step status/output into the in-memory plan;
6. stop on failure;
7. stop on client disconnect;
8. stop when deadline is exhausted;
9. finalize using shared finalization service;
10. return business response.

Do not publish sync intermediate steps to Kafka.

### 6.3 Executor HTTP client

Use one shared async HTTP client per Orchestration process.

Configure:
- connection pool limits;
- connect timeout;
- pool-acquisition timeout;
- read timeout from remaining deadline;
- internal Executor URL from configuration.

Do not automatically retry after a request may have been accepted.

## 7. Executor changes

### 7.1 Extract one-step execution core

Refactor current Executor routing so transport is separate from execution.

Target:

```text
Kafka consumer
  -> shared one-step execution
  -> existing Kafka next/final routing

HTTP internal route
  -> shared one-step execution
  -> direct step result
```

Conceptually:

```python
async def run_single_step(task_payload, execution_mode) -> TaskPayloadModel:
    ...
```

Reuse current:
- `AgentExecutionService.execute()`;
- `parse_content()`;
- `_convert_response()`;
- config lookup;
- AgentFactory/runtime;
- SessionManager;
- tools;
- audit behavior;
- step output conventions.

When `execution_mode == SYNC_DIRECT`, do not publish next/final Kafka messages.

### 7.2 Private Executor HTTP route

Locate the real Executor FastAPI route registration and add an internal route, e.g.:

```text
POST /internal/v1/agent-step
```

Requirements:
- one already-planned step only;
- internal-only exposure;
- internal service auth if supported;
- schema validation;
- sync-capacity enforcement;
- return updated step/task result;
- normalized failure response;
- feature flag.

### 7.3 Persist `FAILED`

For terminal step failures, explicitly persist:

```text
agent_execution.status = FAILED
```

before returning the failure result.

Keep existing `IN_PROGRESS` and `COMPLETED` behavior.

## 8. Protect async capacity

Sync HTTP and async Kafka work share one Executor event loop, CPU/memory, and DB pool.

Use separate budgets:

```python
async_execution_semaphore = Semaphore(ASYNC_EXECUTOR_MAX_ACTIVE)
sync_execution_semaphore = Semaphore(SYNC_EXECUTOR_MAX_ACTIVE)
```

Requirements:
- async retains guaranteed headroom;
- sync cannot consume async reservation;
- when sync capacity is full, fail fast with 429/503;
- do not let sync saturation block Kafka polling.

Metrics:

```text
executor_active_steps{execution_mode="ASYNC_KAFKA"}
executor_active_steps{execution_mode="SYNC_DIRECT"}
executor_step_wait_seconds{execution_mode="..."}
executor_sync_rejected_total
kafka_consumer_lag
db_pool_checked_out
db_pool_wait_seconds
```

Do not choose final capacity values before mixed-load testing.

## 9. DB pool protection

Both execution modes share the existing DB pool.

Do not solve contention only by increasing pool size.

Required:
- monitor checked-out connections;
- monitor pool wait time;
- include DB contention in mixed sync+async load tests;
- choose execution budgets below empirically safe DB capacity.

Future hardening if needed:

```text
executor-async deployment: Kafka enabled, sync HTTP disabled
executor-sync deployment:  sync HTTP enabled, Kafka disabled
```

Same code image, separate runtime roles.

## 10. Deadline model

Use:

```text
overall_sync_deadline
configured_step_timeout
response_safety_margin
```

For each step:

```text
remaining = overall_deadline - now

effective_step_timeout =
    min(configured_step_timeout,
        remaining - response_safety_margin)
```

If no safe time remains, do not dispatch another step.

Timeout values must be configuration.

No `202` fallback in Option A Phase 1.

## 11. Client disconnect

While awaiting Executor, concurrently monitor caller connection:

```text
race:
  Executor HTTP task
  request.is_disconnected() watcher
```

Use `asyncio.wait(..., FIRST_COMPLETED)` or equivalent.

If caller disconnects:
- stop future steps;
- cancel/close Orchestration-side Executor wait;
- log current step and correlation ID;
- increment disconnect metric.

Do not assume this cancels a currently running LLM/tool inside Executor.

## 12. Retry policy

No automatic retry after Executor may have started.

Reason: response loss may occur after a tool/side effect already completed.

Only failures proven to occur before request acceptance may be considered retryable, and Phase 1 should remain conservative.

## 13. HIL, streaming, conversational, multimodal

### HIL
Out of scope. Reject HIL-enabled sync use cases. Never auto-approve.

### AG-UI/SSE
Out of scope. The sync path should not depend on Kafka to preserve streaming events.

### Conversational
Enable conversational sync routes only after session-state persistence/recovery is verified. Golden tests must validate `chat_history` and memory behavior.

### Multimodal parts
Do not remove current offload/reattach behavior blindly. Preserve compatible behavior or keep parts-enabled use cases out of sync until its purpose is confirmed.

## 14. Status-model consistency

Resolve the discovered `StepStatus.IN_PROGRESS` model difference between repos before shared sync contracts.

Do not create a third sync-only status vocabulary.

## 15. Response-equivalence contract

Must match:
- `x_correlation_id`;
- business status;
- response;
- state;
- canonical error semantics;
- chat history when applicable;
- relevant agent/tool side effects;
- relevant DB/audit effects.

May differ:
- Kafka metadata;
- webhook headers;
- delivery timestamps;
- HTTP-only transport metadata.

Testing:
- mocked/deterministic calls: exact structural equality;
- live LLM calls: schema/control-flow equivalence, not exact text equality.

## 16. Implementation phases

### Phase 0 — Baseline
- commit/archive `OPTION-A-PHASE-0-DISCOVERY.md`;
- run both repo test suites;
- capture async golden fixtures;
- confirm Executor FastAPI entrypoint;
- verify session persistence;
- review parts flow;
- confirm HIL flag;
- identify config/Helm patterns.

Exit: baseline green and first selected sync route has no blocker.

### Phase 1 — Shared Orchestration finalization
- extract finalization;
- async consumer uses shared service;
- `ResponseService` unchanged;
- add unit tests.

Exit: no async regression.

### Phase 2 — Shared Executor one-step core
- extract one-step execution;
- Kafka behavior unchanged;
- explicit `FAILED` persistence;
- execution-mode context;
- separate semaphores;
- metrics.

Exit: async regression green.

### Phase 3 — Executor private HTTP step API
- add internal route;
- call shared one-step core;
- use `SYNC_DIRECT`;
- no Kafka next/final publication;
- integration tests.

Exit: one-step HTTP behavior matches one-step Kafka behavior.

### Phase 4 — Orchestration sync coordinator + `/task-executor/sync`
- same auth/guardrails/config/planner;
- UUID validation;
- `agent_execution` seeding;
- sequential step loop;
- deadline propagation;
- disconnect watcher;
- shared finalization;
- feature flag.

Exit: single- and multi-step golden tests pass.

### Phase 5 — `/agent-testing/sync`
Reuse the same coordinator. No duplicate loop.

### Phase 6 — Conversational routes
Only after session-state gate passes.

### Phase 7 — Mixed-load/failure testing
Run sync and async together and test:
- sync saturation;
- Kafka lag;
- DB pool pressure;
- pod restarts;
- disconnect;
- overall and step timeout;
- draining;
- failures around side effects.

Gate: async SLO does not materially regress under maximum allowed sync load.

### Phase 8 — Controlled pilot
Allowlist only sync-eligible use cases.

Eligibility:

```text
P99 workflow duration + platform overhead + safety margin
< configured sync deadline
```

Start with low-risk, no-HIL, proven-compatible use cases.

## 17. Kubernetes/OpenShift requirements

- readiness false immediately when draining starts;
- no new sync admissions while draining;
- allow current sync requests to finish where possible;
- configure:

```text
terminationGracePeriodSeconds
>
maximum sync deadline
+ preStop/drain allowance
+ shutdown cleanup allowance
```

- monitor Kafka lag separately from CPU-based HPA.

## 18. Required metrics

Orchestration:

```text
sync_requests_total
sync_request_duration_seconds
sync_timeout_total
client_disconnect_total
sync_active_requests
sync_step_http_duration_seconds
sync_step_http_error_total
sync_workflow_steps
```

Executor:

```text
executor_active_steps{execution_mode}
executor_step_duration_seconds{execution_mode}
executor_sync_rejected_total
executor_step_failure_total{execution_mode}
db_pool_checked_out
db_pool_wait_seconds
```

Async protection:

```text
kafka_consumer_lag
async_execution_duration_seconds
async_error_rate
```

All relevant logs should include correlation ID, execution mode, step ID, and agent name.

## 19. Minimum tests

Unit:
- UUID validation;
- finalization paths;
- canonical error;
- chat history;
- timeout calculation;
- sync semaphore rejection;
- explicit FAILED persistence.

Integration:
- one-step sync success;
- multi-step success;
- output-generator success;
- tool success/failure;
- runtime failure;
- step/overall timeout;
- disconnect;
- concurrent sync;
- sync while Kafka load is active.

Regression:
- all existing async endpoints unchanged;
- business response/state/errors and relevant DB/audit side effects equivalent to async golden fixtures.

## 20. Rollback

All sync behavior must be feature-flagged.

Rollback:
1. disable public sync routes;
2. leave async routes unchanged;
3. leave Kafka Executor path enabled;
4. optionally disable Executor private sync route.

Do not introduce a schema migration that the current async path depends on solely for Option A Phase 1.

## 21. Definition of done

Option A base implementation is complete when:
- `/task-executor/sync` works for eligible single- and multi-agent plans;
- Orchestration owns the sync loop;
- Executor performs exactly one step per internal HTTP call;
- Kafka and HTTP reuse the same execution core;
- async path remains unchanged;
- success persists `COMPLETED`;
- failure persists `FAILED`;
- sync reuses async finalization logic;
- UUID validation exists;
- no uncertain-outcome automatic retry exists;
- HIL/streaming are excluded;
- sync cannot consume async reserved execution capacity;
- timeout/disconnect/draining behaviors are tested;
- mixed-load testing protects async SLOs;
- rollout is feature-flagged and reversible.
