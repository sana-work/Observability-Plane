# Sync API Manual Test Plan

**Scope:** Orchestrator and executor flow for `ASYNC`, `SYNC`, and `HYBRID` execution.

**Primary routes:**

- Orchestrator: `POST /task-executor`
- Orchestrator: `POST /sync/task-executor/`
- Orchestrator: `POST /resume`
- Orchestrator: `POST /sync/resume`
- Executor: `POST /internal/v1/agent-executor`
- Admin: `POST /reload-configs`

This document is based on the latest implementation screenshots supplied for the orchestrator and executor. Replace placeholders with environment-specific values. Do not place real bearer tokens, private keys, or passwords in test evidence.

## 1. Expected Architecture

### 1.1 Normal asynchronous or hybrid start

1. Client calls `POST /task-executor` with the task body and identity headers.
2. `JWTBearer` validates the client token.
3. The orchestrator derives the consumer COIN from the token claims.
4. The orchestrator loads the use-case configuration and orchestrator configuration.
5. The mode is resolved from `orchestrator_config.metadata.mode`; if it is absent, the normal route defaults to `ASYNC`.
6. `SYNC` is rejected by this normal route. `ASYNC` and `HYBRID` continue.
7. The guardrail check runs.
8. The planner creates a runtime payload, including `execution_mode`.
9. The runtime snapshot is persisted in the runtime store.
10. The asynchronous path continues through the existing Kafka/event flow.
11. If an agent pauses for human or tool input, the runtime snapshot contains the interruption data and an interruption ID.
12. The client sends the response to `POST /resume`.
13. The orchestrator loads the runtime by interruption ID, validates application/user ownership and tool confirmations, stores the interruption response, and publishes an `INTERRUPTION_RESPONSE` through the normal interruption/Kafka service.
14. The worker resumes from the saved plan and current step and eventually produces the next interruption or final response.

### 1.2 Synchronous start

1. Client calls `POST /sync/task-executor/` with the task body and identity headers.
2. The orchestrator validates the client token and derives the consumer COIN.
3. It loads the use-case and orchestrator configurations.
4. It runs the guardrail check.
5. It invokes the planner with `execution_mode=SYNC`. This value is forced by the sync route; the database mode is not used to choose the transport.
6. The planner creates and persists a runtime payload containing `execution_mode=SYNC`, the plan, `current_step`, and `next_agent`.
7. `SyncExecutionCoordinator` starts a loop with the configured overall deadline.
8. For each step, the coordinator chooses either an ARC-specific executor URL or the default `SYNC_EXECUTOR_URL`.
9. `SyncStepClient` obtains a COIN token and calls the executor over HTTP. It sends the runtime payload plus `X-Authorization-Coin: Bearer <fresh-token>`.
10. The executor validates the plan and step index, creates its runtime model, and executes exactly one agent step.
11. The executor returns the updated `current_step`, `next_agent`, `event_type`, `plan`, `state`, and any interruption or error.
12. The coordinator merges that response into the current runtime payload and dispatches the next step.
13. The loop ends when the task reaches a final response, an interruption request, an error, or the overall timeout.
14. On a final response, the orchestrator builds the synchronous business response.
15. On an interruption, the orchestrator returns the interruption request and the caller must later call `POST /sync/resume`.

### 1.3 Synchronous resume

1. Client calls `POST /sync/resume` with a `ToolInterruptionResponse` or `AgentInterruptionResponse`.
2. The request includes `Config-ID`, `X-Application-ID`, and the optional `X-SOEID` used by the original request.
3. The orchestrator derives the consumer COIN and loads the use-case configuration.
4. It obtains `db_name` and `db_schema` from the use-case database parameters.
5. `RuntimeService` loads the saved runtime by `request.interruption_id`.
6. It rejects the request if the runtime is missing, the application does not match, or the user does not match.
7. For a tool interruption, it compares the response confirmation keys with the pending confirmation keys.
8. The response is stored in `runtime_state.interruption.interruption_response`.
9. The event type is changed to `INTERRUPTION_RESPONSE` and the updated runtime is persisted.
10. `SyncExecutionCoordinator` is called directly with the saved runtime. The planner is not run again.
11. The coordinator repeats the same one-step HTTP loop from the saved `current_step` and `next_agent` until the next interruption, final response, error, or timeout.

### 1.4 Important route distinction

`/resume` and `/sync/resume` are different transports:

| Route | Resume mechanism | Intended start path |
|---|---|---|
| `/resume` | Updates runtime and uses `InterruptionService`, normally the Kafka/event path | `/task-executor` |
| `/sync/resume` | Updates runtime and invokes `SyncExecutionCoordinator` directly over HTTP | `/sync/task-executor/` |

Calling `/resume` after a synchronous interruption does not exercise the synchronous round trip. Use `/sync/resume` unless the implementation has deliberately added a compatibility bridge.

## 2. Configuration and Mode

### 2.1 Authoritative mode field

The latest orchestrator model reads the operational mode from:

```text
orchestrator_config.metadata.mode
```

Valid values are:

```text
ASYNC
SYNC
HYBRID
```

Case and surrounding whitespace are normalized by the model. The `.env` file does not select the per-use-case mode in the latest code shown.

The authoritative mode field is confirmed to be `orchestrator_config.metadata.mode`. The `response_config.executor.mode` value shown in the use-case configuration should not be used as the source of truth unless a separate compatibility mapping is intentionally maintained.

### 2.2 Route/mode matrix

| Route | `ASYNC` | `SYNC` | `HYBRID` | Notes |
|---|---:|---:|---:|---|
| `/task-executor` | Allowed | Rejected | Allowed when not blocked by HIL/eligibility checks | Uses `metadata.mode`, defaults to `ASYNC` when missing |
| `/sync/task-executor/` | Forced internally | Forced internally | Forced internally to `SYNC` transport | The route passes `ExecutionMode.SYNC` to the planner |
| `/resume` | Intended | Not the sync transport | Depends on the original async/event flow | Uses interruption service/Kafka behavior |
| `/sync/resume` | Not the intended transport | Intended | Not a hybrid selector | Reuses the saved runtime and direct HTTP coordinator |

### 2.3 Database locations

Use the physical configuration table or view that feeds `get_orchestrator_configs()` and set the selected use-case's `metadata.mode`. Verify the loaded configuration object contains the same value after reload.

The runtime store is separate from configuration. The latest runtime model shows a `runtime` table with these important columns:

| Table | Column | Purpose |
|---|---|---|
| `runtime` | `id` | Runtime ID and resume lookup key; the interruption request carries the interruption ID used for lookup |
| `runtime` | `session_id` | Indexed session association |
| `runtime` | `runtime_state` | JSON snapshot containing plan, current step, next agent, mode, interruption, state, and headers/IDs |
| `runtime` | `created_at` / `updated_at` | Snapshot timestamps |

The use-case configuration also supplies the database name/schema used to load the runtime during resume. The original start path and resume path must resolve to the same runtime database and schema.

### 2.4 Environment values relevant to sync

The orchestrator screenshots show these sync settings:

```dotenv
SYNC_EXECUTOR_URL=http://<executor-host>:<port>/internal/v1/agent-executor
SYNC_EXECUTOR_COIN_SCOPE=<executor-coin-scope>
SYNC_OVERALL_TIMEOUT_SECONDS=120
```

The executor must have its normal COIN/JWKS configuration so that `X-Authorization-Coin` is accepted. The executor receives `execution_mode` in the request payload; it does not choose the mode from its `.env` file.

`/internal/v1/agent-executor` is the confirmed executor route. Verify that `SYNC_EXECUTOR_URL` points to this route, including any application-level API prefix used by the deployment.

After changing database-backed configuration, restart the service or call the orchestrator reload endpoint and confirm that the new in-memory configuration is loaded before testing.

## 3. Common Request Data

### 3.1 Headers

Use the exact header names below:

```text
Authorization: Bearer <client-jwt>
Config-ID: <use-case-id>
X-Correlation-ID: <unique-correlation-id>
X-Application-ID: <application-id>
X-SOEID: <user-id>                 # optional in the route, required for ownership tests
Content-Type: application/json
```

The orchestrator-to-executor call additionally uses:

```text
X-Authorization-Coin: Bearer <fresh-executor-coin-token>
```

Do not reuse an expired Swagger token. The earlier `401 ER003` evidence confirms that stale or malformed authorization values fail before task execution.

### 3.2 Start body template

```json
{
  "context": "<business question or task>",
  "state": {}
}
```

Use the complete request model required by the selected endpoint if `parts`, metadata, native mode, or other fields are enabled by the use case.

### 3.3 Resume body

Use the exact generated OpenAPI schema for either `ToolInterruptionResponse` or `AgentInterruptionResponse`. At minimum, preserve the returned `interruption_id`. A tool resume must include the same set of pending confirmation keys that was returned in the interruption request.

Do not invent or rename fields while testing. Capture one real interruption response and use its schema as the fixture.

## 4. Manual End-to-End Runbooks

### Runbook A: Async interruption and `/resume`

1. Set the selected use case's `orchestrator_config.metadata.mode` to `ASYNC`, or omit it and verify the default.
2. Reload orchestrator configuration.
3. Submit a task to `/task-executor` with a use case that is known to interrupt.
4. Confirm the start response is an initiation response, not a synchronous final response.
5. Locate the runtime row and record the interruption ID, session ID, current step, next agent, and `execution_mode`.
6. Submit the returned tool or agent response to `/resume`.
7. Confirm application/user ownership validation passes.
8. Confirm the interruption event is published through the normal interruption/Kafka service.
9. Confirm the workflow resumes from the saved step and eventually produces a final event or another interruption.

### Runbook B: Sync interruption and `/sync/resume`

1. Set `SYNC_EXECUTOR_URL` to the real executor route and confirm it is reachable from the orchestrator host.
2. Confirm `SYNC_EXECUTOR_COIN_SCOPE` is valid and the orchestrator can obtain a fresh token.
3. Confirm executor route health and authorization independently.
4. Submit a task to `/sync/task-executor/`.
5. Confirm executor logs show one HTTP request per planned step.
6. If no interruption occurs, confirm the final synchronous response.
7. If an interruption occurs, record the interruption ID and inspect the runtime JSON.
8. Submit the matching response to `/sync/resume`.
9. Confirm the runtime is loaded from the same database/schema and the mode remains `SYNC`.
10. Confirm no planner call occurs during resume.
11. Confirm the coordinator continues from the saved `current_step` and `next_agent`.
12. Confirm the final response or next interruption.

### Runbook C: Hybrid behavior

1. Set `orchestrator_config.metadata.mode` to `HYBRID`.
2. Ensure HIL is disabled for the first test.
3. Reload configuration.
4. Submit to `/task-executor`, not `/sync/task-executor/`.
5. Confirm the planner/runtime payload contains `execution_mode=HYBRID`.
6. Verify whether the current implementation uses Kafka, direct HTTP, or a planner-specific branch for the selected use case.
7. Repeat with HIL enabled. The shared sync eligibility check currently rejects `SYNC` and `HYBRID` for HIL-enabled workflows; record the actual error and status.

## 5. Test Cases

| ID | Test | Procedure | Expected result |
|---|---|---|---|
| CFG-01 | Authoritative mode | Set `metadata.mode=ASYNC`, reload, start through `/task-executor` | Runtime mode is `ASYNC`; task starts |
| CFG-02 | Hybrid mode | Set `metadata.mode=HYBRID`, reload, start through `/task-executor` | Runtime mode is `HYBRID`; no SYNC-route rejection when eligibility allows it |
| CFG-03 | Missing mode | Remove `metadata.mode`, reload, start through `/task-executor` | Normal route defaults to `ASYNC`; no runtime `None` mode |
| CFG-04 | Mode source isolation | Change only `response_config.executor.mode`, keep `metadata.mode` unchanged | Effective mode remains `metadata.mode`; the compatibility field must not silently override it |
| CFG-05 | Sync route override | Set database mode to `ASYNC` or `HYBRID`, call `/sync/task-executor/` | Planner/runtime still contain `SYNC`; confirms route override |
| CFG-06 | Reload | Change the database mode, call `/reload-configs`, run a new request | New request uses the changed in-memory configuration |
| AUTH-01 | Valid client token | Call any public route with a fresh valid token | Request passes JWT/COIN validation |
| AUTH-02 | Expired client token | Reuse an expired token | `401` with authorization error; no planner or executor call |
| AUTH-03 | Missing auth | Remove `Authorization` | `401`; no runtime created |
| AUTH-04 | Invalid internal token | Call executor with invalid `X-Authorization-Coin` | `401`; orchestrator should expose a useful downstream error |
| CFG-07 | Consumer COIN mismatch | Use a token whose `sub`/`aud` do not map to the configured consumer | Clean missing-config error; no generic unexplained failure |
| GRD-01 | Guardrail reject | Use a request rejected by guardrails | Rejection response; no plan dispatch |
| ASY-01 | Async final task | Use `metadata.mode=ASYNC` and a non-interrupting task | Initiation response, Kafka/event execution, final event |
| ASY-02 | Async tool interruption | Use an interrupting task | Runtime contains interruption and valid interruption ID |
| ASY-03 | Async `/resume` | Respond using `/resume` | `INTERRUPTION_RESPONSE` is published through normal interruption service |
| ASY-04 | Async wrong application | Change `X-Application-ID` on `/resume` | Resume rejected; runtime unchanged |
| ASY-05 | Async wrong user | Change `X-SOEID` on `/resume` | Resume rejected; runtime unchanged |
| ASY-06 | Async missing runtime | Use an unknown interruption ID | Resumability error; no new runtime created |
| SYN-01 | Sync final task | Call `/sync/task-executor/` with a non-interrupting task | Direct executor calls occur; final response returned synchronously |
| SYN-02 | Sync multi-step | Use a plan with at least two agents | One executor request per step; `current_step`/`next_agent` advance correctly |
| SYN-03 | Sync interruption | Use a task that pauses | Route returns interruption request and runtime is persisted |
| SYN-04 | Sync tool resume | Call `/sync/resume` with matching confirmation keys | Runtime response is saved; coordinator continues from saved step |
| SYN-05 | Sync agent resume | Call `/sync/resume` with an agent response | Same continuation behavior without tool confirmation requirements |
| SYN-06 | Sync wrong application | Change `X-Application-ID` | Resume is rejected before dispatch |
| SYN-07 | Sync wrong user | Change `X-SOEID` | Resume is rejected before dispatch |
| SYN-08 | Sync unknown interruption | Use an unknown ID | Resumability error; executor is not called |
| SYN-09 | Tool key mismatch | Omit, add, or rename one confirmation key | `AP011` validation error; coordinator is not called |
| SYN-10 | Duplicate sync resume | Submit the same resume twice | Behavior must be defined: idempotent replay or explicit already-resumed error; no duplicate business action |
| SYN-11 | Runtime DB routing | Store runtime, then alter configured db/schema before resume | Resume must fail clearly if it cannot find the runtime; restore configuration and repeat successfully |
| SYN-12 | Sync timeout | Set a very small overall timeout | `ER006` timeout; no further step dispatch after deadline |
| SYN-13 | Executor step timeout | Make executor exceed the step timeout | `ER006` step timeout; runtime/error state is observable |
| SYN-14 | Executor URL | Configure an invalid URL/path | Clear downstream error; verify no silent fallback to Kafka |
| SYN-15 | ARC route | Enable ARC with a valid per-agent URL | Selected agent uses the ARC URL |
| SYN-16 | ARC URL missing | Enable ARC without an ARC executor URL | `AP010`; no step is dispatched |
| SYN-17 | Missing plan | Send executor payload without `plan.steps` | Validation error; no agent execution |
| SYN-18 | Invalid current step | Send `current_step=null`, negative, or out of range | Validation error; no agent execution |
| SYN-19 | Missing execution mode | Remove `execution_mode` from direct executor payload | Confirm whether default `ASYNC` is acceptable; sync callers must never rely on this default |
| SYN-20 | Executor route contract | Call `/internal/v1/agent-executor` directly with a valid one-step payload | Exactly one step executes and updated payload is returned |
| HYB-01 | Hybrid no HIL | Use `HYBRID` with HIL disabled | Route accepts it and runtime preserves `HYBRID` |
| HYB-02 | Hybrid with HIL | Use `HYBRID` with HIL enabled | Current eligibility check rejects it; confirm error code/message is intentional |
| HYB-03 | Hybrid sync route confusion | Set mode `HYBRID`, call `/sync/task-executor/` | Direct route still runs as `SYNC`; database mode does not make it hybrid |
| OPS-01 | Service restart | Interrupt a task, restart orchestrator, then resume | Runtime can be recovered from DB; caches do not control resumability |
| OPS-02 | Concurrent resume | Send two resumes for one interruption at once | No duplicate continuation; locking or idempotency behavior must be defined |
| OPS-03 | Config cache isolation | Change one use case mode and test another use case | Other use cases retain their own configuration |

## 6. Database Verification Queries

Use read-only queries appropriate to the configured database. The exact physical configuration table must be confirmed from the configuration manager.

### 6.1 Configuration verification

Verify the object returned by the configuration service contains:

```text
usecase_id = <Config-ID>
metadata.mode = ASYNC | SYNC | HYBRID
metadata.is_human_in_loop_enabled = true | false
metadata.database_parameters.db_name = <runtime-db>
metadata.database_parameters.db_schema = <runtime-schema>
```

### 6.2 Runtime verification

For each start or interruption, inspect the `runtime` row selected by the interruption ID and verify the JSON contains:

```text
execution_mode
plan
current_step
next_agent
event_type
x_correlation_id
x_application_id
x_soeid
interruption.interruption_cursor
interruption.interruption_request
interruption.interruption_response
```

After `/sync/resume`, verify that:

- `event_type` becomes `INTERRUPTION_RESPONSE` before the next dispatch.
- `execution_mode` remains `SYNC`.
- `current_step` and `next_agent` are preserved or advanced by the executor response.
- `updated_at` changes.
- A second resume does not repeat the business action.

## 7. Confirmed Bugs and Risks

| Severity | Finding | Impact | Recommended action |
|---|---|---|---|
| High | `/resume` and `/sync/resume` use different transports | Calling `/resume` after a sync interruption bypasses the direct sync coordinator | Make clients use `/sync/resume` for sync tasks and document the distinction |
| High | Historical runtime error showed `execution_mode=None` even though the enum requires `ASYNC`, `SYNC`, or `HYBRID` | Runtime construction fails after planning | Resolve and validate mode before every planner/runtime construction; add a regression test |
| High | The executor defaults a missing payload mode to `ASYNC` | A malformed sync payload can silently execute under async semantics | Make `execution_mode` required for internal sync calls or reject missing mode |
| Medium | `/sync/task-executor/` always passes `SYNC`, regardless of database mode | Database mode is not the source of truth for this route and can confuse operations/testing | Treat the route as explicitly sync and validate/document that behavior |
| Medium | Normal endpoint error text says “For async api it should be ASYNC or HYBRID” and calls the field “Response mode” | Misleading diagnostics and possibly the wrong client error category | Use an execution-mode message and a consistent 4xx error contract |
| Medium | Token `sub`/`aud` selection controls consumer COIN and config lookup | A claim-shape mismatch appears as missing configuration | Log the selected non-secret identifier and add claim-shape tests |
| Medium | Sync client calls `raise_for_status()` before preserving downstream error details | Executor `401`/`404`/`500` can become a generic coordinator error | Preserve status, response body, correlation ID, and target URL in structured errors |
| Medium | `/sync/resume` persists the interruption response before coordinator execution completes | A failed continuation can leave a runtime marked as responded | Add transaction/idempotency state or make repeated resume behavior explicit |
| Medium | Resume DB name/schema comes from current use-case configuration | A config change can make an existing runtime appear missing | Persist runtime storage location or guarantee immutable routing metadata |
| Medium | Tool resume checks confirmation key sets but not necessarily values/allowed decisions | Invalid confirmation values may pass key validation | Validate each confirmation value against the response model and pending request |
| Medium | Resume assumes `runtime_state.interruption.interruption_cursor` exists for tool responses | Malformed state can produce an unhandled attribute error | Return a controlled resumability error for incomplete interruption state |
| Low | Executor accepts `float` for `current_step` and then casts it to `int` | Invalid values can be silently truncated | Require a non-negative integer at the model boundary |
| Low | The sync route has a trailing slash | Clients without the slash may receive a redirect that changes POST behavior in some clients | Publish and test the canonical URL exactly |
| Low | `validate_sync_eligibility` receives `usecase_config` but the visible implementation does not use it | Minor maintainability issue; may hide missing policy checks | Remove the unused parameter or implement the intended policy |

## 8. Evidence to Capture for Each Test

Record the following for every failed or successful end-to-end test:

- UTC timestamp
- route and exact URL, including trailing slash
- `Config-ID`
- `X-Correlation-ID`
- redacted token identity (`sub`/`aud` names only, never the token)
- request and response JSON with secrets removed
- HTTP status and response body
- orchestrator logs for planner, runtime update, coordinator, and downstream status
- executor logs for route, step index, agent name, and mode
- runtime row before and after resume
- Kafka topic/event only for the async path
- executor URL actually selected, including ARC/default choice
- elapsed time and timeout settings

## 9. Exit Criteria

The implementation is ready for sign-off when:

1. `ASYNC` starts and resumes through `/resume`.
2. `SYNC` completes a multi-step task through direct executor calls.
3. `SYNC` interrupts and resumes through `/sync/resume` without rerunning the planner.
4. `HYBRID` behavior is demonstrated and documented for at least one real use case.
5. Mode is read from one confirmed database field and survives reload/restart.
6. Runtime mode is never `None`.
7. The configured executor URL matches the deployed route.
8. Wrong user, wrong application, missing runtime, expired token, confirmation mismatch, invalid step, and timeout cases are controlled errors.
9. Duplicate and concurrent resume behavior is explicitly defined and tested.
10. No test evidence contains live bearer tokens or private credentials.
