# Sync Task Executor to Resume Flow

## Complete Flow

### 1. Start: `/sync/task-executor/`

1. The client sends the task with:
   - `Authorization`
   - `Config-ID`
   - `X-Correlation-ID`
   - `X-Application-ID`
   - optional `X-SOEID`
   - request body containing `context`

2. The orchestrator validates the JWT and derives the consumer COIN.

3. It loads:
   - use-case configuration
   - orchestrator configuration
   - database name/schema
   - planner configuration

4. Guardrail validation runs.

5. The sync route explicitly calls the planner with:

```text
execution_mode=SYNC
```

The database mode is not used to select the transport for this route. The sync route forces synchronous execution.

6. The planner creates a runtime payload containing:

```text
plan
current_step
next_agent
execution_mode=SYNC
session_id
correlation/application/user IDs
```

7. Runtime state is saved in the `runtime` table.

8. `SyncExecutionCoordinator` starts executing steps.

9. For each step, the orchestrator calls the executor over HTTP:

```text
POST /internal/v1/agent-executor
```

10. The executor runs exactly one agent step and returns:

```text
current_step
next_agent
event_type
plan
state
interruption
error
```

11. The coordinator merges the response and calls the executor again until the task:
   - completes;
   - pauses for human/tool input;
   - fails; or
   - reaches timeout.

## When an Interruption Happens

The executor returns an interruption request. The orchestrator returns it to the caller and saves it in the runtime JSON.

The runtime should contain:

```text
interruption_id
interruption_request
interruption_cursor
current_step
next_agent
execution_mode=SYNC
plan
state
```

The client must then respond using:

```text
POST /sync/resume
```

For a synchronous task, `/resume` is the wrong route because it uses the normal asynchronous interruption path.

## `/sync/resume` Flow

1. The client sends the interruption response with:
   - `Config-ID`
   - `X-Application-ID`
   - `X-SOEID`
   - `interruption_id`

2. The orchestrator loads the runtime using `interruption_id`.

3. It loads the runtime database/schema from the use-case configuration.

4. It validates:
   - runtime exists;
   - application ID matches;
   - SOEID matches;
   - tool confirmation keys match the pending confirmations.

5. It saves the response into:

```text
runtime_state.interruption.interruption_response
```

6. It changes:

```text
event_type=INTERRUPTION_RESPONSE
```

7. It updates the runtime table.

8. It calls `SyncExecutionCoordinator` directly.

9. The planner is not called again.

10. Execution continues from the saved:

```text
current_step
next_agent
plan
state
execution_mode=SYNC
```

11. The same executor roundtrip continues until a final response or another interruption.

## `/resume` Versus `/sync/resume`

| Route | Used for | Continuation |
|---|---|---|
| `/resume` | Normal async/Kafka workflow | `InterruptionService` and Kafka |
| `/sync/resume` | Synchronous workflow | Direct `SyncExecutionCoordinator` and HTTP executor |

Therefore:

```text
/task-executor       -> /resume
/sync/task-executor  -> /sync/resume
```

Calling `/resume` after `/sync/task-executor` bypasses the intended synchronous execution path.

## Mode Configuration

The authoritative mode field is:

```text
orchestrator_config.metadata.mode
```

Valid values are:

```text
ASYNC
SYNC
HYBRID
```

The field is database-backed and is the value used by the normal route.

Do not use the following field as the authoritative mode unless an explicit compatibility mapping exists:

```text
agentic_usecase_config.response_config.executor.mode
```

## Mode Behavior

| Mode | Normal `/task-executor` | `/sync/task-executor/` |
|---|---|---|
| `ASYNC` | Allowed | Forced to `SYNC` |
| `SYNC` | Rejected | Allowed and forced |
| `HYBRID` | Allowed if eligibility checks pass | Forced to `SYNC` |

The runtime table does not configure the mode. It stores the runtime snapshot.

Important runtime columns:

```text
runtime.id
runtime.session_id
runtime.runtime_state
runtime.created_at
runtime.updated_at
```

The mode is inside the JSON field:

```text
runtime.runtime_state.execution_mode
```

## Confirmed Configuration

The following values are confirmed to be correct in the current implementation:

```text
Mode field:       orchestrator_config.metadata.mode
Executor route:   /internal/v1/agent-executor
```

The sync environment should point to the confirmed executor route:

```dotenv
SYNC_EXECUTOR_URL=http://<executor-host>:<port>/internal/v1/agent-executor
SYNC_EXECUTOR_COIN_SCOPE=<sync-executor-coin-scope>
SYNC_OVERALL_TIMEOUT_SECONDS=120
```

After changing database-backed configuration, call `/reload-configs` or restart the orchestrator before starting a new test.

## Confirmed Bugs and Remaining Risks

### 1. Wrong resume endpoint risk

Sync interruptions must use `/sync/resume`. Calling `/resume` uses the asynchronous interruption/Kafka path.

### 2. Historical `execution_mode=None` bug

Previous logs showed `RuntimeState` being created with `execution_mode=None`, although only `ASYNC`, `SYNC`, and `HYBRID` are valid.

The mode field and executor route are correct, so this should now be investigated as a runtime-loading, stale-process, or alternate-code-path problem.

### 3. Executor silently defaults a missing mode to `ASYNC`

A malformed sync payload could accidentally execute as async. The executor should reject a missing mode for internal sync calls, or the coordinator must guarantee that the field is always present.

### 4. Resume is saved before continuation completes

If execution fails after `/sync/resume` updates the runtime, the runtime may look already resumed even though execution did not finish. Duplicate/idempotent resume behavior needs to be defined.

### 5. HIL and HYBRID conflict

The shared eligibility check rejects both `SYNC` and `HYBRID` when HIL is enabled. Confirm whether hybrid HIL workflows are supposed to be supported.

### 6. Potentially misleading errors

The normal endpoint reports:

```text
For async api it should be ASYNC or HYBRID
```

The error refers to “response mode” even though the field is execution mode.

### 7. Consumer COIN/config lookup risk

The token `sub` and `aud` claims affect consumer COIN selection. If the token claim shape does not match the configured consumer, the request may appear to have missing configuration.

### 8. Downstream error visibility risk

Executor HTTP errors may be converted into a generic coordinator error. Preserve the downstream HTTP status, response body, correlation ID, and target URL in logs.

### 9. Runtime database routing risk

Resume uses the database name/schema from the current use-case configuration. A configuration change can make an existing runtime appear missing.

### 10. Tool confirmation validation risk

The current validation compares confirmation key sets. Confirm that each confirmation value is also validated against the allowed response values.

## Manual Test Document

The complete manual test plan includes:

- async, sync, and hybrid test cases;
- `/sync/task-executor` to `/sync/resume`;
- `/task-executor` to `/resume`;
- database and runtime verification;
- authentication and COIN tests;
- HIL checks;
- timeout tests;
- ARC executor tests;
- duplicate resume tests;
- wrong user/application tests;
- executor route validation;
- bug and risk register.

## Required Fixes Before Sign-Off

The following items are the implementation fixes or decisions required before the sync/resume flow can be considered complete. They are documented here; they are not automatically applied to the source code by this document.

| Priority | Required action | Current status |
|---|---|---|
| High | Keep `orchestrator_config.metadata.mode` as the single authoritative per-use-case mode field | Confirmed configuration; verify every loader and planner uses it |
| High | Keep `/internal/v1/agent-executor` as the configured direct executor route | Confirmed route; verify deployment prefix and `SYNC_EXECUTOR_URL` |
| High | Keep `/task-executor -> /resume` and `/sync/task-executor -> /sync/resume` as separate flows | Required API contract |
| High | Guarantee a non-null `execution_mode` before every `RuntimeState` construction | Required because previous logs showed `None` |
| High | Ensure `/sync/task-executor/` always creates and persists `execution_mode=SYNC` | Required route behavior |
| High | Ensure `/sync/resume` preserves `SYNC`, plan, current step, next agent, and state | Required resume behavior |
| High | Reject missing `execution_mode` in internal sync payloads instead of silently defaulting to `ASYNC` | Recommended code hardening |
| High | Define idempotency and concurrency behavior for repeated `/sync/resume` calls | Required before production |
| Medium | Preserve executor HTTP status and response details instead of converting them to only a generic coordinator error | Recommended observability fix |
| Medium | Return controlled errors for missing runtime, invalid interruption state, wrong application, wrong SOEID, and confirmation mismatch | Required API behavior |
| Medium | Validate tool confirmation values, not only confirmation key names | Recommended validation fix |
| Medium | Confirm and document whether `HYBRID` with HIL is intentionally rejected | Required product decision |
| Medium | Guarantee that start and resume resolve the same runtime database/schema | Required deployment/configuration guarantee |
| Low | Change executor `current_step` to a non-negative integer instead of accepting/truncating floats | Recommended model hardening |
| Low | Publish the canonical trailing-slash form of `/sync/task-executor/` and test clients against it | Recommended API consistency fix |

### What this document does not do

This page does not modify Python code, database records, environment files, credentials, or deployed services. After implementing the required actions, run the manual test plan and update each status above with evidence from logs, runtime rows, and API responses.
