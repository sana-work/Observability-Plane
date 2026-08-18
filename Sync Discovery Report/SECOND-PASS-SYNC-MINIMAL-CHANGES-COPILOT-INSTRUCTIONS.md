# Second-Pass Sync API Simplification — GitHub Copilot Implementation Instructions

## Purpose

Apply a **second-pass simplification** to the synchronous execution implementation across:

- `181229.genaiservices.agentic-orchestration`
- `181229.genaiservices.agentic-agent-executor`

The goal is to keep the production change set **small, easy to review, and low risk**.

The sync implementation should reuse as much of the existing async behavior as possible. Do **not** add unrelated production-hardening features in this change.

---

# 1. Target Architecture

The existing async flow must remain unchanged:

```text
Caller
  -> Orchestration async API
  -> build/plan TaskPayload
  -> Kafka
  -> Executor
  -> Kafka next/final events
  -> Orchestration final-response processing
  -> ResponseService
  -> caller webhook/Kafka
```

The new sync flow should be:

```text
Caller
  -> Orchestration sync API
  -> reuse same validation/config/guardrail/planning/payload construction
  -> SyncExecutionCoordinator
  -> direct HTTP call to Executor /internal/v1/agent-step
  -> Executor executes exactly one planned step using existing agent runtime
  -> updated TaskPayload returned to Orchestration
  -> repeat until terminal step
  -> shared final business-response builder
  -> response returned on the same caller HTTP request
```

Core principle:

```text
Same planning
Same TaskPayload
Same agent execution
Same DB/status behavior
Same final business-response logic

Only transport/control differs:
ASYNC -> Kafka
SYNC  -> direct internal HTTP
```

---

# 2. Rename the Four Public Sync APIs

Replace the current `/.../sync` suffix routes with a common `/sync/.../` prefix.

The final routes must be exactly:

```text
POST /api/v1/agentic-orchestration/sync/task-executor/
POST /api/v1/agentic-orchestration/sync/conversational-task-executor/
POST /api/v1/agentic-orchestration/sync/native-conversational-task-executor/
POST /api/v1/agentic-orchestration/sync/agent-testing/
```

Remove the old sync route definitions such as:

```text
/task-executor/sync
/conversational-task-executor/sync
/native-conversational-task-executor/sync
/agent-testing/sync
```

The existing async APIs must remain exactly as they are.

Each sync API must continue to mirror its async equivalent:

- same request model
- same required headers
- same JWT authentication
- same consumer-COIN lookup
- same guardrail behavior
- same planner/static-planner behavior
- same session/native-conversation behavior where applicable

The only important behavioral difference is how the prepared task is executed and how the final result is returned.

---

# 3. Remove UUID Validation for `X-Correlation-ID`

Remove the sync-only UUID validation added for `X-Correlation-ID`.

Delete code such as:

```python
_validate_correlation_id(...)
uuid.UUID(...)
```

and remove imports that exist only for this validation.

`X-Correlation-ID` should behave the same way as in the existing async APIs: it is passed through as the caller-provided correlation identifier.

Do not introduce a new idempotency mechanism in this change.

---

# 4. Remove Sync Feature Flags

Both async and sync APIs must be available by default. Do not use a feature flag to switch between them.

## Orchestration

Remove:

```text
SYNC_EXECUTION_ENABLED
sync_execution_enabled
syncExecutionEnabled
```

Remove the runtime check that rejects sync requests when the flag is false.

Remove corresponding environment-model fields, Helm values, deployment environment variables, and documentation.

## Executor

Remove:

```text
EXECUTOR_SYNC_DIRECT_STEP_API_ENABLED
sync_direct_step_api_enabled
syncDirectStepApiEnabled
```

Remove the internal-endpoint runtime check for this flag.

The Executor internal endpoint should simply exist and remain protected by COIN M2M authentication.

---

# 5. Reuse the Existing Payload-Building / Planning Logic

Avoid maintaining a separate sync copy of the payload-building logic.

The current implementation introduced code such as:

```text
orchestration/service/sync_planning_service.py
build_sync_task_payload(...)
```

Refactor this so sync and async share the same payload-construction and planning logic wherever practical.

## Desired separation

Payload construction should be independent of delivery transport.

Prefer this pattern conceptually:

```python
task_payload = await build_task_payload(...)

if sync:
    return await SyncExecutionCoordinator().execute(
        task_payload,
        orchestrator_config,
        executor_base_url,
    )

publish_to_existing_kafka_path(task_payload)
return existing_async_ack
```

The exact existing helper/function names should be discovered from the repository and reused where possible.

A shared payload builder should:

1. validate/load required configuration
2. create the TaskPayload
3. run the appropriate planner
4. populate the plan/current step/next agent
5. seed existing `agent_execution` records as required
6. return the prepared payload

Then the calling path decides:

```text
ASYNC -> publish prepared payload to Kafka
SYNC  -> give prepared payload to SyncExecutionCoordinator
```

Do **not** put Kafka publication deep inside a generic payload builder.

If existing async code currently combines payload construction and Kafka publication, make the smallest safe extraction needed to separate those responsibilities.

Remove `sync_planning_service.py` if, after the refactor, it contains only duplicated logic and is no longer needed.

---

# 6. Keep `ExecutionMode`, But Only for the Essential Executor Difference

Keep:

```python
ExecutionMode.ASYNC_KAFKA
ExecutionMode.SYNC_DIRECT
```

The same `AgentOrchestrator`, `AgentExecutionService`, and agent runtime should execute both modes.

Expected behavior:

```text
ASYNC_KAFKA
  -> execute one step
  -> advance TaskPayload
  -> publish next/final Kafka event exactly as existing async behavior expects

SYNC_DIRECT
  -> execute one step
  -> advance TaskPayload
  -> DO NOT publish next/final Kafka event
  -> return updated TaskPayload to the internal HTTP endpoint
```

Do not duplicate the agent-execution implementation for sync.

---

# 7. Keep the Executor Internal One-Step API

Keep:

```text
POST /internal/v1/agent-step
```

It should:

1. authenticate the caller using COIN/JWT bearer authentication
2. receive `TaskPayloadModel`
3. determine the current agent from the existing payload/plan
4. call the existing shared `AgentOrchestrator`
5. force `execution_mode=ExecutionMode.SYNC_DIRECT`
6. execute exactly one planned step
7. return the updated payload fields needed by Orchestration
8. never start its own sync workflow loop
9. never publish continuation/final Kafka events in `SYNC_DIRECT`

The workflow loop belongs in Orchestration.

---

# 8. Keep COIN M2M Authentication Between Services

Keep the COIN-based service-to-service authentication.

## Orchestration -> Executor

Keep a cached token roller/helper such as:

```python
get_sync_executor_token_roller()
```

using existing Orchestration COIN credentials and:

```text
SYNC_EXECUTOR_COIN_SCOPE
```

The `SyncStepClient` must send:

```http
Authorization: Bearer <COIN M2M token>
```

## Executor

Keep the JWT/COIN bearer verifier and:

```text
COIN_PROVIDER_ROLE
```

The Executor must verify that the incoming bearer token is valid for its registered provider role/audience.

Do not restore the old static internal API key.

---

# 9. Read Executor Base URL From `agentic_usecase_config.response_config`

Remove the global Executor base URL configuration from Orchestration.

Remove:

```text
SYNC_EXECUTOR_BASE_URL
sync_executor_base_url
syncExecutorBaseUrl
executorServiceName
```

where those values exist only to construct the sync Executor base URL.

## New use-case configuration value

`agentic_usecase_config.response_config` will contain a **new dedicated Executor service URL**.

This is separate from the existing async callback configuration.

Do **not** use:

```text
response_config.webhook.url
```

for Executor routing.

Preferred logical shape:

```json
{
  "response_config": {
    "webhook": {
      "url": "https://caller-callback.example"
    },
    "executor": {
      "url": "http://agentic-agent-executor:7998"
    }
  }
}
```

If the repository's response-config model requires a flatter field, use an equivalent dedicated name such as `executor_service_url`, but keep the concept separate from `webhook.url`.

Required sync behavior:

```text
1. Load agentic_usecase_config using the existing usecase/consumer lookup.
2. Read the new Executor URL from response_config.
3. Validate that it is configured.
4. Pass it to SyncExecutionCoordinator / SyncStepClient.
5. Append the configured internal step endpoint.
```

Conceptually:

```python
executor_base_url = usecase_config.response_config.executor.url

result = await coordinator.execute(
    task_payload,
    orchestrator_config,
    executor_base_url,
)
```

The existing async webhook URL must continue to be used only by existing async response delivery.

---

# 10. Keep Only the Minimal Sync Configuration

## Orchestration — keep

```text
SYNC_EXECUTOR_COIN_SCOPE
SYNC_EXECUTOR_STEP_ENDPOINT
SYNC_OVERALL_TIMEOUT_SECONDS
```

The Executor base URL is no longer an environment variable because it comes from `agentic_usecase_config.response_config`.

Use only one overall timeout configuration for the sync workflow.

Remove:

```text
SYNC_STEP_TIMEOUT_SECONDS
SYNC_TIMEOUT_SAFETY_MARGIN_SECONDS
SYNC_HTTP_MAX_CONNECTIONS
SYNC_HTTP_MAX_CONNECTIONS_PER_HOST
```

and corresponding environment fields, Helm values, deployment env entries, documentation, and tests that exist only for those settings.

The HTTP client may still use normal connection pooling internally; do not expose extra tuning knobs in this change.

---

# 11. Simplify `SyncExecutionCoordinator`

Keep `SyncExecutionCoordinator`, but keep it focused.

Its responsibilities should be only:

```text
1. receive an already-prepared TaskPayload
2. use the Executor base URL supplied for the current use case
3. call Executor one step at a time
4. replace/update the local TaskPayload with the returned payload
5. continue while another step remains
6. stop on terminal failure
7. call the shared final business-response builder
8. return the final response
```

Use `SYNC_OVERALL_TIMEOUT_SECONDS` as the only sync timeout configuration.

Do not add:

- separate per-step timeout configuration
- safety-margin calculations
- caller-disconnect watchers
- cross-service cancellation
- capacity reservation
- retry/idempotency infrastructure
- Kafka fallback

No automatic retry should be introduced for uncertain Executor outcomes.

---

# 12. Simplify `SyncStepClient`

Keep one small internal HTTP client for Orchestration -> Executor communication.

It should:

1. receive the Executor base URL for the current use case
2. append `SYNC_EXECUTOR_STEP_ENDPOINT`
3. mint/get the COIN M2M token
4. send the TaskPayload
5. deserialize the updated TaskPayload/result
6. raise/propagate existing application errors cleanly

A shared `aiohttp.ClientSession` or the already-selected existing client implementation is fine.

If a shared session is used, keep the small lifespan cleanup needed to close it:

```python
await SyncStepClient.close()
```

Do not add configurable connection-pool tuning as part of this change.

---

# 13. Rename `finalization_service.py`

Keep the extracted shared final-response logic.

Rename:

```text
orchestration/service/finalization_service.py
```

to:

```text
orchestration/service/execution_response_builder.py
```

or another equally clear name if the repository has a stronger existing naming convention.

The file represents **business response construction**, not transport/delivery.

Keep the shared function such as:

```python
finalize(task_payload, orchestrator_config)
```

and keep existing logic for:

- failed-step response
- canonical error mapping
- single-agent response
- multi-agent last-step response
- optional output-generator LLM response
- conversational chat-history update
- `x_correlation_id`
- `status`
- `response`
- `event_type`
- `state`
- optional `error`
- optional `chat_history`

Update imports everywhere after the rename.

---

# 14. Async Finalization Must Reuse the Same Builder

Keep the simplification in:

```text
orchestration/service/message_processing_service.py
```

The async Kafka final-response consumer should call the same:

```python
response = await finalize(task_payload, orchestrator_config)
```

instead of carrying a duplicate copy of final-response assembly logic.

Then it should continue using the existing `ResponseService` for webhook/Kafka delivery exactly as before.

The shared response builder must not itself send HTTP, webhook, or Kafka messages.

Its job is only:

```text
terminal TaskPayload -> final business response dict
```

---

# 15. Remove Observability Additions From This Change

Do not include the new Prometheus/metrics work in this sync PR.

Remove sync-related additions such as:

```text
/metrics
prometheus_client
executor/util/metrics.py
orchestration/util/metrics.py
step-duration metrics
active-step metrics
sync rejection metrics
DB-pool metrics
Kafka-consumer-lag metrics
sync request metrics
```

Remove related imports and instrumentation calls from Executor, Orchestration, Kafka consumers, DB dependency helpers, and main application modules.

Do not change the service's existing observability behavior beyond what already existed before the sync work.

---

# 16. Remove Separate Semaphore / Capacity Management

Do not include the separate sync/async capacity-manager implementation in this second-pass PR.

Remove code such as:

```text
executor/service/execution_capacity.py
EXECUTOR_ASYNC_STEP_CONCURRENCY
EXECUTOR_SYNC_STEP_CONCURRENCY
async_step_semaphore
sync_step_semaphore
try_reserve_sync_direct()
reserve_async_kafka()
reserve_sync_direct()
```

The Executor should route both modes through the same existing one-step execution core.

This second pass is intentionally minimizing infrastructure changes.

---

# 17. Remove Drain / Shutdown-Drain Features Added for Sync

Remove the new sync-specific drain functionality:

```text
POST /drain
drain_state.py
is_draining()
start_drain()
```

Restore `/ready` to its previous behavior unless unrelated existing production logic must remain.

Remove sync-specific lifecycle/preStop additions from Helm.

Remove termination-grace-period changes that were added only for this sync implementation.

Do not include a new drain architecture in this PR.

---

# 18. Avoid Unrelated Kafka Changes

The existing async Kafka workflow is production behavior and should be changed as little as possible.

Only make Kafka-related changes required to let the shared Executor support `SYNC_DIRECT`.

Essential pattern:

```python
if execution_mode == ExecutionMode.ASYNC_KAFKA:
    existing_kafka_publish(...)
```

For `SYNC_DIRECT`, skip those publishes and return the updated payload.

Do not otherwise refactor:

- Kafka producer structure
- Kafka consumer structure
- topic configuration
- consumer lag handling
- async response delivery
- message contracts

unless strictly required for compilation/correctness.

---

# 19. Preserve Existing DB / Step Execution Semantics

Sync must continue to use the existing execution and persistence code.

Do not create a separate sync database model.

The same `agent_execution` records and same step statuses should be used for both paths.

The Executor should continue using the shared:

```text
AgentOrchestrator
AgentExecutionService
AgentFactory / agent runtime
tool execution
LLM execution
agent_execution persistence
```

Avoid new workflow-state tables or idempotency tables in this change.

---

# 20. Behavior of the Four Sync APIs

## 20.1 `/sync/task-executor/`

Use the same normal planner/configuration flow as async `task-executor`.

Difference:

```text
prepared TaskPayload
  -> SyncExecutionCoordinator
  -> direct Executor HTTP step calls
  -> shared final response
  -> caller
```

## 20.2 `/sync/conversational-task-executor/`

Use the same conversational request model and chat-history semantics as the async endpoint.

The final response builder must preserve existing conversational `chat_history` behavior.

## 20.3 `/sync/native-conversational-task-executor/`

Preserve existing native conversational/session behavior.

Set/use the same native marker/session information the async path expects.

Do not create a separate native sync execution engine.

## 20.4 `/sync/agent-testing/`

Use existing static-planner/test-agent behavior.

Only the transport after payload creation changes from Kafka to the sync coordinator.

---

# 21. Expected Shared Flow After Refactor

```text
                    Public Orchestration APIs
                              |
             +----------------+----------------+
             |                                 |
          ASYNC APIs                        SYNC APIs
             |                                 |
             +------------- shared ------------+
                           |
                  auth / config / guardrail
                           |
                      build TaskPayload
                           |
                         planner
                           |
                 seed agent_execution
                           |
             +-------------+-------------+
             |                           |
           ASYNC                        SYNC
             |                           |
          Kafka              SyncExecutionCoordinator
             |                           |
             |                   SyncStepClient
             |                           |
             |             COIN M2M authenticated HTTP
             |                           |
             +--------------------> Executor
                                      |
                               shared AgentOrchestrator
                                      |
                              shared agent execution
                                      |
                       +--------------+--------------+
                       |                             |
                  ASYNC_KAFKA                   SYNC_DIRECT
                       |                             |
                 publish Kafka                return payload
                       |                             |
                       |                   Orchestration loops
                       |                             |
                       +-------------+---------------+
                                     |
                        shared execution_response_builder
                                     |
                       +-------------+-------------+
                       |                           |
                    ASYNC                        SYNC
                       |                           |
                ResponseService              HTTP response
                webhook/Kafka                 to caller
```

---

# 22. Configuration Summary After Second Pass

## Orchestration — keep

```text
SYNC_EXECUTOR_COIN_SCOPE
SYNC_EXECUTOR_STEP_ENDPOINT
SYNC_OVERALL_TIMEOUT_SECONDS
```

## Orchestration — remove

```text
SYNC_EXECUTION_ENABLED
SYNC_EXECUTOR_BASE_URL
SYNC_STEP_TIMEOUT_SECONDS
SYNC_TIMEOUT_SAFETY_MARGIN_SECONDS
SYNC_HTTP_MAX_CONNECTIONS
SYNC_HTTP_MAX_CONNECTIONS_PER_HOST
```

## Executor — keep

```text
COIN_PROVIDER_ROLE
```

## Executor — remove

```text
EXECUTOR_SYNC_DIRECT_STEP_API_ENABLED
EXECUTOR_ASYNC_STEP_CONCURRENCY
EXECUTOR_SYNC_STEP_CONCURRENCY
```

The sync Executor destination comes from:

```text
agentic_usecase_config.response_config.<dedicated executor URL field>
```

and must be separate from:

```text
agentic_usecase_config.response_config.webhook.url
```

---

# 23. Files/Areas Copilot Must Review

Do not blindly edit only this list; search both repositories for all references.

## Orchestration

```text
orchestration/api/api.py
orchestration/config/environment.py
orchestration/dependencies.py
orchestration/main.py
orchestration/service/sync_execution_coordinator.py
orchestration/service/sync_step_client.py
orchestration/service/sync_planning_service.py
orchestration/service/finalization_service.py
orchestration/service/message_processing_service.py
orchestration/service/response_service.py
orchestration/models/*
orchestration/core/factory/*
helm/values.yaml
helm/*-values.yaml
helm/templates/deployment.yaml
README.md
AGENTS.md
tests/**
```

## Executor

```text
executor/api/api.py
executor/api/auth.py
executor/config/environment.py
executor/models/execution_mode.py
executor/service/agent_orchestrator.py
executor/service/agent_execution_service.py
executor/dependencies.py
executor/main.py
helm/values.yaml
helm/*-values.yaml
helm/templates/deployment.yaml
README.md
tests/**
```

Search for every removed configuration/key/file name to ensure there are no stale references.

---

# 24. Testing Requirements

Update/add focused tests only for the final simplified behavior.

## Orchestration

Verify:

- all four new `/sync/.../` routes exist
- old `/.../sync` routes are removed
- no UUID-format requirement for `X-Correlation-ID`
- sync API is not gated by a feature flag
- async API behavior remains unchanged
- same guardrail/config/planner logic is reused
- agent-testing sync uses static planner behavior
- native conversational sync preserves session/native behavior
- dedicated Executor URL is read from `agentic_usecase_config.response_config`
- webhook URL is not used for Executor calls
- missing Executor URL fails clearly
- prepared payload is sent to `SyncExecutionCoordinator`
- coordinator executes steps sequentially
- only overall sync timeout is used
- sync final response comes from the shared response builder
- async final-message processing uses the same response builder

## Executor

Verify:

- `/internal/v1/agent-step` requires valid COIN bearer authentication
- no sync endpoint feature flag is required
- internal endpoint uses `ExecutionMode.SYNC_DIRECT`
- shared agent execution code is used
- `SYNC_DIRECT` does not publish Kafka continuation/final events
- `ASYNC_KAFKA` preserves existing Kafka behavior
- no semaphore/capacity-manager behavior remains

Run the existing full unit suites for both repositories after focused tests pass.

---

# 25. Explicit Non-Goals

Do **not** implement or reintroduce the following in this second-pass change:

```text
Prometheus metrics
/metrics endpoints
DB pool instrumentation
Kafka lag instrumentation
separate sync/async semaphores
capacity reservation
/drain endpoints
drain state
preStop drain hooks
new shutdown architecture
multiple sync timeout knobs
client disconnect monitoring
cross-service cancellation
automatic retry
new idempotency storage
new workflow-state tables
Kafka fallback for sync
new sync-specific agent execution code
static internal API keys
Executor URL from webhook.url
global SYNC_EXECUTOR_BASE_URL
sync enable/disable feature flags
```

---

# 26. Final Acceptance Criteria

The implementation is complete when:

1. Four public sync routes use the new `/sync/.../` prefix.
2. Existing four async routes are unchanged.
3. `X-Correlation-ID` is no longer UUID-validated only for sync.
4. No sync-enable feature flags remain.
5. Sync and async reuse the same payload/planning code as much as safely possible.
6. Executor keeps one shared execution core with `ASYNC_KAFKA` and `SYNC_DIRECT`.
7. `SYNC_DIRECT` returns the updated payload and does not chain through Kafka.
8. Orchestration drives the sync multi-step loop.
9. Orchestration -> Executor uses COIN M2M bearer authentication.
10. Executor base URL comes from the new dedicated field in `agentic_usecase_config.response_config`.
11. `response_config.webhook.url` remains untouched for async callback delivery.
12. `SYNC_EXECUTOR_BASE_URL` is removed.
13. Only one sync timeout configuration remains: `SYNC_OVERALL_TIMEOUT_SECONDS`.
14. `finalization_service.py` is renamed to a clearer business-response-builder name.
15. Sync and async both use the same final business-response builder.
16. Metrics, drain, capacity/semaphore, and other nonessential hardening additions are removed.
17. Existing async Kafka behavior remains backward compatible.
18. Existing unit tests plus new focused sync tests pass.

---

# 27. Implementation Style

This is a production service. Favor:

```text
small diffs
existing abstractions
shared code
clear separation of payload construction vs transport
no unnecessary new framework
no unrelated refactors
backward compatibility for async
```

Before changing code, inspect the current implementation and trace the existing async path end-to-end.

If the repository already has a helper that performs one of the required responsibilities, reuse it rather than creating another sync-specific helper.

After implementation, provide a concise summary containing:

1. files changed
2. files deleted
3. configuration removed
4. new/changed response-config field used for Executor URL
5. tests added/updated
6. confirmation that async behavior was not intentionally changed
