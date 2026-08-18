# GitHub Copilot Instructions — Simplify Sync API PR to Minimal Production Change

## Objective

Refactor the current synchronous API implementation in **both repositories** so the PR contains only the minimum changes required to add synchronous execution safely to the existing production platform.

Repositories:

- `181229.genaiservices.agentic-orchestration`
- `181229.genaiservices.agentic-agent-executor`

The current branch contains the core sync feature **plus several unrelated production-hardening changes** such as metrics, drain/readiness changes, Kafka lag instrumentation, DB-pool observability, capacity/semaphore controls, several timeout knobs, and deployment lifecycle changes.

The reviewer direction is:

> **Keep the sync feature simple. Minimize the production change surface. Do not introduce unrelated observability, Kafka, lifecycle, or tuning changes in this PR.**

The existing asynchronous Kafka path is production behavior and must remain functionally unchanged.

---

# 1. Mandatory Working Rules

Before editing:

1. Inspect the complete current diff against `main` in both repositories.
2. Read the actual implementations, tests, Helm charts, and configuration classes before modifying anything.
3. Treat the **current code** as the source of truth. Do not rely only on old design documents.
4. Do not rewrite large existing modules when a small conditional or helper extraction is enough.
5. Do not redesign the async flow.
6. Do not add new abstractions unless they are required for the sync path.
7. Preserve current request/response contracts for all existing async APIs.
8. Preserve current Kafka topics, message formats, routing behavior, and response delivery behavior.
9. Keep comments/docstrings concise and focused on why sync-specific behavior differs.
10. **Reuse the existing payload/planning construction for both sync and async wherever possible.**
    The shared payload builder must only build/initialize the execution payload; it must not decide Kafka vs HTTP transport.
    Make the sync-vs-async dispatch decision **after** the common payload has been built.
11. After cleanup, the PR should tell one simple story:

```text
Existing async path:
Caller -> Orchestration -> Kafka -> Executor -> Kafka -> Orchestration -> existing delivery

New sync path:
Caller -> Orchestration -> internal HTTP -> Executor(one step) -> Orchestration
       -> repeat until final -> shared finalize() -> same HTTP response
```

---

# 2. Target Architecture

The preferred implementation is to build the execution payload **once using shared code**, and only then choose the transport/execution path.

```text
                         CALLER
                           |
                           v
                    ORCHESTRATION
                existing JWT authentication
                existing request validation
                existing configuration loading
                existing guardrail
                existing planner behavior
                           |
                           v
              shared prepare/build execution payload
              (plan + TaskPayloadModel + initial DB state)
                           |
                 +---------+---------+
                 |                   |
               ASYNC                SYNC
                 |                   |
                 v                   v
        existing Kafka prep     SyncExecutionCoordinator
        if required (for        |
        example parts offload)  v
                 |              SyncStepClient
                 v                   |
               Kafka          COIN M2M Bearer token
                                     |
                                 internal HTTP
                                     |
                                     v
                                  EXECUTOR
                         POST /internal/v1/agent-step
                                     |
                             verify COIN bearer token
                                     |
                                     v
                              AgentOrchestrator
                                     |
                             existing agent runtime
                             AgentExecutionService
                             ADK / LLM / tools / DB
                                     |
                                     v
                              return updated payload
                                     |
                                     v
                                ORCHESTRATION
                                 another step?
                                  /        \
                                yes         no
                                 |           |
                               repeat     finalize()
                                              |
                                              v
                                       HTTP final response
```

### Important design rule

Do **not** make a payload builder responsible for transport, for example:

```python
build_payload(..., is_sync=True)  # avoid if this function also dispatches
```

Prefer:

```python
task_payload = await build_or_prepare_execution_payload(...)

if execution_mode == ExecutionMode.SYNC_DIRECT:
    return await SyncExecutionCoordinator().execute(
        task_payload,
        orchestrator_config,
    )

# Existing async/Kafka-specific preparation and publication remains here.
await publish_to_kafka(task_payload)
return existing_async_ack
```

If a mode/flag is required, it belongs in the higher-level **dispatch/execution decision**, not inside the pure payload construction itself.

Prefer an explicit enum/name such as:

```python
ExecutionMode.ASYNC_KAFKA
ExecutionMode.SYNC_DIRECT
```

over a vague boolean when that can be done without unnecessary refactoring.

---

# 3. Four Sync APIs That Must Remain

Keep synchronous counterparts for all four existing API families:

```text
POST /api/v1/agentic-orchestration/task-executor/sync
POST /api/v1/agentic-orchestration/conversational-task-executor/sync
POST /api/v1/agentic-orchestration/native-conversational-task-executor/sync
POST /api/v1/agentic-orchestration/agent-testing/sync
```

The four routes must **not** become four separate implementations.

They should remain thin wrappers over one common sync execution helper, currently represented by logic such as:

```python
_execute_sync_task(...)
```

Differences between the routes should be limited to existing API-specific behavior such as:

- request model,
- session ID handling,
- native conversational flag/session behavior,
- chat history behavior,
- static planner / test-agent selection.

After request preparation, all four sync APIs should enter the same coordinator.

---

# 4. Orchestration Repository — KEEP

## 4.1 Public sync routes

Keep the four `/sync` routes in:

```text
orchestration/api/api.py
```

Each route should remain small and call the shared sync helper.

Do not duplicate:

- config loading,
- guardrail calls,
- planning,
- sync coordination,
- finalization.

---

## 4.2 Existing public authentication

Keep the existing public authentication pattern:

```python
Depends(JWTBearer())
```

Do not create a second public authentication mechanism for sync.

---

## 4.3 Correlation ID validation

Keep UUID validation for `X-Correlation-ID` if it is already part of the sync implementation.

The correlation ID is for:

- tracing,
- logging,
- `agent_execution.task_id`,
- workflow correlation.

It is **not** an idempotency key.

Do not add an idempotency framework in this PR.

---

## 4.4 Existing configuration and guardrail behavior

Keep sync aligned with async request admission:

```text
authentication
    ->
configuration lookup
    ->
sync eligibility check
    ->
existing guardrail
    ->
planning
```

Keep use-case/orchestrator configuration validation.

Keep the existing `run_guardrail_check(...)` behavior.

Do not build new guardrail logic.

---

## 4.5 Sync eligibility check

Keep only the small sync-specific compatibility guard if current code requires it.

For example, reject sync for functionality that fundamentally requires asynchronous interaction, such as:

- HIL-enabled workflow,
- AG-UI/event streaming workflow.

Keep `_ensure_sync_eligibility(...)` or equivalent concise.

Do not expand this into a large policy framework.

---

## 4.6 Shared execution payload preparation — PREFERRED CHANGE

Before keeping a separate sync-only planning/payload implementation, inspect the current async route and identify the exact code that:

- invokes the configured planner,
- creates the plan,
- builds `TaskPayloadModel`,
- sets common request/header/session fields,
- initializes `agent_execution` rows,
- preserves static/dynamic planning behavior,
- preserves native/session information.

### Preferred structure

Extract or reuse the **smallest common payload-preparation function** so both async and sync receive the same base payload.

Conceptually:

```python
async def prepare_execution_payload(...):
    # shared only:
    # - planner
    # - plan
    # - TaskPayloadModel
    # - common metadata/session fields
    # - initial agent_execution state
    return task_payload
```

Then the transport-specific decision happens outside:

```python
task_payload = await prepare_execution_payload(...)

if execution_mode == ExecutionMode.SYNC_DIRECT:
    return await SyncExecutionCoordinator().execute(
        task_payload,
        orchestrator_config,
    )

# Keep existing async-only preparation here.
task_payload = await prepare_for_kafka_if_required(task_payload)
await publish_to_kafka(task_payload)
return existing_async_ack
```

### `sync_planning_service.py`

If the current:

```text
orchestration/service/sync_planning_service.py
```

duplicates payload/planner construction that already exists in the async path, **remove or collapse it** and reuse the shared builder instead.

Keep a separate file/helper only if inspection proves there is genuinely sync-specific payload preparation that cannot be cleanly shared.

Do not create a second copy of the planner/payload-building code just for sync.

### Responsibility boundary

The shared payload builder must **not**:

- publish to Kafka,
- call Executor,
- call `SyncExecutionCoordinator`,
- decide response delivery,
- perform Kafka-only large-parts offload,
- contain `if sync: ... else Kafka...` transport side effects.

It should build and initialize the common execution payload only.

### Important multimodal rule

The latest sync implementation should **not** use the Kafka-specific large-parts offload/reattach workaround.

After the shared payload is built:

```text
ASYNC_KAFKA:
    common payload
        ->
    existing Kafka-specific parts preparation/offload if required
        ->
    Kafka

SYNC_DIRECT:
    common payload
        ->
    keep parts in HTTP payload
        ->
    SyncExecutionCoordinator
```

Remove sync-only DB-name/schema/offload/reattach code that exists solely to work around Kafka payload limits.

Do not change the existing async Kafka parts behavior.

---

## 4.7 SyncExecutionCoordinator

Keep:

```text
orchestration/service/sync_execution_coordinator.py
```

but simplify it.

Its core responsibility should be only:

1. receive an already-planned task payload,
2. call Executor for the current step,
3. merge/use the updated payload returned by Executor,
4. check whether the payload is terminal,
5. repeat for the next step when required,
6. call shared `finalize(...)` once terminal,
7. return the final business response to the public API.

Conceptually:

```python
async def execute(task_payload, orchestrator_config):
    while True:
        task_payload = await step_client.execute_step(task_payload)

        if task_payload.event_type == AGENT_EXECUTION_FINAL_RESPONSE:
            return await finalize(task_payload, orchestrator_config)
```

Keep required error handling, but avoid turning this class into a new workflow framework.

---

## 4.8 SyncStepClient

Keep:

```text
orchestration/service/sync_step_client.py
```

Its responsibilities should remain very small:

1. resolve Executor internal URL,
2. obtain COIN M2M token,
3. call `POST /internal/v1/agent-step`,
4. send the task payload,
5. deserialize the Executor response,
6. surface HTTP/transport failures using existing exception conventions.

Use a reusable `aiohttp.ClientSession` if the current implementation already does so.

Do **not** add custom connection-pool tuning knobs in this PR unless a default is strictly required by the library.

---

## 4.9 COIN M2M token creation

Keep the Orchestration-side token roller/helper in:

```text
orchestration/dependencies.py
```

such as:

```python
get_sync_executor_token_roller()
```

The sync internal call must use a COIN-issued bearer token.

Keep only the essential configuration required to mint the token, such as:

```text
SYNC_EXECUTOR_COIN_SCOPE
```

Do not restore the old static `X-Internal-Api-Key` model.

---

# 5. Shared Finalization — MUST KEEP

Keep:

```text
orchestration/service/finalization_service.py
```

This is a good and necessary extraction because both sync and async need the same final business-response logic.

Keep the shared behavior for:

- failure response construction,
- canonical error object,
- single-agent success,
- multi-agent success,
- output-generator-enabled response synthesis,
- last-step response when output generator is disabled,
- conversational chat-history preparation/update,
- `state`,
- `event_type`,
- `x_correlation_id`.

Relevant functions may include:

```python
prepare_response(...)
_prepare_chat_history(...)
finalize(...)
```

The objective is:

```text
Before:
async finalization logic existed inline in async message processing

After:
same business logic lives in reusable finalize()
```

Do **not** redesign the async business response.

Update the existing async final-response path to call `finalize(...)` with the smallest possible code change.

---

# 6. Orchestration Repository — REMOVE / REVERT

The following changes are outside the minimal sync feature and should be removed from this PR unless they were already present on `main`.

## 6.1 Remove Prometheus/observability additions

Remove sync-added observability code such as:

```text
orchestration/util/metrics.py
```

Remove instrumentation added only by this branch, including examples such as:

```text
SyncRequestDurationScope
sync timeout counters
client-disconnect counters
Executor HTTP failure counters
DB pool usage metrics
Kafka consumer lag metrics
```

Remove new Prometheus imports and metric helpers.

Do not change unrelated existing logging.

---

## 6.2 Remove `/metrics`

Remove the newly added:

```text
GET /metrics
```

from Orchestration if it did not already exist on `main`.

Do not add Prometheus endpoint support in this PR.

---

## 6.3 Remove drain state and `/drain`

Remove sync-added:

```text
orchestration/util/drain_state.py
POST /drain
```

Restore `/ready` to its pre-branch behavior unless the readiness change existed independently on `main`.

Remove sync-specific drain checks such as:

```python
if is_draining():
    ...
```

from the public sync request path.

Drain support can be a separate production-hardening PR.

---

## 6.4 Remove deployment lifecycle changes added only for drain

Revert branch-only additions such as:

```text
preStop hook calling /drain
preStopSleepSeconds
terminationGracePeriodSeconds changes introduced for sync drain
```

unless they are independently required by an approved platform change.

Do not mix Kubernetes lifecycle redesign with the sync API feature.

---

## 6.5 Remove DB-pool observability hooks

Remove branch-only calls such as:

```python
observe_db_pool_usage(...)
```

from:

```text
orchestration/dependencies.py
```

unless such monitoring already exists on `main`.

---

## 6.6 Remove Kafka observability changes

Remove branch-only Kafka lag instrumentation.

Do not modify Kafka consumer semantics for this PR.

---

# 7. Timeout Simplification

The current branch contains too many sync tuning parameters.

The reviewer direction is to keep timeout logic simple.

Prefer **one overall sync execution timeout**.

Target configuration:

```text
SYNC_EXECUTION_TIMEOUT_SECONDS
```

or reuse the current overall timeout setting if renaming would create more unnecessary diff.

Suggested default:

```text
120 seconds
```

The timeout should bound the overall synchronous request/workflow.

Avoid exposing all of the following as separate configuration knobs in this PR:

```text
SYNC_OVERALL_TIMEOUT_SECONDS
SYNC_STEP_TIMEOUT_SECONDS
SYNC_TIMEOUT_SAFETY_MARGIN_SECONDS
```

Do not implement complicated calculations such as:

```text
min(step_timeout, remaining_overall_time - safety_margin)
```

unless the reviewer explicitly requests them.

If `aiohttp` requires a transport timeout internally, keep it implementation-local and simple rather than creating multiple new Helm values.

---

# 8. Client Disconnect Simplification

Remove complex explicit client-disconnect polling unless it is a hard product requirement.

Remove branch-only logic such as:

```text
request.is_disconnected() polling
100ms polling loop
asyncio.wait(... FIRST_COMPLETED)
manual cancellation race
disconnect metrics
```

For this first minimal implementation, rely on:

- the overall sync timeout,
- normal FastAPI/aiohttp request cancellation/error behavior,
- existing exception handling.

If work already executing in Executor may continue after caller disconnect, document that as a known limitation rather than adding a distributed cancellation protocol.

---

# 9. HTTP Connection Configuration Simplification

Remove branch-only external configuration for:

```text
SYNC_HTTP_MAX_CONNECTIONS
SYNC_HTTP_MAX_CONNECTIONS_PER_HOST
```

unless the current production HTTP client architecture absolutely requires them.

A normal reusable `aiohttp.ClientSession` already provides connection reuse.

Avoid adding operational tuning settings before there is production data requiring them.

---

# 10. Orchestration Environment / Helm — KEEP ONLY ESSENTIAL SETTINGS

After cleanup, Orchestration should ideally require only a small set of sync configuration.

Keep the minimum equivalent of:

```text
SYNC_EXECUTION_ENABLED
SYNC_EXECUTOR_BASE_URL
SYNC_EXECUTOR_STEP_ENDPOINT
SYNC_EXECUTOR_COIN_SCOPE
SYNC_EXECUTION_TIMEOUT_SECONDS
```

If the code currently uses slightly different names, prefer minimizing rename churn.

### Executor service discovery

`SYNC_EXECUTOR_BASE_URL` may use internal Kubernetes/OpenShift service DNS.

It must address the **Executor Service**, not a specific pod.

Example conceptually:

```text
http://<executor-service>.<namespace>.svc.cluster.local:<port>
```

Do not implement pod discovery in application code.

---

# 11. Existing Async API Metadata

Avoid unrelated OpenAPI/documentation churn.

If the branch changed all existing endpoint tags from:

```text
Task Execution
```

to:

```text
Async Execution
```

consider reverting that cosmetic change unless the reviewer specifically wants it.

It is acceptable to tag the **new** routes as `Sync Execution`, but existing production APIs should not be reorganized merely to support this feature.

---

# 12. Executor Repository — KEEP

## 12.1 Internal one-step API

Keep the new internal endpoint:

```text
POST /internal/v1/agent-step
```

in:

```text
executor/api/api.py
```

It should remain hidden from the public OpenAPI schema if that is the current design.

Its responsibilities should be limited to:

1. authenticate internal caller,
2. validate current planned step,
3. identify current agent,
4. execute exactly one agent step,
5. return updated workflow payload fields.

It must **not** own the full sync workflow loop.

Orchestration owns the sync loop.

---

## 12.2 COIN bearer verification

Keep the Executor-side COIN authorization implementation, currently represented by:

```text
executor/api/auth.py
JWTBearer
COINAuthorizer
COIN_PROVIDER_ROLE
```

The request should use:

```http
Authorization: Bearer <COIN M2M token>
```

Keep:

- bearer scheme validation,
- COIN token verification,
- expected provider role/audience validation,
- generic authorization failure behavior.

Do not restore the previous static internal API key / HMAC comparison.

---

## 12.3 ExecutionMode

Keep a minimal execution-mode mechanism such as:

```python
class ExecutionMode(str, Enum):
    ASYNC_KAFKA = "ASYNC_KAFKA"
    SYNC_DIRECT = "SYNC_DIRECT"
```

This distinction is required because the shared runtime must know whether to publish continuation/final events to Kafka.

---

## 12.4 Shared AgentOrchestrator execution

Keep the minimal refactor that allows both entry paths to use the same execution core.

Conceptually:

```text
Kafka consumer -------------------\
                                   -> AgentOrchestrator -> shared execution runtime
/internal/v1/agent-step ----------/
```

Prefer one shared internal execution function such as:

```python
_execute_one_step(...)
```

Do not duplicate AgentExecutionService/agent execution logic for sync.

---

## 12.5 Kafka publish gating — MUST KEEP

This is a required sync change.

Any Kafka continuation/final publication must remain async-only.

Conceptually:

```python
if execution_mode == ExecutionMode.ASYNC_KAFKA:
    self._send_to_kafka(...)
```

For:

```text
SYNC_DIRECT
```

the Executor must update the in-memory payload and return it to Orchestration without publishing the next step/final event to Kafka.

This is the **only category of Kafka behavior change that is required for sync**.

Do not remove this while cleaning up unrelated Kafka changes.

---

## 12.6 Sync payload response

Keep the response from `/internal/v1/agent-step` containing the fields Orchestration needs to continue the workflow, such as:

```text
x_correlation_id
current_step
next_agent
event_type
plan
state
error
```

Do not create a second business-final-response format in Executor.

Executor returns a **step/workflow payload**.

Orchestration `finalize(...)` creates the caller-facing final business response.

---

## 12.7 Executor sync feature flag

Keep a simple feature flag for the private sync endpoint if required for controlled rollout, such as:

```text
EXECUTOR_SYNC_DIRECT_STEP_API_ENABLED
```

Do not couple it to unrelated observability or drain settings.

---

# 13. Executor Repository — REMOVE / REVERT

## 13.1 Remove capacity/semaphore manager for this PR

Unless the reviewer explicitly requests workload isolation now, remove:

```text
executor/service/execution_capacity.py
ExecutionCapacityManager
async_step_semaphore
sync_step_semaphore
try_reserve_sync_direct()
reserve_async_kafka()
reserve_sync_direct()
release_sync_direct()
```

Remove related settings:

```text
EXECUTOR_ASYNC_STEP_CONCURRENCY
EXECUTOR_SYNC_STEP_CONCURRENCY
```

Remove `fail_fast_on_sync_capacity`.

Remove GR008 behavior that exists solely for semaphore exhaustion.

The first PR should prove functional sync execution with the smallest possible Executor change.

Capacity isolation can be a separate follow-up change after load testing.

---

## 13.2 Remove Executor Prometheus metrics

Remove branch-only:

```text
executor/util/metrics.py
```

and all branch-only metrics such as:

```text
executor_active_steps
executor_step_duration_seconds
executor_sync_rejected_total
executor_db_pool_*
executor_kafka_consumer_lag
```

Remove `StepMetricsScope`.

Remove branch-only calls to these helpers.

---

## 13.3 Remove Executor `/metrics`

Remove the new:

```text
GET /metrics
```

from Executor if it did not already exist on `main`.

---

## 13.4 Remove Executor drain support

Remove:

```text
executor/util/drain_state.py
POST /drain
```

Restore `/ready` to its original behavior.

Remove internal endpoint drain rejection logic added only by this branch.

Drain behavior should be a separate hardening PR.

---

## 13.5 Remove DB-pool instrumentation

Remove branch-only:

```python
observe_db_pool_usage(...)
```

from:

```text
executor/dependencies.py
```

---

## 13.6 Remove Kafka consumer lag instrumentation

Revert branch-only Kafka consumer changes such as:

```text
TopicPartition
highwater(...)
observe_kafka_consumer_lag(...)
```

in:

```text
executor/service/kafka_consumer_service.py
```

The existing Kafka consumer must behave as it did before the sync branch.

---

## 13.7 Remove NetworkPolicy if introduced only by this feature branch

If:

```text
helm/templates/networkpolicy.yaml
```

was introduced only by this sync branch and is not an explicit security/platform requirement for this PR, move it to a follow-up hardening change.

COIN M2M authentication remains the required application-level protection for the internal endpoint.

Do not change cluster networking behavior unnecessarily in the functional sync PR.

---

## 13.8 Remove lifecycle/preStop changes

Remove branch-only Executor Helm changes associated with drain:

```text
preStop hook
/drain call
preStopSleepSeconds
termination grace changes introduced only for this behavior
```

Keep only the environment values required for:

- sync endpoint enablement,
- COIN provider role.

---

# 14. Terminal FAILED DB Persistence

The branch currently appears to add explicit awaited persistence of terminal `FAILED` step status.

This is a useful fix, but it is not strictly required to introduce the sync transport.

For minimal production scope:

- If this change is necessary to make sync and async status behavior correct and the reviewer accepts it, keep it.
- Otherwise, move it to a separate small bug-fix PR.

Do not combine unrelated DB status semantic changes with sync unless there is a clear functional dependency.

Copilot should inspect tests and existing `main` behavior before deciding.

---

# 15. Final Minimal Executor Configuration

After cleanup, Executor should ideally need only the equivalent of:

```text
EXECUTOR_SYNC_DIRECT_STEP_API_ENABLED
COIN_PROVIDER_ROLE
```

plus existing COIN configuration already required by the service.

Remove sync concurrency values from this PR if semaphore isolation is removed.

---

# 16. Expected Shared vs Transport-Specific Code

After cleanup, the architecture should have this boundary.

## Shared by sync and async

```text
common request/config/guardrail admission behavior
shared planner invocation
shared execution-payload preparation
TaskPayloadModel
plan/step structures
initial agent_execution creation
AgentOrchestrator
AgentExecutionService
AgentFactory
Agent configuration
ADK Runner
session handling
LLM execution
tool execution
existing audit/DB stores
agent_execution status infrastructure
finalization business logic in Orchestration
```

## Sync-only

```text
four /sync public routes
_execute_sync_task (or equivalent thin sync dispatcher)
SyncExecutionCoordinator
SyncStepClient
COIN M2M call to Executor
Executor /internal/v1/agent-step
SYNC_DIRECT execution mode
direct HTTP step response
overall sync timeout
```

## Async-only

```text
existing async public routes
Kafka publish/consume
Kafka continuation routing
Kafka final event
Kafka/webhook response delivery
Kafka-specific parts offload/reattach
```

---

# 17. Required Behavior for the Four Sync APIs

## 17.1 `/task-executor/sync`

Use the standard task request and normal configured planning behavior.

Flow:

```text
request
 -> auth/config/guardrail
 -> plan
 -> coordinator
 -> Executor step(s)
 -> finalize()
 -> HTTP final response
```

---

## 17.2 `/conversational-task-executor/sync`

Use the conversational request model.

Preserve existing conversational behavior and chat-history handling.

Finalization should use the shared `_prepare_chat_history(...)` logic where applicable.

Do not create separate conversational finalization for sync.

---

## 17.3 `/native-conversational-task-executor/sync`

Preserve the native/session behavior.

Keep required `Session-ID` handling and existing native flag/session semantics.

Execution after planning must use the same sync coordinator as the other routes.

---

## 17.4 `/agent-testing/sync`

Preserve agent-testing/static-planner semantics.

Keep support for the existing test-agent selector/header if currently present.

The difference should be planning selection only.

Once the plan exists, use the same common sync coordinator.

---

# 18. Final Response Rules

`finalize(...)` must remain the single shared business finalization path.

Expected logic:

## Failure

If a plan step failed:

```text
status = FAILED
response = failed step output / existing failure payload
error = canonical error object
event_type = EXECUTION_FINAL_RESPONSE
state = current state
```

## Single-agent success

Use the existing single-step output behavior.

## Multi-agent without output generator

Use the existing last-step output behavior.

## Multi-agent with output generator

Collect agent outputs and run the existing output prompt/LLM synthesis.

## Conversational

Update/preserve chat history using the shared existing memory/chat-history logic.

Do not create different final business semantics between sync and async.

---

# 19. Testing Requirements

After the cleanup, update tests to match the simplified implementation.

Run all existing unit tests in both repositories.

Add/keep focused tests for the new functional behavior only.

## Orchestration tests should cover

1. all four sync routes call the common sync helper,
2. async and sync reuse the same common payload/planning preparation where applicable,
3. common payload preparation performs no Kafka publish and no Executor HTTP call,
4. transport selection happens only after common payload creation,
5. sync feature flag disabled,
6. invalid correlation UUID,
7. missing use-case/orchestrator config,
8. guardrail rejection,
9. unsupported HIL/streaming sync workflow if eligibility checks remain,
10. one-step success,
11. multi-step success,
12. Executor HTTP failure,
13. overall timeout,
14. terminal failure -> shared finalization,
15. output-generator finalization,
16. conversational chat-history finalization,
17. native/session sync path,
18. agent-testing/static-planner sync path,
19. async final response still uses shared `finalize(...)`,
20. existing async route tests continue to pass unchanged.

## Executor tests should cover

1. internal endpoint requires valid COIN bearer token,
2. sync feature flag disabled,
3. invalid current step,
4. one-step SYNC_DIRECT success,
5. SYNC_DIRECT returns updated payload,
6. SYNC_DIRECT does not publish Kafka continuation,
7. SYNC_DIRECT does not publish Kafka final event,
8. ASYNC_KAFKA still publishes exactly as before,
9. shared agent runtime is used by both modes,
10. error path returns/updates terminal payload correctly,
11. all existing Kafka consumer tests still pass.

Do **not** add tests for removed metrics, drain, Kafka lag, DB-pool metrics, or semaphore capacity.

---

# 20. Diff Review Checklist

Before finishing, compare the final branch to `main` and verify:

### Functional sync

- [ ] Four sync APIs exist.
- [ ] All four converge on one common helper/coordinator.
- [ ] Async and sync reuse the same common planner/payload-preparation code where possible.
- [ ] The common payload builder has no Kafka publish, Executor HTTP call, or response-delivery side effects.
- [ ] Kafka-vs-sync dispatch happens only after the common payload has been built.
- [ ] Sync does not publish the initial Kafka execution request.
- [ ] Sync does not use Kafka-specific parts offload.
- [ ] Orchestration calls Executor one step at a time over HTTP.
- [ ] Orchestration obtains a COIN M2M bearer token.
- [ ] Executor verifies the COIN bearer token.
- [ ] Executor executes exactly one planned step.
- [ ] SYNC_DIRECT never Kafka-chains.
- [ ] Orchestration controls next-step looping.
- [ ] Shared `finalize(...)` creates the final business response.
- [ ] Async finalization also uses the shared function.
- [ ] Existing async behavior remains functionally unchanged.

### Removed unrelated scope

- [ ] No new `/metrics` endpoint in either service.
- [ ] No new `/drain` endpoint in either service.
- [ ] No new drain-state module.
- [ ] No Kafka consumer lag instrumentation.
- [ ] No DB-pool observability instrumentation.
- [ ] No sync Prometheus metrics module.
- [ ] No extra observability imports.
- [ ] No semaphore/capacity manager unless explicitly approved.
- [ ] No sync/async concurrency environment variables unless explicitly approved.
- [ ] No unnecessary NetworkPolicy change.
- [ ] No preStop `/drain` lifecycle change.
- [ ] No unnecessary termination grace-period change.
- [ ] No complex client-disconnect polling.
- [ ] No per-step + safety-margin timeout framework.
- [ ] No HTTP connection-pool tuning environment variables.
- [ ] No unrelated Swagger/tag reorganization.

### Quality

- [ ] Minimal code duplication.
- [ ] No large unrelated refactors.
- [ ] New code follows existing repository conventions.
- [ ] Existing async tests pass.
- [ ] New sync tests pass.
- [ ] Helm templates render successfully.
- [ ] No obsolete sync environment variables remain.
- [ ] README/AGENTS documentation reflects the simplified design rather than removed features.

---

# 21. Cleanup Documentation

After the code cleanup, update documentation so it no longer claims the removed features exist.

Remove references to:

```text
/metrics
/drain
draining readiness
sync/async semaphore reservation
GR008 capacity exhaustion
Kafka lag metrics
DB pool metrics
sync capacity metrics
per-step timeout + safety margin
HTTP max connection tuning
```

Document only the final implementation that actually remains.

---

# 22. Final Expected PR Story

The final diff should be explainable in a few sentences:

> This PR adds synchronous counterparts to the four existing execution APIs. Sync and async reuse the same request admission, planner, and common execution-payload construction; only after the common payload is built does Orchestration choose the transport. The existing async path performs its existing Kafka-specific preparation/publication, while the sync path passes the same base payload to `SyncExecutionCoordinator`. Orchestration then executes the planned workflow sequentially by calling a COIN-authenticated internal Executor one-step HTTP endpoint. Executor reuses the existing agent execution engine but suppresses Kafka continuation/final publication in `SYNC_DIRECT` mode and instead returns the updated payload to Orchestration. Once terminal, Orchestration uses the shared `finalize()` function—also used by the async path—to build the final caller-facing response and returns it on the original HTTP connection. Existing asynchronous Kafka execution remains otherwise unchanged.

---

# 23. Copilot Execution Instructions

Perform the work in this order:

1. Inspect both current branch diffs against `main`.
2. Create a short inventory:
   - required sync functional change,
   - shared refactor required by sync,
   - unrelated hardening/observability change.
3. Revert/remove unrelated hardening first.
4. Inspect the existing async planner/payload-building code and extract/reuse the smallest common execution-payload preparation for both async and sync.
5. Remove/collapse `sync_planning_service.py` if it only duplicates the existing async payload/planner construction.
6. Keep Kafka-specific parts preparation/publication outside the shared payload builder.
7. Make the transport decision only after the common payload is built: async -> existing Kafka path, sync -> `SyncExecutionCoordinator`.
8. Simplify configuration and Helm values.
9. Simplify `SyncExecutionCoordinator`.
10. Simplify `SyncStepClient`.
11. Simplify Executor `AgentOrchestrator` changes to only execution-mode/Kafka-gating behavior.
12. Preserve COIN M2M authentication.
13. Preserve `finalization_service.py`.
14. Preserve the four sync APIs and common sync helper.
15. Remove/update stale tests for deleted features.
16. Run the full test suites.
17. Render/validate Helm templates.
18. Search the repositories for removed names to ensure no stale references remain.
19. Produce a final summary listing:
    - files kept/modified,
    - files deleted/reverted,
    - environment variables remaining,
    - tests run and results,
    - any unresolved issues.

Do not implement additional improvements during this cleanup without explicit approval.

## Final Payload-Reuse Decision Rule

When Copilot inspects the current Orchestration implementation, use this decision:

```text
Can the existing async payload/planning construction be extracted/reused
without changing its business behavior?
        |
      YES
        |
        v
Create/reuse ONE common payload-preparation function.
        |
        +--> ASYNC: existing Kafka-specific preparation + publish
        |
        +--> SYNC: SyncExecutionCoordinator

      NO
        |
        v
Keep the smallest sync-specific adapter necessary,
but still reuse lower-level planner/model/DB helpers and avoid copying logic.
```

Do not keep `sync_planning_service.py` merely because it already exists on the branch. Keep it only if the code inspection proves it has a real, minimal sync-specific responsibility after the shared payload preparation is extracted.
