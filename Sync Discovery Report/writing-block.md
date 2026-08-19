# Copilot Implementation Instructions — Minimal Changes for Sync / Async / Hybrid Execution Mode

## Objective

Implement the minimum code changes required so that:

1. The request follows the **existing planner flow first**.
2. Static or Dynamic Planner continues to create the execution plan exactly as today.
3. The execution mode is read from `response_config.mode`.
4. `executor_url` and `coin_scope` are also read from `response_config`.
5. These values are carried forward in the execution payload/runtime state.
6. After the planner has created the plan, inspect the mode and choose the execution transport:
   - `ASYNC` → preserve existing Kafka-based execution.
   - `SYNC` → call `SyncExecutionCoordinator`.
   - `HYBRID` → call `SyncExecutionCoordinator`, then publish the final response through the existing Kafka response path.
7. If `executor_url` is missing from `response_config` for `SYNC` or `HYBRID`, fall back to a Helm/environment-configured default executor URL.
8. Preserve the existing async behavior as much as possible. Avoid large refactoring.

---

## 1. Important Architecture Rule

Do **not** create a separate planner implementation for sync.

Do **not** bypass `StaticPlanner` or `DynamicPlanner`.

The flow should remain:

```text
API
  ↓
load usecase config
  ↓
load orchestrator config
  ↓
PlannerFactory / existing planner selection
  ↓
StaticPlanner OR DynamicPlanner
  ↓
plan created
  ↓
TaskPayload / RuntimeState assembled
  ↓
CHECK EXECUTION MODE
  ↓
ASYNC / SYNC / HYBRID transport decision
```

The planner decides:

```text
WHAT agents should execute
and
IN WHAT order
```

The execution mode decides:

```text
HOW those planned steps should execute
```

These two responsibilities must remain separate.

---

## 2. Configuration Changes

Extend the existing `response_config` model so it can support:

```yaml
response_config:
  mode: ASYNC
  executor_url: ""
  coin_scope: ""
```

The exact enum/string naming should follow the existing project conventions.

Recommended values:

```python
ASYNC
SYNC
HYBRID
```

Default behavior must remain backward compatible.

If `mode` is absent:

```python
mode = ASYNC
```

This is important because existing use cases should continue using the old Kafka flow without any configuration changes.

---

## 3. Payload / Runtime State Changes

Add the following fields to the payload/runtime model used between planner and execution:

```python
execution_mode
executor_url
coin_scope
```

Use the project's existing model naming conventions. If `mode` is already the preferred field name, keep `mode`; do not introduce duplicate fields unnecessarily.

Suggested shape:

```python
class TaskPayloadModel(...):
    ...
    execution_mode: ExecutionMode = ExecutionMode.ASYNC
    executor_url: str | None = None
    coin_scope: str | None = None
```

If the system now uses `RuntimeState` instead of `TaskPayloadModel` at this layer, add the fields there instead.

Do not create two sources of truth.

The mode and endpoint information should travel with the current execution state once resolved.

---

## 4. Resolve Execution Configuration Once

Create or reuse a small helper responsible for resolving:

```python
mode
executor_url
coin_scope
```

Conceptually:

```python
def resolve_execution_config(usecase_config, environment):
    response_config = usecase_config.response_config

    mode = response_config.mode or ExecutionMode.ASYNC

    executor_url = response_config.executor_url
    coin_scope = response_config.coin_scope

    if mode in (ExecutionMode.SYNC, ExecutionMode.HYBRID):
        if not executor_url:
            executor_url = environment.sync_executor_url

    return mode, executor_url, coin_scope
```

Use actual project configuration names instead of introducing `environment.sync_executor_url` blindly.

The fallback must read from the existing Helm/environment settings.

---

## 5. Helm Fallback for `executor_url`

Add a Helm-configurable fallback executor URL.

Example values structure:

```yaml
sync:
  executorUrl: "http://agent-executor/internal/v1/agent-step"
```

Or reuse the project's current sync configuration section if one already exists.

Expose it to the container using the existing environment-variable pattern, for example:

```yaml
- name: SYNC_EXECUTOR_URL
  value: {{ .Values.sync.executorUrl | quote }}
```

Then expose it through the application's environment settings model.

Example only:

```python
sync_executor_url: str | None = None
```

Do not hard-code:

```text
http://127.0.0.1:7998
```

inside application code.

The resolution order must be:

```text
response_config.executor_url
        ↓ if missing
Helm/environment executor URL
        ↓ if still missing
raise configuration error for SYNC/HYBRID
```

For `ASYNC`, absence of `executor_url` must not cause any failure.

---

## 6. `coin_scope` Resolution

Read `coin_scope` from:

```python
usecase_config.response_config.coin_scope
```

and place it into the execution payload/runtime state.

Do not derive the sync executor scope later inside the HTTP client if it has already been resolved at orchestration level.

Desired flow:

```text
response_config.coin_scope
        ↓
payload/runtime state
        ↓
SyncExecutionCoordinator
        ↓
SyncStepClient / token provider
```

If the current token roller requires the scope during construction rather than reading it from the payload, minimally change it so the coordinator/client can pass the resolved `coin_scope`.

Do not introduce a global hard-coded executor scope if the requirement is per-response-config.

---

## 7. Planner Changes — Keep Existing Planner Logic

### Static Planner

Keep the existing logic:

```python
if agent_name:
    selected_agents = [agent_name]
else:
    selected_agents = self.planner_config.planner_metadata.static_planner
```

Then keep:

```python
plan = await build_static_style_plan(...)
```

Then:

```python
agent_payload = await assemble_task_payload(...)
```

Do not send to Kafka immediately after this without checking execution mode.

Current conceptual behavior:

```python
plan = ...
agent_payload = ...
await send_to_kafka(agent_payload, ...)
```

Change it to:

```python
plan = ...
agent_payload = ...

return await dispatch_planned_execution(
    agent_payload,
    ...
)
```

or perform the small mode check directly if introducing a helper would cause more churn.

---

### Dynamic Planner

Keep the existing LLM-based selection code:

```python
planner_prompt = ...
registered_agents = ...
prompt_context = ...
converted_response = await generate_llm_response(...)
plan = Steps.model_validate_json(...)
selected_agents = [...]
plan = await generate_execution_plan(...)
```

Do not duplicate this logic in `sync_planning_service.py`.

Continue using the existing Dynamic Planner to create the plan.

After:

```python
agent_payload = ...
await add_agent_status(agent_payload)
```

do the same execution-mode decision as Static Planner.

---

## 8. Mode Must Be Checked Only After Plan Creation

This ordering is mandatory.

Correct:

```text
request
  ↓
select planner
  ↓
planner creates plan
  ↓
assemble payload
  ↓
add agent statuses
  ↓
check mode
```

Do not do:

```text
request
  ↓
if SYNC:
   use separate sync planner
```

The sync requirement does not change planning behavior.

It only changes execution transport.

---

## 9. Common Execution Dispatcher

Prefer a very small common dispatcher so Static and Dynamic planners do not duplicate mode handling.

Example:

```python
async def dispatch_planned_execution(
    task_payload,
    orchestrator_config,
    usecase_config,
):
    mode = task_payload.execution_mode

    if mode == ExecutionMode.ASYNC:
        internal_kafka_config = build_agentic_internal_kafka_environment()
        await send_to_kafka(task_payload, internal_kafka_config)
        return task_payload

    if mode in (ExecutionMode.SYNC, ExecutionMode.HYBRID):
        coordinator = SyncExecutionCoordinator()

        final_response = await coordinator.execute(
            task_payload,
            orchestrator_config,
            task_payload.executor_url,
        )

        if mode == ExecutionMode.HYBRID:
            await publish_hybrid_final_response(
                task_payload,
                final_response,
                usecase_config,
            )

        return final_response

    raise GenaiCommonException(...)
```

Use existing return contracts where possible.

Do not introduce this helper if the surrounding API requires planner methods to always return a specific runtime object; adapt the idea to existing contracts.

---

## 10. ASYNC Behavior

For:

```python
mode == ASYNC
```

preserve existing behavior:

```text
plan created
  ↓
payload created
  ↓
send_to_kafka()
  ↓
Executor receives execution request
  ↓
Kafka chaining continues
```

This path should change as little as possible.

Existing async APIs should continue returning the same acknowledgement as before.

Do not modify their external contract.

---

## 11. SYNC Behavior

For:

```python
mode == SYNC
```

after the plan is created:

```text
TaskPayload
  ↓
SyncExecutionCoordinator.execute()
  ↓
direct HTTP call to executor for current step
  ↓
executor returns updated payload
  ↓
merge updated payload
  ↓
next step
  ↓
repeat until final
  ↓
finalize()
  ↓
return final business response directly
```

Do **not** send the initial task payload to Kafka.

Do **not** allow the executor to perform Kafka chaining for this path.

---

## 12. HYBRID Behavior

For:

```python
mode == HYBRID
```

the request may arrive through the existing async API, but internally the workflow must use the sync/direct architecture.

Desired flow:

```text
Existing Async API
  ↓
existing validation / guardrail / planner selection
  ↓
StaticPlanner or DynamicPlanner
  ↓
plan created
  ↓
payload assembled
  ↓
mode == HYBRID
  ↓
SyncExecutionCoordinator
  ↓
direct HTTP executor calls
  ↓
all steps completed
  ↓
final response generated
  ↓
publish final response using Kafka response configuration
```

Very important:

For Hybrid, **do not publish the initial execution request to internal Kafka**.

Otherwise both flows could execute:

```text
Kafka async execution
+
direct sync execution
```

which can cause duplicate execution.

Hybrid should use Kafka only for the **final response/delivery contract**, not for step execution.

---

## 13. API Behavior

Keep the existing async API route.

It should continue invoking the existing planner via:

```python
planner = OrchestratorFactory.get_planner(usecase_id)
await planner.plan(...)
```

or whatever the latest main implementation currently uses.

The API route should not independently recreate Static/Dynamic planning logic.

The planner/payload should contain the mode resolved from response configuration.

For explicit synchronous APIs, the route may force:

```python
mode = SYNC
```

only if that is still a product requirement.

Otherwise configuration mode should be the source of truth.

Do not unnecessarily maintain both:

```python
force_static
force_sync
mode
```

unless those flags are required by existing testing endpoints.

---

## 14. `assemble_task_payload()` Changes

Update the shared payload builder so it accepts/resolves execution configuration.

Conceptually:

```python
async def assemble_task_payload(
    request,
    plan,
    x_correlation_id,
    x_application_id,
    x_soeid,
    usecase_id,
    usecase_name,
    consumer_coin,
    usecase_config,
    orchestrator_config,
    session_id=None,
    execution_mode=ExecutionMode.ASYNC,
    executor_url=None,
    coin_scope=None,
):
```

Then populate:

```python
task_payload = TaskPayloadModel(
    ...
    execution_mode=execution_mode,
    executor_url=executor_url,
    coin_scope=coin_scope,
)
```

Prefer resolving these values before calling this function rather than having `assemble_task_payload()` repeatedly inspect configuration.

---

## 15. Recommended Source of Values

Resolve the fields as follows:

```text
execution_mode
    <- usecase_config.response_config.mode
    <- default ASYNC

executor_url
    <- usecase_config.response_config.executor_url
    <- fallback Helm/environment value for SYNC/HYBRID

coin_scope
    <- usecase_config.response_config.coin_scope
```

If the current configuration model locates these fields under a nested executor object, preserve that model rather than changing the YAML unnecessarily.

For example, if existing config is:

```yaml
response_config:
  executor:
    url: ...
    coin_scope: ...
```

then use that structure.

The important requirement is the behavior, not a specific YAML nesting.

---

## 16. Validation Rules

Apply these validations:

```python
if mode in (SYNC, HYBRID) and not executor_url:
    raise configuration exception
```

For `coin_scope`, follow the authentication requirement.

If executor authentication requires a COIN M2M token for both Sync and Hybrid, then:

```python
if mode in (SYNC, HYBRID) and not coin_scope:
    raise configuration exception
```

unless there is already a valid environment/global fallback for the scope.

For Async:

```python
executor_url is optional
coin_scope is optional
```

because neither is required by the current Kafka execution architecture.

---

## 17. Executor Service

Keep the executor's current one-step endpoint:

```text
POST /internal/v1/agent-step
```

The orchestration service should call it using:

```python
payload.executor_url
```

or the resolved executor URL passed into the coordinator.

Executor execution mode should remain:

```python
ExecutionMode.SYNC_DIRECT
```

for direct calls.

Hybrid does not require a new executor-side execution mode unless there is some executor-specific behavior that truly differs.

From the Executor's perspective:

```text
SYNC and HYBRID are both direct one-step requests.
```

The difference between Sync and Hybrid belongs primarily in Orchestration because that is where final delivery is decided.

---

## 18. Sync HTTP Authentication

The token used for:

```text
Orchestration → Executor
```

must use the resolved:

```python
coin_scope
```

Do not continue using a hard-coded/global scope if `coin_scope` is now configured per use case.

Conceptual API:

```python
token = get_sync_executor_token_roller(
    scope=task_payload.coin_scope
).get_token()
```

If `ProxyTokenRoller` instances are cached, the cache key must account for scope.

Do not cache one token roller globally and then reuse it for different scopes.

---

## 19. Final Response Handling

Keep the existing shared final response builder/finalizer.

Both:

```text
SYNC
HYBRID
```

should use the same business-response generation logic after all agent steps complete.

Then:

```text
SYNC
   ↓
return response over HTTP
```

while:

```text
HYBRID
   ↓
publish response through existing response Kafka configuration
```

Avoid duplicating output-generation, error-normalization, or chat-history code.

Reuse:

```python
finalize(...)
```

or the latest shared response-builder implementation.

---

## 20. Conversational / Native Conversational Flows

Do not remove existing:

```text
chat history
native conversational state
session_id
parts/offload behavior
runtime state
```

from the planner/payload path.

These should continue to be assembled before the mode switch.

Execution mode must not alter request semantics.

It only alters execution transport and final delivery.

---

## 21. Human-in-the-Loop

Sync and Hybrid currently should not silently enter the existing Kafka-based HIL flow unless explicitly supported.

If current business rules say HIL is unsupported for synchronous direct execution, preserve the eligibility validation:

```python
if mode in (SYNC, HYBRID) and orchestrator_config.metadata.is_human_in_loop_enabled:
    raise GenaiCommonException(...)
```

Do not remove existing async HIL behavior.

---

## 22. Minimal Files Expected to Change

Prefer limiting implementation to files equivalent to:

```text
orchestration/models/...response_config...
orchestration/models/task_payload.py or runtime.py
orchestration/config/environment.py
orchestration/service/task_payload_builder.py
orchestration/planner/static_planner.py
orchestration/planner/dynamic_planner.py
orchestration/service/sync_execution_coordinator.py
orchestration/service/sync_step_client.py
orchestration/dependencies.py
helm/templates/deployment.yaml
helm/*-values.yaml
```

Possibly a small shared execution dispatcher/service.

Avoid major changes to:

```text
message_processing_service.py
existing Kafka consumer
executor agent execution internals
dynamic planner LLM logic
```

unless required for Hybrid final response publishing.

---

## 23. Do Not Do These

Do not:

```text
create a separate duplicated sync dynamic planner
create a separate duplicated sync static planner
perform agent selection in API layer
skip PlannerFactory
send Hybrid initial task to Kafka
hard-code executor_url
hard-code coin_scope
change existing Async API response contract
duplicate final response generation
remove chat-history/runtime functionality
replace current planner implementation wholesale
```

---

## 24. Target Static Planner Shape

The resulting static planner should conceptually look like:

```python
async def plan(...):

    if agent_name:
        selected_agents = [agent_name]
    else:
        selected_agents = self.planner_config.planner_metadata.static_planner

    if not selected_agents:
        raise GenaiCommonException(...)

    plan = await build_static_style_plan(
        selected_agents,
        plan_name="selected_static_plan",
    )

    mode, executor_url, coin_scope = resolve_execution_config(
        usecase_config,
        environment,
    )

    agent_payload = await assemble_task_payload(
        request=request,
        plan=plan,
        x_correlation_id=x_correlation_id,
        x_application_id=x_application_id,
        x_soeid=x_soeid,
        usecase_id=usecase_id,
        usecase_name=usecase_name,
        consumer_coin=consumer_coin,
        usecase_config=usecase_config,
        orchestrator_config=orchestrator_config,
        session_id=session_id,
        execution_mode=mode,
        executor_url=executor_url,
        coin_scope=coin_scope,
    )

    return await dispatch_planned_execution(
        agent_payload,
        orchestrator_config,
        usecase_config,
    )
```

Use actual project classes/types and latest-main signatures.

---

## 25. Target Dynamic Planner Shape

The resulting Dynamic Planner should conceptually remain:

```python
async def plan(...):

    planner_prompt = ...
    registered_agents = ...

    converted_response = await generate_llm_response(...)

    plan_obj = Steps.model_validate_json(...)

    selected_agents = [
        step.agent_name
        for step in plan_obj.steps
    ]

    plan = await generate_execution_plan(
        selected_agents,
        plan_obj,
    )

    mode, executor_url, coin_scope = resolve_execution_config(
        usecase_config,
        environment,
    )

    agent_payload = await assemble_task_payload(
        request=request_body,
        plan=plan,
        ...
        execution_mode=mode,
        executor_url=executor_url,
        coin_scope=coin_scope,
    )

    return await dispatch_planned_execution(
        agent_payload,
        orchestrator_config,
        usecase_config,
    )
```

Everything before payload dispatch should stay as close as possible to the existing Dynamic Planner.

---

## 26. Final Expected Flow

### ASYNC

```text
Async API
   ↓
PlannerFactory
   ↓
Static/Dynamic Planner
   ↓
plan
   ↓
assemble payload
   ↓
mode = ASYNC
   ↓
Kafka
   ↓
Executor async flow
```

### SYNC

```text
Sync API / configured sync request
   ↓
PlannerFactory
   ↓
Static/Dynamic Planner
   ↓
plan
   ↓
assemble payload
     mode
     executor_url
     coin_scope
   ↓
mode = SYNC
   ↓
SyncExecutionCoordinator
   ↓
Executor direct HTTP
   ↓
finalize
   ↓
HTTP final response
```

### HYBRID

```text
Async API
   ↓
PlannerFactory
   ↓
Static/Dynamic Planner
   ↓
plan
   ↓
assemble payload
     mode
     executor_url
     coin_scope
   ↓
mode = HYBRID
   ↓
SyncExecutionCoordinator
   ↓
Executor direct HTTP
   ↓
finalize
   ↓
Kafka final response
```

---

## 27. Acceptance Criteria

Implementation is complete only when all of the following are true:

- [ ] Existing Static Planner is reused.
- [ ] Existing Dynamic Planner is reused.
- [ ] No separate sync planning implementation duplicates planner logic.
- [ ] `response_config.mode` is read.
- [ ] Missing mode defaults to `ASYNC`.
- [ ] `response_config.executor_url` is read.
- [ ] `response_config.coin_scope` is read.
- [ ] Mode, executor URL and COIN scope are carried in the execution state/payload.
- [ ] If Sync/Hybrid executor URL is missing in response config, Helm/environment fallback is used.
- [ ] If both response config and Helm executor URL are missing for Sync/Hybrid, configuration error is raised.
- [ ] Async does not require an executor URL.
- [ ] Planner always creates the execution plan before execution-mode branching.
- [ ] Async still sends the initial payload to Kafka.
- [ ] Sync does not send the initial payload to Kafka.
- [ ] Hybrid does not send the initial payload to Kafka.
- [ ] Sync executes all steps through `SyncExecutionCoordinator`.
- [ ] Hybrid executes all steps through `SyncExecutionCoordinator`.
- [ ] Sync final response is returned over HTTP.
- [ ] Hybrid final response is published using the existing Kafka response configuration.
- [ ] Existing async behavior remains backward compatible.
- [ ] Existing conversational/native/session/parts behavior remains intact.
- [ ] No unnecessary changes are made to Executor's core agent execution logic.
- [ ] Unit tests cover Async, Sync and Hybrid dispatch decisions.
- [ ] Unit tests cover executor URL config precedence.
- [ ] Unit tests verify Hybrid never starts Kafka-based step execution.

---

## 28. Implementation Priority

Make changes in this order:

```text
1. ExecutionMode/config model
2. Helm/environment fallback
3. Payload/runtime fields
4. Shared config resolver
5. Shared payload assembly
6. Static Planner mode-aware dispatch
7. Dynamic Planner mode-aware dispatch
8. SyncExecutionCoordinator use resolved URL/scope
9. Hybrid final Kafka publishing
10. Unit tests
```

Keep the patch small and reuse existing functions whenever they already perform the required responsibility.