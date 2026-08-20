# SYNC HITL + `/sync/resume` Implementation Changes

> **Repositories**
>
> - `181229.genaiservices.agentic-agent-executor`
> - `181229.genaiservices.agentic-orchestration`
>
> **Note:** Line numbers are approximate based on the screenshots. Use the function names as the exact code anchors.

---

# 1. Executor — Initialize correlation/audit context

## File

```text
181229.genaiservices.agentic-agent-executor/
executor/api/api.py
```

## Change 1A — Add import

Near the other Executor imports, around lines **15–20**:

```python
from executor.core.tracking import add_log_param
```

## Change 1B — Set ContextVars at the HTTP entry point

Inside:

```python
async def execute_agent_step_sync(
    task_payload: TaskPayloadModel,
    token_claims: dict = Depends(JWTBearer()),
) -> Dict[str, Any]:
```

Immediately after the docstring add:

```python
add_log_param(
    task_payload.x_correlation_id,
    task_payload.x_application_id,
    task_payload.x_soeid,
)
```

The beginning should look like:

```python
@api_router.post(
    "/internal/v1/agent-step",
    tags=["Internal"],
    include_in_schema=False,
)
async def execute_agent_step_sync(
    task_payload: TaskPayloadModel,
    token_claims: dict = Depends(JWTBearer()),
) -> Dict[str, Any]:
    """Execute exactly one planned step via direct HTTP without Kafka chaining."""

    add_log_param(
        task_payload.x_correlation_id,
        task_payload.x_application_id,
        task_payload.x_soeid,
    )

    steps = task_payload.plan.get("steps", []) if task_payload.plan else []

    if (
        task_payload.current_step is None
        or int(task_payload.current_step) >= len(steps)
    ):
        raise GenaiCommonException(
            code=ErrorCodes.GR006,
            message="Invalid current_step for one-step execution",
        )

    current_step = steps[int(task_payload.current_step)]
    agent_name = task_payload.next_agent or current_step.agent_name

    if not agent_name:
        raise GenaiCommonException(
            code=ErrorCodes.GR006,
            message="next_agent or current step agent_name is required",
        )

    runtime_state = RuntimeState(**task_payload.model_dump())

    orchestrator = AgentOrchestrator()

    updated_payload = await orchestrator.handle_request(
        runtime_state,
        agent_name,
        execution_mode=runtime_state.execution_mode,
    )
```

This fixes Executor logs and `audit_log` correlation propagation.

---

# 2. Executor — Return `interruption` from direct step API

## File

```text
181229.genaiservices.agentic-agent-executor/
executor/api/api.py
```

## Location

Inside:

```python
execute_agent_step_sync()
```

Around the response dictionary near lines **50–65**.

## Current

```python
response: Dict[str, Any] = {
    "x_correlation_id": updated_payload.x_correlation_id,
    "current_step": updated_payload.current_step,
    "next_agent": updated_payload.next_agent,
    "event_type": updated_payload.event_type,
    "plan": updated_payload.plan,
    "state": updated_payload.state,
    "error": (
        updated_payload.error.model_dump()
        if updated_payload.error
        else None
    ),
}
```

## Change to

```python
response: Dict[str, Any] = {
    "x_correlation_id": updated_payload.x_correlation_id,
    "current_step": updated_payload.current_step,
    "next_agent": updated_payload.next_agent,
    "event_type": updated_payload.event_type,
    "plan": updated_payload.plan,
    "state": updated_payload.state,
    "interruption": (
        updated_payload.interruption.model_dump()
        if updated_payload.interruption
        else None
    ),
    "error": (
        updated_payload.error.model_dump()
        if updated_payload.error
        else None
    ),
}
```

## Reason

Executor already creates:

```python
runtime_state.event_type = INTERRUPTION_REQUEST
```

and:

```python
runtime_state.interruption = RuntimeInterruption(...)
```

but without this change the actual interruption information is lost across the HTTP boundary.

---

# 3. Orchestration — Make `SyncExecutionCoordinator` interruption-aware

## File

```text
181229.genaiservices.agentic-orchestration/
orchestration/service/sync_execution_coordinator.py
```

---

## Change 3A — Import `INTERRUPTION_REQUEST`

Current:

```python
from orchestration.models.constants import AGENT_EXECUTION_FINAL_RESPONSE
```

Change to:

```python
from orchestration.models.constants import (
    AGENT_EXECUTION_FINAL_RESPONSE,
    INTERRUPTION_REQUEST,
)
```

---

## Change 3B — Add `RuntimeService`

Add import:

```python
from orchestration.service.runtime_service import RuntimeService
```

Find:

```python
def __init__(self) -> None:
    self.environment = get_environment()
```

Change to:

```python
def __init__(
    self,
    runtime_service: RuntimeService | None = None,
) -> None:
    self.environment = get_environment()
    self.runtime_service = runtime_service or RuntimeService()
```

## Reason

When the Executor pauses, the fully updated RuntimeState must be persisted before returning the interruption to the caller.

---

# 4. Orchestration — Merge interruption returned by Executor

## File

```text
181229.genaiservices.agentic-orchestration/
orchestration/service/sync_execution_coordinator.py
```

## Function

```python
_merge_step_response(...)
```

Inside:

```python
merged = {
    **current_payload.model_dump(),
    ...
}
```

add:

```python
"interruption": response_payload.get(
    "interruption",
    (
        current_payload.interruption.model_dump()
        if current_payload.interruption
        else None
    ),
),
```

The relevant block should become:

```python
merged = {
    **current_payload.model_dump(),

    "current_step": response_payload.get(
        "current_step",
        current_payload.current_step,
    ),

    "next_agent": response_payload.get(
        "next_agent",
        current_payload.next_agent,
    ),

    "event_type": response_payload.get(
        "event_type",
        current_payload.event_type,
    ),

    "plan": response_payload.get(
        "plan",
        current_payload.plan,
    ),

    "state": response_payload.get(
        "state",
        current_payload.state,
    ),

    "interruption": response_payload.get(
        "interruption",
        (
            current_payload.interruption.model_dump()
            if current_payload.interruption
            else None
        ),
    ),

    "error": response_payload.get(
        "error",
        (
            current_payload.error.model_dump()
            if current_payload.error
            else None
        ),
    ),
}
```

---

## Change 4B — Preserve `RuntimeState`

At the bottom of `_merge_step_response()`:

### Current

```python
return TaskPayloadModel(**merged)
```

### Change to

```python
return type(current_payload)(**merged)
```

## Reason

If the coordinator is operating on a persisted:

```python
RuntimeState
```

we want:

```text
RuntimeState
    ↓
merge
    ↓
RuntimeState
```

not:

```text
RuntimeState
    ↓
merge
    ↓
TaskPayloadModel
```

Otherwise RuntimeState-specific persistence information may be lost.

---

# 5. Orchestration — Stop and persist on `INTERRUPTION_REQUEST`

## File

```text
181229.genaiservices.agentic-orchestration/
orchestration/service/sync_execution_coordinator.py
```

## Function

```python
async def execute(...)
```

Find this section:

```python
response_payload = await self._dispatch_one_step(
    task_payload,
    executor_url,
    remaining,
    coin_scope,
)

task_payload = self._merge_step_response(
    task_payload,
    response_payload,
)

if task_payload.event_type == AGENT_EXECUTION_FINAL_RESPONSE:
    return await build_execution_response(
        task_payload,
        orchestrator_config,
    )
```

Change it to:

```python
response_payload = await self._dispatch_one_step(
    task_payload,
    executor_url,
    remaining,
    coin_scope,
)

task_payload = self._merge_step_response(
    task_payload,
    response_payload,
)

if task_payload.event_type == INTERRUPTION_REQUEST:
    task_payload = await self.runtime_service.update(task_payload)

    interruption_request = (
        task_payload.interruption
        .interruption_request
        .model_dump(by_alias=True)
    )

    return {
        "x_correlation_id": task_payload.x_correlation_id,
        "status": "SUCCESS",
        "response": interruption_request,
        "event_type": INTERRUPTION_REQUEST,
    }

if task_payload.event_type == AGENT_EXECUTION_FINAL_RESPONSE:
    return await build_execution_response(
        task_payload,
        orchestrator_config,
    )
```

## Important

The interruption check must be **before** the final-response check/next loop iteration.

When Executor says:

```text
INTERRUPTION_REQUEST
```

the coordinator must stop.

It must not dispatch the next executor call automatically.

---

# 6. Existing ASYNC behavior must remain unchanged

## File

```text
181229.genaiservices.agentic-orchestration/
orchestration/service/interruption_service.py
```

Do **not** change the existing ASYNC implementation.

It currently does:

```text
/resume
   ↓
RuntimeState
   ↓
event_type = INTERRUPTION_RESPONSE
   ↓
Kafka
   ↓
Executor
```

That remains the existing ASYNC behavior.

---

# 7. Extract common resume validation

## File

```text
181229.genaiservices.agentic-orchestration/
orchestration/api/api.py
```

## Location

Immediately above the current:

```python
@api_router.post("/resume", ...)
```

route.

The existing `/resume` already contains logic for:

- `get_consumer_coin()`
- use-case configuration
- DB name/schema
- `runtime_service.get(interruption_id)`
- `X-SOEID` validation
- `X-Application-ID` validation
- pending tool confirmation validation
- attaching `interruption_response`

Move that common logic into a helper.

## Add

```python
async def _prepare_resume_runtime_state(
    request: ToolInterruptionResponse | AgentInterruptionResponse,
    usecase_id: str,
    x_application_id: str,
    x_soeid: str | None,
    token_claims: dict,
    runtime_service: RuntimeService,
) -> RuntimeState:

    consumer_coin = get_consumer_coin(token_claims)

    usecase_config = AgenticUsecaseConfigManager.get_use_case(
        usecase_id,
        consumer_coin,
    )

    if usecase_config is None:
        logger.error(
            "Use case configuration missing for "
            f"usecase_id: {usecase_id}, "
            f"consumer_coin: {consumer_coin}"
        )

        raise GenaiCommonException(
            error_code=ErrorCodes.GR004,
            message=(
                "Use case configuration missing for "
                f"config_id: {usecase_id}, "
                f"consumer_coin: {consumer_coin}"
            ),
            original_exception=ValueError(
                ErrorCodes.GR004.get_description()
            ),
        )

    db_name = (
        usecase_config.metadata.database_parameters.db_name
        if (
            usecase_config.metadata
            and usecase_config.metadata.database_parameters
        )
        else None
    )

    db_schema = (
        usecase_config.metadata.database_parameters.db_schema
        if (
            usecase_config.metadata
            and usecase_config.metadata.database_parameters
        )
        else None
    )

    runtime_state = await runtime_service.get(
        request.interruption_id,
        db_name=db_name,
        db_schema=db_schema,
    )

    if runtime_state is None:
        raise ValueError(
            "[Resumibility Error] - No stashed workflow found for "
            f"interruption_id: {request.interruption_id}."
        )

    if runtime_state.x_soeid != x_soeid:
        raise ValueError(
            "[Resumibility Error] - The user attempting to resume "
            f"the workflow (x_soeid: {x_soeid}) does not match "
            "the original user."
        )

    if runtime_state.x_application_id != x_application_id:
        raise ValueError(
            "[Resumibility Error] - The application attempting to "
            "resume the workflow "
            f"(x_application_id: {x_application_id}) does not "
            "match the original application."
        )

    if isinstance(request, ToolInterruptionResponse):

        interruption_cursor = (
            runtime_state.interruption.interruption_cursor
        )

        pending_tool_confirmations = getattr(
            interruption_cursor,
            "requested_tool_confirmations",
            {},
        )

        responded_tool_confirmations = (
            request.responded_tool_confirmations
        )

        if (
            not responded_tool_confirmations
            or set(responded_tool_confirmations.keys())
            != set(pending_tool_confirmations.keys())
        ):
            raise GenaiCommonException(
                error_code=ErrorCodes.AP011,
                message=ErrorCodes.AP011.get_description(),
            )

    runtime_state.interruption.interruption_response = request

    return runtime_state
```

> If your current `/resume` uses slightly different exception constructors or messages, preserve the existing implementation exactly and only extract it into this helper.

---

# 8. Refactor existing `/resume`

## File

```text
181229.genaiservices.agentic-orchestration/
orchestration/api/api.py
```

## Existing route

```python
@api_router.post("/resume", ...)
async def resume(...):
```

After extracting the common logic, the end of the existing ASYNC `/resume` should effectively become:

```python
runtime_state = await _prepare_resume_runtime_state(
    request=request,
    usecase_id=usecase_id,
    x_application_id=x_application_id,
    x_soeid=x_soeid,
    token_claims=token_claims,
    runtime_service=runtime_service,
)

runtime_state = await runtime_service.update(runtime_state)

return await interruption_service.resume(runtime_state)
```

Do not change:

```python
interruption_service.resume(runtime_state)
```

for ASYNC.

---

# 9. Add `/sync/resume`

## File

```text
181229.genaiservices.agentic-orchestration/
orchestration/api/api.py
```

## Location

Add immediately after the existing `/resume` route.

## Imports

Use the exact existing module paths in your repository.

You need equivalents of:

```python
from orchestration.models.constants import INTERRUPTION_RESPONSE
from orchestration.models.execution_mode import ExecutionMode

from orchestration.service.sync_execution_coordinator import (
    SyncExecutionCoordinator,
)
```

You also need the existing:

```python
OrchestratorConfigManager
```

import already used elsewhere in `api.py`.

---

## New endpoint

```python
@api_router.post(
    "/sync/resume",
    summary="Resume a paused synchronous agentic task",
    response_description="Synchronous execution resumed.",
    tags=["Task Execution"],
)
async def sync_resume(
    request: ToolInterruptionResponse | AgentInterruptionResponse,

    usecase_id: Annotated[
        str,
        Header(alias="Config-ID"),
    ],

    x_application_id: Annotated[
        str,
        Header(alias="X-Application-ID"),
    ],

    x_soeid: Annotated[
        str | None,
        Header(),
    ],

    token_claims: dict = Depends(JWTBearer()),

    runtime_service: RuntimeService = Depends(
        lambda: RuntimeService()
    ),

) -> Dict[str, Any]:

    runtime_state = await _prepare_resume_runtime_state(
        request=request,
        usecase_id=usecase_id,
        x_application_id=x_application_id,
        x_soeid=x_soeid,
        token_claims=token_claims,
        runtime_service=runtime_service,
    )

    if runtime_state.execution_mode != ExecutionMode.SYNC:
        raise GenaiCommonException(
            error_code=ErrorCodes.AP002,
            message=(
                "/sync/resume can only resume a SYNC execution. "
                "Stored execution mode is "
                f"{runtime_state.execution_mode}."
            ),
            original_exception=ValueError(
                "Non-SYNC runtime passed to /sync/resume"
            ),
        )

    runtime_state.event_type = INTERRUPTION_RESPONSE

    runtime_state = await runtime_service.update(
        runtime_state
    )

    orchestrator_config = (
        OrchestratorConfigManager.get_config(
            usecase_id
        )
    )

    executor_url = runtime_state.executor_url
    coin_scope = runtime_state.coin_scope

    if not executor_url or not coin_scope:
        raise GenaiCommonException(
            error_code=ErrorCodes.AP002,
            message=(
                "Stored synchronous runtime is missing "
                "executor configuration."
            ),
            original_exception=ValueError(
                "Missing executor_url or coin_scope"
            ),
        )

    coordinator = SyncExecutionCoordinator(
        runtime_service=runtime_service
    )

    return await coordinator.execute(
        runtime_state,
        orchestrator_config,
        executor_url,
        coin_scope,
    )
```

---

# 10. Why `INTERRUPTION_RESPONSE` must be set manually

The ASYNC implementation does this inside the Kafka publishing path:

```python
payload["event_type"] = INTERRUPTION_RESPONSE
```

SYNC does not publish a Kafka payload.

Therefore `/sync/resume` must explicitly do:

```python
runtime_state.event_type = INTERRUPTION_RESPONSE
```

before calling:

```python
SyncExecutionCoordinator.execute(...)
```

Otherwise Executor may receive the old:

```text
INTERRUPTION_REQUEST
```

again.

---

# 11. Verify `executor_url` and `coin_scope`

Before finalizing `/sync/resume`, verify the persisted `RuntimeState` model contains:

```python
runtime_state.executor_url
runtime_state.coin_scope
```

If both exist, use them as shown.

If either one does **not** exist, do not add random fields to `RuntimeState`.

Instead resolve them using the same logic used by the initial SYNC request:

```text
_get_executor_base_url(...)
```

and the existing COIN scope/config resolution.

---

# 12. Files NOT to modify

Do not modify these unless compilation/testing proves another concrete issue:

```text
181229.genaiservices.agentic-agent-executor/
executor/service/agent_execution_service.py

181229.genaiservices.agentic-agent-executor/
executor/util/db_logger_plugin.py

181229.genaiservices.agentic-orchestration/
orchestration/service/interruption_service.py

181229.genaiservices.agentic-orchestration/
orchestration/service/message_processing_service.py

181229.genaiservices.agentic-orchestration/
orchestration/service/sync_step_client.py

181229.genaiservices.agentic-orchestration/
orchestration/planner/static_planner.py

181229.genaiservices.agentic-orchestration/
orchestration/planner/dynamic_planner.py
```

---

# 13. Final SYNC interruption flow

```text
Client
  |
  v
SYNC task endpoint
  |
  v
SyncExecutionCoordinator
  |
  v
Executor /internal/v1/agent-step
  |
  v
ADK tool requires confirmation
  |
  v
AgentExecutionService
  |
  +--> event_type = INTERRUPTION_REQUEST
  |
  +--> interruption = RuntimeInterruption(...)
  |
  v
Executor API response
  |
  +--> event_type
  +--> interruption
  |
  v
SyncExecutionCoordinator
  |
  +--> merge interruption
  |
  +--> RuntimeService.update(...)
  |
  +--> STOP execution loop
  |
  v
Client receives INTERRUPTION_REQUEST
```

Expected response:

```json
{
  "x_correlation_id": "sync-hitl-test-001",
  "status": "SUCCESS",
  "response": {
    "interruption_id": "...",
    "requested_tool_confirmations": {
      "...": "..."
    }
  },
  "event_type": "INTERRUPTION_REQUEST"
}
```

---

# 14. Final `/sync/resume` flow

```text
POST /sync/resume
        |
        v
RuntimeService.get(interruption_id)
        |
        v
validate X-SOEID
        |
        v
validate X-Application-ID
        |
        v
validate responded_tool_confirmations
        |
        v
runtime_state.interruption.interruption_response = request
        |
        v
runtime_state.event_type = INTERRUPTION_RESPONSE
        |
        v
RuntimeService.update(...)
        |
        v
SyncExecutionCoordinator
        |
        v
Executor /internal/v1/agent-step
        |
        v
AgentOrchestrator resumes ADK execution
        |
        +-------------------------------+
        |                               |
        v                               v
AGENT_EXECUTION_FINAL_RESPONSE    INTERRUPTION_REQUEST
        |                               |
        v                               v
build_execution_response()       Persist again and stop
        |                               |
        v                               v
Final sync response              New HITL response
```

---

# 15. Testing checklist

## Initial SYNC interruption

- [ ] Invoke a tool that requires confirmation.
- [ ] Executor creates `INTERRUPTION_REQUEST`.
- [ ] Executor direct API returns `interruption`.
- [ ] Coordinator merges interruption.
- [ ] Coordinator does not dispatch another step.
- [ ] RuntimeState is persisted.
- [ ] Response contains `interruption_id`.
- [ ] Response contains `requested_tool_confirmations`.

## `/sync/resume`

- [ ] Send the same `Config-ID`.
- [ ] Send the same `X-Application-ID`.
- [ ] Send the same `X-SOEID`.
- [ ] Send the returned `interruption_id`.
- [ ] `responded_tool_confirmations` keys exactly match the pending keys.
- [ ] RuntimeState is loaded.
- [ ] `interruption_response` is attached.
- [ ] `event_type` changes to `INTERRUPTION_RESPONSE`.
- [ ] Updated runtime is persisted.
- [ ] No Kafka publish occurs.
- [ ] Executor is called through direct HTTP.
- [ ] Execution resumes.
- [ ] Final result is returned synchronously or another interruption is returned.

## Correlation regression

- [ ] Executor logs contain `X-Correlation-ID`.
- [ ] `audit_log` INVOCATION rows contain `x_correlation_id`.
- [ ] AGENT rows contain `x_correlation_id`.
- [ ] LLM rows contain `x_correlation_id`.
- [ ] TOOL rows contain `x_correlation_id`.

---

# 16. Files changed

```text
181229.genaiservices.agentic-agent-executor/
└── executor/
    └── api/
        └── api.py
```

Changes:

```text
1. import add_log_param
2. call add_log_param(...) in execute_agent_step_sync()
3. return interruption in internal step response
```

```text
181229.genaiservices.agentic-orchestration/
└── orchestration/
    ├── api/
    │   └── api.py
    │
    └── service/
        └── sync_execution_coordinator.py
```

`sync_execution_coordinator.py` changes:

```text
1. import INTERRUPTION_REQUEST
2. add RuntimeService
3. merge interruption
4. preserve RuntimeState type
5. stop on INTERRUPTION_REQUEST
6. persist paused RuntimeState
7. return existing interruption response contract
```

`api.py` changes:

```text
1. extract common resume validation/preparation
2. preserve existing ASYNC /resume
3. add /sync/resume
4. set INTERRUPTION_RESPONSE
5. persist resume response
6. continue using SyncExecutionCoordinator
7. no Kafka
```

---

# 17. Separate issue

The PostgreSQL error:

```text
asyncpg.exceptions.InsufficientPrivilegeError:
permission denied for table adk_internal_metadata
```

is unrelated to the SYNC/HITL changes above.

Handle that database permission/configuration issue separately.