# Codex Deep Implementation Handoff — Agentic Orchestration Sync / Async / Hybrid

## Purpose

This document transfers the architectural and implementation context accumulated during development of synchronous and hybrid agent execution.

The implementation has already been made.

The purpose of this document is **not to ask for another implementation**.

Codex should use this document to understand:

- the original system,
- why the changes were needed,
- the current execution architecture,
- the purpose of each important class,
- the relationship between Orchestration and Executor,
- how ASYNC / SYNC / HYBRID differ,
- why some fields exist only on the Orchestration payload,
- and which existing behaviors must remain untouched.

When this document and current source code disagree on an exact class name, parameter name, enum spelling, or latest-main runtime abstraction, treat the **current repository as source of truth**.

Do not “fix” intentional behavior merely because an older design described here was different.

---

# 1. Repositories

There are two services/repositories.

## Agentic Orchestration

Responsible for:

```text
HTTP API
authentication
use-case configuration
orchestrator configuration
guardrails
planning
execution routing
sync workflow coordination
final response generation
final response delivery
```

Relevant source areas include:

```text
orchestration/api/api.py

orchestration/planner/static_planner.py
orchestration/planner/dynamic_planner.py
orchestration/planner/base_planner.py

orchestration/service/task_payload_builder.py
orchestration/service/sync_execution_coordinator.py
orchestration/service/sync_step_client.py
orchestration/service/execution_response_builder.py
orchestration/service/message_processing_service.py

orchestration/dependencies.py
orchestration/config/environment.py

use-case config models
TaskPayload / RuntimeState models

Helm deployment / values
```

---

## Agent Executor

Responsible for:

```text
receiving execution work
authenticating Orchestration M2M requests
running one agent step
AgentFactory
SessionManager
ADK / Runner
LLM
tools
step status updates
routing state updates
Kafka continuation for the legacy async path
```

Important areas include:

```text
executor/api/api.py
executor/api/auth.py

executor/models/execution_mode.py

executor/service/agent_orchestrator.py
executor/service/agent_execution_service.py

Executor environment / Helm configuration
```

---

# 2. Original architecture before Sync

Originally execution was fully Kafka based.

High-level flow:

```text
Caller
  ↓ HTTP
Orchestration
  ↓
authentication
configuration
guardrails
planner
payload creation
  ↓
internal Kafka
  ↓
Executor
  ↓
execute one agent
  ↓
next step?
  ├─ yes → publish AGENT_EXECUTION_REQUEST to Kafka again
  └─ no  → publish AGENT_EXECUTION_FINAL_RESPONSE
                    ↓
             Orchestration consumer
                    ↓
          message_processing_service
                    ↓
             final response
                    ↓
             ResponseService
                    ↓
          caller Kafka / webhook
```

The execution plan travels with the payload.

Executor historically owns continuation between agent steps.

Example:

```text
Plan:

Agent A
  ↓
Agent B
  ↓
Agent C
```

Async execution:

```text
Kafka
 ↓
Executor runs A
 ↓
Kafka
 ↓
Executor runs B
 ↓
Kafka
 ↓
Executor runs C
 ↓
Kafka final response
```

---

# 3. Existing routing fields

The workflow payload contains routing information such as:

```text
plan
current_step
next_agent
event_type
state
error
```

Initial values conceptually look like:

```text
current_step = 0
next_agent = first agent
event_type = AGENT_EXECUTION_REQUEST
```

After a non-terminal step:

```text
current_step = next step index
next_agent = next agent
event_type = AGENT_EXECUTION_REQUEST
```

After the final step:

```text
next_agent = ""
event_type = AGENT_EXECUTION_FINAL_RESPONSE
```

---

# 4. Sync feature — original implementation direction

The first Sync implementation introduced a direct HTTP execution architecture.

Instead of Kafka between Orchestration and Executor:

```text
Orchestration
   ↓ HTTP
Executor executes one step
   ↓ HTTP
Orchestration
   ↓
Executor next step
```

Orchestration therefore became the owner of the synchronous multi-step loop.

This resulted in the creation of:

```text
SyncExecutionCoordinator
SyncStepClient
Executor internal one-step HTTP endpoint
ExecutionMode.SYNC_DIRECT
COIN M2M authentication
shared execution response builder
```

---

# 5. Important design evolution

An earlier version of the Sync implementation had a separate flow similar to:

```text
build_sync_task_payload()
   ↓
select static or dynamic agents
   ↓
build execution plan
   ↓
assemble payload
   ↓
SyncExecutionCoordinator
```

This was initially done because the existing planner methods ended by publishing directly to Kafka.

However, this duplicated planning responsibility, particularly DynamicPlanner logic.

The final design was changed.

The current architectural rule is:

> **ASYNC, SYNC, and HYBRID all use the existing StaticPlanner / DynamicPlanner planning path. They diverge only after the plan and execution payload have been created.**

Any old sync-specific planner code that still exists should not be assumed to represent the final architecture without checking current call sites.

---

# 6. Final three execution modes

There are now three orchestration-level execution modes:

```text
ASYNC
SYNC
HYBRID
```

These are Orchestration-level execution semantics.

Do not confuse these with Executor's internal mode enum:

```text
ASYNC_KAFKA
SYNC_DIRECT
```

They represent different concepts.

---

# 7. Meaning of ASYNC

ASYNC preserves the legacy architecture.

```text
Caller
  ↓
existing async API
  ↓
StaticPlanner / DynamicPlanner
  ↓
plan
  ↓
payload
  ↓
mode == ASYNC
  ↓
internal Kafka
  ↓
Executor
  ↓
Kafka chaining between steps
  ↓
AGENT_EXECUTION_FINAL_RESPONSE
  ↓
Orchestration
  ↓
shared final response builder
  ↓
ResponseService
  ↓
caller Kafka / webhook
```

The existing async behavior must remain backward compatible.

---

# 8. Meaning of SYNC

SYNC uses the same planner but direct execution.

```text
Caller
  ↓
sync API
  ↓
authentication/configuration/guardrail
  ↓
existing StaticPlanner / DynamicPlanner
  ↓
plan
  ↓
payload
  ↓
mode == SYNC
  ↓
SyncExecutionCoordinator
  ↓
Executor HTTP one-step endpoint
  ↓
Agent A
  ↓ HTTP response
Orchestration
  ↓
Executor Agent B
  ↓
...
  ↓
terminal payload
  ↓
shared final response builder
  ↓
HTTP response returned directly to caller
```

No internal Kafka execution request should be created for the Sync execution path.

---

# 9. Meaning of HYBRID

HYBRID is very important.

Hybrid means:

```text
external API contract = ASYNC
internal agent execution = synchronous/direct
final response delivery = Kafka
```

Flow:

```text
Caller
  ↓
existing async API
  ↓
existing auth/config/guardrail
  ↓
existing StaticPlanner / DynamicPlanner
  ↓
plan
  ↓
payload
  ↓
mode == HYBRID
  ↓
SyncExecutionCoordinator
  ↓
direct HTTP Executor execution
  ↓
all steps complete
  ↓
shared final response generated
  ↓
existing ResponseService
  ↓
caller Kafka response
```

Hybrid MUST NOT publish the initial execution payload to the internal agent-execution Kafka topic.

Otherwise both:

```text
direct HTTP execution
```

and:

```text
Kafka execution
```

could run simultaneously.

That could execute agents twice and duplicate side effects.

---

# 10. Core design rule

Planning and transport are separate concerns.

```text
Planner
=
WHAT should execute
+
IN WHAT order
```

Execution mode:

```text
=
HOW the completed plan should execute
+
HOW the final answer should be delivered
```

Therefore the branch occurs after:

```text
plan creation
+
payload creation
```

not before planner selection.

---

# 11. Common planner flow

Current conceptual flow:

```text
API
  ↓
usecase config
  ↓
orchestrator config
  ↓
guardrail
  ↓
existing planner selection
  ↓
       ┌─────────────────┐
       │                 │
StaticPlanner       DynamicPlanner
       │                 │
       └────────┬────────┘
                ↓
             plan
                ↓
       execution payload
                ↓
        execution_mode?
```

---

# 12. StaticPlanner

StaticPlanner already knows how to determine the static agent list.

Normal path:

```text
planner_metadata.static_planner
```

Agent testing may override this with one selected agent.

Conceptually:

```python
if agent_name:
    selected_agents = [agent_name]
else:
    selected_agents = configured_static_agents
```

It validates that at least one agent exists.

Then it uses shared plan construction such as:

```text
build_static_style_plan(...)
```

Then:

```text
assemble_task_payload(...)
```

The important point:

> Static planning itself is shared for ASYNC / SYNC / HYBRID.

Only execution transport changes after the completed payload exists.

---

# 13. DynamicPlanner

DynamicPlanner contains significant business logic and must be reused.

It performs approximately:

```text
read planner_prompt
  ↓
read registered agents
  ↓
get agent configurations
  ↓
build prompt context:
    registered agents
    task
  ↓
generate_llm_response()
  ↓
validate result as Steps
  ↓
ensure steps exist
  ↓
extract selected agent names
  ↓
generate_execution_plan()
  ↓
payload construction
  ↓
status initialization
```

This logic should not be duplicated in a sync-specific planning implementation.

DynamicPlanner historically also contained:

```text
HIL handling
internal Kafka publishing
```

The execution mode branching was introduced so the same planner can now serve all three modes without rebuilding the dynamic plan elsewhere.

---

# 14. Shared task payload builder

`task_payload_builder.py` contains common payload construction functionality.

Known responsibilities include:

```text
build_static_style_plan()
assemble_task_payload()
dynamic subagent selection where applicable
parts offloading
DB/schema context handling
first agent resolution
request body
state
current_step
next_agent
event_type
session information
selected subagents
add_agent_status()
```

The purpose of extracting this was to reduce duplicated payload-building logic.

---

# 15. New orchestration payload fields

The Orchestration execution state/payload now carries three routing/configuration values:

```text
execution_mode
executor_url
coin_scope
```

These are derived from the use-case `response_config`.

Conceptually:

```yaml
response_config:
  mode: ASYNC
  executor_url: ...
  coin_scope: ...
```

Exact YAML/model naming in current source is authoritative.

---

# 16. `execution_mode`

This is the Orchestration-level mode:

```text
ASYNC
SYNC
HYBRID
```

Default must preserve legacy behavior:

```text
if missing → ASYNC
```

This allows existing configured use cases to continue using Kafka without modification.

---

# 17. `executor_url`

Used by Orchestration to determine where direct Executor HTTP requests should be sent.

For:

```text
SYNC
HYBRID
```

resolution is:

```text
response_config.executor_url
       ↓
if missing
       ↓
Helm/environment fallback executor URL
       ↓
if still missing
       ↓
configuration error
```

ASYNC does not require this URL.

Do not hard-code localhost or a service hostname in application code.

---

# 18. `coin_scope`

`coin_scope` is the COIN scope/audience used by Orchestration to obtain the M2M token required to call Executor.

Flow:

```text
response_config.coin_scope
        ↓
Orchestration execution payload
        ↓
SyncStepClient / token roller
        ↓
COIN access token
        ↓
Authorization: Bearer ...
        ↓
Executor
```

This value belongs primarily to Orchestration.

Executor receives the already-issued bearer token.

---

# 19. Mode branch location

The architectural branch happens only after the planner has produced the plan and execution payload.

Conceptually:

```python
agent_payload = await assemble_task_payload(...)
```

Then:

```text
agent_payload.execution_mode
```

determines the next transport.

---

# 20. ASYNC branch

Existing behavior:

```text
internal_kafka_config
  ↓
send_to_kafka(agent_payload)
```

This should remain effectively unchanged.

Executor receives the request through its existing Kafka consumer.

Executor defaults to:

```text
ExecutionMode.ASYNC_KAFKA
```

and therefore continues publishing subsequent steps/final events through Kafka.

---

# 21. SYNC branch

Conceptually:

```text
SyncExecutionCoordinator.execute(
    agent_payload,
    orchestrator_config,
    agent_payload.executor_url
)
```

The coordinator drives the whole plan.

No initial Kafka execution request is created.

---

# 22. HYBRID branch

Internally:

```text
SyncExecutionCoordinator.execute(...)
```

exactly like Sync.

After terminal response:

```text
ResponseService
```

uses the existing response configuration to deliver the result to Kafka.

The important separation is:

```text
HYBRID execution transport
=
direct HTTP
```

while:

```text
HYBRID final delivery transport
=
Kafka
```

---

# 23. SyncExecutionCoordinator

The coordinator does not plan.

It receives an already-created execution payload.

Its responsibility is approximately:

```text
calculate overall deadline
  ↓
dispatch current step
  ↓
receive Executor response
  ↓
merge changed routing fields
  ↓
terminal?
  ├─ no → call Executor again
  └─ yes → build final business response
```

It should not contain:

```text
StaticPlanner logic
DynamicPlanner logic
agent selection
planner LLM logic
```

---

# 24. Overall timeout

Sync execution uses an overall absolute deadline.

Conceptually:

```python
deadline =
    time.monotonic()
    + sync_overall_timeout_seconds
```

Before each Executor call:

```python
remaining =
    deadline - time.monotonic()
```

That remaining time is the HTTP timeout for the next step.

Example:

```text
overall timeout = 600 sec

Agent A takes 250 sec

Agent B does NOT get a new 600 sec.

Remaining budget = ~350 sec.
```

This prevents N-step workflows from multiplying the configured timeout.

---

# 25. SyncStepClient

`SyncStepClient` owns HTTP transport.

It is distinct from `SyncExecutionCoordinator`.

Coordinator:

```text
workflow control
```

Step client:

```text
network call
```

Responsibilities include:

```text
shared aiohttp ClientSession
connection pooling
POST JSON
per-step remaining timeout
Authorization bearer header
response JSON
session cleanup
```

A class-level session is reused for connection pooling.

An asyncio lock protects lazy session creation.

The session is closed during application lifespan shutdown.

---

# 26. Internal Executor URL

Executor exposes an internal one-step endpoint approximately:

```text
POST /internal/v1/agent-step
```

This endpoint executes exactly one agent step.

It does not execute the entire multi-agent workflow.

That is deliberate.

For Sync/Hybrid:

```text
Orchestration owns the loop.
```

---

# 27. Orchestration → Executor authentication

The caller's JWT is not forwarded as the Executor service identity.

There are two auth boundaries.

External:

```text
Caller JWT
   ↓
Orchestration
```

Internal:

```text
Orchestration COIN M2M token
   ↓
Executor
```

Orchestration obtains the internal token using its configured COIN client credentials and the resolved `coin_scope`.

Executor validates the M2M token using:

```text
JWTBearer
COINAuthorizer
COIN_PROVIDER_ROLE
```

---

# 28. Executor `ExecutionMode`

Executor has an internal execution enum similar to:

```python
class ExecutionMode(str, Enum):
    ASYNC_KAFKA = "ASYNC_KAFKA"
    SYNC_DIRECT = "SYNC_DIRECT"
```

This enum is NOT the same as Orchestration's:

```text
ASYNC
SYNC
HYBRID
```

Executor does not need a HYBRID mode.

For both Orchestration:

```text
SYNC
HYBRID
```

the internal HTTP endpoint invokes:

```text
ExecutionMode.SYNC_DIRECT
```

because the actual agent-step behavior is identical.

Hybrid only differs after Executor returns to Orchestration.

---

# 29. Executor internal API

The internal endpoint performs approximately:

```text
authenticate token
  ↓
validate current_step
  ↓
resolve agent name
  ↓
AgentOrchestrator.handle_request(
    ...,
    execution_mode=SYNC_DIRECT
)
  ↓
return updated routing fields
```

Returned fields include things like:

```text
x_correlation_id
current_step
next_agent
event_type
plan
state
error
```

---

# 30. AgentOrchestrator

`AgentOrchestrator.handle_request()` accepts an execution mode.

Legacy callers omit it and receive:

```text
ASYNC_KAFKA
```

by default.

That default was intentionally chosen for backward compatibility.

Both async and direct execution use the same underlying one-step execution logic.

---

# 31. `_execute_one_step()`

This shared code performs the real agent work.

Conceptually:

```text
resolve agent configuration
resolve DB/schema
mark step IN_PROGRESS
  ↓
AgentExecutionService
  ↓
AgentFactory
SessionManager
ADK / Runner
LLM
tools
  ↓
step output
  ↓
mark COMPLETED / FAILED
  ↓
update routing information
```

The actual agent engine is shared between Kafka and direct execution.

This is a critical design principle.

There is not a separate "sync agent engine".

---

# 32. Kafka gating inside Executor

The important Executor change was to guard Kafka continuation.

Conceptually:

```python
if execution_mode == ExecutionMode.ASYNC_KAFKA:
    send_to_kafka(...)
```

Therefore:

```text
ASYNC_KAFKA
    → execute step
    → publish next/final event
```

while:

```text
SYNC_DIRECT
    → execute step
    → return updated payload over HTTP
```

Without this guard, Sync/Hybrid could execute the next step twice:

```text
HTTP coordinator
+
Kafka continuation
```

---

# 33. Executor failure handling

Failure handling also respects execution mode.

On failure:

```text
step status = FAILED
next_agent = ""
event_type = AGENT_EXECUTION_FINAL_RESPONSE
error = canonical error
```

Terminal failure status is persisted.

For:

```text
ASYNC_KAFKA
```

the failed final payload is sent to Kafka.

For:

```text
SYNC_DIRECT
```

the failed final payload is returned over HTTP.

---

# 34. DB name / schema propagation

`db_name` and `db_schema` were intentionally propagated deeper into Executor error handling.

Reason:

Failure status persistence must target the correct use-case-specific DB/schema.

This is execution context propagation, not a separate Sync database architecture.

Do not remove these parameters merely because they make function signatures longer.

---

# 35. Shared final response builder

Originally final response construction lived inside:

```text
message_processing_service.py
```

because only Kafka final events needed it.

Sync introduced a second terminal path.

Therefore final business-response logic was extracted into a shared builder such as:

```text
execution_response_builder.py
```

The exact current function name may be:

```text
build_execution_response
```

or an evolved equivalent.

Use current source as truth.

---

# 36. Final response behavior

The shared builder handles:

```text
failed steps
canonical error normalization
single-agent output
multi-agent output
optional output generator
conversational chat history
final business response shape
```

Multi-agent with output generation may collect:

```text
agent A output
agent B output
agent C output
```

and run another LLM to synthesize the final response.

Without output generation, the final step output may become the business response.

---

# 37. Final response transport separation

The shared response builder should not decide transport.

Conceptually:

```text
business response generation
        ↓
        ├─ SYNC   → return HTTP
        ├─ HYBRID → ResponseService / Kafka
        └─ ASYNC  → message_processing_service → ResponseService
```

This separation is intentional.

---

# 38. Hybrid final response

Hybrid uses the direct coordinator for execution, then existing async response delivery.

It should reuse the existing:

```text
ResponseService
```

rather than manually implementing Kafka producer logic.

That preserves existing caller-specific response configuration.

---

# 39. Executor payload model decision

Orchestration's payload now contains:

```text
execution_mode
executor_url
coin_scope
```

The Executor's `TaskPayloadModel` was tested with the updated Orchestration payload.

Confirmed behavior:

```text
Pydantic v2 default extra="ignore"
```

The Executor model has no `extra="forbid"` override.

Therefore the three unknown fields are safely ignored during Executor model validation.

The required shared execution fields still deserialize normally.

Result:

> Executor-side `TaskPayloadModel` does NOT need to add `execution_mode`, `executor_url`, or `coin_scope`.

This is intentional.

---

# 40. Why Executor ignores those fields

## `executor_url`

Used by Orchestration to determine where the Executor is.

Once Executor receives the request, it does not need to know its own caller-side routing URL.

---

## `coin_scope`

Used by Orchestration to obtain the M2M bearer token.

Executor receives and validates the token itself.

It does not need the scope that was used to obtain it.

---

## Orchestration `execution_mode`

Used by Orchestration to decide:

```text
ASYNC
SYNC
HYBRID
```

Executor direct HTTP calls already explicitly invoke:

```text
SYNC_DIRECT
```

Therefore Executor does not need the external mode.

---

# 41. Do not add HYBRID to Executor

Do not introduce:

```text
ExecutionMode.HYBRID
```

inside Executor merely because Orchestration supports Hybrid.

Executor sees:

```text
SYNC
HYBRID
```

both as:

```text
SYNC_DIRECT
```

The difference belongs to Orchestration final delivery.

---

# 42. HIL

Existing async Human-in-the-loop behavior should remain untouched.

Direct synchronous execution cannot safely use the current arbitrary-wait HIL flow.

Therefore Sync eligibility logic rejects unsupported HIL workflows.

Hybrid direct execution should follow the same direct-execution eligibility rule unless current code explicitly supports otherwise.

Do not redesign existing async HIL behavior while working on Sync/Hybrid.

---

# 43. AG-UI streaming

AG-UI event streaming is another workflow that historically does not fit the simple blocking Sync path.

Sync eligibility may reject AG-UI streaming use cases.

Do not alter existing async AG-UI behavior unless explicitly requested.

---

# 44. Important identifiers

## `x_correlation_id`

End-to-end execution correlation identifier.

Travels through:

```text
Caller
Orchestration
payload
Executor
DB/logs
final response
```

Historically also used as Kafka correlation/key information.

It is not by itself a complete idempotency guarantee.

---

## `x_application_id`

Calling application identity/context.

---

## `x_soeid`

Optional user identity/context.

---

## `consumer_coin`

Derived from caller authentication and used when resolving use-case configuration.

---

## `session_id`

Conversation/session context for native/conversational flows.

---

# 45. Parts and selected subagents

Payload assembly may include:

```text
offload_parts_to_db()
is_parts_enabled
dynamic subagent selection
selected_subagents
```

These behaviors are part of the shared payload lifecycle.

Do not create separate Sync versions of them.

---

# 46. RuntimeState / RuntimeService

Latest main introduced richer runtime lifecycle abstractions in Orchestration.

Where current code uses:

```text
RuntimeState
RuntimeService
runtime creation
runtime cleanup
interruption handling
```

preserve those latest-main behaviors.

Older design descriptions that only refer to `TaskPayloadModel` should not be used to remove current RuntimeState behavior.

The architectural concepts remain:

```text
plan
execution state
mode
transport
```

even if the exact model evolved.

---

# 47. message_processing_service

For the legacy async terminal event:

```text
AGENT_EXECUTION_FINAL_RESPONSE
```

`message_processing_service` should use the shared final response builder and then existing:

```text
ResponseService
```

It may also contain latest-main behavior such as:

```text
RuntimeState cleanup
interruptions
HIL
chat-history lifecycle
```

Do not remove those unrelated behaviors.

---

# 48. Important historical merge concern

During development there were conflicts between Sync feature code and newer `main` changes.

The correct merge principle was:

```text
preserve latest-main RuntimeState / RuntimeService / interruption / HIL behavior
+
integrate Sync transport changes
```

not:

```text
replace latest main with older feature branch implementation
```

If Codex encounters unusual-looking code in those areas, inspect history/current call sites before refactoring.

---

# 49. Complete current conceptual Sync call stack

```text
Caller
  ↓
FastAPI sync route
  ↓
auth / configuration / guardrail
  ↓
existing planner selection
  ↓
StaticPlanner or DynamicPlanner
  ↓
plan creation
  ↓
payload assembly
  ↓
execution_mode == SYNC
  ↓
SyncExecutionCoordinator.execute()
  ↓
calculate overall deadline
  ↓
_dispatch_one_step()
  ↓
SyncStepClient.post_json()
  ↓
get/reuse aiohttp session
  ↓
obtain COIN M2M token using coin_scope
  ↓
POST executor_url
  ↓
Executor /internal/v1/agent-step
  ↓
JWTBearer
  ↓
COINAuthorizer
  ↓
validate current step
  ↓
AgentOrchestrator.handle_request(
    execution_mode=SYNC_DIRECT
)
  ↓
_execute_one_step()
  ↓
AgentExecutionService
  ↓
AgentFactory / SessionManager / ADK / LLM / tools
  ↓
step status + routing update
  ↓
return HTTP payload
  ↓
Orchestration coordinator
  ↓
merge step response
  ↓
terminal?
  ├─ no → repeat
  └─ yes
       ↓
shared execution response builder
       ↓
HTTP final response to caller
```

---

# 50. Complete current conceptual ASYNC call stack

```text
Caller
  ↓
existing async API
  ↓
auth / configuration / guardrail
  ↓
existing planner
  ↓
plan
  ↓
payload
  ↓
execution_mode == ASYNC
  ↓
internal Kafka
  ↓
Executor Kafka consumer
  ↓
AgentOrchestrator.handle_request()
  ↓
default ASYNC_KAFKA
  ↓
shared _execute_one_step()
  ↓
AgentExecutionService
  ↓
step complete
  ↓
more steps?
  ├─ yes → internal Kafka
  └─ no  → AGENT_EXECUTION_FINAL_RESPONSE Kafka event
                 ↓
         Orchestration consumer
                 ↓
        message_processing_service
                 ↓
        shared response builder
                 ↓
          ResponseService
                 ↓
        caller Kafka / webhook
```

---

# 51. Complete current conceptual HYBRID call stack

```text
Caller
  ↓
existing async API
  ↓
auth / configuration / guardrail
  ↓
existing StaticPlanner / DynamicPlanner
  ↓
plan
  ↓
payload
  ↓
execution_mode == HYBRID
  ↓
DO NOT publish initial internal Kafka execution request
  ↓
SyncExecutionCoordinator
  ↓
Executor HTTP one step at a time
  ↓
SYNC_DIRECT inside Executor
  ↓
all agents complete
  ↓
shared final response builder
  ↓
ResponseService
  ↓
caller Kafka response
```

This is the defining Hybrid behavior.

---

# 52. Configuration behavior

Current intended precedence:

```text
mode
  ← response_config
  ← default ASYNC

executor_url
  ← response_config
  ← Helm/environment fallback for SYNC/HYBRID

coin_scope
  ← response_config
  ← current configured fallback only if implementation intentionally supports it
```

Check actual current source for exact fallback semantics.

Do not introduce additional configuration layers without a requirement.

---

# 53. Backward compatibility principles

The implementation was designed so:

```text
existing use cases without mode
→ ASYNC
```

Existing Executor callers that do not pass internal execution mode:

```text
→ ASYNC_KAFKA
```

Existing Kafka continuation remains unchanged for legacy callers.

Sync and Hybrid are additive behaviors.

---

# 54. Known limitations

Direct Sync/Hybrid execution changes transport guarantees.

Important limitations include:

```text
HTTP timeout can occur while remote agent/tool work continues
client disconnect does not guarantee LLM/tool cancellation
direct HTTP lacks Kafka's durability/backpressure
retries can duplicate side effects without step-level idempotency
one held request couples request lifetime to service availability
```

Do not silently add retry logic without an explicit idempotency strategy.

---

# 55. No automatic retries

If Orchestration sends an HTTP step request and the connection fails after Executor performed a side effect, Orchestration may not know whether the step completed.

Blind retry can execute the step twice.

Therefore no automatic retry mechanism should be introduced casually.

---

# 56. Internal endpoint security

`include_in_schema=False` only hides the internal endpoint from standard OpenAPI presentation.

It is not security.

Security relies on:

```text
M2M JWT authentication
COIN provider role validation
network/service exposure controls
```

---

# 57. Important "do not change" list

Unless explicitly requested, do not:

```text
create another sync planner
duplicate DynamicPlanner logic
duplicate StaticPlanner logic
create a second agent execution engine
introduce HYBRID into Executor
change Executor TaskPayloadModel just to store orchestration-only fields
remove ASYNC_KAFKA default behavior
remove Kafka guards
rewrite existing HIL
rewrite conversational memory
rewrite RuntimeState lifecycle
change final business response behavior
add automatic direct-call retries
hard-code executor URL
hard-code COIN scope
move planning into SyncExecutionCoordinator
```

---

# 58. Mental model

The cleanest mental model is:

```text
                 EXISTING PLANNER
                      ↓
                 execution plan
                      ↓
                 execution payload
                      ↓
                orchestration mode
           ___________|___________
          |            |          |
        ASYNC         SYNC      HYBRID
          |            |          |
        Kafka      direct HTTP  direct HTTP
          |            |          |
          |         Executor    Executor
          |            |          |
          |          final      final
          |            |          |
          |           HTTP      Kafka
```

---

# 59. Key design sentence

The implementation can be summarized as:

> The existing StaticPlanner and DynamicPlanner remain the single planning implementation. After they create the plan and execution payload, Orchestration uses the payload execution mode to choose transport. ASYNC retains the existing Kafka choreography. SYNC uses SyncExecutionCoordinator to drive the same Executor agent engine one HTTP step at a time and returns the final response directly. HYBRID uses that same direct synchronous internal execution but preserves the existing asynchronous caller contract by publishing the final business response through ResponseService/Kafka.

---

# 60. What Codex should do first

Before making any future changes:

1. Read this entire document.
2. Inspect the current implementation in both repositories.
3. Confirm the real current call stack.
4. Identify any differences between this document and current source.
5. Treat current source as authoritative for exact signatures/naming.
6. Do not edit anything until explicitly asked.

When asked to explain the implementation, reason from the actual call stack rather than from filenames alone.

When asked to modify it, preserve the architecture and make the smallest localized change possible.