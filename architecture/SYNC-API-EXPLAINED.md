# The Sync API, explained from first principles

A walkthrough of how Orchestration and the Executor talk to each other today, what breaks for teams without Kafka, and what we propose to add.

Written to be read start to finish, and to be enough to redraw the diagrams yourself in Excalidraw.

Diagrams: `01-current-async-architecture.png` · `02-proposed-sync-sse-architecture.png`

---

## Part 1 — The cast

Six things participate. Everything else is detail hanging off these.

| # | Component | What it really is | Where |
|---|---|---|---|
| 1 | **Caller** | Another team's application | outside |
| 2 | **Orchestration** | FastAPI app titled `Agentic-Planner`. Takes HTTP in, decides *which agent runs next*, puts work on Kafka, and sends results back out. | `orchestration/` |
| 3 | **Kafka** | The message bus between the two services. One topic today. | platform |
| 4 | **Executor** | Also a FastAPI app (`Agent-Executor`), but almost nothing calls it over HTTP. Its real job is consuming Kafka and running agents. | `excutor/` |
| 5 | **PostgreSQL** | Schema `gssp_agentic`, shared by both services. Configs, execution status, audit trail. | platform |
| 6 | **Caller's channel** | The caller's *own* Kafka topic or webhook URL, where the final answer is delivered. | outside |

Note the spelling: the executor's source lives under `excutor/` — one "e". Not a typo in this document.

### The identifiers, and why there are so many

Confusing these is the single easiest way to misread the system.

| Identifier | Scope | Set by | Notes |
|---|---|---|---|
| **`x_correlation_id`** | **The whole request, end to end** | **The caller**, as the `X-Correlation-ID` HTTP header | Required. Returned in the HTTP response. Used as the **Kafka message key** everywhere. Survives across all agent hops. **This is the one that matters.** |
| `session_id` | A conversation across multiple requests | Caller (`Session-ID` header, native endpoint) | Used for conversational memory |
| `invocation_id` | One agent turn | ADK runtime | Used to group audit rows |
| `run_id` | One agent execution, AG-UI only | `str(uuid.uuid4())` | ⚠️ **Not** the correlation id, despite what the docstring in `agui_events.py` says |
| `usecase_id` | Which agent configuration to run | Caller (`Config-ID` header) | Looks up the use-case config in Postgres |
| `consumer_coin` | Who is calling | Derived from the JWT's `sub` + `aud` | Used for authorisation on config lookup |

**Rule of thumb:** if you want to follow one user request through the entire system — logs, Kafka, audit table, everything — you follow `x_correlation_id`.

---

## Part 2 — How it works today, step by step

Read alongside `01-current-async-architecture.png`.

### Step 1 — The caller makes one HTTP POST

```
POST /api/v1/agentic-orchestration/task-executor
X-Correlation-ID:      3f9c1a...        ← the caller generates this
X-Authorization-Coin:  <COIN JWT>
Config-ID:             my-usecase-id
X-Application-ID:      my-app
x_soeid:               user123

{ "context": ..., "parts": [...], "metadata": {...}, "state": {...} }
```

There are four such endpoints, differing only in what they accept:

| Endpoint | Adds |
|---|---|
| `/task-executor` | the base case |
| `/conversational-task-executor` | `chat_history` |
| `/native-conversational-task-executor` | server-side session management (`Session-ID`) |
| `/agent-testing` | forces the static planner, for testing |

### Step 2 — Orchestration authenticates

`JWTBearer` reads the `X-Authorization-Coin` header and validates it through `COINAuthorizer`. Note this is *not* a standard `Authorization: Bearer` header.

### Step 3 — It loads the use-case config

`AgenticUsecaseConfigManager.get_use_case(usecase_id, consumer_coin)` reads from Postgres: which agents exist, which tools they may call, which model, and **where to send the response** (`response_config` — a Kafka topic, a webhook, or both).

### Step 4 — The planner builds a work order

`StaticPlanner.plan()` constructs a `TaskPayloadModel` — the envelope that travels over Kafka. It carries the correlation id, the user input, the plan, the identity fields, and `event_type = "AGENT_EXECUTION_REQUEST"`.

It also writes an initial row to the `agent_execution` table so status is queryable.

### Step 5 — It publishes to Kafka and returns immediately

```
topic:  181229_agentic_events_NAM_001_dev
key:    x_correlation_id
value:  TaskPayloadModel(event_type="AGENT_EXECUTION_REQUEST", ...)
```

Keying by correlation id means **all messages for one request land on the same partition**, so Kafka preserves their order. That property is load-bearing later.

The HTTP response returns straight away:

```json
{ "x_correlation_id": "3f9c1a...", "message": "Execution Initiated Successfully" }
```

**The HTTP request is now over.** Everything that follows happens with no HTTP connection open.

### Step 6 — The executor picks up the work

```python
# excutor/service/kafka_consumer_service.py:67
task = asyncio.create_task(process_message(message.topic, message.key, message.value))
```

Note it is **not awaited** — the loop immediately fetches the next message. The executor therefore handles **many executions concurrently**, bounded by the event loop and the DB pool, not one at a time.

### Step 7 — The agent actually runs

`MessageProcessingService` → `AgentOrchestrator.handle_request()` → `AgentFactory.create_agent()` → `AgentExecutionService.execute()`, which drives Google ADK's `Runner.run_async()`. The LLM (Vertex AI / R2D2) is called, tools execute, sub-agents may spawn.

Throughout, `DbLoggerPlugin` writes to `audit_table` at each boundary — `INVOCATION`, `AGENT`, `LLM_REQUEST`, `LLM_RESPONSE`, `TOOL`, `ERROR`.

> **Two properties of `audit_table` that matter later:** writes are **fire-and-forget** (`asyncio.create_task`, so a row may not exist yet when you look), and rows are **UPDATEd in place** (a `TOOL` row starts as `STARTED` and is later mutated to `COMPLETED`). It is a compliance record, not an event log. This is why we cannot stream from it.

### Step 8 — Multi-agent hops loop back through Kafka

`_prepare_response()` decides what happens next:

- **Another agent to run?** Publish a *new* `AGENT_EXECUTION_REQUEST` to the same topic. The executor consumes its own message and runs the next agent. Steps 6–8 repeat.
- **Done?** Publish `AGENT_EXECUTION_FINAL_RESPONSE`.

Every hop carries the **same `x_correlation_id`**. This loop is why a single request can produce several agent executions.

### Step 9 — Orchestration hears the final response

Orchestration runs its own consumer (group `agentic_internal_planner_group_{topic}`) on that same topic. It sees `AGENT_EXECUTION_FINAL_RESPONSE` and assembles the result.

### Step 10 — The result is delivered to the caller's channel

`ResponseService` either POSTs it to the caller's webhook, or produces it to the caller's Kafka topic — whichever `response_config` specifies.

Meanwhile the caller may poll `GET /execution-status?x_correlation_id=...` for coarse status. (That endpoint has **no authentication** — a pre-existing gap.)

---

## Part 3 — What's wrong with this for some teams

Five gaps, in order of importance.

**1. A team with no Kafka and no webhook simply cannot be served.** Delivery only ever goes to `response_config.kafka` or `response_config.webhook`. No topic, no webhook, no answer. For many consuming teams, standing up a Kafka consumer just to call an API is a disproportionate ask.

**2. No step visibility.** The caller gets "Execution Initiated", then silence for possibly minutes, then an answer. For an agent that calls four tools and three LLMs, that is a very long unexplained pause. Users assume it has hung.

**3. Polling is a poor substitute.** `GET /execution-status` gives coarse status, costs a round trip each time, and is unauthenticated.

**4. Rich step events already exist — but are unreachable.** `AGUIKafkaStreamService` already emits **AG-UI protocol events** during execution: `RUN_STARTED`, `TOOL_CALL_START/ARGS/END`, `TEXT_MESSAGE_START/CONTENT/END`, `STATE_SNAPSHOT`, `STATE_DELTA`, `RUN_FINISHED`, `RUN_ERROR`. Exactly what a live view needs. But:

```python
# excutor/service/agui_kafka_stream_service.py:96-109
if not (usecase_config.metadata and usecase_config.metadata.ag_ui_events_streaming):
    return None
kafka_env = build_kafka_environment(usecase_config.response_config.kafka)   # ← the caller's topic
if kafka_env is None:
    return None
```

It publishes to **the caller's own Kafka topic**. So the one feature that would give live visibility is available only to teams who already have Kafka — precisely the teams who least need help.

**5. A pod restart orphans an execution.** Both consumers use `auto_offset_reset='latest'` with auto-commit, so a run in flight when a pod dies is never reprocessed and sits `IN_PROGRESS` forever.

---

## Part 4 — What we propose

> **One sentence:** publish the AG-UI events that already exist to a topic *the platform* owns, and add four HTTP endpoints that stream them straight back on the caller's own request.

Read alongside `02-proposed-sync-sse-architecture.png`.

### Two modes, and the caller picks

| | **Async — existing, untouched** | **Sync — new** |
|---|---|---|
| Caller needs Kafka | Yes | **No** |
| Endpoint | `POST .../task-executor` | `POST .../task-executor/stream` |
| Response | immediate `{x_correlation_id}` | `text/event-stream`, held open |
| Step visibility | none | every event, live |
| Final result via | caller's Kafka or webhook | **the stream itself** |
| Caller needs | broker creds, topic, consumer | an HTTP client |

### The new flow

1. Caller POSTs to `…/task-executor/stream`. Orchestration authenticates, loads the config, and **registers the correlation id in an in-memory registry — before publishing anything.** (Register first, or events produced in the gap are lost.)
2. It publishes `AGENT_EXECUTION_REQUEST` to the internal topic exactly as the async endpoint does, plus one field: `ag_ui_streaming = true`.
3. The executor consumes and runs the agent — unchanged code path.
4. `AGUIKafkaStreamService` publishes AG-UI events to the **platform** AG-UI topic, keyed by `x_correlation_id`.
5. **Consumer A** on every orchestration pod tails that topic, matches the message key against its local registry, and pushes hits into that stream's queue. The SSE generator writes them to the open HTTP response.
6. When the plan finishes, the executor publishes `AGENT_EXECUTION_FINAL_RESPONSE` — unchanged.
7. **Consumer B** sees it. The generator emits that payload as a final `execution.completed` frame and closes. **For a caller with no Kafka and no webhook, this frame is the answer.**
8. The caller has received every step *and* the result on the one HTTP response it opened.
9. In parallel and untouched: the existing consumer group and `ResponseService` still deliver to Kafka/webhook callers.

### What the caller sees on the wire

```
event: run.accepted
data: {"x_correlation_id":"3f9c1a..."}

event: RUN_STARTED
data: {"type":"RUN_STARTED","timestamp":1754280000123,"threadId":"3f9c1a...","runId":"8b2e..."}

event: TOOL_CALL_START
data: {"type":"TOOL_CALL_START","toolCallId":"tc_1","toolCallName":"search_policy"}

event: TOOL_CALL_END
data: {"type":"TOOL_CALL_END","toolCallId":"tc_1"}

event: TEXT_MESSAGE_CONTENT
data: {"type":"TEXT_MESSAGE_CONTENT","messageId":"m_1","delta":"Based on the policy..."}

: keepalive

event: RUN_FINISHED
data: {"type":"RUN_FINISHED","threadId":"3f9c1a...","runId":"8b2e..."}

event: execution.completed
data: {"x_correlation_id":"3f9c1a...","status":"COMPLETED","response":{...},"state":{...}}
```

This is the **AG-UI wire format**, unmodified — camelCase keys, standard event names. Off-the-shelf AG-UI client libraries work against this endpoint with no adaptation.

---

## Part 5 — Why these decisions

### Why SSE and not WebSocket

| | **SSE** | WebSocket |
|---|---|---|
| Shape of the problem | One request in, many events out — **exactly this** | Bidirectional; we'd use half of it |
| Our ingress | Plain HTTP over the existing OpenShift Route — **works today** | Edge-terminated Routes need `haproxy.router.openshift.io/websocket: "true"`. **Not present in `route.yaml`** — a WebSocket build needs an infra change first |
| Correlation | Same request starts the run *and* streams it. No gap. | POST → get id → open socket → subscribe. Events in that gap are lost unless buffered |
| Auth | The existing `JWTBearer` dependency works unchanged | Handshake auth needs a separate mechanism (query-param tokens or a first-message protocol) |
| Reconnect | `Last-Event-ID` is part of the standard | You write your own |
| Ops | It's an HTTP request — existing logs, traces, metrics apply | Separate connection metrics |
| Client cost | `EventSource` in a browser; `curl -N` from a terminal | A WebSocket library |

**Choose WebSocket only if** you need mid-run traffic *from* the client: human-in-the-loop approvals, cancel/steer commands, or one socket multiplexing many runs for a dashboard. We need none of those today. If human-in-the-loop arrives later (there is a `hil_tool.py` in the executor, so it may), that is the moment to revisit — and SSE plus a small POST endpoint for the approval covers it without a socket.

### Why not the other transports

| Option | Why not |
|---|---|
| **Long-polling** `GET .../events?after=N` | Works through any infra, but N round trips per run, and there is no per-event cursor in AG-UI to poll against |
| **Webhook callback** | This is the *existing* async mode. Requires the caller to run an inbound HTTPS endpoint — a bigger ask than Kafka for many teams |
| **gRPC streaming** | Clean and typed, but dies at most enterprise HTTP gateways, and no caller has asked for it |
| **Polling `audit_table`** | Considered and rejected. Writes are fire-and-forget so rows may not exist yet; rows are UPDATEd in place so a cursor sees every start and no completion; and there is no index on `x_correlation_id`, so polling would sequentially scan a growing audit table twice a second |

### Why a platform-owned AG-UI topic, not the caller's

This is the decision that makes the feature possible at all. If AG-UI events go to `usecase_config.response_config.kafka`, then a caller with no Kafka produces **no events**, and the SSE endpoint has nothing to stream. The design would only work for callers who don't need it.

Moving the destination to a platform-owned topic breaks that circularity. Orchestration owns both ends, and the caller owns nothing.

**Why a new topic rather than reusing the existing internal one:** the internal topic is control plane — it carries agent dispatch. AG-UI is content, at message or token granularity. Mixing them risks adding lag to dispatch itself, and forces one retention policy on two very different kinds of data. A separate topic also means the consumer never has to tell AG-UI events apart from task payloads.

### Why `assign()` with no consumer group

Orchestration runs one process per pod but can run several pods (there's an HPA). With a normal consumer group, Kafka gives each partition to *one* pod — which is very unlikely to be the pod holding a given caller's HTTP connection.

`assign()` at `OFFSET_END` with **no `group.id`** makes every pod tail every partition and filter locally by message key. Consequences:

- No group coordinator state, no rebalances
- No offset commits, so restarting pods leave no dead groups behind
- Zero interference with the existing `agentic_internal_planner_group_{topic}` consumer
- Cost: every pod reads all AG-UI traffic. Fine at this volume; revisit if it grows

### Why route by the Kafka message key, not the payload

Only `RUN_STARTED` and `RUN_FINISHED` carry any run identifier. `TEXT_MESSAGE_CONTENT` has only `messageId` and `delta`. `TOOL_CALL_ARGS` has only `toolCallId` and `delta`. **Most event types contain no correlation field at all.**

The Kafka key is `x_correlation_id` (`agui_kafka_stream_service.py:138`), so the key is the only reliable router. Filtering on the payload would deliver `RUN_STARTED` and then silently drop everything else — a stream that appears to hang immediately after starting.

### Why we do NOT close the stream on `RUN_FINISHED`

`AGUIKafkaStreamService` is constructed **per agent execution**, and a multi-agent plan runs several hops under one correlation id. Each hop emits its own `RUN_STARTED` / `RUN_FINISHED` with a fresh `run_id`.

Closing on the first `RUN_FINISHED` would end the stream after agent one and present a partial answer as complete — a failure that looks exactly like success. The stream closes only on `AGENT_EXECUTION_FINAL_RESPONSE`, which is orchestration's own signal that the whole plan is done.

### Why streaming is request-scoped, not a use-case config flag

`ag_ui_streaming` is a field on the request payload, set by the `/stream` endpoints.

- **Calling `/stream` is the opt-in.** No config to forget. If it were a per-use-case flag, forgetting it would produce a stream that connects fine and emits nothing but keepalives for 870 seconds — a confusing failure.
- **Async callers generate zero AG-UI traffic.** You only pay for streams someone is watching.
- The existing per-use-case flag is still honoured (OR'd in) for anyone who wants always-on.

### Why the SSE path touches no database

The connection pool is `pool_size=5` + `max_overflow=10` = 15 per pod, shared with the existing endpoints. A FastAPI-injected session lives for the whole request — and these requests last up to 15 minutes. Roughly 30 concurrent streams holding sessions would drain the pool and start failing the **existing** endpoints.

That is the one way this feature could break what already works without editing a single line of it. So the streaming path opens no session at all.

### Why 870 seconds

`haproxy.router.openshift.io/timeout` on the OpenShift Route defaults to **900s** (`helm/values.yaml:8`). We cap at 870 and close cleanly with a `stream.timeout` event, rather than letting HAProxy sever the connection and leave the client guessing.

### Why we duplicate dispatch logic instead of extracting a shared helper

Refactoring a working handler to share code with a new one is a behaviour change to a frozen endpoint. Under a zero-impact requirement, a little duplication is the cheaper risk. The dispatch logic is copied into `stream_routes.py`; `api.py` is not touched.

---

## Part 6 — Exact changes required

### Executor — 2 files

| File | Change | Risk |
|---|---|---|
| `excutor/models/task_payload.py` | Add `ag_ui_streaming: bool = False` | Optional and defaulted, so old messages still parse. ⚠️ Check `model_config` — if `extra='forbid'`, the executor **must** deploy before orchestration |
| `excutor/service/agui_kafka_stream_service.py` | Resolve the topic from the new platform Helm value instead of `response_config.kafka`; enable on `task_payload.ag_ui_streaming OR usecase_config.metadata.ag_ui_events_streaming` | This class returns `None` and does nothing unless streaming is enabled. If no use case has the flag on today, **you are modifying code that does not currently execute** |

Everything else in the executor is untouched: same `to_dict()` payloads, same message key, same fire-and-forget publishing, same swallowed exceptions.

### Orchestration — 4 files

| File | Change |
|---|---|
| `orchestration/api/stream_routes.py` | **New.** Four `/stream` routes + one shared SSE generator |
| `orchestration/service/agui_stream_registry.py` | **New.** `dict[x_correlation_id → asyncio.Queue]`, bounded queues, capped registrations |
| `orchestration/service/agui_consumer_service.py` | **New.** Consumers A and B, both `assign()` at `OFFSET_END`, no group |
| `orchestration/main.py` | One `include_router`, plus consumer start/stop inside the **existing** `lifespan` |
| `orchestration/config/environment.py` | New keys only: `SSE_ENABLED` (default `False`), `SSE_MAX_DURATION_SECONDS` (870), AG-UI topic name |

### Infrastructure

- Create the platform AG-UI topic on the cluster
- Add `internal_kafka_agui_events_topic` to `values.yaml` and every env file in **both** repos
- Reuse the existing `internal_kafka_bootstrap_servers` and credentials

### Database

**Nothing.** No table, no index, no migration, no query.

---

## Part 7 — How backward compatibility is guaranteed

Not by intention — by construction, plus one test.

1. **The four existing POSTs and `GET /execution-status` are never edited.** New routes live in a new module.
2. **An OpenAPI snapshot test**, written *first*, captures `/openapi.json` filtered to the existing paths and asserts this work leaves it byte-identical. This is the acceptance gate — "no impact" becomes a failing test rather than a promise.
3. **`SSE_ENABLED` defaults to `False`.** When off, the routes are not registered at all. Ship dark; switch off instantly without a rollback.
4. **The executor cannot tell the two request types apart** beyond one boolean. Same envelope, same topic, same key, same code path.
5. **The existing consumer group and `ResponseService` are untouched.** Async callers keep receiving results on their Kafka topic or webhook exactly as today.
6. **The new consumers use no consumer group**, so they cannot perturb the existing group's partition assignment or offsets.
7. **No database session in the streaming path**, so the shared pool is left to the existing endpoints.
8. **Dual-publish if needed.** If any use case *is* already using AG-UI on its own topic, the executor keeps publishing there too.

---

## Part 8 — What the new architecture gives you

Beyond "it works without Kafka":

| Capability | Why it follows |
|---|---|
| **Live step visibility** | Tool calls, LLM turns, state changes as they happen — instead of a silent multi-minute wait |
| **A standards-compliant agent stream** | This is the AG-UI protocol, wire-format unmodified. Off-the-shelf AG-UI client libraries and UI components work against it with no adaptation |
| **A dramatically lower bar to onboard** | A team needs an HTTP client. No broker credentials, no topic provisioning, no consumer to operate, no inbound webhook endpoint to expose and secure |
| **Progressive UI is now possible** | "Searching policy documents…" → "Reading section 4…" → answer. Perceived latency drops even when real latency does not |
| **Token-level streaming, if the call site supports it** | ⚠️ Unverified — see Open Questions |
| **Sub-agent transparency** | Nested agents share the correlation id, so their events stream too. Multi-agent plans stop being a black box |
| **A platform-owned event stream** | Once AG-UI events land on a topic we own, anything can consume them — the observability plane, a live ops console, quality evaluation — without touching either service again |
| **Debugging by attaching to a live run** | With Phase 2, `GET .../execution-stream` lets an engineer attach to an execution already in flight |
| **A cleaner error path** | `execution.failed` arrives on the same channel with the error body, instead of a caller inferring failure from a status poll |

---

## Part 9 — How a new team onboards

### Async (existing, unchanged)

Provision a Kafka topic or a webhook, configure `response_config`, POST to `/task-executor`, consume the result.

### Sync (new)

**1. Nothing to provision.** No topic, no webhook, no broker credentials.

**2. Register a use case** as normal — the same `Config-ID`.

**3. Call the `/stream` endpoint:**

```bash
curl -N -X POST \
  https://<host>/api/v1/agentic-orchestration/task-executor/stream \
  -H "X-Correlation-ID: $(uuidgen)" \
  -H "X-Authorization-Coin: <COIN JWT>" \
  -H "Config-ID: my-usecase-id" \
  -H "X-Application-ID: my-app" \
  -H "Content-Type: application/json" \
  -d '{"parts":[{"text":"What is our travel policy for international trips?"}]}'
```

`-N` disables curl's buffering. Without it you'll see nothing until the stream ends.

**4. Or from a browser / Node:**

```js
// EventSource is GET-only, so use fetch for a POST body
const res = await fetch(url, { method: 'POST', headers, body });
const reader = res.body.pipeThrough(new TextDecoderStream()).getReader();

for (;;) {
  const { value, done } = await reader.read();
  if (done) break;
  // frames are separated by a blank line; each has `event:` and `data:` lines
  for (const frame of value.split('\n\n').filter(Boolean)) {
    const type = frame.match(/^event: (.+)$/m)?.[1];
    const data = JSON.parse(frame.match(/^data: (.+)$/m)?.[1] ?? '{}');

    if (type === 'TOOL_CALL_START')      setStatus(`Running ${data.toolCallName}…`);
    if (type === 'TEXT_MESSAGE_CONTENT') appendAnswer(data.delta);
    if (type === 'execution.completed')  finish(data);
    if (type === 'execution.failed')     showError(data);
  }
}
```

**5. What to handle**

| Frame | Meaning |
|---|---|
| `run.accepted` | Accepted and dispatched. Show a spinner. |
| `RUN_STARTED` | An agent hop began. **Several per request in a multi-agent plan** |
| `TOOL_CALL_START` / `_ARGS` / `_END` | A tool is running. Good status text |
| `TEXT_MESSAGE_START` / `_CONTENT` / `_END` | Assistant text. `delta` is the content |
| `STATE_SNAPSHOT` / `STATE_DELTA` | Working state. `STATE_DELTA` is RFC 6902 JSON-Patch — apply against the last snapshot, not standalone |
| `RUN_FINISHED` | A hop finished. **Not** the end of the request |
| `: keepalive` | A comment line. Ignore it |
| `execution.completed` | **The answer.** Stream closes after this |
| `execution.failed` | Error body. Stream closes after this |
| `stream.timeout` | Hit the 870s cap. The execution continues — fall back to `GET /execution-status` |

**Two rules to give any consuming team:**

1. **Do not treat `RUN_FINISHED` as the end.** Wait for `execution.completed`.
2. **Treat the stream as best-effort, and the final frame as authoritative.** AG-UI publishes are fire-and-forget, so a broker hiccup can drop an intermediate frame without failing the execution. Never reconstruct the answer by concatenating deltas alone.

---

## Part 10 — Open questions

| # | Question | Why it matters |
|---|---|---|
| 1 | Does `agent_execution_service.py:139` call `emit_text_message()` (one event per whole message) or `emit_text_message_content()` incrementally? | Decides whether we can honestly offer **token-level** streaming. The models support it; the call site may not use it |
| 2 | Does any use case currently have `ag_ui_events_streaming` enabled? | If none, the executor change touches dormant code — near-zero risk — and we publish to the platform topic only, no dual-publish |
| 3 | Does `TaskPayloadModel.model_config` set `extra='forbid'`? | If yes, deploy order is mandatory: executor first, then orchestration |
| 4 | Does SSE survive the OpenShift Route in practice? | No buffering annotations exist either way. HAProxy shouldn't buffer, but verify with `curl -N` **through the Route**, not against the pod |
| 5 | What is the AG-UI event volume per execution? | Sizes the new topic's partitions and retention |

---

## Appendix — Drawing this in Excalidraw

**Five columns**, left to right: Caller · Orchestration · Kafka · Executor · Caller's channel. One wide box underneath for PostgreSQL.

**Colour code:** grey = unchanged · green = new · amber = modified. Two boxes are amber (`task_payload.py`, `agui_kafka_stream_service.py`); everything green sits in Orchestration plus the one new Kafka topic.

**Number every arrow** and put the numbered walkthrough beside the diagram — people follow numbers far better than they follow arrowheads.

**Draw both topics as separate boxes.** The single most common misunderstanding will be that AG-UI events and dispatch messages share a topic. They do not, and the separation is a deliberate decision worth showing.

**Label the arrows with what actually travels**, not just direction: `AGENT_EXECUTION_REQUEST + ag_ui_streaming=true`, `AG-UI events keyed by x_correlation_id`, `AGENT_EXECUTION_FINAL_RESPONSE`.

**Show the multi-hop loop explicitly** — an arrow from `_prepare_response()` back into the internal topic, annotated "↻ once per agent hop, same x_correlation_id". It explains both why multiple `RUN_STARTED` events appear and why we can't close on `RUN_FINISHED`.

**If you have two slides**, put the async diagram first and let the "What this design cannot do" box land before you show the fix. The proposal is much easier to justify once the gap is concrete.
