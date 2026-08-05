# Supporting Sync and Async Execution — Architecture Decision Document

**Date:** 2026-08-04
**Scope:** How the Agentic Orchestration + Agent Executor platform should serve both asynchronous callers (who own Kafka) and synchronous callers (who do not).
**Status:** Recommendation for review.

Companion documents: `SYNC-API-EXPLAINED.md` (conceptual walkthrough), `SYNC-API-IMPLEMENTATION-PLAN.md` (build detail), `DISCOVERY-v2.md` (evidence base).

---

# Part 1 — Current Architecture

## 1.1 The two services

| | Orchestration | Executor |
|---|---|---|
| App title | `Agentic-Planner` | `Agent-Executor` |
| Source root | `orchestration/` | `excutor/` *(one "e" — not a typo)* |
| Framework | FastAPI, `uvicorn --workers 1` | FastAPI, `uvicorn --workers 1` |
| Primary role | HTTP surface, planning, response delivery | Consumes Kafka, runs agents |
| Scaling | Multiple pods, HPA present | Multiple pods |
| Kafka client | `aiokafka` | `aiokafka` |

Both share one PostgreSQL instance, schema `gssp_agentic`. Connection pool is `pool_size=5` + `max_overflow=10` = **15 connections per pod**.

## 1.2 HTTP surface (frozen — these must not change)

| Route | Auth |
|---|---|
| `POST /api/v1/agentic-orchestration/task-executor` | `JWTBearer` |
| `POST /api/v1/agentic-orchestration/conversational-task-executor` | `JWTBearer` |
| `POST /api/v1/agentic-orchestration/native-conversational-task-executor` | `JWTBearer` |
| `POST /api/v1/agentic-orchestration/agent-testing` | `JWTBearer` |
| `GET /api/v1/agentic-orchestration/execution-status` | ⚠️ **none** |
| `GET /api/v1/agentic-orchestration/registered-agents` | `JWTBearer` |
| `POST /api/v1/agentic-orchestration/reload-configs` | none |

Authentication is `JWTBearer` reading the **`X-Authorization-Coin`** header via `COINAuthorizer` — not a standard `Authorization: Bearer` header.

## 1.3 Identifiers

| Identifier | Scope | Origin |
|---|---|---|
| **`x_correlation_id`** | **The whole request, end to end** | Caller-supplied `X-Correlation-ID` header. Returned in the HTTP response. **Used as the Kafka message key.** |
| `session_id` | A conversation across requests | Caller (`Session-ID` header) |
| `invocation_id` | One agent turn | ADK runtime |
| `run_id` | One agent execution, AG-UI only | `str(uuid.uuid4())` — ⚠️ **not** the correlation id, despite the docstring |
| `usecase_id` | Which agent config to run | Caller (`Config-ID` header) |
| `consumer_coin` | Caller identity | Derived from JWT |

## 1.4 Execution flow

```
Caller ──POST──► Orchestration ──Kafka──► Executor ──┐
                      │                              │
                      │                     runs agent, writes audit
                      │                              │
                      │      ┌───────────────────────┘
                      │      │  next agent? republish to same topic ↻
                      │      │  done? AGENT_EXECUTION_FINAL_RESPONSE
                      │      ▼
                      └──consumes──► ResponseService ──► caller's Kafka topic
                                                          or webhook
```

**One Kafka topic** carries both directions: `internal_kafka_agentic_events_topic` = `181229_agentic_events_NAM_001_dev`, keyed by `x_correlation_id`, with two message types — `AGENT_EXECUTION_REQUEST` and `AGENT_EXECUTION_FINAL_RESPONSE`.

**Both services consume it, with different consumer groups:**
- Orchestration: `agentic_internal_planner_group_{topic}`
- Executor: `agent_internal_operations_group_{topic}`

Both use `auto_offset_reset='latest'` with auto-commit.

## 1.5 The execution model — routing slip, not orchestration

This is the single most important property to understand, because every option below is really a position on it.

**Planning is centralized. Execution is choreographed.**

`StaticPlanner.plan()` builds a plan object up front with an ordered list of steps. That plan then **travels with the Kafka message**. At each hop, the executor's `_prepare_response()` reads it and decides what happens next:

- Another step? Publish a **new** `AGENT_EXECUTION_REQUEST` to the same topic. Any executor pod picks it up.
- Done? Publish `AGENT_EXECUTION_FINAL_RESPONSE`.

This is the **routing-slip pattern** — a document carrying its own itinerary, with each participant reading it to know where to forward it. Orchestration authors the itinerary once and is then a bystander until the terminal message arrives.

**Consequences that matter:**

- A multi-agent plan is **N Kafka hops**, not one function call
- Orchestration never knows the plan's progress — it cannot say "step 2 of 4"
- Different hops of one plan may run on **different executor pods**
- Plan progress does **not depend on orchestration being alive**
- There is no stable owner of "the current step" — hence no natural place to send a cancel signal

## 1.6 Concurrency

The executor is **not** serial. `excutor/service/kafka_consumer_service.py:67`:

```python
task = asyncio.create_task(process_message(message.topic, message.key, message.value))
```

Not awaited — the loop immediately fetches the next message. Many executions run concurrently, bounded by the event loop and the 15-connection DB pool, not by message ordering.

## 1.7 Observability layers that already exist

| Layer | Granularity | Delivery | Reliability |
|---|---|---|---|
| **`gssp_agentic.audit_table`** | Every step: `INVOCATION`, `AGENT`, `LLM_REQUEST`, `LLM_RESPONSE`, `TOOL`, `ERROR` | Postgres | ⚠️ **fire-and-forget** writes; rows **UPDATEd in place** |
| **`agent_execution` table** | Step status | Postgres | Read by `GET /execution-status` |
| **`AGUIKafkaStreamService`** | Every step, AG-UI protocol | Kafka | ⚠️ publishes to **the caller's own topic** |

**`audit_table` is a compliance ledger, not an event stream.** Because writes are fire-and-forget and rows mutate in place, a cursor-based poll would see every "started" and no "completed." It cannot back a live stream.

**`AGUIKafkaStreamService` is the interesting one.** It already emits a complete, spec-compliant AG-UI event stream — 14 event types (`RUN_STARTED`, `TOOL_CALL_START/ARGS/END`, `TEXT_MESSAGE_START/CONTENT/END`, `STATE_SNAPSHOT`, `STATE_DELTA`, `RUN_FINISHED`, `RUN_ERROR`, …), camelCase wire format, keyed by `x_correlation_id`. But:

```python
# excutor/service/agui_kafka_stream_service.py:96-109
if not (usecase_config.metadata and usecase_config.metadata.ag_ui_events_streaming):
    return None
kafka_env = build_kafka_environment(usecase_config.response_config.kafka)  # ← caller's topic
if kafka_env is None:
    return None
```

**A caller without Kafka produces no events at all.** The one feature that would give live visibility is structurally available only to teams who already have Kafka — precisely those who least need it.

## 1.8 Deployment and network

- Helm charts in both repos; OpenShift `Route` with edge TLS termination
- **HAProxy Route timeout: 900s** (`helm/values.yaml:8`) — the hard ceiling on any held-open HTTP connection
- **WebSocket is not enabled** — `haproxy.router.openshift.io/websocket: "true"` is absent from `route.yaml`
- No response-buffering annotations either way
- **No Redis** anywhere in either repo
- **No Alembic** or any migration tooling — tables pre-exist or come from `Base.metadata.create_all`

## 1.9 What today's architecture cannot do

1. **Serve a caller with no Kafka and no webhook** — there is no delivery path
2. **Show intermediate progress** — "Execution Initiated", silence, then an answer
3. **Cancel a running execution** — no endpoint, no flag, no mechanism anywhere
4. **Report plan progress** — orchestration doesn't hold the plan during execution
5. **Recover an execution orphaned by a pod restart** — `auto_offset_reset='latest'` + auto-commit means an in-flight run whose pod dies is never reprocessed and sits `IN_PROGRESS` indefinitely

---

# Part 2 — The Options

## Option A — Fully synchronous, separate API set, direct service-to-service

**Shape:** New `/sync` endpoints. Orchestration calls the Executor **directly over HTTP** and holds that call open until the answer returns. Kafka is bypassed entirely for this path.

```
Caller ──POST──► Orchestration ──HTTP (held open)──► Executor
                       │                                 │
                       └──────── one JSON response ◄─────┘
```

### The blocker

**The multi-agent loop runs through Kafka.** `_prepare_response()` republishes to the topic for the next hop. A direct HTTP call to the executor returns after the **first agent hop only**.

Making this work requires reimplementing the entire planner loop **in-process inside the executor** — internal agent chaining, sub-agent dispatch, planner re-entry. That is a second implementation of the orchestration engine running alongside the Kafka one.

### Strengths

- **No fan-in problem at all.** The pod holding the caller's connection is the pod that made the call. No registry, no correlation matching, no consumer taps
- **No read amplification**
- Lowest latency — no Kafka hop
- Simplest mental model

### Issues

- **Requires rewriting the executor's core execution loop** — not additive, high blast radius
- **Two divergent execution paths** in the executor (Kafka-entered and HTTP-entered), which will drift
- **Loses backpressure.** Kafka absorbs load spikes and drains at capacity. Direct HTTP saturates executor pods; you must build connection limits, circuit breakers, retry/backoff
- **Loses durability.** Executor pod dies → request is gone. With Kafka the message is still on the topic
- **Couples two pod lifetimes to one request.** Today only the orchestration pod must survive a streaming request. Here **both** the orchestration pod and one specific executor pod must survive the whole run — in an environment where pods are routinely killed by deploys, HPA, and evictions
- Still bounded by the 900s Route timeout

### Verdict

**Not viable as scoped.** It is not a delivery-mechanism change; it is an execution-engine rewrite. And counterintuitively it is *less* cloud-native than the current design, because Kafka in the middle is precisely what makes executor pods disposable.

---

## Option B — Hybrid: Kafka internally, HTTP delivery outward

**Shape:** New endpoints hold the caller's HTTP connection open. Internally, nothing changes — the same Kafka dispatch, the same executor, the same multi-hop loop. Orchestration adds read-only Kafka **taps** to learn when things happen, and forwards to the waiting connection.

```
Caller ──POST──► Orchestration ──Kafka──► Executor
                   │    ▲                    │
                   │    │  taps (no group)   │
                   │    └────────────────────┘
                   └── SSE frames / one JSON reply ──► Caller
```

Two sub-modes:

- **Blocking (`/sync`)** — connection waits silently, returns one JSON body. Needs only the terminal-signal tap on the **existing** internal topic. **No executor change required.**
- **Streaming (`/stream`)** — connection streams every AG-UI event live. Needs the executor to publish AG-UI events to a **platform-owned** topic instead of the caller's.

### The fan-in mechanism

Multiple orchestration pods, one topic. A normal consumer group would split partitions across pods with no relationship to which pod holds which caller's connection.

**Solution:** each pod uses `assign()` at `OFFSET_END` with **no `group_id`** — every pod reads every message, then filters locally against an in-memory registry of `x_correlation_id → waiting connection`. Broadcast to all, act only where it's yours.

### Strengths

- **Additive.** Executor changes are one file (`/stream` only); `/sync` needs none at all
- **Multi-agent hops work unchanged** — the routing slip keeps running as-is
- **Reuses Kafka's durability, backpressure, and pod decoupling**
- **One execution path** in the executor, not two
- **Ships incrementally** — blocking mode first, streaming later
- **Standards-compliant output** — AG-UI over SSE; off-the-shelf clients work
- No new infrastructure beyond one Kafka topic
- No database changes

### Issues

- **Read amplification.** N pods each read the full topic. Compounds with hop count — a 5-hop plan emits 5× the AG-UI volume of a single-hop one
- **Connection affinity.** The pod holding the connection must stay alive for the run's duration. A rolling deploy, HPA scale-down, or eviction breaks that connection
- **In-memory registry** dies with the process — an orphaned connection while the execution continues invisibly
- **Unbounded exposure window.** Orchestration doesn't hold the plan, so it cannot know how many hops remain or bound how long it must stay alive
- **No progress reporting** — a flat sequence of per-hop `RUN_STARTED`/`RUN_FINISHED` with no denominator
- Bounded by the 900s Route timeout

---

## Option C — Option B, with blocking mode backed by the database

**Shape:** Refinement of B. For **blocking** mode only, skip the Kafka tap entirely — poll Postgres for the execution's terminal state.

```
Caller ──POST──► Orchestration ──Kafka──► Executor
                   │                         │
                   │  poll every ~500ms      ▼
                   └────────────► Postgres ◄─┘
```

### Strengths (over plain B, for blocking mode)

- **Completely stateless and pod-agnostic.** Any pod can serve any request. The fan-in problem **disappears** for this path
- **No Kafka consumer, no registry, no correlation matching** for blocking mode
- **Pod affinity stops mattering** — any pod can resume checking after a restart
- **Ships fastest** — no new topic, no executor change, no consumer infrastructure
- ~50 indexed lookups per request at a 25s budget — trivial for Postgres, invisible latency against a multi-second execution

### Issues

- **Depends on an unverified fact** — see §4.1. The final **response payload** must be durably persisted, not just the status. `audit_table` has an `agent_response` column but is fire-and-forget and racy; whether `agent_execution` holds the payload is unconfirmed
- **Does not help streaming mode.** Live intermediate events exist only on Kafka; a database cannot back them
- Adds polling load proportional to concurrent blocking requests

---

## Option D — Orchestration-driven execution (true orchestration)

**Shape:** Move the "what happens next" decision out of the executor's `_prepare_response()` and into orchestration. The executor runs **one step** and stops. Orchestration awaits each step's result, consults its own plan, and dispatches the next.

Kafka stays. What changes is **who holds the steering wheel between hops**.

```
Orchestration ──step 1──► Kafka ──► Executor ──result──► Kafka ──► Orchestration
      │                                                                 │
      └──────────────── decides: step 2? cancel? policy check? ◄────────┘
```

### Strengths

- **Cancellation becomes trivial** — "don't dispatch the next step." No new mechanism needed
- **Per-step policy enforcement** — budget checks, content filtering, business rules, in one place
- **Real plan-level observability** — a single component sees the whole run for the first time
- **Progress reporting** — "step 2 of 4" becomes possible
- **A stable owner of the current step** — something to address, cancel, or time out
- Partially simplifies streaming: the per-hop boundary signal orchestration already needs to drive its loop is nearly the same signal `/stream` needs

### Issues

- **Touches the core execution path for every caller, async included.** Not additive; real blast radius
- **Re-couples plan liveness to orchestration.** Today executor pods keep a plan moving regardless of orchestration's state. Afterwards, if the driving orchestration pod dies mid-plan, **the plan stalls** — unless you checkpoint loop progress durably and let another pod resume
- Needs a per-step correlation scheme — `x_correlation_id` currently spans the whole plan, not one hop
- A substantially larger project than any of A/B/C

---

## Option E — Endpoint surface: one route with a flag, or two routes

Not an architecture — a surface choice that applies on top of B or C.

| | Two routes (`/stream` + `/sync`) | One route + flag |
|---|---|---|
| Routes added | 8 | 4 |
| Precedent | — | OpenAI `chat/completions` with `"stream": true` |
| Discoverability | Explicit in OpenAPI | Response shape branches on input |

**Note:** a single-endpoint-plus-flag design is only possible because both modes are ordinary HTTP POSTs differing in response treatment. **WebSocket cannot be a branch** — it requires its own `Upgrade` handshake, a different protocol negotiation, so it would force a separate URL and defeat the unification.

---

# Part 3 — Comparison

## 3.1 Against the requirement

| | A — fully sync | **B — hybrid** | **C — B + DB polling** | D — true orchestrator |
|---|---|---|---|---|
| Serves Kafka-less callers | ✅ | ✅ | ✅ | ✅ (with B/C on top) |
| Live step visibility | ✅ | ✅ | ✅ (streaming path) | ✅ |
| Multi-agent plans work | ❌ **needs rewrite** | ✅ unchanged | ✅ unchanged | ✅ (rewritten) |
| Async path untouched | ✅ | ✅ | ✅ | ❌ |
| Executor changes | **Core rewrite** | 1 file (`/stream` only) | 1 file (`/stream` only) | Core change |
| Ships incrementally | ❌ | ✅ | ✅ | ❌ |
| New infrastructure | none | 1 Kafka topic | 1 Kafka topic | none |
| Database changes | none | none | none | checkpoint table |

## 3.2 Cloud-native properties (multi-pod)

| | A | **B** | **C** | D |
|---|---|---|---|---|
| Fan-in complexity | none | registry + taps | **none for blocking** | registry + taps |
| Read amplification | none | N pods × hop count | **blocking: none** | N pods × hop count |
| Pods that must survive a request | **2** (orch + executor) | 1 (orch) | **0 for blocking** | 1 (orch), + plan stalls |
| Backpressure | ❌ build it | ✅ Kafka | ✅ Kafka | ✅ Kafka |
| Durability on pod death | ❌ request lost | ✅ message on topic | ✅ message on topic | ✅ + checkpoint |
| Executor pods disposable | ❌ | ✅ | ✅ | ✅ |

## 3.3 Risk

| | A | **B** | **C** | D |
|---|---|---|---|---|
| Blast radius | **Very high** | Low | **Lowest** | High |
| Rollback | Redeploy both | Config flag | Config flag | Redeploy both |
| Divergent code paths | **2 execution engines** | 1 | 1 | 1 |
| Unverified dependencies | — | 3 (see §4.1) | 4 (payload persistence) | many |

---

# Part 4 — Recommendation

## **Adopt Option B, refined by Option C where it applies. Defer Option D as a separate, later project.**

### 4.0 Why

**Option A is out** — not because of effort, but because it cannot deliver a complete multi-agent answer without rewriting the executor's core loop, and because it makes the system *less* resilient in a multi-pod environment by coupling two pod lifetimes to every request.

**Option D is right, but not now.** True orchestration is where this platform should end up — cancellation alone justifies it, and it fixes progress reporting and step ownership as a side effect. But it modifies the execution path for **every** caller, and it needs its own resilience design (checkpointing so a stalled plan can be resumed) before it is safe. Bundling it into the sync API work would put a large, risky change on the critical path of a small, additive one.

**Option B is the only path that delivers the requirement additively**, and **C removes its worst property** — the fan-in — from the mode most callers will use.

### 4.1 Verify these first

| # | Question | Impacts | Why it matters |
|---|---|---|---|
| 1 | Does `agent_execution` (or any durable store) hold the final **response payload**, not just status? | **C** | If yes, blocking mode needs no Kafka consumer at all. If no, fall back to B's tap |
| 2 | Does any use case have `ag_ui_events_streaming` enabled today? | B `/stream` | If none, the executor change touches dormant code — near-zero risk |
| 3 | Does `TaskPayloadModel.model_config` set `extra='forbid'`? | B `/stream` | If yes, deploy order is mandatory: executor before orchestration |
| 4 | Does `agent_execution_service.py:139` stream text incrementally or emit whole messages? | B `/stream` | Determines whether token-level streaming can be offered |
| 5 | Average and P99 **hop count** per plan × hop duration | B, C | Sizes connection hold time, read amplification, and HPA for this tier. **This number does not exist yet and capacity cannot be planned without it** |

### 4.2 Target architecture

**Three delivery modes, one execution engine.**

| Mode | Endpoint | Mechanism | Caller needs |
|---|---|---|---|
| **Async** *(existing, untouched)* | `POST .../task-executor` | Kafka / webhook out | broker + topic, or a webhook |
| **Blocking** | `POST .../task-executor/sync` | Poll Postgres for terminal state *(or Kafka tap if §4.1 #1 fails)* | HTTP client |
| **Streaming** | same route, `stream: true` | Group-less tap on a platform-owned AG-UI topic → SSE | HTTP client |

**Surface:** one route per existing endpoint, with a `stream` flag (Option E). Four new routes, not eight.

**Transport:** SSE, not WebSocket — the ingress supports it today without change, the existing auth dependency works unmodified, and it is the only option that can live behind a flag on an ordinary POST.

### 4.3 Required, not optional: durable registration

Regardless of B or C, **the in-memory registry must be backed by a durable record.** Write a row on registration (`x_correlation_id`, registered-at, mode, status) to Postgres.

This does not prevent a dead pod from dropping a live TCP connection — nothing can. It converts **"orphaned forever, invisibly"** into **"recoverable on reconnect by any pod."** Given that multi-hop plans have no bounded duration, this is a real gap, not a hypothetical one.

Pair it with **graceful draining**: on `SIGTERM`, fail the readiness probe immediately (no new connections routed) but let existing streams finish; set `terminationGracePeriodSeconds` above the stream cap. This converts routine deploys and scale-downs — the *majority* of real disconnections — from disruptions into non-events.

### 4.4 Sequence

| Step | What | Depends on |
|---|---|---|
| **1** | Verify §4.1 questions | — |
| **2** | **Blocking mode** — the four routes with `stream: false`, DB polling (or tap), durable registration, graceful draining | §4.1 #1 |
| **3** | **AG-UI platform topic** — create it; executor publishes there instead of the caller's topic; request-scoped `ag_ui_streaming` flag | §4.1 #2, #3 |
| **4** | **Streaming mode** — `stream: true`, Consumer A tap, SSE generator | Step 3 |
| **5** | **Reconnect** — resume a dropped stream using the durable registration from step 2 | Steps 2, 4 |
| **6** | **Orchestration-driven execution (Option D)** — separate project, own design review, checkpointing designed up front | — |

**Step 2 alone closes the primary gap:** a team with no Kafka can get an answer from one HTTP call. Everything after it is progressively better experience, not a missing capability.

### 4.5 Guarantees this preserves

1. The four existing POSTs and three GETs stay **byte-identical**, enforced by an OpenAPI snapshot test written before any new code
2. Every new mode sits behind its own config flag, defaulting **off** — ship dark, disable without rollback
3. The executor's execution path is **unchanged** for async callers, and unchanged for blocking-mode callers too
4. **No database schema change** in steps 2–5 except the durable-registration table
5. The existing consumer group and `ResponseService` are never touched — new consumers use no group at all
6. No new infrastructure beyond one Kafka topic

### 4.6 Known limitations of the recommendation

Stated plainly, so they are decisions rather than surprises:

- **A hard pod kill drops a live streaming connection.** Draining covers planned churn; nothing covers an instant kill. Mitigated by durable registration + reconnect (step 5), not eliminated
- **900s ceiling** on any held connection, imposed by the Route. Executions that routinely exceed it belong on the async path
- **No progress reporting** until Option D — the choreographed model means orchestration doesn't hold the plan
- **No cancellation** until Option D
- **Read amplification scales with pods × hops.** Acceptable at current scale; §4.1 #5 is the number that tells you when it isn't. Mitigations in order: batch token deltas at the source, then partition-aware `assign()`. Do **not** introduce Redis or another pub/sub layer for this without measurement first
- **AG-UI publishes are fire-and-forget.** The stream is best-effort; the final frame is authoritative. This must be in the API contract given to callers
