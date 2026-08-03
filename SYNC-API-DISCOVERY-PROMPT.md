# Discovery Prompt — Sync/Streaming API for Agentic Orchestration + Agent Executor

Run this in Claude Code (or your agent of choice) **on the machine that has both repos**, with both repo roots accessible. It produces a single report file that can be handed back for a concrete change plan.

---

## PROMPT (copy everything below)

You are doing a **read-only architecture discovery pass**. Do not modify any code. Produce one markdown report at `./SYNC-API-DISCOVERY-REPORT.md`.

Context: We have two services — **Agentic Orchestration** and **Agent Executor**. Today, a client POSTs to an API to run an agent; the work is dispatched over Kafka, the executor runs it, and a final response eventually gets back to the caller. We want to add a **synchronous/streaming API** so a caller can issue one request and receive **each intermediate step** as it happens (likely via Server-Sent Events, possibly WebSocket). I need to know exactly where to make changes.

Investigate both repos and answer ALL of the following. **Cite `file_path:line` for every claim** and paste the relevant code snippet (trim to the meaningful lines). Where something does not exist, say "not present" explicitly rather than omitting the section.

### 1. Service shape
- Language, framework, and version for each service (e.g. FastAPI/Flask/Spring Boot/Express/Go).
- **Sync or async runtime?** FastAPI-async vs WSGI-sync, Spring WebFlux vs MVC, thread-per-request vs event loop. Quote the server startup/entrypoint (`main.py`, `Application.java`, `server.ts`, gunicorn/uvicorn worker config, `Dockerfile` CMD).
- Worker/thread/connection pool sizes and where configured.

### 2. The existing "run agent" POST endpoint
- Exact route, HTTP method, handler file:line.
- Full request model/DTO and response model/DTO (paste the class/schema).
- Trace the handler line by line: validation → persistence → Kafka produce → what it returns to the caller.
- **How does the final response reach the caller today?** Pick one and prove it with code: (a) handler blocks waiting on a reply topic, (b) handler blocks polling a DB row, (c) handler returns a job id and the client polls a second endpoint, (d) a callback/webhook is fired, (e) something else. This is the single most important answer — include the exact blocking/waiting code and its timeout.

### 3. Kafka topology
- Every topic name involved in the run flow, and its role (command / step-event / result / DLQ).
- Partition count, replication, **message key** used for each produce.
- All `group.id` values, and whether they are static or per-instance.
- Producer and consumer config blocks (acks, linger, max.poll.interval, auto.offset.reset, isolation level).
- The **full message envelope schema** for each topic — paste the serializer/schema class. Note whether there is a correlation/run/trace id and what it's called.
- Where topics are declared/created (config file, Terraform, admin client).

### 4. Executor step loop — event granularity
- Where does the executor actually run an agent turn? File:line of the main loop.
- Enumerate the discrete step boundaries that exist today: tool call start/end, LLM call start/end, planner step, retry, sub-agent spawn, etc.
- **Are per-step events already emitted anywhere** (Kafka, logs, observability SDK, DB writes)? If yes, paste the emit call sites — these are the natural SSE event sources.
- Is there any token-level or partial-output streaming from the LLM client? Is the LLM SDK called in streaming mode?
- Typical run duration: find any recorded latency metrics/logs, or estimate from timeouts. Give p50/p95 if obtainable.

### 5. State store
- DB engine(s). Paste the schema (DDL or ORM models) for any run/job/execution/step tables.
- Is there already a monotonic sequence or ordering column per run? If not, note what would need to be added for event replay.
- Migration tooling in use (Alembic/Flyway/Liquibase/etc.) and where migrations live.

### 6. Deployment + network path — REQUIRED, do not skip
- Kubernetes? Paste Deployment/Service/Ingress manifests or Helm values. Replica counts and HPA.
- What sits in front of the service: nginx, Istio/Envoy, an API gateway (Apigee/Kong/AWS ALB/APIM)? Paste config.
- **Idle/read/write timeouts** at every hop (ingress annotations, gateway policy, LB config, framework-level timeout). List each with its value — an SSE stream dies at the shortest one.
- Is response buffering enabled anywhere (`proxy_buffering`, gateway response buffering)? SSE requires it off.
- Any evidence WebSocket upgrade is or is not permitted through the ingress/gateway.
- Is there an existing Redis, or any other shared cache/pubsub, in the deployment?

### 7. Auth + tenancy
- How is the POST endpoint authenticated (bearer/JWT/mTLS/API key)? Middleware file:line.
- Is there a tenant/team/caller identity on the request, and is it carried into the Kafka envelope? This determines who is allowed to stream a given run.

### 8. Existing streaming, if any
- Any existing SSE, WebSocket, chunked, long-poll, or gRPC-streaming endpoint in either repo. Paste it — we'd rather follow an existing pattern than invent one.
- Any existing webhook/callback-out mechanism.

### 9. Observability instrumentation
- Which observability/tracing SDK is wired in, and where spans/events are emitted in the run path.
- Is there a trace id propagated end to end through Kafka? What header/field?

### 10. Cancellation and failure
- Can a run currently be cancelled? How?
- Where are executor failures turned into a caller-visible error? Paste the error envelope.
- Retry/DLQ behavior on the executor consumer.

### Output format
Write `./SYNC-API-DISCOVERY-REPORT.md` with one `##` section per numbered item above, in order. Every claim gets a `file:line`. End with:

- **`## Sequence diagram`** — an ASCII or mermaid diagram of today's exact end-to-end flow from POST to final response, with topic names on the arrows.
- **`## Repo tree`** — output of `git ls-files` for both repos, filtered to source/config (exclude tests, fixtures, lockfiles, assets).
- **`## Open questions`** — anything you could not determine from the code.

Do not propose a design. Only report what exists.
