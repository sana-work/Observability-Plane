# Discovery Prompt v2 — evidence-forced

The first discovery pass produced a confident, well-formatted report that was substantially fabricated. This prompt is built to prevent a repeat.

**Run it in a fresh session** (no prior conversation context), in the workspace containing both repos, with an agent that can actually read files and run commands. Work through it in order.

---

## PROMPT (copy everything below)

You are producing a **factual inventory** of two repositories. Write the result to `./DISCOVERY-v2.md`.

### Ground rules — read before starting

A previous attempt at this task produced a detailed, plausible, and largely **fabricated** report. It invented `app/agent/runner.py`, `app/services/kafka_consumer.py`, an `agent_runs` table, an `AgentRunRequest` model, and endpoints `POST /api/v1/agent/run` and `GET /api/v1/agent/run/{run_id}/status`. **None of these exist.** It also completely missed `gssp_agentic.audit_log`, which is the single most important object in the executor. Every fabricated detail was formatted identically to the true ones, so the report was worse than useless — it was actively misleading.

Therefore:

1. **Every factual claim must be preceded by the command that produced it and that command's raw, unedited output.** Claim first, evidence second is not acceptable — paste the evidence, then interpret it.
2. **Never write a path, a line number, an identifier, or a value you have not seen in output.** Do not reconstruct a plausible one from framework conventions.
3. **If something is absent, write `NOT FOUND` and show the search that came up empty.** Do not substitute the nearest similar thing without saying so explicitly.
4. **Do not use the words** *typically, usually, likely, presumably, appears to, should be, standard practice*. If you are inferring, the sentence must begin with `INFERENCE (unverified):`.
5. **Quote code with real line numbers** from a tool that displays them. Do not hand-number quoted code.
6. **Do not summarize across repos.** Handle one repository completely, then the other.
7. Report counts. When you grep, state how many matches there were and paste all of them (or, above 20, paste the first 20 and say how many were suppressed).

### Section 0 — Inventory first, before describing anything

Do not open a single file until this section is written.

For **each** repository, run and paste the complete raw output:

```
git ls-files
```

Then paste the top-level directory listing. Then state, in one sentence per repo, the actual top-level source directory name.

Everything you claim later must reference a path that appears verbatim in this output. If a path you want to cite is not in this listing, you have made an error — stop and re-check.

### Section 1 — HTTP surface

The running service exposes these five routes (observed in its Swagger UI at `localhost:8080/docs`, title `Agentic-Planner`):

- `POST /api/v1/agentic-orchestration/task-executor`
- `POST /api/v1/agentic-orchestration/conversational-task-executor`
- `POST /api/v1/agentic-orchestration/native-conversational-task-executor`
- `POST /api/v1/agentic-orchestration/agent-testing`
- `GET  /api/v1/agentic-orchestration/execution-status`

For each: the handler's `file:line`, the decorator line quoted, the full request model, the full response model, and a line-by-line trace of the handler body. If you find routes beyond these five, list them. If you cannot find one of these five in the code, say so explicitly — that is important information, not something to paper over.

Also state where the FastAPI app is constructed, every `include_router` call, and how the app is started (`Dockerfile` CMD / entrypoint / worker count).

### Section 2 — How work reaches the executor

Trace the complete path from an HTTP handler to the executor beginning work. Quote every hop. Name the transport explicitly and prove it: if Kafka, give every topic name with the `file:line` of each produce and consume; if HTTP, gRPC, a shared table, or a queue, say so and prove that instead.

Do not assume Kafka. Verify it.

Then: what starts the executor process, and what does its main loop look like? Quote it in full. State whether it can process more than one execution concurrently, and prove it from the code.

### Section 3 — `gssp_agentic.audit_log` (highest priority)

This table is defined in `excutor/core/db/audit_table_pg_store.py` and is written from `excutor/core/agent/runner.py`. Verify both paths against your Section 0 inventory before proceeding.

Report:

- The full table definition, quoted, with line numbers.
- **Every** call site that writes a row: `file:line`, the `event_type` / `agent_status` / `tool_status` values passed, and where in the control flow it sits. Show the grep and its match count.
- Whether writes are synchronous and committed before execution continues. Quote the session/commit handling.
- How `sequence_id` is assigned and incremented. Quote it. State whether it is per-execution or global, and whether it can collide or interleave across concurrent executions or sub-agents.
- The exact string literals used for the terminal row — quote the `finally` block verbatim.
- **Every index, constraint, and partition on the table.** If the only index is the primary key on `id`, say exactly that. Also run, if you have database access:
  ```sql
  \d gssp_agentic.audit_log
  ```
  and paste the result. If you have no database access, say so and report only what the code declares.
- **Every read site** of this table, anywhere in either repo: `file:line` and the query.
- Any retention, archival, purge, or partition-rotation job that touches it, in either repo or in any config file.

### Section 4 — `x_correlation_id` end to end

The most important question in this report. Trace it with `file:line` at every hop:

- Where the value originates in the orchestration service — middleware, header extraction, generation? Quote it.
- How it travels to the executor. Quote the propagation.
- Where the executor reads it and sets it on audit rows. Quote it.
- **State plainly whether the orchestration service can know this exact value at the moment it dispatches an execution**, before any audit row exists. Yes or no, with the evidence that settles it.
- How `x_correlation_id` relates to `invocation_id` and `session_id`, which are separate columns on the same table.
- For sub-agent executions (`root_agent_name`, `agent_name`): do nested agents share one `x_correlation_id`? Quote the code that determines this.

### Section 5 — Database

- Every database, schema, and connection string in either repo, and which service uses which. Quote the config.
- Every table either service defines, with its DDL or ORM model. If `audit_log` is the only one in the executor, say so.
- Connection pool sizes per service, quoted, and the resulting maximum connections per process and per pod.
- Migration tooling, if any, and where migrations live. If there is none, say `NOT FOUND`.
- Whether both services connect as the same database user, and what grants are visible in code or config.

### Section 6 — Auth and identity

How each of the five endpoints authenticates. Quote the dependency or middleware. What identity fields exist on an authenticated request, and which of them are carried into the executor and onto audit rows.

### Section 7 — Deployment and network path

Kubernetes manifests, Helm charts, ingress, gateway, or reverse-proxy config, anywhere in either repo. If absent, say `NOT FOUND` and say so plainly rather than describing what such config would contain.

Every timeout value you can find anywhere — HTTP client, server, Kafka, database, gateway — with `file:line` for each.

Whether any response buffering or compression is configured.

### Section 8 — Existing streaming and long-running responses

Search both repos for: `StreamingResponse`, `EventSourceResponse`, `text/event-stream`, `WebSocket`, `websocket`, `yield` inside a route handler, `stream=True`, chunked transfer, gRPC streaming. Paste each grep with its match count. Report what exists, or `NOT FOUND` per item.

### Section 9 — Failure handling

How a failed execution is recorded and surfaced to a caller. Retry behaviour, dead-letter handling, poison-message handling. Whether cancellation exists anywhere. Quote the relevant code or state `NOT FOUND`.

### Section 10 — Contradictions

List every place where what you found contradicts any of the following, which were reported by a previous pass or observed externally:

- Executor source lives under `excutor/core/…`
- `gssp_agentic.audit_log` exists with columns including `x_correlation_id`, `sequence_id`, `event_type`, `agent_status`, `tool_name`, `tool_status`, `input_token_count`, `output_token_count`, `start_timestamp`, `end_timestamp`
- Audit event types are `INVOCATION`, `LLM_REQUEST`, `LLM_RESPONSE`, `TOOL_CALL`, `TOOL_RESULT`, `ERROR`
- The five HTTP routes listed in Section 1
- The executor processes one execution at a time
- No Redis exists in either repo

For each: confirmed, contradicted, or not found. If contradicted, show the evidence.

### Output format

`./DISCOVERY-v2.md`, one `##` section per number above, in order. Every section opens with the commands run and their raw output, then the findings.

End with a section titled **`## Self-audit`** containing:

- Every path, identifier, and line number cited anywhere in the report that you did **not** directly observe in tool output. If this list is not empty, go back and remove or fix those claims before submitting.
- Every question above you could not answer, and why.
- Anything you found that this prompt did not ask about but that materially affects building a streaming API on top of this system.

Do not propose a design. Do not recommend anything. Report only what is in the code.
