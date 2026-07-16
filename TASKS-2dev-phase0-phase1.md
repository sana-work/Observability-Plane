# Task Board — Building Phase 0 + Phase 1 with Two Developers

*Read [BUILD-GUIDE.md](BUILD-GUIDE.md) first for the big picture and the
glossary. This document breaks Phases 0 and 1 into individual tasks that two
developers can execute from scratch.*

---

## How to read this document

Every task is a card with the same five parts:

- **Goal** — one sentence: what exists when you're done.
- **Why** — the problem this solves. If you understand the why, you can make
  good judgment calls when the details get fuzzy.
- **What to do** — concrete steps.
- **Done when** — the acceptance test. Not "code written" but "this observable
  thing is true".
- **Depends on / Time** — ordering and a focused-days estimate.

Task IDs: `J-*` = both developers together, `A-*` = Dev A, `B-*` = Dev B.

> **Note:** finished reference implementations for every task already exist in
> this repo (`observability-iac/`, `ai-observability-sdk/`). You can build
> against them as the target state, or rebuild from scratch using only this
> document — it is written to be self-sufficient.

## Scope guardrails (firm)

- **No Snowflake.** The analytics store is partitioned PostgreSQL
  (`obs_events.*`). Task A-10 builds it. Snowflake is a possible future swap.
- **No Redis.** Budget counting uses a Postgres table + atomic function
  (A-8). Caches are in-process TTL caches (B-7). Swap paths are documented,
  not built.
- **`user_id` is raw.** Events carry the user's SOE ID unhashed, by platform
  decision. Do not add user hashing anywhere. Prompts/free text are still
  hashed or redacted, because those can contain anything.

## The two roles

**Dev A — Platform/IaC owner.** Builds everything that lives in shared
infrastructure: Kafka topics, two Postgres schemas, Elasticsearch templates,
S3, Kubernetes monitoring, CI. Mostly SQL, bash, YAML, and one Python test
file. Output: applied infrastructure.

**Dev B — SDK owner.** Builds the Python library the 8 services will install:
config, context, Kafka producer, tracing, logging, middleware, decorators,
prompt client, tests, docs. Output: a published wheel.

**Rule: every PR is reviewed by the other developer.** Dev A's schemas must
make sense to the person writing the emitter; Dev B's field names must make
sense to the person writing the mappings. Cross-review *is* the integration
test you get for free.

## The one hard ordering rule

**Nothing is built before J-1 (the contract freeze) merges.** The contract —
what an event looks like — is the interface between the two tracks. Freeze it
first and the two developers barely need to talk for three weeks. Skip the
freeze and every disagreement about a field name becomes rework on both sides.

## Timeline at a glance

| Week | Dev A | Dev B | Together |
|---|---|---|---|
| 1 | A-1 dev stack | B-1 package scaffold | J-0 repo/CI, **J-1 contract freeze** |
| 2 | A-2 Kafka, A-3 schema+runner, A-4 registries | B-2 config+context, B-3 hashing+headers | PR cross-reviews |
| 3 | A-5 prompts, A-6 catalogs, A-7 governance/SLO/aggregates | B-4 emitter, B-5 tracing+logging | PR cross-reviews |
| 4 | A-8 budget fn, A-9 seeds, A-10 firehose | B-6 middleware, B-7 decorators+cost+prompts | PR cross-reviews |
| 5 | A-11 Elasticsearch, A-12 S3, A-13 infra+replay | B-8 tests/CI/docs/publish | — |
| 6 | A-14 policy gate + CI | (review support) | J-2 smoke, J-3 audit, J-4 sign-off |

Totals: Dev A ≈ 17 days, Dev B ≈ 12.5 days (+review load), joint ≈ 4.5 days
each. Fits the roadmap's 6 weeks with normal meeting overhead.

---

# Week 1 — Joint foundation

## J-0 · Repo, tooling, CI skeleton — *both · 0.5d*

**Goal:** a repo where a PR from either developer runs lint + tests
automatically.

**Why:** every later task lands through this gate. Setting it up later means
retro-fitting checks onto work that never ran them.

**What to do:**
1. Create the two top-level folders: `observability-iac/`, `ai-observability-sdk/`.
2. Agree the toolchain: Python 3.11+, `ruff` for lint, `pytest` for tests.
3. Add a CI pipeline with one job per folder, triggered on PRs touching that
   folder. For now each job just runs a placeholder test.
4. Add branch protection: CI green + one review required.

**Done when:** each developer opens a trivial PR and sees CI run and pass.

---

## J-1 · Contract freeze — *both · 1.5d* ⛔ blocks all other tasks

**Goal:** three reviewed, tested Python files in `observability-iac/contracts/`
that define exactly what an event is.

**Why:** the SDK serialises this shape, the enrichment consumer validates it,
Elasticsearch maps it, Postgres columns mirror it, dashboards query it. It is
the single most-load-bearing artifact in the platform. Ninety minutes of
argument now saves weeks of migration later.

**What to do:**

1. **`event_schema.py` — the `ObsEvent` Pydantic model.** Every event carries:
   - *Identity:* `event_id` (uuid, auto), `schema_version` (fixed `"1.0"`),
     `event_type` (validated against the enum), `telemetry_type`
     (`event|log|metric`).
   - *Time:* `timestamp` and `emitted_at`, both UTC ISO-8601 strings, auto-set.
   - *Correlation:* `correlation_id` (the request-wide ID), `request_id`,
     `trace_id`, `span_id`, `parent_span_id` (span nesting for trace trees).
   - *Ownership:* `service_name` (validated against the 8), `component`,
     `environment` (`dev|staging|prod`), `application_id`, `lob`, `tenant_id`,
     and **`user_id` — the raw SOE ID, unhashed by platform decision** (put
     that sentence in a comment so nobody "fixes" it later).
   - *Outcome:* `status`, `latency_ms`, `error_code`, `http_status`.
   - *Everything else:* `payload: dict` — domain fields (tokens, chunk counts,
     tool names...) go here, not in the envelope.
   - Validators that **reject** unknown `event_type`, `service_name`,
     `telemetry_type` — bad events must fail loudly at the edges.
2. **`event_types.py` — a 50-value string Enum**, grouped: request (4),
   orchestration (6), kafka (4), agent (8), llm (5), tool (4), rag (5),
   guardrail (4), feedback (3), document (7). End the file with
   `assert len(EventType) == 50` so nobody adds a 51st casually.
3. **`service_names.py` — an 8-value string Enum** (agentic-orchestration,
   agent-executor, gssp-gs, gssp-qs, gssp-rs, consumer-service,
   data-ingestion, user-feedback) with `assert len == 8`.
4. **`contracts/tests/test_contract.py`:** a minimal event round-trips through
   JSON; unknown event_type/service_name raise; the counts are 50 and 8.
5. Write the **versioning policy** into the module docstring: *any* envelope
   change bumps `schema_version` and must update the SDK's vendored copy in
   the same PR.

**Done when:** both developers approve the PR; tests green; both can name the
envelope fields from memory (that's the real freeze).

---

## A-1 · Local dev stack — *Dev A · 1d*

**Goal:** `docker compose up -d` gives both developers a laptop-sized copy of
the entire infrastructure.

**Why:** neither developer should need cluster access to make progress, and
Dev B's smoke tests need somewhere real to emit to.

**What to do:** write `observability-iac/docker-compose.dev.yml` with five
services — Kafka (single broker, KRaft mode, **auto-create topics off** so the
topic script is the only creator), Postgres 16, Elasticsearch 8 (single node,
security off), Kibana, and single-binary Tempo (local disk, OTLP port 4317
exposed — that's where the SDK will send spans). Document start/stop/reset in
the README.

**Done when:** fresh clone → `docker compose up -d` → all five containers
healthy; `curl localhost:9200` answers; `psql` connects.

---

## B-1 · SDK package scaffold + vendored contracts — *Dev B · 1d*

**Goal:** an installable empty package with the frozen contract inside it.

**Why:** services must get everything from `pip install` — they will never
have the IaC folder at runtime. So the contract is *copied* ("vendored") into
the SDK. Copying invites drift; CI closes that gap (A-14 + B-8 both diff the
copies).

**What to do:**
1. `pyproject.toml` — name `ai-observability-sdk`, Python ≥ 3.11, deps:
   `pydantic` v2, `pydantic-settings`, `confluent-kafka`, `structlog`,
   OpenTelemetry (api, sdk, OTLP-gRPC exporter, fastapi/httpx/asyncpg
   instrumentations), `prometheus-fastapi-instrumentator`, `httpx`.
   Dev extras: pytest, pytest-asyncio, fastapi, ruff.
2. Copy the three contract files **byte-for-byte** into
   `ai_obs_sdk/contracts/` with an `__init__.py` re-exporting `ObsEvent`,
   `EventType`, `ServiceName`. Add a header: "vendored — do not edit here".
3. Wire the SDK's contract tests to run against the vendored copy.

**Done when:** `pip install -e ".[dev]"` works; contract tests pass from
inside the SDK.

---

# Weeks 2–5 · Dev A track — the platform

## A-2 · Kafka topics as code — *1d*

**Goal:** one YAML file describing the three topics + one script that makes
reality match the YAML, safely, every time.

**Why:** topics created by hand drift between environments and nobody
remembers why prod has 8 partitions. Declarative + idempotent means the file
in git *is* the truth.

**What to do:**
1. `kafka/topics.yaml`:
   - `ai-obs-events-raw` — 12 partitions, replication 3, retention 7 days,
     lz4 compression, `min.insync.replicas=2`, `max.message.bytes=1MiB`
     (big payloads belong in S3, not Kafka).
   - `ai-obs-events-processed` — 12 partitions, 3 days (storage consumers
     re-read within hours; 3 days covers a long weekend outage).
   - `ai-obs-dead-letter` — 3 partitions, 14 days (humans debug these; give
     them two weeks).
   - Document in the file: producers key every message by `correlation_id`
     → all events of one request stay ordered on one partition.
2. `kafka/create_topics.sh`, reading the YAML:
   - topic missing → create it with the configs;
   - topic exists → align configs (`kafka-configs --alter`) and grow
     partitions if below spec (never shrink — Kafka can't);
   - `KAFKA_ENV=dev` → replication 1 and drop `min.insync.replicas`
     (single-broker laptop stack);
   - end by describing all three topics so the run log shows the result.

**Done when:** run twice against A-1's stack — first run creates, second run
changes nothing. Topic names must equal the SDK config defaults (A-14 tests
this).

## A-3 · Control-plane schema, roles, migration runner — *1d*

**Goal:** the empty `observability` schema, five database roles, and a
migration runner so every later SQL file applies exactly once, in order.

**Why:** migrations without a runner turn into "did staging get 004?"
archaeology. Roles created first mean grants (A-8) are written against roles,
so onboarding a new consumer later is one GRANT, not a schema change.

**What to do:**
1. `migrations/001_create_schema.sql` — `CREATE SCHEMA observability` + five
   NOLOGIN group roles: `obs_admin` (migrations), `obs_enrichment` (the
   enrichment consumer), `obs_storage` (the storage consumer),
   `obs_dashboard` (dashboard backend, read-mostly + config writes),
   `dashboard_ro` (pure read-only: chatbot, BI).
2. `postgres/apply.sh` — records applied filenames in a
   `public.obs_schema_migrations` table; skips applied files; runs each
   migration in its own transaction; `--with-seed` also runs `seed/*.sql`
   (seeds are upserts, always safe to re-run).

**Done when:** `./apply.sh` twice → second run prints all "skip".

## A-4 · Registries — *1.5d*

**Goal:** the tables that say what exists and who owns it: applications,
services, agents, tools, knowledge bases.

**Why:** raw events carry IDs (`application_id`, `agent_id`...). The
enrichment consumer joins these registries in so every stored event also
carries owner, team, LOB, criticality — which is what makes "page the right
team" and "per-LOB dashboards" possible.

**What to do (one table at a time, all in `002_registries.sql`):**
- `application_registry` — PK `application_id` (what services send as
  `AI_OBS_APPLICATION_ID`); name, type, **`lob`** (drives index routing and
  RBAC), usecase_id, CSI inventory id, owner team + email, criticality,
  allowed environments, status, timestamps.
- `service_registry` — exactly the 8 platform services; must mirror the
  `ServiceName` enum (A-14 asserts it); a `kafka_enabled` flag flips true as
  Phase 2 onboards each service — it doubles as a rollout tracker.
- `agent_registry` — PK (agent_id, version); type
  (planner/executor/router/evaluator), execution mode, owning application,
  default model, `max_steps` loop guard, status.
- `tool_registry` — PK (tool_id, version); **`tool_type` CHECK-constrained to
  `REST|DB|ServiceNow|RAG|InternalAPI`** — the same vocabulary the SDK's
  `@trace_tool` documents, so dashboards can trust the values; SLA endpoint
  URL + target p95 for dependency-health views.
- `rag_registry` — PK rag_id; knowledge base name, vector index name,
  embedding model + dimension, chunking strategy, refresh schedule, owner.

**Done when:** migration applies cleanly; every registry column the roadmap
mentions exists; Dev B has reviewed the naming (these names surface in
`payload` fields).

## A-5 · Prompt registry — *1d*

**Goal:** versioned storage for prompt templates with a controlled activation
workflow and an A/B split.

**Why:** prompts are code that never went through code review. Versioning
them, hashing them, and stamping the version onto every LLM event is what
lets you answer "did the new prompt make quality worse?".

**What to do (`003_prompt_registry.sql`):**
1. `prompt_template_registry` — PK (template_id, version); template_text,
   declared variables (jsonb list), `prompt_hash` (sha256 of the text — same
   function the SDK uses, so hashes match), status
   (`draft → active → archived`), `ab_bucket` + `traffic_pct` for
   experiments, owner, created_by, activated_at.
2. A **partial unique index** enforcing *one active version per template per
   A/B bucket* — the DB guarantees you can't serve two "actives".
3. `prompt_activation_audit` — append-only log of who
   activated/archived/rolled back what, when, why (compliance).
4. In the migration header, write down the **API response contract** the
   future prompt endpoint must return, because the SDK's `get_prompt()` (B-7)
   deserialises exactly this:
   `{"template_id", "version", "text", "prompt_hash", "ab_bucket"}`.

**Done when:** inserting two active rows for the same template+bucket fails
with a constraint error; the API shape is documented where Phase Q will find it.

## A-6 · Catalogs + model pricing — *1d*

**Goal:** three lookup tables: what errors mean, what metrics mean, what
models cost.

**Why:** *error_code_catalog* turns "raw exception soup" into a stable
taxonomy (`A0001`, `T0001`...) that dashboards can group by. *metric_catalog*
is the semantic layer — the chatbot answers "what was our error rate?" by
reading the formula from here, not by hallucinating SQL. *model_pricing* is
what the enrichment consumer bills with.

**What to do (`004_catalogs.sql`):**
- `error_code_catalog` — PK error_code; category, title, **`match_pattern`
  (a regex tested against `"ExceptionClass: message"`)**, `priority` (lower
  = tried first; keep a catch-all at 999), severity, `retryable`, runbook URL.
- `metric_catalog` — PK metric_id; human description, formula (SQL/PromQL/ES
  aggregation), source system (CHECK: obs_events | elasticsearch |
  prometheus | observability), source object, unit, aggregation, allowed
  group-by dimensions, **synonyms array** (how users phrase it — feeds the
  chatbot's intent matching), LOB, owner.
- `model_pricing` — PK (model_name, `effective_from` date); input/output USD
  per 1k tokens. Effective-dating means a price change never rewrites
  historical costs. **Sync rule:** rows must match the SDK's `cost.py`
  estimate table — A-14 enforces this with a test.

**Done when:** applies cleanly; the catch-all error row exists; Dev B
confirms the pricing rows match `cost.py`.

## A-7 · Governance, SLOs, aggregates — *1.5d*

**Goal:** budget limits, alert rules, dashboard config, feedback workflow,
SLO definitions + daily compliance, and 7 pre-aggregated rollup tables.

**Why:** *budgets* — cost governance is a headline goal; limits need a home
before enforcement can exist. *SLOs* — "are we meeting our promises"
computed daily with burn rates. *Aggregates* — dashboards asking "requests
per hour for 30 days" should read 720 pre-computed rows, not scan millions
of events. Creating them now (though the writer arrives in Phase 4) avoids a
mid-flight migration.

**What to do:**
1. `005_governance.sql` — `budget_limits` (per app/model/period; max USD,
   `alert_at_pct`, hard_stop flag; `model_name='*'` = all models),
   `alert_threshold` (metric_id FK, comparator, threshold, window, scope
   jsonb, notify channel), `dashboard_config` (widget definitions),
   `feedback_case` (feedback workflow: open → reviewed → fixed; linked
   incident; joined to traces by correlation_id).
2. `006_slo.sql` — `slo_definitions` (per app; sli_type
   availability|latency|quality|cost; target %; event filter jsonb; 30-day
   window) and `daily_slo_compliance` (PK slo+day; good/total counts, SLI %,
   worst 1h and 6h burn rates, error-budget-consumed %, breached flag).
3. `007_aggregates.sql` — the 7 rollups, PKs matching the roadmap grain:
   hourly application / agent / tool / llm (model × prompt × agent × app) /
   rag metrics; daily feedback per agent; daily KPI values.

**Done when:** all tables apply; each aggregate PK matches the roadmap's
stated grain ("1 row / app / hour" etc.).

## A-8 · Budget accumulator — the Redis stand-in — *1d*

**Goal:** a table + one atomic function that counts spend and says "you just
crossed the threshold" **exactly once**, even with concurrent writers.

**Why:** live spend counting is the classic Redis `INCRBYFLOAT` job — but
Redis isn't onboarded. A plain Postgres UPDATE would work except for the
alert problem: three enrichment pods crossing 80% simultaneously would emit
three alerts. Solving that once, in the database, keeps the consumer code
trivial.

**What to do (`008_budget_accumulator.sql` + `009_grants.sql`):**
1. `budget_accumulator` — PK (application_id, model_name, period,
   period_start); `spend_usd`; `alert_emitted` / `cap_emitted` booleans.
2. Function `add_spend(app, model, cost)` (plpgsql): for each matching row in
   `budget_limits` — compute the period start (day / ISO week / month);
   `INSERT ... ON CONFLICT ... DO UPDATE SET spend_usd = spend_usd + cost`
   (atomic); then flip `alert_emitted` (and `cap_emitted`) with an UPDATE
   whose WHERE clause includes `NOT alert_emitted` — only **one** concurrent
   caller wins that update, and only that caller gets `alert_crossed=true`
   back. Return (period, new spend, limit, alert_crossed, cap_crossed).
3. `009_grants.sql` — least privilege per role: enrichment reads
   registries/catalogs and writes accumulator + SLO compliance + EXECUTE on
   add_spend; storage writes aggregates + feedback cases; dashboard reads all
   and writes config/prompts/cases; dashboard_ro reads only.
4. Document the swap path in the header (Redis INCRBYFLOAT later; table
   becomes nightly reconciliation) — **do not build it**.

**Done when:** a test script hammering `add_spend` from two parallel
connections yields exactly one `alert_crossed=true` across all calls.

## A-9 · Seed data — *1d*

**Goal:** the minimum rows that make an empty environment usable.

**Why:** an empty registry means enrichment can't enrich and dashboards are
blank. Seeds also encode cross-repo agreements: the dev app id here must
match the SDK's `.env.example`, or the out-of-the-box demo breaks.

**What to do (three files in `postgres/seed/`, all idempotent upserts):**
1. `001_registries.sql` — the 8 services (values must equal the enum — A-14
   checks); dev application **`app-1234`** with a LOB and a monthly $100
   budget (matches `AI_OBS_APPLICATION_ID` in the SDK's `.env.example`).
2. `002_error_code_catalog.sql` — ~20 starter codes with regex patterns:
   LLM (rate-limited, safety-blocked, context-length, provider-down), tool
   (timeout, auth, schema), agent (max-steps, handoff, timeout), RAG (index
   down, embedding failed), guardrail, platform (5xx, DB), kafka — plus the
   `P0999` catch-all at priority 999.
3. `003_metric_catalog.sql` — ~10 core metrics (request count, error rate,
   p95 latency, LLM cost, tokens, agent success rate, RAG no-result rate and
   relevance, feedback rating, consumer lag) with formulas and synonyms; plus
   the `model_pricing` rows **matching the SDK `cost.py` table**.

**Done when:** `apply.sh --with-seed` twice → identical row counts both times.

## A-10 · The `obs_events` firehose — the Snowflake stand-in — *2d*

**Goal:** a separate `obs_events` schema that stores every enriched event in
SQL-queryable form for ~90 days, with partitions that manage themselves.

**Why separate from `observability`:** if Snowflake is approved later, the
swap replaces this schema's *writer and readers* only — the control plane
never notices. **Why partitioned monthly:** dropping a month-partition is
instant; `DELETE FROM` on a billion rows is an outage. **Why domain tables:**
`SUM(cost)` on a real column is 100× cheaper than extracting it from JSON a
billion times.

**What to do (`postgres-events/migrations/`, own `apply.sh` like A-3's):**
1. `001` — `obs_events.events`, `PARTITION BY RANGE (event_ts)`, columns
   mirroring the ObsEvent envelope **field-for-field** (including raw
   `user_id`), `payload JSONB` + GIN index, and secondary indexes:
   (application_id, ts), (correlation_id), (event_type, ts),
   (service_name, ts), partial on error_code. Plus two functions:
   - `ensure_month_partitions(n)` — finds **every** partitioned parent in
     the schema and creates partitions for the current + n future months
     (works on vanilla Postgres — no pg_partman dependency);
   - `drop_old_partitions(keep)` — drops partitions older than the window,
     returning what it dropped. Header note: the S3 archiver must export a
     month before it is dropped.
2. `002`–`007` — domain tables, monthly-partitioned, hot fields as columns:
   - `llm_events`: model identity, prompt_template_id/version/hash,
     temperature, input/output/total tokens, estimated_cost_usd, latency,
     time-to-first-token, retries, rate_limit/safety flags, finish_reason.
   - `agent_events`: agent id/version/type, step/loop/handoff counts,
     planner decision, termination reason, tools_used[], models_used[], cost.
   - `rag_events`: rag_id, KB, index, embedding model, query_hash, top_k,
     chunk_count, `no_result_flag`, relevance, citation coverage,
     context_tokens, truncation flag.
   - `feedback_events`: rating (1–5 CHECK), thumbs, sentiment, category,
     redacted free text, resolution status.
   - `quality_scores`: eval type (CHECK faithfulness | hallucination |
     answer_relevance | custom), score, judge model — empty until Phase Q.
   - `slo_history`: long-horizon copy of daily SLO compliance.
3. `008` — call `ensure_month_partitions(2)` to bootstrap; grants
   (obs_storage writes, dashboard_ro reads).
4. Note the nightly maintenance job (a K8s CronJob runs the two functions).

**Done when:** apply is idempotent; `ensure_month_partitions(2)` creates
partitions for **all seven** parents; a row inserted with next month's
timestamp lands in the right partition (verify with `EXPLAIN` or `\d+`).

## A-11 · Elasticsearch templates — *2d*

**Goal:** ES configured so that when the storage consumer writes its first
event, the index is born with the right field types, lifecycle, and settings.

**Why templates first:** without them ES guesses field types from the first
document — and guesses wrong (ids become analyzed text, latencies become
strings). Fixing a mapping later means reindexing everything.

**What to do:**
1. Two **ILM policies**: `hot-warm-30d` (readonly + force-merge at 2 days,
   delete at 30) and `compliance-180d` (delete at 180 — for guardrail and
   quality data).
2. Two **component templates** (shared building blocks):
   `obs-common-settings` (1 shard, 1 replica, 5s refresh, best_compression,
   default ILM, total-fields limit) and `obs-common-mappings` — **every
   envelope field with an explicit type**: ids/enums = `keyword`, timestamps
   = `date`, latency = `double`, http_status = `short`, `user_id` =
   `keyword` (raw, so "by SOEID" queries are exact-match terms).
3. **Eleven index templates**, one per event family — requests, errors,
   agent-steps, llm-calls, tool-calls, rag-events, guardrail-events (pinned
   to 180d ILM), feedback, traces, quality-scores (180d), anomalies. Each:
   pattern `ai-obs-*-<family>-*` (the `*` in the middle is the LOB — index
   names carry LOB so per-LOB access control = index privileges), composes
   the two component templates, and maps only its own `payload.*` fields
   (e.g. llm-calls maps payload.model_name, payload.total_tokens,
   payload.estimated_cost_usd...).
4. `apply.sh`: PUT ILM → component → index templates via curl; **fail loudly
   on any non-2xx**; print the resulting template list.
5. Document for Phase 4: the storage consumer writes with `_id = event_id`,
   which makes replays idempotent (same event twice = one document).

**Done when:** apply runs green against A-1's ES; indexing one sample event
into `ai-obs-testlob-requests-2026.07.15` produces correctly-typed mappings
(check `GET .../_mapping`); re-apply is a no-op; A-14's coverage test (every
envelope field mapped) passes.

## A-12 · S3 archive — *0.5d*

**Goal:** one encrypted, tiered, locked-down bucket with the standard prefix
layout.

**Why:** full prompts/responses/traces are too big for Kafka and too cold for
ES. S3 with automatic tiering (Standard → IA at 30d → Glacier at 180d) makes
"keep everything" affordable.

**What to do:** `s3/apply.sh` + `lifecycle.json`: create bucket (tolerate
"already exists"), block all public access, enable versioning, default
encryption SSE-KMS (SSE-S3 fallback for dev with a loud warning), apply
lifecycle (special cases: `debug-bundles/` expire at 90d; `audit-evidence/`
never expires), create the nine prefixes: redacted-prompts, redacted-responses,
raw-traces, rag-contexts, uploaded-documents, audit-evidence, debug-bundles,
rca-reports, iac-dashboards.

**Done when:** `aws s3api get-bucket-encryption` / `get-bucket-lifecycle-configuration`
match the spec; rerun changes nothing.

## A-13 · Kubernetes monitoring + DLQ replay — *2d*

**Goal:** the pipeline watches itself — lag, dead-letters, dead pods — and
there's a safe tool to replay quarantined events.

**Why:** an observability pipeline that fails silently is worse than none:
people trust dashboards that have quietly stopped updating. The two golden
signals are **consumer lag** (pipeline falling behind) and **DLQ rate**
(pipeline rejecting events).

**What to do (four config files + one script in `infra/` and `scripts/`):**
1. `kube-prometheus-stack-values.yaml` — Grafana enabled as an
   **internal-only ops console**: ClusterIP service, `ingress.enabled=false`,
   no anonymous access, no sign-up, admin credentials from a pre-created
   secret (`grafana-admin-credentials`), Tempo added as a datasource next to
   the auto-provisioned Prometheus. Header comment must state the scoping:
   platform team only, via `kubectl port-forward`; stakeholders use the
   Custom Dashboard (COIN-JWT + per-LOB RBAC that Grafana can't model); do
   not add an ingress or SSO without an architecture review. Alert rules:
   lag > 1000 sustained 10 min → page; DLQ > 1% of raw volume → warn +
   runbook link; enrichment consumer has no live pods → page; any service
   5xx > 5% → warn.
2. `tempo-values.yaml` — OTLP gRPC on 4317 (the SDK's target), S3 backend,
   720h retention, metrics-generator → Prometheus.
3. `kminion.yaml` — Deployment + ServiceMonitor exporting lag for groups
   obs-enrichment / obs-storage / obs-eval / obs-dlq-replay on the 3 topics.
4. `fluent-bit-configmap.yaml` — tail pod logs, parse the structlog JSON so
   `correlation_id` is a searchable field, **strip secrets**
   (authorization/api_key/password — `user_id` stays, raw by design), ship
   to a logs index in ES.
5. `scripts/replay_dead_letter.py` — consumer group `obs-dlq-replay`;
   unwraps `{reason, failed_at, original}`; re-produces `original` to the raw
   topic; **commits the DLQ offset only after the re-produce is flushed** (a
   crash never loses a quarantined event); `--dry-run`, `--reason-filter`,
   `--limit` flags.

**Done when:** helm-template renders both charts; a hand-crafted DLQ message
on the dev stack is listed by `--dry-run` and re-driven without it.

## A-14 · Policy gate + IaC CI — *1d*

**Goal:** a test file that makes Phase 0 ↔ Phase 1 compatibility a
red/green CI property instead of a convention.

**Why:** every cross-repo agreement made so far (field names, topic names,
seed values, prices) will silently rot unless a machine checks it on every PR.

**What to do:**
1. `tests/test_policy.py` asserting:
   - the three contract files are **byte-identical** to the SDK's vendored
     copies;
   - event-type count = 50, service count = 8;
   - topic names in `topics.yaml` = the SDK config defaults, retentions =
     7d/3d/14d;
   - the ES common mapping covers **every** `ObsEvent` field (introspect the
     Pydantic model — new envelope fields auto-break this test until mapped);
   - all 11 index families exist, compose both component templates, and any
     ILM override names a real policy;
   - the service seed contains exactly the enum values;
   - the pricing seed matches the SDK `cost.py` table;
   - migration numbers are unique in both migration folders.
2. `ci/deploy.yml` — *validate* job on every PR: contract tests → policy
   gate → `bash -n` every script → **apply all SQL to a throwaway postgres:16
   container** (catches semantic SQL errors, not just syntax). *apply-dev*
   job on merge: topics → control plane (+seed) → firehose → ES → S3.
   Staging/prod: same steps behind environment approvals.

**Done when:** deliberately renaming one contract field on either side turns
CI red on the next PR.

---

# Weeks 2–5 · Dev B track — the SDK

## B-2 · Configuration + request context — *1.5d*

**Goal:** all SDK knobs come from `AI_OBS_*` environment variables, and a
per-request context object travels invisibly with each request.

**Why config-from-env:** service teams should configure the SDK from Helm
values without writing code. **Why a contextvar:** the alternative is passing
a context argument through every function signature in eight codebases —
contextvars give the same result invisibly, and work correctly under asyncio.

**What to do:**
1. `config.py` — `ObsSettings(BaseSettings)`, prefix `AI_OBS_`:
   - required (no defaults): `service_name`, `lob`, `application_id`;
   - `environment` (default dev), `enabled` master switch (false → every SDK
     call becomes a no-op — the panic button);
   - Kafka: bootstrap servers, topic (default **`ai-obs-events-raw`** — A-14
     pins this), optional SASL, `linger_ms=50`, lz4, queue cap, delivery
     timeout;
   - tracing: OTLP endpoint (Tempo :4317), sample ratio;
   - logging: level, `log_json` (false = pretty console for laptops);
   - metrics on/off; prompt registry URL + cache TTL (default 300 s — the
     Redis stand-in knob).
   - Wrap in `@lru_cache` — read env once per process.
2. `context.py` — `@dataclass ObsContext`: correlation_id (default new
   uuid4), request_id, trace_id, span_id (16 hex chars — the W3C width),
   parent_span_id, usecase_id, agent_id, tenant_id, **`user_id` (the raw SOE
   ID — comment the platform decision)**. Methods/functions:
   - `.child()` — copy with `parent_span_id = old span_id` and a fresh
     span_id (how decorators nest);
   - module-level contextvar + `bind_context` / `reset_context` /
     `get_context` — where `get_context()` on an unbound context (background
     job, scheduler) creates a fresh detached one rather than failing.

**Done when:** unit tests — missing required env fails clearly; `.child()`
chains span ids; unbound `get_context()` returns a context with a
correlation_id.

## B-3 · Text hashes + Kafka trace headers — *0.5d*

**Goal:** small utilities: content hashes for large texts, and W3C trace
context in/out of Kafka message headers.

**Why hashes:** you want to group "the same prompt" or "the same query"
across thousands of events without shipping the full text in every event —
a 16-hex sha256 prefix does that. **Note: user identity is NOT hashed** —
`user_id` travels raw by platform decision; these hashes are grouping keys
for texts, not privacy controls. **Why trace headers:** when a service emits
to Kafka and a consumer picks it up, the trace must continue rather than
starting a new one — that's what the `traceparent` header carries.

**What to do:**
1. `hashing.py` — `prompt_hash(text)` and `query_hash(text)`: sha256, first
   16 hex chars.
2. `kafka_headers.py` — `inject_trace_headers(correlation_id)`: uses the
   OTEL W3C propagator to fill `traceparent` (+`tracestate`) from the active
   span, appends a `correlation_id` header, returns Kafka-shaped
   `list[(str, bytes)]`. `extract_trace_context(headers)`: the reverse, for
   consumers. `current_trace_ids()`: the active span's trace/span ids as hex
   (the emitter stamps them onto events so events ↔ Tempo spans join).

**Done when:** inject → extract round-trips a span context; same text → same
hash.

## B-4 · The fire-and-forget emitter — *2d* ← the heart of the SDK

**Goal:** `emit_event(...)` — one call that builds a valid ObsEvent from
settings + context and hands it to Kafka without ever blocking or raising.

**Why the two never-rules:** this code runs inside the request path of eight
production services. If observability can add latency or throw, teams will
rip it out after the first incident — correctly. So: **never block** (async
producer, no waiting for acks in-line) and **never raise** (catch everything,
log, count, move on). Observability data is allowed to be lossy; the product
is not allowed to be slow.

**What to do:**
1. `KafkaEmitter` class wrapping `confluent_kafka.Producer`:
   - config from settings: idempotent producer, lz4, linger 50 ms, bounded
     local queue, delivery timeout, optional SASL, client id
     `ai-obs-sdk.<service>`;
   - a delivery callback (runs on the poll thread) counting
     `delivered`/`dropped` and logging failures;
   - `emit(event)`: `produce(topic, key=correlation_id or event_id,
     value=event.model_dump_json(), headers=inject_trace_headers(...))` then
     `poll(0)`. Catch `BufferError` (local queue full → drop + warn — do
     **not** wait for space) and any other exception (log, never propagate);
   - `flush()` registered via `atexit` so a clean shutdown drains the queue;
   - module-level singleton with double-checked locking.
2. `emit_event(event_type, *, status="success", latency_ms=None,
   error_code=None, http_status=None, payload=None, component=None)`:
   - return immediately if `settings.enabled` is false;
   - build the `ObsEvent` from settings (service_name, environment,
     application_id, lob) + current context (correlation/request/span ids,
     tenant_id, `user_id`) + live OTEL trace ids (B-3);
   - constructing the ObsEvent **validates against the vendored contract** —
     a typo'd event_type fails here, in unit tests, not in production;
   - the whole body wrapped in try/except that logs and swallows.

**Done when:** tests (using a fake emitter that captures events — no broker):
envelope fully populated from settings + context; a bogus event_type is
swallowed silently (nothing emitted, nothing raised); an unbound context gets
an auto correlation_id; the delivery-failure path increments `dropped`.

## B-5 · Tracing + logging bootstrap — *1.5d*

**Goal:** two idempotent setup functions: `init_tracing(app)` (OTEL spans →
Tempo) and `configure_logging()` (structlog JSON where every line carries the
correlation_id).

**Why:** spans give the *timing tree* (where did 3 seconds go?); events give
the *business facts* (which prompt, how many tokens). Logs are where humans
read details. All three join on correlation_id — but only if the SDK stamps
it everywhere automatically.

**What to do:**
1. `tracing.py` — `init_tracing(app=None)`: build a `TracerProvider` with
   resource attrs (service.name, namespace, environment, lob), parent-based
   ratio sampler, `BatchSpanProcessor` → OTLP gRPC exporter at
   `settings.otlp_endpoint`. If `app` given → FastAPI instrumentation
   (excluding /metrics,/health,/ready). Try/except-wrap httpx and asyncpg
   auto-instrumentation (not every service uses both). Guard with an
   `_initialized` flag; no-op when disabled.
2. `log_config.py` — `configure_logging()`: structlog pipeline — merge
   contextvars, **a custom processor that reads the current ObsContext and
   injects correlation_id/span_id/request_id into every event dict**, level
   + logger name, ISO UTC timestamps, exception formatting, then JSON
   renderer (or pretty console when `log_json=false`). Route **stdlib**
   logging through the same formatter so uvicorn/library logs come out in
   the same JSON shape (Fluent Bit parses one format, not five).

**Done when:** with a bound context, `structlog.get_logger().info("x")`
emits JSON containing the correlation_id; calling either init twice is safe.

## B-6 · Middleware + the one-liner — *1.5d*

**Goal:** `init_observability(app)` — the single line a service team adds —
and the ASGI middleware behind it.

**Why:** adoption. Eight teams will integrate this; if integration is one
line, it happens in a sprint. The middleware is also where the
correlation_id is born (or adopted from the caller) — everything downstream
depends on that happening reliably.

**What to do:**
1. `ObservabilityMiddleware` (Starlette `BaseHTTPMiddleware`):
   - skip /metrics, /health, /ready, /livez (nobody wants 10k health-check
     events/day);
   - build an ObsContext from inbound headers: `X-Correlation-ID` (or
     generate a uuid), `X-Request-ID`, `X-Usecase-ID`, `X-Tenant-ID`, and
     **`X-User-ID` / `X-SOE-ID` copied verbatim into `user_id`** (raw by
     platform decision);
   - bind the contextvar (keep the token);
   - emit `REQUEST_RECEIVED` (method + path in payload);
   - call the app; on exception → emit `REQUEST_FAILED` (error_code = the
     exception class name), reset context, re-raise;
   - on response → `REQUEST_COMPLETED` (or FAILED when status ≥ 500) with
     latency + http_status; **echo `X-Correlation-ID` on the response** so a
     user's bug report can quote the exact trace id; reset the context token
     (always — leaking context across asyncio requests is a subtle,
     nasty bug).
2. `init_observability(app)` = `configure_logging()` + `init_tracing(app)` +
   `add_middleware(...)` + prometheus-fastapi-instrumentator exposing
   `/metrics` (auto latency histograms for every route).

**Done when:** FastAPI TestClient tests — a request emits the
RECEIVED/COMPLETED pair sharing one correlation_id; missing header → id
generated and echoed; a handler exception → REQUEST_FAILED with the exception
class; `user_id` equals the inbound header verbatim; /health emits nothing.

## B-7 · Decorators, cost, prompt client — *2.5d*

**Goal:** the four instrumentation decorators (`@trace_llm`, `@trace_rag`,
`@trace_tool`, `@trace_agent`), the producer-side cost estimate, and
`get_prompt()` with its TTL cache.

**Why decorators:** the alternative — teams hand-writing STARTED/COMPLETED
pairs with correct timing, nesting, and failure handling at every call site —
produces eight inconsistent implementations. The decorator does it once,
correctly.

**What to do:**
1. A shared **decorator factory** (all four are configurations of it):
   - takes: started/completed/failed event types, an optional timeout event,
     the OTEL span name, and which static kwargs to lift into the payload;
   - the wrapper (support **both sync and async** functions — check with
     `inspect.iscoroutinefunction`): bind `get_context().child()` (nesting) →
     emit `*_STARTED` → run inside an OTEL span → on success compute latency
     and emit `*_COMPLETED`; on exception emit `*_FAILED` — or the timeout
     event if the exception is a TimeoutError — with error_code + truncated
     message, then **re-raise** (the SDK never eats business exceptions) →
     always reset the context token;
   - payload merging, three sources: static decorator kwargs ∪ a
     `result.obs_payload` dict (the function can attach runtime facts like
     token counts to its return value) ∪ an `obs_extra=` kwarg from the
     caller.
2. Per-decorator specifics:
   - `trace_llm(model_provider, model_name, ...)` — a finalizer computes
     `total_tokens` and `estimated_cost_usd` when token counts are present,
     and **replaces any `prompt_text` in the payload with its
     `prompt_hash`** — full prompts never ride in events (they're archived
     to S3 by enrichment);
   - `trace_tool(tool_id, tool_type, ...)` — TimeoutError →
     `TOOL_CALL_TIMEOUT`; tool_type uses the registry vocabulary (A-4);
   - `trace_rag(vector_db_index, top_k, ...)`;
   - `trace_agent(agent_id, agent_type, ...)` — timeout → `AGENT_TIMEOUT`.
3. `cost.py` — `PRICING` dict (per-1k input/output USD per model) + a DEFAULT
   for unknown models + `estimate_cost_usd()`. Header comment: this is the
   *estimate*; enrichment recomputes from `model_pricing` (authoritative);
   **the rows must match the A-9 seed — A-14 tests it**.
4. `prompts.py` — `get_prompt(template_id, version="active")`: httpx GET to
   `settings.prompt_registry_url` (3 s timeout), deserialise the A-5 response
   shape into a frozen `Prompt` dataclass (`.format(**vars)` helper), wrapped
   in a **thread-safe in-process TTL cache** (default 300 s — the Redis
   stand-in; swapping to a shared cache later touches only this module).
   No URL configured → clear RuntimeError telling teams to keep a baked-in
   fallback.

**Done when:** tests — sync LLM call emits STARTED then COMPLETED with
total_tokens + cost > 0 and correct span parentage; async RAG failure emits
RAG_RETRIEVAL_FAILED and re-raises; TimeoutError in a tool emits
TOOL_CALL_TIMEOUT; `obs_extra` lands in the terminal payload; `get_prompt`
hits the network once, then serves from cache until the TTL expires.

## B-8 · Test suite, CI, docs, publish — *2d*

**Goal:** a releasable package: green CI with a drift check, a README a
service team can integrate from without asking questions, version 0.1.0 on
the internal index.

**What to do:**
1. `tests/conftest.py` — set the required `AI_OBS_*` env for determinism, and
   a **FakeEmitter fixture** (captures ObsEvents in a list, monkeypatched in
   place of the singleton) — the whole suite runs with no broker.
2. `ci/sdk-ci.yml` — ruff → contract tests (the merge gate) → full pytest →
   **drift check**: byte-diff `ai_obs_sdk/contracts/*` against
   `observability-iac/contracts/*`, fail on any difference.
3. `README.md` — quick start (the one-liner), copy-paste examples for all
   four decorators **plus the no-middleware pattern for Kafka consumers /
   schedulers** (Consumer Service has no HTTP requests — it binds context
   from message headers instead), the hard rules (never blocks/raises;
   `user_id` raw by design; prompts hashed; key = correlation_id), and the
   local smoke recipe against the A-1 stack. `.env.example` documenting
   every variable with comments.
4. Build the wheel and publish 0.1.0 to the internal index; services will pin
   the minor version.

**Done when:** CI green including drift check; a colleague follows only the
README in a fresh venv and sees their events on the dev stack's raw topic.

---

# Weeks 5–6 — Joint integration

## J-2 · End-to-end smoke — *both · 1d*

**Goal:** one user request visibly traced across all three signal types.

**What to do:** Dev A brings the dev stack fully applied; Dev B a ~20-line
sample FastAPI app (`init_observability` + one `@trace_llm`-decorated fake
LLM call). Send one request with `X-Correlation-ID: demo-123` and
`X-User-ID: SOE99999`, then verify:
1. a console consumer on `ai-obs-events-raw` shows REQUEST_RECEIVED →
   LLM_CALL_STARTED → LLM_CALL_COMPLETED → REQUEST_COMPLETED, all with
   `correlation_id=demo-123` and `user_id=SOE99999`, keyed to one partition,
   headers carrying `traceparent`;
2. Tempo shows the request span with the LLM child span;
3. the app's JSON logs carry `demo-123`;
4. `/metrics` shows the request in the latency histogram.

**Done when:** all four checks pass; the recipe is written into the repo
README as the canonical "is it working?" procedure.

## J-3 · Data-hygiene + failure-mode audit — *both · 1d*

**Goal:** adversarial proof of the two promises: the right data is captured,
and observability can never hurt the product.

**What to do (each dev attacks the other's side):**
1. *Data hygiene:* send requests with identity in every plausible slot;
   verify `user_id` captures the header value **raw** (by design), while
   credentials (Authorization headers, api keys), raw request/response
   bodies, and full prompt text appear in **zero** of ~500 captured events
   (grep the captured JSON). Turn that grep into a permanent test.
2. *Broker down:* `docker stop kafka` mid-traffic → the sample app's latency
   and error rate must not change; the SDK logs drops and keeps serving.
3. *Queue overflow:* set a tiny producer queue → floods must drop-with-warning,
   never block a request.

**Done when:** all findings fixed; the hygiene grep runs in CI.

## J-4 · Exit review + sign-off — *both · 0.5d*

**Goal:** an honest checklist walk before declaring Phases 0–1 done.

**What to do:** walk the roadmap exit criteria — contract frozen and
CI-gated; 3 topics live with correct retention; both Postgres schemas +
seeds + grants; 2 ILM + 2 component + 11 index templates; S3 compliant;
infra values merged; alerts firing in a test; SDK published; the J-2 smoke
reproducible by a third person. File the known follow-ups as Phase 2
tickets: pricing table vs real billing, store-level RBAC for raw `user_id`,
staging Kafka credentials, nightly broker-integration CI job.

**Done when:** a short sign-off note is committed; Phase 2 (instrumenting
the 8 services) is unblocked.

---

## Dependency map (critical path in bold)

```
J-0 ─► **J-1 contract freeze**
          │
          ├─► Dev A: A-2 kafka ─┐
          │   A-3 ► A-4 ► A-5 ► A-6 ► A-7 ► A-8 ► A-9   (control plane chain)
          │   A-10 firehose     ├─► **A-14 policy gate**
          │   A-11 ES ► A-12 S3 ┘
          │   A-13 infra+replay
          │
          └─► Dev B: B-1 ─► **B-2 ► B-3 ► B-4 emitter** ─► B-5 ─► **B-6 ► B-7 ► B-8**

A-1 dev stack (week 1, independent) ──────────► **J-2 smoke ► J-3 audit ► J-4 sign-off**
```

Two properties worth noticing:

- **Dev B never waits on Dev A after the freeze.** The FakeEmitter makes the
  whole SDK test suite broker-free; the dev stack is only needed for B-8's
  final smoke and J-2.
- **A-14 must land before J-2**, so the integration week runs against a
  CI-locked contract, not a handshake.
