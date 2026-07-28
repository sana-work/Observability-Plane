# Task Board — Build `ai-observability-sdk` + `obs-enrichment-consumer` from Scratch (Team of 3)

**Scope:** the two Python codebases only. Assumes Phase 0 infrastructure
(Kafka topics, Postgres schemas `observability.*` + `obs_events.*`, seeds, ES
templates) is applied — the dev docker-compose stack in
`observability-iac/docker-compose.dev.yml` provides all of it locally.

**Fixed decisions (no task may violate these):**
`user_id` = raw SOE ID, never hashed · no Redis (in-process TTL caches +
Postgres `add_spend()`) · no Snowflake · SDK never blocks/raises on the
request path · contract = frozen `ObsEvent` + 50 event types + 8 service names.

**How to use this board:** tasks are pickable. Respect the *Depends on*
column; anything with no unmet dependency is free to grab. Every task has a
"Done when" — that's the review checklist. Reference implementations exist in
this repo; use them as the target state or build clean-room from the spec.

---

## Quick-pick task list (claim by writing your name; details in the tables below)

**Foundation — do first, in order (~2.5d total)**
- [ ] `____` **F1** · Contract freeze — ObsEvent envelope + 50 event types + 8 service names + tests *(1d · blocks EVERYTHING)*
- [ ] `____` **F2** · Repo + CI skeletons for both packages *(0.5d)*
- [ ] `____` **F3** · Vendor contract into both packages + drift-check CI *(0.5d · needs F1, F2)*
- [ ] `____` **F4** · Dev docker stack verified + README'd *(0.5d)*

**ai-observability-sdk (~11d — S-tasks are one chain per row, parallel across rows)**
- [ ] `____` **S1** · `config.py` (AI_OBS_* settings) + `context.py` (ObsContext contextvar) *(1.5d · needs F3)*
- [ ] `____` **S2** · `hashing.py` + `kafka_headers.py` (W3C traceparent) *(0.5d · needs S1)*
- [ ] `____` **S3** · `emitter.py` — fire-and-forget KafkaEmitter + `emit_event()` ★core *(2d · needs S2)*
- [ ] `____` **S4** · `tracing.py` (OTEL→Tempo) + `log_config.py` (structlog JSON) *(1.5d · needs S1)*
- [ ] `____` **S5** · `middleware.py` + `init_observability()` one-liner *(1.5d · needs S3, S4)*
- [ ] `____` **S6** · `decorators.py` — @trace_llm/_tool/_rag/_agent *(1.5d · needs S3)*
- [ ] `____` **S7** · `cost.py` pricing + `prompts.py` get_prompt + TTL cache *(1d · needs S1)*
- [ ] `____` **S8** · Test suite + FakeEmitter + README/.env.example + publish 0.1.0 *(1.5d · needs S5, S6, S7)*

**obs-enrichment-consumer (~9.5d)**
- [ ] `____` **E1** · `config.py` + `errors.py` + `metrics.py` (prometheus) *(1d · needs F3)*
- [ ] `____` **E2** · `control_plane.py` — Postgres pool + TTL-cached registry/pricing/SLO reads, add_spend writes *(1.5d · needs E1)*
- [ ] `____` **E3** · `redactor.py` — regex always, GLiNER optional w/ fallback *(1d · needs E1)*
- [ ] `____` **E4** · `s3_archiver.py` + `slo.py` burn-rate tracker *(1.5d · needs E2)*
- [ ] `____` **E5** · `pipeline.py` — the 9 stages ★core *(2d · needs E2, E3, E4)*
- [ ] `____` **E6** · `consumer.py` — consume→produce→commit-after-flush loop + DLQ *(1.5d · needs E5)*
- [ ] `____` **E7** · Dockerfile + k8s + CI + README walkthrough + .env.example *(1d · needs E6)*

**Joint verification — final week (~2.5d, all together)**
- [ ] `____` **V1** · End-to-end smoke: one correlation_id across events/trace/logs *(1d · needs S8, E7)*
- [ ] `____` **V2** · Failure drills: broker down, queue full, PG down, DLQ replay *(1d · needs V1)*
- [ ] `____` **V3** · Sign-off + file follow-up tickets *(0.5d · needs V2)*

★ = the two "heart" tasks — assign your strongest reviewers there.

---

## Table 0 — Foundation (do these first, together — everything depends on F1)

| ID | Task | What to build / how | Depends on | Est | Done when |
|----|------|----------------------|------------|-----|-----------|
| **F1** | **Contract freeze** ⛔ | The 3 files both codebases share, in `observability-iac/contracts/`: **(a)** `event_schema.py` — Pydantic `ObsEvent`: identity (`event_id` uuid auto, `schema_version="1.0"`, `event_type`, `telemetry_type` event/log/metric), UTC ISO `timestamp`+`emitted_at`, correlation (`correlation_id`, `request_id`, `trace_id`, `span_id`, `parent_span_id`), ownership (`service_name`, `component`, `environment`, `application_id`, `lob`, `tenant_id`, **`user_id` raw**), outcome (`status`, `latency_ms`, `error_code`, `http_status`), `payload: dict`. Validators reject unknown event_type/service_name/telemetry_type. **(b)** `event_types.py` — 50-value str-Enum in 10 groups + `assert len==50`. **(c)** `service_names.py` — 8 services + `assert len==8`. Plus round-trip/rejection tests. All 3 devs review; versioning rule in docstring: envelope change ⇒ bump `schema_version` + re-vendor into both packages in the same PR | — | 1d | PR approved by all 3; tests green; counts asserted |
| **F2** | Repo + CI skeletons | Two package folders with `pyproject.toml` (Python ≥3.11, hatchling), ruff+pytest config, one CI job per folder on PR paths, branch protection. SDK deps: pydantic v2, pydantic-settings, confluent-kafka, structlog, OTEL (api/sdk/otlp-grpc/fastapi/httpx/asyncpg instr.), prometheus-fastapi-instrumentator, httpx. Consumer deps: pydantic(+settings), confluent-kafka, psycopg[binary,pool], boto3, prometheus-client; `gliner` as optional extra; console script `obs-enrichment` | — | 0.5d | trivial PR from each dev runs CI green in both folders |
| **F3** | Vendor the contract into both packages | Copy the 3 files byte-for-byte to `ai_obs_sdk/contracts/` and `obs_enrichment/contracts/` with re-exporting `__init__.py` ("vendored — do not edit" header). Add a **drift-check CI step in both packages**: `diff` each vendored file against `observability-iac/contracts/` — fail on any difference | F1, F2 | 0.5d | changing one contract byte on any side turns CI red |
| **F4** | Local dev stack verified | Bring up `observability-iac/docker-compose.dev.yml` (Kafka, Postgres, ES, Kibana, Tempo); apply topics + both PG schemas `--with-seed` + ES templates; write the exact commands into both READMEs | — | 0.5d | fresh clone → stack up + applied in <15 min following the README only |

---

## Table 1 — `ai-observability-sdk` (the library the 8 services install)

| ID | Task | What to build / how | Depends on | Est | Done when |
|----|------|----------------------|------------|-----|-----------|
| **S1** | Config + request context | `config.py`: `ObsSettings(BaseSettings)` prefix `AI_OBS_`; required no-default: `service_name`, `lob`, `application_id`; `enabled` master no-op switch; Kafka block (bootstrap, topic default `ai-obs-events-raw`, optional SASL, linger 50ms, lz4, queue cap 100k, delivery timeout 10s); tracing (OTLP endpoint, sample ratio); logging (level, json on/off); metrics switch; prompt registry URL + TTL 300s; `@lru_cache` accessor. `context.py`: `@dataclass ObsContext` (correlation_id default uuid4, request_id, trace_id, span_id 16-hex, parent_span_id, usecase/agent/tenant, **user_id raw**); module contextvar + `bind_context`/`reset_context`/`get_context` (unbound ⇒ create detached ctx, never fail); `.child()` = parent_span_id←span_id + fresh span_id | F3 | 1.5d | missing required env fails at startup; `.child()` chains span ids; unbound `get_context()` returns ctx with correlation_id |
| **S2** | Hashing + Kafka trace headers | `hashing.py`: `prompt_hash`/`query_hash` = sha256[:16] (grouping keys for big texts — **no user hashing anywhere**). `kafka_headers.py`: W3C propagator — `inject_trace_headers(corr_id)` → `[(str, bytes)]` with traceparent(+tracestate)+correlation_id from the active OTEL span; `extract_trace_context(headers)` inverse; `current_trace_ids()` → active (trace_id, span_id) hex | S1 | 0.5d | inject→extract round-trips a span context; deterministic hashes |
| **S3** | **Fire-and-forget emitter** (core) | `emitter.py`: `KafkaEmitter` over confluent-kafka Producer (idempotent, lz4, linger 50, bounded queue, SASL from settings, `client.id=ai-obs-sdk.<svc>`, delivery callback counting delivered/dropped + warn-log, `atexit` flush, double-checked-lock singleton). `emit_event(event_type, *, status="success", latency_ms, error_code, http_status, payload, component)`: no-op if disabled; build `ObsEvent` from settings+context+`current_trace_ids()` (construction **validates**); `produce(topic, key=correlation_id, value=model_dump_json(), headers=inject_trace_headers(...))` + `poll(0)`. Hard rules in docstring + code: **never raises** (whole body try/except→log), **never blocks** (`BufferError` on full queue ⇒ drop+warn, never wait) | S2 | 2d | tests: envelope fully populated; bogus event_type swallowed silently; unbound ctx auto-correlates; delivery failure increments `dropped` |
| **S4** | Tracing + logging bootstrap | `tracing.py`: `init_tracing(app)` — TracerProvider (resource: service.name/namespace/env/lob), ParentBasedTraceIdRatio sampler, BatchSpanProcessor→OTLP gRPC (Tempo :4317), FastAPI instr. (exclude /metrics,/health,/ready), httpx+asyncpg instr. in try/except, `_initialized` guard. `log_config.py`: `configure_logging()` — structlog JSON pipeline with a custom processor stamping correlation_id/span_id/request_id from current ObsContext onto **every** line; stdlib logging routed through the same formatter (uvicorn etc. come out as the same JSON); pretty console when `AI_OBS_LOG_JSON=false` | S1 | 1.5d | log line inside a bound ctx carries correlation_id; double-init safe; disabled ⇒ no exporter built |
| **S5** | Middleware + `init_observability()` | `middleware.py`: ASGI middleware — skip /metrics,/health,/ready,/livez; ObsContext from headers (X-Correlation-ID or mint uuid; X-Request-ID; X-Usecase-ID; X-Tenant-ID; **X-User-ID/X-SOE-ID verbatim → user_id**); bind ctx; emit REQUEST_RECEIVED{method,path}; run handler — exception ⇒ REQUEST_FAILED(error_code=exc class)+re-raise; else REQUEST_COMPLETED (FAILED if ≥500) with latency+http_status; **echo X-Correlation-ID on response**; always reset token. `init_observability(app)` = configure_logging + init_tracing + middleware + prometheus instrumentator `/metrics` | S3, S4 | 1.5d | TestClient: event pair shares corr id; missing header ⇒ minted+echoed; exception ⇒ REQUEST_FAILED; `user_id` verbatim; /health emits nothing |
| **S6** | The 4 decorators | `decorators.py`: one factory → `@trace_llm/_tool/_rag/_agent`; sync **and** async (inspect.iscoroutinefunction); per call: bind `ctx.child()` → emit `*_STARTED`(static kwargs) → OTEL span → success ⇒ `*_COMPLETED`(latency + merged payload: static ∪ `result.obs_payload` ∪ `obs_extra=` kwarg); exception ⇒ `*_FAILED` — TimeoutError ⇒ `TOOL_CALL_TIMEOUT`/`AGENT_TIMEOUT` — error_code=class, msg[:500], **re-raise**; always reset. LLM finalizer: total_tokens + `estimated_cost_usd` from cost.py; any `prompt_text` in payload ⇒ replaced by `prompt_hash` | S3 | 1.5d | STARTED/COMPLETED pair w/ cost + span parentage; async RAG failure ⇒ FAILED + re-raise; timeout mapping; obs_extra merged |
| **S7** | Cost table + prompt client | `cost.py`: `PRICING` per-1k dict + DEFAULT + `estimate_cost_usd()`; **rows must equal `observability-iac/postgres/seed/003_metric_catalog.sql` model_pricing** (IaC policy test enforces). `prompts.py`: `get_prompt(template_id, version="active")` — httpx GET `{registry_url}/{id}` 3s timeout → frozen `Prompt(template_id, version, text, prompt_hash, ab_bucket)` with `.format(**vars)`; thread-safe in-process TTL cache (settings TTL; **the Redis stand-in** — swap later touches only this module); no URL configured ⇒ clear RuntimeError | S1 | 1d | prompt fetched once then cached until TTL (monkeypatched clock); pricing sync test passes |
| **S8** | Test suite + FakeEmitter + docs + publish | `tests/conftest.py`: deterministic `AI_OBS_*` env + `FakeEmitter` fixture (captures ObsEvents, monkeypatched over singleton — whole suite broker-free). Consolidate ~17 tests (contract gate, emitter never-raises, decorators, middleware). README: quick start, 4 decorator examples + **no-middleware Kafka-consumer pattern**, hard rules, local smoke. `.env.example` all vars. Publish wheel 0.1.0 to internal index | S5, S6, S7 | 1.5d | CI green incl. drift check; colleague reproduces README quick start on the dev stack unaided |

---

## Table 2 — `obs-enrichment-consumer` (raw → processed + dead-letter)

| ID | Task | What to build / how | Depends on | Est | Done when |
|----|------|----------------------|------------|-----|-----------|
| **E1** | Config + errors + metrics | `config.py`: `EnrichSettings` prefix `OBS_ENRICH_` — topics (raw/processed/dead-letter), group `obs-enrichment`, batch 200, poll 1s, flush 10s, SASL block; `pg_dsn` + pool sizes + cache TTL 300s; gliner on/off+model+labels; s3 on/off+bucket+threshold 2048+field list; slo on/off; `quality_sample_pct` 5.0; metrics port 9108. `errors.py`: `DeadLetterError(reason)` (event is *wrong*) vs `TransientError` (infra hiccup — retry, never DLQ). `metrics.py`: prometheus counters `events_processed_total{event_type}`, `events_dead_lettered_total{stage}`, `stage_errors_total{stage}`, `budget_threshold_exceeded_total{kind}`, batch histogram, `obs_enrich_up` gauge, `start_http_server` | F3 | 1d | settings load from env; metrics server serves /metrics |
| **E2** | Control-plane access + TTL cache | `control_plane.py`: psycopg3 `ConnectionPool` (lazy import so tests never need psycopg) behind one `ControlPlane` class — **the only DB surface**, so tests fake it. Cached reads (in-process TTL): `application(id)` → dict(lob, owner_team, criticality, usecase_id); `error_rules()` → compiled-regex list ordered by priority (skip rows with bad regex — a bad seed must not kill the pipeline); `model_price(model)` → latest `effective_from` row; `slo_definitions(app)`. Uncached writes: `add_spend(app, model, cost)` → SELECT from `observability.add_spend()` returning (period, spend, limit, alert_crossed, cap_crossed); `upsert_slo_compliance(...)` → INSERT..ON CONFLICT accumulating good/total, keeping MAX burn rates, OR-ing breached | E1 | 1.5d | fake-backed unit tests for cache TTL + rule ordering; real queries run against the seeded dev Postgres |
| **E3** | PII redactor (pluggable) | `redactor.py`: `RegexRedactor` (always available) — email/card/SSN/phone patterns → `[REDACTED:<label>]`. `GlinerRedactor` — loads GLiNER once at init (~512Mi), `predict_entities` per string, replace right-to-left to keep offsets; **degrades to regex-only** if package/model missing or inference throws (log, never crash-loop). `build_redactor(settings)` picks by flag. **Never touches envelope `user_id`** — payload strings only | E1 | 1d | emails/cards redacted; user_id untouched; GLiNER-missing path falls back cleanly |
| **E4** | S3 archiver + SLO tracker | `s3_archiver.py`: boto3 client (lazy import); `put_text(key, text)` → `s3://bucket/key`; `archive_key(field, event_id)` maps field→prefix (prompt→redacted-prompts, response→redacted-responses, rag_context→rag-contexts, trace_json→raw-traces). `slo.py`: `SloTracker(control_plane, clock)` — per-slo sliding windows of per-minute (good,total) buckets for 1h and 6h; `observe(app, status, latency_ms)`: availability SLI = status==success, latency SLI = latency≤target; burn rate = bad_fraction / (1 − target_pct/100); upsert daily row per observation. Docstring: burn>14.4 on 1h ≈ exhausts 30-day budget in ~2 days (breach flag) | E2 | 1.5d | burn math unit-tested (98 good+2 bad @99% target ⇒ 2.0×; events age out of 1h but stay in 6h; latency SLI counts slow successes as bad) |
| **E5** | **The 9-stage pipeline** (core) | `pipeline.py`: `process(raw_bytes, headers, deps) -> ObsEvent`. Stage 1 `validate`: json.loads + `ObsEvent.model_validate` — failure ⇒ `DeadLetterError("stage1-validate: …")`. Stage 2 trace: parse `traceparent` header (`00-<32>-<16>-flags`) → fill missing trace_id/parent_span_id. Stages 3–9 run through a **best-effort loop**: any unexpected exception ⇒ `stage_errors_total{stage}` + log + continue (degrade, don't die). 3 pii: redact payload strings ≤ max chars. 4 metadata: registry join — fill empty `lob`, add app_owner_team/criticality/usecase_id, `app_registered=false` if unknown. 5 errmap: on failed/error_code — match `"{error_code}: {payload.error_message}"` against rules in priority order; keep original as `raw_error_code`, set normalized `error_code` + category + retryable. 6 cost: LLM_* with tokens ⇒ recompute from `model_price` (overwrite SDK estimate, `cost_source=control_plane`); cost>0 ⇒ `add_spend`; alert_crossed ⇒ metrics+`payload.budget_alert`+warn-log; cap ⇒ `budget_cap_breached`+error-log. 7 s3: configured fields > threshold ⇒ upload, replace with s3:// pointer + `<field>_bytes`. 8 slo: REQUEST_COMPLETED/FAILED ⇒ `tracker.observe`. 9 quality: LLM/RAG completed + success ⇒ `rng.uniform(0,100) < sample_pct` ⇒ `payload.quality_sample=true`. Finally `payload.enriched_at` UTC ISO. `deps.py`: `Deps` dataclass (settings, control_plane, redactor, archiver|None, slo_tracker|None, injectable rng) — stages construct nothing | E2, E3, E4 | 2d | ~12 pipeline tests: valid flows enriched; not-JSON + unknown type ⇒ DLQ; traceparent fills ids; redaction spares user_id; errmap normalizes; cost overwritten + spend recorded; budget alert marked; oversized field offloaded; sampling ≈ pct; **DB-down stage failure degrades, event still flows** |
| **E6** | Consume→produce→commit loop | `consumer.py`: `EnrichmentLoop(settings, deps, consumer, producer)` — subscribe raw; loop: `consume(batch, timeout)`; per msg: `process()` ok ⇒ produce to processed (same key or correlation_id, same headers, delivery callback); `DeadLetterError` ⇒ produce `dead_letter.wrap(raw, reason)` (shape `{reason, failed_at, original}` — must match the replay script) to DLQ + counter; then `flush(timeout)`; **commit offsets only if flush drained AND zero delivery failures AND no consume errors** — else log + don't commit (batch redelivers; downstream dedupes on event_id). Consumer conf: manual commit, `auto.offset.reset=earliest`, `max.poll.interval.ms=600000` (GLiNER cold batches). Producer: idempotent, lz4, linger 20. SIGTERM/SIGINT ⇒ finish batch, close consumer, flush, exit. JSON stdlib logging; `main()` wires settings→metrics server→Deps (ControlPlane, redactor, archiver, SloTracker)→loop; console script | E5 | 1.5d | fake-Kafka tests: good batch ⇒ 2 produces + 1 commit; bad msg ⇒ DLQ w/ replayable shape, batch still commits; **delivery failure ⇒ commit=0** |
| **E7** | Packaging + deploy + docs | Dockerfile (slim; commented prod variant baking the GLiNER model into the image). `k8s/deployment.yaml`: 3 replicas (≤12 partitions), 768Mi–1.5Gi (GLiNER), termination grace 60s, metrics annotations + PodMonitor, env from Secret+ConfigMap. CI yml: ruff → pytest → contract drift diff → docker build. README: how-it-works diagram, guarantees (at-least-once, degrade-don't-die, raw user_id, no Redis), **full two-repo local walkthrough** (stack up → apply IaC → run consumer → demo SDK service → curl → watch processed topic + /metrics), note that BUDGET_THRESHOLD_EXCEEDED event type is proposed for contract v1.1. `.env.example` | E6 | 1d | docker build succeeds; teammate reproduces the README walkthrough end-to-end unaided |

---

## Table 3 — Joint verification (all 3 devs, final week)

| ID | Task | What to do | Depends on | Est | Done when |
|----|------|-----------|------------|-----|-----------|
| **V1** | End-to-end smoke | Dev stack + demo SDK service + running consumer: one `curl` with `X-Correlation-ID: demo-123`, `X-User-ID: SOE99999` ⇒ verify on **processed** topic: ordered chain REQUEST_RECEIVED→LLM_*→REQUEST_COMPLETED, one partition, enriched fields (owner_team, control-plane cost, enriched_at), user_id raw; garbage into raw ⇒ DLQ wrapper; Tempo span; JSON logs carry demo-123; both /metrics live | S8, E7 | 1d | recipe committed to README as the canonical "is it working?" check |
| **V2** | Failure drills | (a) `docker stop kafka` mid-traffic ⇒ demo app latency/error-rate unchanged (SDK drops+warns); (b) tiny producer queue ⇒ drop-with-warning never block; (c) stop Postgres while consumer runs ⇒ events still flow with `stage_errors_total` rising, none dead-lettered; (d) DLQ replay: re-drive a quarantined event with `observability-iac/scripts/replay_dead_letter.py`, verify it lands processed | V1 | 1d | all four drills pass; hygiene grep (no credentials/prompt text in 500 sampled events) added to CI |
| **V3** | Sign-off + follow-ups | Walk exit criteria: both packages published/buildable, CI drift gates on, 35+ tests green across packages, V1/V2 documented. File follow-ups: pricing vs real billing, COIN-JWT claim extraction in middleware, SASL creds per env, contract v1.1 (BUDGET_THRESHOLD_EXCEEDED), GLiNER prod image | V2 | 0.5d | sign-off note committed; Phase 2 service onboarding unblocked |

---

## Suggested split for 3 developers (≈3 weeks each; swap freely — deps are honest)

| Week | Dev 1 (SDK core) | Dev 2 (SDK surface / consumer core) | Dev 3 (consumer platform) |
|------|------------------|--------------------------------------|---------------------------|
| 1 | F1 (lead) → S1, S2 | F1 → F2, F3 | F1 → F4, E1 |
| 2 | S3 → S4 | S6 → S7 | E2 → E3, E4 |
| 3 | S5 → S8 | E5 (pipeline) | E6 → E7 |
| 4 | V1 | V2 | V2/V3 |

Rules that keep this parallel: **F1 blocks everything**; nothing else crosses
tracks (SDK and consumer meet only at the contract + Kafka wire format).
Every PR is reviewed by someone *not* on that package — the reviewer is the
compatibility check. All tests are broker/DB-free by design (FakeEmitter,
FakeControlPlane, fake Kafka objects), so nobody waits on infrastructure.

**Totals:** SDK ≈ 11d · consumer ≈ 9.5d · foundation+verification ≈ 5d
⇒ ~25.5 dev-days ≈ 3 people × 3.5 weeks with reviews and meetings.
