# Copilot Build Prompts — genai-user-feedback

Copy-paste prompts to build every feature in `2026-06-02_user-feedback-read-api-enhancements.md` with GitHub Copilot
in VS Code. Run them **in order** — each step compiles on its own (dependency-safe).

## Before you start
1. Copy `2026-06-02_user-feedback-read-api-enhancements.md` into the repo at `docs/USER_FEEDBACK_GET_API.md`.
2. Copy `2026-06-02_copilot-instructions-genai-user-feedback.md` to `.github/copilot-instructions.md`.
3. Open Copilot Chat → set mode to **Agent** (preferred) or **Edit**.
4. Pick a large-context model (GPT-4o / Claude Sonnet) in the model dropdown.
5. **Commit after every step** so you can revert one bad generation.

> **Why this order differs from the spec's "Build order" table:** that table is ordered by
> rollout *risk*. The order below is ordered by *code dependency* so each step references
> only things that already exist. The mapping column shows which spec sprints/IDs each covers.

---

## Step 1 — Scaffold foundational modules
**Covers:** E-P0-4, E-P0-5, E-P0-7 (partial), E-P1-9 (partial) · **Spec:** Sections 1, 2, 4

```
#file:docs/USER_FEEDBACK_GET_API.md

Create three NEW files exactly as written in the spec:
- feedback/responses.py  — from Section 1 (ApiResponse envelope + success/failure helpers).
- feedback/errors.py     — from Section 2 (ErrorCode enum, AppException, not_found,
                           duplicate_feedback, db_error).
- feedback/metrics.py    — from Section 4 (prometheus-client counters/histograms).
Use Pydantic v2 for the envelope (BaseModel + Generic[T], not GenericModel).
Do not touch any other files yet.
```

---

## Step 2 — Schema, models, migration
**Covers:** E-P1-2 (constraint), E-P1-3, E-P1-4 (models), correlation_id mandatory
**Spec:** Sections 3, 5, 10

```
#file:docs/USER_FEEDBACK_GET_API.md

1. Update feedback/schemas.py per Section 3: add correlation_id (String, NOT NULL,
   indexed), status, created_at, updated_at; unique constraint (soe_id, correlation_id)
   named uq_feedback_soe_corr. Keep soe_id raw — no user_hash.
2. Update feedback/models.py per Section 5: make correlation_id mandatory on the
   existing UserFeedbackModel; add read-side models FeedbackRecord, FeedbackListData,
   DailyFeedbackPoint, FeedbackSummaryData, ProblemRecord, FeedbackStatusUpdate.
   FeedbackRecord/ProblemRecord must include correlation_id, usecase_id, and soe_id;
   they must NOT include prompt, comments, or response_text.
3. Create alembic/versions/002_feedback_readiness.py exactly as in Section 10, including
   the nullable→backfill(from trace_id)→NOT NULL sequence for correlation_id and the
   de-dup step before the unique constraint.
```

---

## Step 3 — Repository layer
**Covers:** E-P0-1, E-P0-3, E-P1-2 (dedup), E-P1-10, E-P2-1 (repo), reads · **Spec:** Section 6 (+ 12)

```
#file:docs/USER_FEEDBACK_GET_API.md

Rewrite feedback/repositories.py UserFeedbackRepo per Section 6:
- create(): persist with correlation_id; catch IntegrityError → raise duplicate_feedback()
  (FB_DUPLICATE) and increment FEEDBACK_DUPLICATE; catch other errors → log
  feedback_db_error and raise db_error(); on success emit FEEDBACK_SUBMITTED metric and an
  audit.info("feedback_submitted") log including feedback_id, correlation_id, trace_id,
  usecase_id, feedback_type, correctness, soe_id.
- get_by_id, list_feedback (with a correlation_id filter), get_summary, list_problems.
- Also add by_type() from Section 12.
Use the `audit` logger (logging.getLogger("audit")). No Kafka, no Langfuse, no hashing.
```

---

## Step 4 — API routes (envelope-wrapped)
**Covers:** E-P0-4, E-P1-3 (PATCH), E-P1-4, E-P2-1, E-P2-7, E-P1-1 · **Spec:** Sections 7, 11, 12, 17

```
#file:docs/USER_FEEDBACK_GET_API.md

Update feedback/api/v1/api.py per Sections 7, 12, 17:
- Wrap EVERY route's response in the ApiResponse envelope via success()/failure().
- POST /feedback (keep JWTBearer; return feedback_id + correlation_id).
- GET /feedback (filters incl. correlation_id), GET /feedback/summary,
  GET /feedback/problems, GET /feedback/by-type, GET /feedback/{id},
  PATCH /feedback/{id}/status, DELETE /feedback/{id} (anonymise, admin-only).
- IMPORTANT: declare /feedback/summary, /feedback/problems, /feedback/by-type BEFORE
  /feedback/{feedback_id} so they are not captured as an id.
- Add the Section 11 feedback_type enum check in permissive mode (env
  FEEDBACK_TYPE_STRICT=false): unknown value → log + count + coerce to "other".
Every GET response model must expose correlation_id and soe_id; never prompt/comments.
```

---

## Step 5 — main.py: handlers, /metrics, redaction, lifespan, OpenAPI
**Covers:** E-P0-6, E-P0-7, E-P1-6, E-P1-7, E-P1-9, E-P2-5, P1-f extras · **Spec:** Sections 8, 16, 18

```
#file:docs/USER_FEEDBACK_GET_API.md

Update feedback/main.py per Sections 8, 16, 18:
- Exception handlers: AppException → uniform error envelope with its http_status;
  catch-all Exception → log unhandled_exception + return 500 FB_INTERNAL_ERROR. Never
  leak tracebacks.
- GET /metrics endpoint (prometheus_client.generate_latest).
- Replace the calculate_process_time middleware: log http_request via extra={} with
  method, path, status_code, latency_ms (milliseconds), slow_response. Redact only
  prompt/comments/response_text (_REDACT); soe_id passes through unredacted.
- lifespan handler emitting service_started / service_stopped (Section 18a).
- FastAPI(...) app metadata: title, version, contact, servers, lifespan (Section 16).
- Add feedback_request_received / feedback_request_completed logs to the POST route
  (Section 18b).
```

---

## Step 6 — Logging wiring + auth-failure logging
**Covers:** E-P0-1, E-P0-2, E-P1-5, E-P1-8, E-P1-10 · **Spec:** Sections 8, 9

```
#file:docs/USER_FEEDBACK_GET_API.md

Per Section 9:
- Create feedback/log_filters.py with AppInfoFilter: stamp environment + service_name on
  every record; default correlation_id, trace_id, feedback_id, soe_id, event_type,
  error_code, component, status_code, latency_ms to "-".
- In feedback/tracking.py rename the ContextVar application_id_var → usecase_id_var, and
  update the middleware that reads it to use the header X-Usecase-ID (renamed from
  X-Application-ID).
- Update logconfig.yaml: register the app_info filter on the json_console handler, add the
  `audit` logger (propagate: false), and add all new fields (incl. correlation_id) to the
  JSON formatter.
Per Section 8: in feedback/auth.py log every auth rejection on the `audit` logger as
auth_failed with error_code, increment AUTH_FAILURES, and raise AppException(INVALID_TOKEN, 401).
```

---

## Step 7 — Optional: Likert rating
**Covers:** E-P2-2 · **Spec:** Section 13

```
#file:docs/USER_FEEDBACK_GET_API.md

Add the optional 1–5 rating field per Section 13: nullable `rating` column + CHECK
constraint in a new Alembic migration; optional rating: Optional[int] = Field(None, ge=1, le=5)
on UserFeedbackModel; expose rating on FeedbackRecord. Do not make it mandatory.
```

---

## Step 8 — Tests + sanity check
**Covers:** verification

```
#file:docs/USER_FEEDBACK_GET_API.md

Generate pytest tests under tests/ that assert:
- POST returns 201 with the ApiResponse envelope and a feedback_id + correlation_id.
- A second POST with the same (soe_id, correlation_id) returns 409 FB_DUPLICATE.
- GET /feedback, /feedback/{id}, /feedback/summary, /feedback/problems, /feedback/by-type
  all return the envelope and include correlation_id + soe_id but NEVER prompt/comments.
- A request missing correlation_id returns 422 FB_VALIDATION_ERROR.
- /metrics returns Prometheus text.
Use FastAPI TestClient with the JWTBearer dependency overridden.
```

Then run locally:
```
pip install -r requirements.txt        # add prometheus-client>=0.20.0
alembic upgrade head                   # against a scratch DB first
pytest -q
```

---

## Deferred (build only when scheduled)
| Item | Spec | Why deferred |
|---|---|---|
| OpenTelemetry spans (E-P2-3) | Section 14 | needs the platform OTel collector endpoint |
| RBAC roles (E-P2-4) | Section 15 | High risk — changes auth model; gate behind a flag |

Prompt when ready, e.g.:
```
#file:docs/USER_FEEDBACK_GET_API.md
Implement RBAC per Section 15: JWTBearer(required_role=...) gating PATCH status and DELETE
behind FEEDBACK_ADMIN_ROLE; submit + reads stay open. Ship with the role env unset (no
enforcement) first.
```

---

## Sprint → step cross-reference
| Spec sprint | Backlog IDs | Built in step |
|---|---|---|
| P0-a | E-P0-1, E-P0-2, E-P0-3, E-P0-6 | 3, 5, 6 |
| P0-b | E-P0-5, E-P0-7 | 1, 5 |
| P0-c | E-P0-4 | 1, 4 |
| P1-a | E-P1-3, E-P1-4 | 2, 3, 4 |
| P1-b | E-P1-2 | 2, 3 |
| P1-c | E-P1-5 … E-P1-8 | 5, 6 |
| P1-d | E-P1-9, E-P1-10 | 1, 3, 5, 6 |
| P1-e | E-P1-1 | 4 |
| P1-f | extras | 5 |
| P2 | E-P2-1, E-P2-2, E-P2-5, E-P2-7 | 3, 4, 5, 7 |
| P2-deferred | E-P2-3, E-P2-4 | deferred |
