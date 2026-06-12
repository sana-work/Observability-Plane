<!--
  PLACEMENT: copy this file to the genai-user-feedback repo at:
      genai-user-feedback/.github/copilot-instructions.md
  GitHub Copilot auto-loads it for every Chat / Edit / Agent request in that repo.
  (This copy lives in the Observability Plane folder only as a template.)
-->

# Copilot build rules — genai-user-feedback service

## Source of truth
- Follow `docs/USER_FEEDBACK_GET_API.md` exactly. When a prompt says "Section N",
  it means that numbered section of that spec.
- Implement the code as written in the spec; do not invent alternative designs.

## Hard constraints (do NOT violate)
- **NO Langfuse, NO obs_sdk/ai-observability-sdk, NO Kafka / emit_event.** This service
  is self-contained and uses plain Python `logging` only.
- **Keep raw `soe_id`.** Do NOT hash it, do NOT add a `user_hash` field. `soe_id` IS
  returned by the GET endpoints (per requirement).
- **Business field is `usecase_id`** (never `application_id`). The matching request
  header is **`X-Usecase-ID`** (renamed from `X-Application-ID`).
- **`correlation_id` is mandatory everywhere** — request body, DB column (NOT NULL),
  every GET response, and logs. It identifies the unique chat event.
- **Redact only `prompt`, `comments`, `response_text` in logs.** `soe_id` is NOT redacted.
- Every endpoint returns the uniform `ApiResponse` envelope (Section 1).
- Every error is an `AppException` with a structured `ErrorCode` (Section 2); the global
  handlers in `main.py` convert them — never leak raw tracebacks to clients.
- Duplicate key is **`(soe_id, correlation_id)`** → HTTP 409 `FB_DUPLICATE`.

## Tech stack
- Python 3.11, FastAPI, SQLAlchemy, Pydantic **v2**, Alembic, prometheus-client.
- Auth: existing `JWTBearer` (COIN OIDC) dependency on every route — keep it.
- Logging: structured JSON via `logconfig.yaml`; all log calls use `extra={...}`
  key-values, never f-strings.

## Style
- Match the existing module layout under `feedback/`.
- Type-hint everything. Keep functions small. Add a one-line docstring per public method.
- Do not reformat or rewrite unrelated code.
