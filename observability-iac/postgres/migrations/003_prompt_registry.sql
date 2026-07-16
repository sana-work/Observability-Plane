-- Prompt registry — versioned prompt templates with A/B traffic split.
-- This is the table behind the control-plane API that ai-observability-sdk
-- get_prompt() calls. API response contract (must not drift from
-- ai_obs_sdk/prompts.py::Prompt):
--   { "template_id": ..., "version": "<int-as-string or 'active'-resolved>",
--     "text": ..., "prompt_hash": ..., "ab_bucket": <variant or null> }

CREATE TABLE IF NOT EXISTS observability.prompt_template_registry (
  template_id    VARCHAR(64)  NOT NULL,
  version        INT          NOT NULL,
  template_name  TEXT         NOT NULL,
  template_text  TEXT         NOT NULL,
  variables      JSONB        NOT NULL DEFAULT '[]',   -- ["question", "context", ...]
  prompt_hash    VARCHAR(64)  NOT NULL,                -- sha256(template_text)[:16] — same fn as SDK hashing.prompt_hash
  model_hint     TEXT,                                 -- model the template was tuned for
  status         VARCHAR(16)  NOT NULL DEFAULT 'draft'
                 CHECK (status IN ('draft','active','archived')),
  ab_bucket      VARCHAR(16),                          -- NULL = no experiment; else 'A' | 'B' | ...
  traffic_pct    INT          NOT NULL DEFAULT 100
                 CHECK (traffic_pct BETWEEN 0 AND 100),
  owner_team     TEXT,
  created_by     VARCHAR(64),
  created_at     TIMESTAMPTZ  NOT NULL DEFAULT now(),
  activated_at   TIMESTAMPTZ,
  PRIMARY KEY (template_id, version)
);

-- At most one active version per template per A/B bucket.
CREATE UNIQUE INDEX IF NOT EXISTS ux_prompt_active_per_bucket
  ON observability.prompt_template_registry (template_id, COALESCE(ab_bucket, ''))
  WHERE status = 'active';

-- Immutable audit of activations/rollbacks (compliance requirement).
CREATE TABLE IF NOT EXISTS observability.prompt_activation_audit (
  audit_id     BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  template_id  VARCHAR(64) NOT NULL,
  version      INT         NOT NULL,
  action       VARCHAR(16) NOT NULL CHECK (action IN ('activated','archived','rolled_back')),
  actor        VARCHAR(64) NOT NULL,
  reason       TEXT,
  occurred_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
