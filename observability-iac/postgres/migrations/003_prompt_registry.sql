-- Custom AI-quality layer — prompt registry + versioning (replaces Langfuse Prompt Mgmt).
-- Services fetch via SDK get_prompt(prompt_id, version="active"); each LLM event records
-- prompt_template_id + prompt_version + prompt_hash for drift detection and A/B comparison.
CREATE TABLE IF NOT EXISTS observability.prompt_registry (
  prompt_id    VARCHAR(64),
  version      INT,
  name         TEXT,
  template     TEXT,
  variables    JSONB,
  prompt_hash  VARCHAR(64),
  status       VARCHAR(16) DEFAULT 'draft',   -- draft | active | archived
  ab_variant   VARCHAR(16),
  traffic_pct  INT DEFAULT 100,
  created_by   VARCHAR(64),
  created_at   TIMESTAMPTZ DEFAULT now(),
  PRIMARY KEY (prompt_id, version)
);

-- Only one active version per prompt_id at a time.
CREATE UNIQUE INDEX IF NOT EXISTS ux_prompt_active
  ON observability.prompt_registry (prompt_id)
  WHERE status = 'active';
