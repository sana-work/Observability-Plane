-- LLM events — hot domain columns promoted out of payload for cheap
-- aggregation (cost trends, token usage, model reliability).
-- Written by obs-storage-consumer for event_type LIKE 'LLM_%'.

CREATE TABLE IF NOT EXISTS obs_events.llm_events (
  event_id             VARCHAR(64) NOT NULL,
  event_ts             TIMESTAMPTZ NOT NULL,
  event_type           VARCHAR(64) NOT NULL,       -- LLM_CALL_COMPLETED / _FAILED / RATE_LIMITED / SAFETY_BLOCKED
  correlation_id       VARCHAR(64),
  application_id       VARCHAR(64),
  lob                  VARCHAR(32),
  agent_id             VARCHAR(64),
  -- model identity
  model_provider       VARCHAR(64),
  model_name           TEXT,
  model_version        VARCHAR(64),
  -- prompt governance
  prompt_template_id   VARCHAR(64),
  prompt_version       VARCHAR(32),
  prompt_hash          VARCHAR(64),
  temperature          REAL,
  -- usage & cost
  input_tokens         INT,
  output_tokens        INT,
  total_tokens         INT,
  estimated_cost_usd   NUMERIC(12,8),
  -- performance & outcome
  latency_ms           DOUBLE PRECISION,
  time_to_first_token_ms DOUBLE PRECISION,
  retry_count          SMALLINT,
  rate_limit_hit       BOOLEAN DEFAULT false,
  safety_blocked       BOOLEAN DEFAULT false,
  finish_reason        VARCHAR(32),
  llm_error_code       VARCHAR(64),
  status               VARCHAR(32),
  PRIMARY KEY (event_id, event_ts)
) PARTITION BY RANGE (event_ts);

CREATE INDEX IF NOT EXISTS ix_llm_model_ts ON obs_events.llm_events (model_name, event_ts DESC);
CREATE INDEX IF NOT EXISTS ix_llm_app_ts   ON obs_events.llm_events (application_id, event_ts DESC);
CREATE INDEX IF NOT EXISTS ix_llm_prompt   ON obs_events.llm_events (prompt_template_id, prompt_version);
CREATE INDEX IF NOT EXISTS ix_llm_corr     ON obs_events.llm_events (correlation_id);
