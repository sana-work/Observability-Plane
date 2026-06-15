-- LLM events with domain columns promoted out of payload for fast aggregation.
CREATE TABLE IF NOT EXISTS obs_events.llm_events (
  event_id        VARCHAR(64),
  correlation_id  VARCHAR(64),
  application_id  VARCHAR(64),
  lob             VARCHAR(32),
  model_name      VARCHAR(128),
  model_provider  VARCHAR(64),
  input_tokens    INT,
  output_tokens   INT,
  total_tokens    INT,
  estimated_cost  NUMERIC(12,6),
  llm_latency_ms  DOUBLE PRECISION,
  finish_reason   VARCHAR(32),
  rate_limited    BOOLEAN DEFAULT FALSE,
  safety_blocked  BOOLEAN DEFAULT FALSE,
  prompt_template_id VARCHAR(64),
  prompt_version  INT,
  prompt_hash     VARCHAR(64),
  payload         JSONB,
  event_ts        TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (event_id, event_ts)
) PARTITION BY RANGE (event_ts);
CREATE INDEX IF NOT EXISTS ix_llm_app_ts ON obs_events.llm_events (application_id, event_ts);
CREATE INDEX IF NOT EXISTS ix_llm_model  ON obs_events.llm_events (model_name, event_ts);
CREATE TABLE IF NOT EXISTS obs_events.llm_events_2026_06 PARTITION OF obs_events.llm_events
  FOR VALUES FROM ('2026-06-01') TO ('2026-07-01');
