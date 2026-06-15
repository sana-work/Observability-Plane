-- Written by obs-eval-service (custom LLM-as-judge). Mirrors the future sf_quality table.
CREATE TABLE IF NOT EXISTS obs_events.quality_scores (
  event_id            VARCHAR(64),
  correlation_id      VARCHAR(64),
  application_id      VARCHAR(64),
  lob                 VARCHAR(32),
  faithfulness_score  NUMERIC(6,4),
  hallucination_flag  BOOLEAN,
  relevance_score     NUMERIC(6,4),
  judge_model         VARCHAR(128),
  judge_prompt_version INT,
  payload             JSONB,
  event_ts            TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (event_id, event_ts)
) PARTITION BY RANGE (event_ts);
CREATE INDEX IF NOT EXISTS ix_quality_corr ON obs_events.quality_scores (correlation_id);
CREATE TABLE IF NOT EXISTS obs_events.quality_scores_2026_06 PARTITION OF obs_events.quality_scores
  FOR VALUES FROM ('2026-06-01') TO ('2026-07-01');
