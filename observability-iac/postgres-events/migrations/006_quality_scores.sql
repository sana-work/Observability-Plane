-- Quality scores — output of obs-eval-service (LLM-as-judge, Phase Q).
-- One row per evaluated event; also indexed into ai-obs-quality-scores-* in ES.

CREATE TABLE IF NOT EXISTS obs_events.quality_scores (
  eval_id             VARCHAR(64) NOT NULL,
  event_ts            TIMESTAMPTZ NOT NULL,       -- ts of the EVALUATED event
  evaluated_event_id  VARCHAR(64) NOT NULL,       -- FK-by-convention → obs_events.events
  correlation_id      VARCHAR(64),
  application_id      VARCHAR(64),
  lob                 VARCHAR(32),
  agent_id            VARCHAR(64),
  rag_id              VARCHAR(64),
  eval_type           VARCHAR(32) NOT NULL
                      CHECK (eval_type IN ('faithfulness','hallucination','answer_relevance','custom')),
  score               DOUBLE PRECISION,           -- 0..1
  hallucination_flag  BOOLEAN,
  judge_model         TEXT NOT NULL,
  judge_prompt_version VARCHAR(32),
  rationale_s3_key    TEXT,                       -- full judge output archived to S3
  sampled_pct         REAL,                       -- sampling rate in force when picked
  evaluated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (eval_id, event_ts)
) PARTITION BY RANGE (event_ts);

CREATE INDEX IF NOT EXISTS ix_qs_corr ON obs_events.quality_scores (correlation_id);
CREATE INDEX IF NOT EXISTS ix_qs_type ON obs_events.quality_scores (eval_type, event_ts DESC);
CREATE INDEX IF NOT EXISTS ix_qs_bad  ON obs_events.quality_scores (event_ts DESC)
  WHERE hallucination_flag OR score < 0.5;
