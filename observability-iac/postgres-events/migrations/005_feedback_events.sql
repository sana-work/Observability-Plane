-- Feedback events — user feedback linked to traces by correlation_id.
-- Written by obs-storage-consumer for event_type LIKE 'FEEDBACK_%'.

CREATE TABLE IF NOT EXISTS obs_events.feedback_events (
  event_id            VARCHAR(64) NOT NULL,
  event_ts            TIMESTAMPTZ NOT NULL,
  event_type          VARCHAR(64) NOT NULL,
  correlation_id      VARCHAR(64),
  application_id      VARCHAR(64),
  lob                 VARCHAR(32),
  agent_id            VARCHAR(64),
  feedback_id         VARCHAR(64),
  usecase_id          VARCHAR(64),
  response_id         VARCHAR(64),
  rating              SMALLINT CHECK (rating BETWEEN 1 AND 5),
  thumbs              VARCHAR(8),                 -- up | down
  sentiment           VARCHAR(16),                -- positive | neutral | negative
  feedback_category   VARCHAR(32),                -- wrong-answer | slow | tool-failed | irrelevant | unsafe
  free_text_redacted  TEXT,                       -- post-GLiNER text only
  resolution_status   VARCHAR(16),
  status              VARCHAR(32),
  PRIMARY KEY (event_id, event_ts)
) PARTITION BY RANGE (event_ts);

CREATE INDEX IF NOT EXISTS ix_fb_corr   ON obs_events.feedback_events (correlation_id);
CREATE INDEX IF NOT EXISTS ix_fb_agent  ON obs_events.feedback_events (agent_id, event_ts DESC);
CREATE INDEX IF NOT EXISTS ix_fb_neg    ON obs_events.feedback_events (event_ts DESC)
  WHERE sentiment = 'negative' OR thumbs = 'down';
