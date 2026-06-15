CREATE TABLE IF NOT EXISTS obs_events.feedback_events (
  event_id        VARCHAR(64),
  feedback_id     VARCHAR(64),
  correlation_id  VARCHAR(64),
  application_id  VARCHAR(64),
  lob             VARCHAR(32),
  rating          INT,
  thumbs          VARCHAR(8),
  sentiment       VARCHAR(16),
  category        VARCHAR(64),
  submitted_by_role VARCHAR(32),
  payload         JSONB,
  event_ts        TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (event_id, event_ts)
) PARTITION BY RANGE (event_ts);
CREATE INDEX IF NOT EXISTS ix_fb_corr ON obs_events.feedback_events (correlation_id);
CREATE TABLE IF NOT EXISTS obs_events.feedback_events_2026_06 PARTITION OF obs_events.feedback_events
  FOR VALUES FROM ('2026-06-01') TO ('2026-07-01');
