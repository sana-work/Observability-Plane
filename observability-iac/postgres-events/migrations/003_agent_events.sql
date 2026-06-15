CREATE TABLE IF NOT EXISTS obs_events.agent_events (
  event_id          VARCHAR(64),
  correlation_id    VARCHAR(64),
  application_id    VARCHAR(64),
  lob               VARCHAR(32),
  agent_id          VARCHAR(64),
  agent_version     VARCHAR(32),
  status            VARCHAR(16),     -- started | completed | failed | timeout
  step_count        INT,
  loop_count        INT,
  handoff_count     INT,
  termination_reason VARCHAR(64),
  agent_latency_ms  DOUBLE PRECISION,
  payload           JSONB,
  event_ts          TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (event_id, event_ts)
) PARTITION BY RANGE (event_ts);
CREATE INDEX IF NOT EXISTS ix_agent_app_ts ON obs_events.agent_events (application_id, event_ts);
CREATE TABLE IF NOT EXISTS obs_events.agent_events_2026_06 PARTITION OF obs_events.agent_events
  FOR VALUES FROM ('2026-06-01') TO ('2026-07-01');
