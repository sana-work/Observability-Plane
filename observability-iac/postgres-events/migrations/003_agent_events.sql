-- Agent events — step/loop/handoff analytics for the Agent dashboard.
-- Written by obs-storage-consumer for event_type LIKE 'AGENT_%'.

CREATE TABLE IF NOT EXISTS obs_events.agent_events (
  event_id           VARCHAR(64) NOT NULL,
  event_ts           TIMESTAMPTZ NOT NULL,
  event_type         VARCHAR(64) NOT NULL,     -- AGENT_STARTED/.../AGENT_TIMEOUT
  correlation_id     VARCHAR(64),
  application_id     VARCHAR(64),
  lob                VARCHAR(32),
  agent_id           VARCHAR(64),
  agent_version      VARCHAR(32),
  agent_type         VARCHAR(32),
  execution_mode     VARCHAR(32),
  step_count         INT,
  loop_count         INT,
  handoff_count      INT,
  handoff_to_agent   VARCHAR(64),
  planner_decision   TEXT,
  termination_reason VARCHAR(64),
  tools_used         TEXT[],
  models_used        TEXT[],
  latency_ms         DOUBLE PRECISION,
  estimated_cost_usd NUMERIC(12,8),
  status             VARCHAR(32),
  PRIMARY KEY (event_id, event_ts)
) PARTITION BY RANGE (event_ts);

CREATE INDEX IF NOT EXISTS ix_agent_id_ts ON obs_events.agent_events (agent_id, event_ts DESC);
CREATE INDEX IF NOT EXISTS ix_agent_corr  ON obs_events.agent_events (correlation_id);
