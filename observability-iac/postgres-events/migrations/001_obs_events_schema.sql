-- Phase 0 / Task 0.5 — INTERIM event firehose (Snowflake stand-in until onboarded).
-- Separate schema from `observability` (control plane) so the swap-back is clean.
-- See plan.md §5.9 / §15.6 for the migration to Snowflake.
CREATE SCHEMA IF NOT EXISTS obs_events;

-- All enriched events. Monthly range partitions; ~90-day hot window, older detached to S3.
CREATE TABLE IF NOT EXISTS obs_events.events (
  event_id       VARCHAR(64),
  event_type     VARCHAR(64),
  telemetry_type VARCHAR(16),
  correlation_id VARCHAR(64),
  trace_id       VARCHAR(64),
  span_id        VARCHAR(64),
  parent_span_id VARCHAR(64),
  service_name   VARCHAR(64),
  environment    VARCHAR(16),
  application_id VARCHAR(64),
  lob            VARCHAR(32),
  status         VARCHAR(16),
  latency_ms     DOUBLE PRECISION,
  error_code     VARCHAR(64),
  payload        JSONB,                         -- VARIANT stand-in
  event_ts       TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (event_id, event_ts)
) PARTITION BY RANGE (event_ts);

CREATE INDEX IF NOT EXISTS ix_events_app_ts  ON obs_events.events (application_id, event_ts);
CREATE INDEX IF NOT EXISTS ix_events_corr    ON obs_events.events (correlation_id);
CREATE INDEX IF NOT EXISTS ix_events_type_ts ON obs_events.events (event_type, event_ts);
CREATE INDEX IF NOT EXISTS ix_events_payload ON obs_events.events USING GIN (payload);

-- Bootstrap partitions (pg_partman keeps rolling these forward — see 009).
CREATE TABLE IF NOT EXISTS obs_events.events_2026_06 PARTITION OF obs_events.events
  FOR VALUES FROM ('2026-06-01') TO ('2026-07-01');
CREATE TABLE IF NOT EXISTS obs_events.events_2026_07 PARTITION OF obs_events.events
  FOR VALUES FROM ('2026-07-01') TO ('2026-08-01');
CREATE TABLE IF NOT EXISTS obs_events.events_2026_08 PARTITION OF obs_events.events
  FOR VALUES FROM ('2026-08-01') TO ('2026-09-01');

GRANT USAGE ON SCHEMA obs_events TO dashboard_ro;
GRANT SELECT ON ALL TABLES IN SCHEMA obs_events TO dashboard_ro;
ALTER DEFAULT PRIVILEGES IN SCHEMA obs_events GRANT SELECT ON TABLES TO dashboard_ro;
