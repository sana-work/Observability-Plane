-- obs_events — INTERIM event firehose (Snowflake stand-in until onboarding is
-- approved). Separate schema from `observability` (control plane) so the swap
-- is clean: only the Storage Consumer writer + dashboard queries change.
--
-- Column set mirrors the frozen ObsEvent envelope (contracts/event_schema.py,
-- schema_version 1.0) 1:1, plus event_ts (the parsed timestamp used for
-- partitioning). Domain payload stays in JSONB; hot domain fields are promoted
-- into the obs_events.llm_events / agent_events / ... tables (002-007).
--
-- Retention: ~90-day hot window. Monthly RANGE partitions; a nightly job calls
-- ensure_month_partitions() + drop_old_partitions() (see 008). Dropped
-- partitions are first COPYed to S3 raw-traces/ by the archiver CronJob.

CREATE SCHEMA IF NOT EXISTS obs_events;

CREATE TABLE IF NOT EXISTS obs_events.events (
  -- identity (envelope)
  event_id        VARCHAR(64)  NOT NULL,
  schema_version  VARCHAR(8)   NOT NULL DEFAULT '1.0',
  event_type      VARCHAR(64)  NOT NULL,
  telemetry_type  VARCHAR(16)  NOT NULL DEFAULT 'event',
  -- time
  event_ts        TIMESTAMPTZ  NOT NULL,           -- parsed envelope `timestamp`
  emitted_at      TIMESTAMPTZ,
  ingested_at     TIMESTAMPTZ  NOT NULL DEFAULT now(),
  -- correlation / trace
  correlation_id  VARCHAR(64),
  request_id      VARCHAR(64),
  trace_id        VARCHAR(64),
  span_id         VARCHAR(64),
  parent_span_id  VARCHAR(64),
  -- ownership
  service_name    VARCHAR(64)  NOT NULL,
  component       VARCHAR(256),
  environment     VARCHAR(16)  NOT NULL,
  application_id  VARCHAR(64),
  lob             VARCHAR(32),
  tenant_id       VARCHAR(64),
  user_id         VARCHAR(64),                     -- raw SOE ID, kept unhashed by platform decision; RBAC-gated
  -- outcome
  status          VARCHAR(32)  NOT NULL,
  latency_ms      DOUBLE PRECISION,
  error_code      VARCHAR(64),
  http_status     SMALLINT,
  -- domain payload (VARIANT stand-in)
  payload         JSONB        NOT NULL DEFAULT '{}',
  PRIMARY KEY (event_id, event_ts)
) PARTITION BY RANGE (event_ts);

CREATE INDEX IF NOT EXISTS ix_events_app_ts   ON obs_events.events (application_id, event_ts DESC);
CREATE INDEX IF NOT EXISTS ix_events_corr     ON obs_events.events (correlation_id);
CREATE INDEX IF NOT EXISTS ix_events_type_ts  ON obs_events.events (event_type, event_ts DESC);
CREATE INDEX IF NOT EXISTS ix_events_svc_ts   ON obs_events.events (service_name, event_ts DESC);
CREATE INDEX IF NOT EXISTS ix_events_err      ON obs_events.events (error_code, event_ts DESC) WHERE error_code IS NOT NULL;
CREATE INDEX IF NOT EXISTS ix_events_payload  ON obs_events.events USING GIN (payload jsonb_path_ops);

-- ---------------------------------------------------------------------------
-- Partition management for ALL partitioned tables in this schema.
-- pg_partman-free implementation so it also runs on vanilla Postgres;
-- swap for pg_partman if the cluster has it.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION obs_events.ensure_month_partitions(months_ahead INT DEFAULT 2)
RETURNS void LANGUAGE plpgsql AS $$
DECLARE
  parent RECORD;
  m INT;
  from_month DATE;
  part_name TEXT;
BEGIN
  FOR parent IN
    SELECT c.relname
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = 'obs_events' AND c.relkind = 'p'   -- partitioned parents
  LOOP
    FOR m IN 0..months_ahead LOOP
      from_month := date_trunc('month', CURRENT_DATE)::date + make_interval(months => m);
      part_name := format('%s_%s', parent.relname, to_char(from_month, 'YYYY_MM'));
      EXECUTE format(
        'CREATE TABLE IF NOT EXISTS obs_events.%I PARTITION OF obs_events.%I
           FOR VALUES FROM (%L) TO (%L)',
        part_name, parent.relname,
        from_month, from_month + interval '1 month');
    END LOOP;
  END LOOP;
END;
$$;

CREATE OR REPLACE FUNCTION obs_events.drop_old_partitions(keep_months INT DEFAULT 3)
RETURNS SETOF TEXT LANGUAGE plpgsql AS $$
DECLARE
  part RECORD;
  cutoff DATE := date_trunc('month', CURRENT_DATE)::date - make_interval(months => keep_months);
BEGIN
  FOR part IN
    SELECT n.nspname, c.relname,
           to_date(right(c.relname, 7), 'YYYY_MM') AS part_month
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = 'obs_events'
      AND c.relkind = 'r'
      AND c.relname ~ '_\d{4}_\d{2}$'
  LOOP
    IF part.part_month < cutoff THEN
      -- ARCHIVE FIRST: the s3-archiver CronJob must have exported this month.
      EXECUTE format('DROP TABLE obs_events.%I', part.relname);
      RETURN NEXT part.relname;
    END IF;
  END LOOP;
END;
$$;
