-- Bootstrap partitions for every partitioned table in obs_events (current
-- month + 2 ahead), and grants. Ongoing maintenance: a nightly K8s CronJob runs
--   SELECT obs_events.ensure_month_partitions(2);
--   SELECT obs_events.drop_old_partitions(3);   -- AFTER the S3 archiver exported the month
-- (If the cluster ships pg_cron, schedule the same two calls there instead.)

SELECT obs_events.ensure_month_partitions(2);

-- Writer role for obs-storage-consumer.
DO $$
BEGIN
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'obs_storage') THEN
    CREATE ROLE obs_storage NOLOGIN;
  END IF;
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'dashboard_ro') THEN
    CREATE ROLE dashboard_ro NOLOGIN;
  END IF;
END
$$;

GRANT USAGE ON SCHEMA obs_events TO obs_storage;
GRANT INSERT, SELECT ON ALL TABLES IN SCHEMA obs_events TO obs_storage;
ALTER DEFAULT PRIVILEGES IN SCHEMA obs_events GRANT INSERT, SELECT ON TABLES TO obs_storage;
GRANT EXECUTE ON FUNCTION obs_events.ensure_month_partitions(INT) TO obs_storage;

GRANT USAGE ON SCHEMA obs_events TO dashboard_ro;
GRANT SELECT ON ALL TABLES IN SCHEMA obs_events TO dashboard_ro;
ALTER DEFAULT PRIVILEGES IN SCHEMA obs_events GRANT SELECT ON TABLES TO dashboard_ro;
