-- Monthly partition management for the interim Postgres firehose.
-- Requires the pg_partman extension. Keeps a ~90-day hot window (retention=3 months)
-- and detaches older partitions (archived to S3 raw-traces/ by a separate job).
CREATE EXTENSION IF NOT EXISTS pg_partman;

-- Register each partitioned parent with pg_partman (monthly).
SELECT partman.create_parent(
  p_parent_table := 'obs_events.events',
  p_control      := 'event_ts',
  p_type         := 'native',
  p_interval     := '1 month',
  p_premake      := 3
);
SELECT partman.create_parent('obs_events.llm_events',      'event_ts', 'native', '1 month', p_premake := 3);
SELECT partman.create_parent('obs_events.agent_events',    'event_ts', 'native', '1 month', p_premake := 3);
SELECT partman.create_parent('obs_events.rag_events',      'event_ts', 'native', '1 month', p_premake := 3);
SELECT partman.create_parent('obs_events.feedback_events', 'event_ts', 'native', '1 month', p_premake := 3);
SELECT partman.create_parent('obs_events.quality_scores',  'event_ts', 'native', '1 month', p_premake := 3);

-- ~90-day retention: detach (not drop) so the archive job can ship to S3 first.
UPDATE partman.part_config
SET retention = '3 months', retention_keep_table = true
WHERE parent_table LIKE 'obs_events.%';

-- Schedule partman maintenance via pg_cron (or an external scheduler):
--   SELECT cron.schedule('partman-maint', '@daily', $$CALL partman.run_maintenance_proc()$$);
