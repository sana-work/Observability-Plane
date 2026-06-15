-- DEFERRED — target firehose table (mirrors obs_events.events). Apply at onboarding.
CREATE TABLE IF NOT EXISTS OBS_DB.PUBLIC.SF_EVENTS (
  event_id STRING, event_type STRING, telemetry_type STRING,
  correlation_id STRING, trace_id STRING, span_id STRING, parent_span_id STRING,
  service_name STRING, environment STRING, application_id STRING, lob STRING,
  status STRING, latency_ms FLOAT, error_code STRING,
  payload VARIANT, event_ts TIMESTAMP_NTZ, event_date DATE
) CLUSTER BY (application_id, event_date);

-- Per-domain tables (sf_llm_events, sf_agent_events, sf_rag_events, sf_feedback_events,
-- sf_quality, sf_slo) follow the same shape with domain columns promoted out of payload,
-- mirroring obs_events.* — same CLUSTER BY (application_id, event_date).
