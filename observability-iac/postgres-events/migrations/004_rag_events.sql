CREATE TABLE IF NOT EXISTS obs_events.rag_events (
  event_id              VARCHAR(64),
  correlation_id        VARCHAR(64),
  application_id        VARCHAR(64),
  lob                   VARCHAR(32),
  rag_id                VARCHAR(64),
  knowledge_base        VARCHAR(128),
  embedding_model       VARCHAR(128),
  retrieved_chunk_count INT,
  avg_relevance_score   NUMERIC(6,4),
  no_result             BOOLEAN DEFAULT FALSE,
  citation_coverage_pct NUMERIC(5,2),
  embedding_latency_ms  DOUBLE PRECISION,
  payload               JSONB,
  event_ts              TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (event_id, event_ts)
) PARTITION BY RANGE (event_ts);
CREATE INDEX IF NOT EXISTS ix_rag_app_ts ON obs_events.rag_events (application_id, event_ts);
CREATE TABLE IF NOT EXISTS obs_events.rag_events_2026_06 PARTITION OF obs_events.rag_events
  FOR VALUES FROM ('2026-06-01') TO ('2026-07-01');
