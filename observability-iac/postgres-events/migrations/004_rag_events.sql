-- RAG events — retrieval quality analytics.
-- Written by obs-storage-consumer for event_type LIKE 'RAG_%'.

CREATE TABLE IF NOT EXISTS obs_events.rag_events (
  event_id             VARCHAR(64) NOT NULL,
  event_ts             TIMESTAMPTZ NOT NULL,
  event_type           VARCHAR(64) NOT NULL,
  correlation_id       VARCHAR(64),
  application_id       VARCHAR(64),
  lob                  VARCHAR(32),
  rag_id               VARCHAR(64),
  knowledge_base       TEXT,
  vector_db_index      TEXT,
  embedding_model      TEXT,
  query_hash           VARCHAR(64),
  top_k                SMALLINT,
  chunk_count          INT,
  no_result_flag       BOOLEAN DEFAULT false,
  avg_relevance_score  DOUBLE PRECISION,
  reranker_score       DOUBLE PRECISION,
  citation_coverage    DOUBLE PRECISION,
  context_tokens       INT,
  truncation_flag      BOOLEAN DEFAULT false,
  source_docs_used     TEXT[],
  latency_ms           DOUBLE PRECISION,
  status               VARCHAR(32),
  PRIMARY KEY (event_id, event_ts)
) PARTITION BY RANGE (event_ts);

CREATE INDEX IF NOT EXISTS ix_rag_kb_ts  ON obs_events.rag_events (rag_id, event_ts DESC);
CREATE INDEX IF NOT EXISTS ix_rag_nores  ON obs_events.rag_events (event_ts DESC) WHERE no_result_flag;
CREATE INDEX IF NOT EXISTS ix_rag_corr   ON obs_events.rag_events (correlation_id);
