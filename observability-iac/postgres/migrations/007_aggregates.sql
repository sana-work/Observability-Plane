-- Aggregate rollup tables — written by obs-storage-consumer (micro-batch
-- upserts), read by the Custom Dashboard for fast trend queries so the
-- firehose tables are never scanned for simple charts.
-- Naming and grain match the roadmap's "Aggregate Metric Tables" section.

CREATE TABLE IF NOT EXISTS observability.agg_hourly_application_metrics (
  application_id   VARCHAR(64) NOT NULL,
  hour             TIMESTAMPTZ NOT NULL,          -- truncated to hour, UTC
  request_count    BIGINT      NOT NULL DEFAULT 0,
  error_count      BIGINT      NOT NULL DEFAULT 0,
  p50_latency_ms   DOUBLE PRECISION,
  p95_latency_ms   DOUBLE PRECISION,
  p99_latency_ms   DOUBLE PRECISION,
  total_cost_usd   NUMERIC(14,6) NOT NULL DEFAULT 0,
  total_tokens     BIGINT      NOT NULL DEFAULT 0,
  PRIMARY KEY (application_id, hour)
);

CREATE TABLE IF NOT EXISTS observability.agg_hourly_agent_metrics (
  agent_id            VARCHAR(64) NOT NULL,
  hour                TIMESTAMPTZ NOT NULL,
  run_count           BIGINT NOT NULL DEFAULT 0,
  success_count       BIGINT NOT NULL DEFAULT 0,
  failure_count       BIGINT NOT NULL DEFAULT 0,
  timeout_count       BIGINT NOT NULL DEFAULT 0,
  avg_steps           DOUBLE PRECISION,
  avg_loop_count      DOUBLE PRECISION,
  handoff_count       BIGINT NOT NULL DEFAULT 0,
  avg_latency_ms      DOUBLE PRECISION,
  total_cost_usd      NUMERIC(14,6) NOT NULL DEFAULT 0,
  PRIMARY KEY (agent_id, hour)
);

CREATE TABLE IF NOT EXISTS observability.agg_hourly_tool_metrics (
  tool_id          VARCHAR(64) NOT NULL,
  hour             TIMESTAMPTZ NOT NULL,
  call_count       BIGINT NOT NULL DEFAULT 0,
  failure_count    BIGINT NOT NULL DEFAULT 0,
  timeout_count    BIGINT NOT NULL DEFAULT 0,
  retry_count      BIGINT NOT NULL DEFAULT 0,
  p95_latency_ms   DOUBLE PRECISION,
  PRIMARY KEY (tool_id, hour)
);

CREATE TABLE IF NOT EXISTS observability.agg_hourly_llm_metrics (
  model_name          TEXT        NOT NULL,
  prompt_template_id  VARCHAR(64) NOT NULL DEFAULT '*',
  agent_id            VARCHAR(64) NOT NULL DEFAULT '*',
  application_id      VARCHAR(64) NOT NULL DEFAULT '*',
  hour                TIMESTAMPTZ NOT NULL,
  call_count          BIGINT NOT NULL DEFAULT 0,
  failure_count       BIGINT NOT NULL DEFAULT 0,
  rate_limit_count    BIGINT NOT NULL DEFAULT 0,
  safety_block_count  BIGINT NOT NULL DEFAULT 0,
  input_tokens        BIGINT NOT NULL DEFAULT 0,
  output_tokens       BIGINT NOT NULL DEFAULT 0,
  total_cost_usd      NUMERIC(14,6) NOT NULL DEFAULT 0,
  p50_latency_ms      DOUBLE PRECISION,
  p99_latency_ms      DOUBLE PRECISION,
  avg_ttft_ms         DOUBLE PRECISION,
  PRIMARY KEY (model_name, prompt_template_id, agent_id, application_id, hour)
);

CREATE TABLE IF NOT EXISTS observability.agg_hourly_rag_metrics (
  rag_id                 VARCHAR(64) NOT NULL,
  hour                   TIMESTAMPTZ NOT NULL,
  retrieval_count        BIGINT NOT NULL DEFAULT 0,
  no_result_count        BIGINT NOT NULL DEFAULT 0,
  failure_count          BIGINT NOT NULL DEFAULT 0,
  avg_relevance_score    DOUBLE PRECISION,
  avg_citation_coverage  DOUBLE PRECISION,
  truncation_count       BIGINT NOT NULL DEFAULT 0,
  p95_latency_ms         DOUBLE PRECISION,
  PRIMARY KEY (rag_id, hour)
);

CREATE TABLE IF NOT EXISTS observability.agg_daily_feedback_metrics (
  agent_id          VARCHAR(64) NOT NULL,
  day               DATE        NOT NULL,
  feedback_count    BIGINT NOT NULL DEFAULT 0,
  positive_count    BIGINT NOT NULL DEFAULT 0,
  negative_count    BIGINT NOT NULL DEFAULT 0,
  avg_rating        DOUBLE PRECISION,
  category_counts   JSONB   NOT NULL DEFAULT '{}',    -- {"wrong-answer": 3, "slow": 1}
  PRIMARY KEY (agent_id, day)
);

CREATE TABLE IF NOT EXISTS observability.agg_daily_kpi_metric (
  metric_id        VARCHAR(64) NOT NULL REFERENCES observability.metric_catalog (metric_id),
  application_id   VARCHAR(64) NOT NULL DEFAULT '*',
  agent_id         VARCHAR(64) NOT NULL DEFAULT '*',
  day              DATE        NOT NULL,
  value            NUMERIC     NOT NULL,
  threshold_status VARCHAR(16) NOT NULL DEFAULT 'ok',  -- ok | warning | breached
  computed_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (metric_id, application_id, agent_id, day)
);
