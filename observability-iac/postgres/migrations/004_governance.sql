-- Governance / config — semantic catalog, error mapping, budgets, SLO, alerts, dashboards, feedback.
CREATE TABLE IF NOT EXISTS observability.metric_catalog (
  metric_name  VARCHAR(64) PRIMARY KEY,
  formula      TEXT,
  source_table TEXT,           -- points at obs_events.* now; sf_* after Snowflake onboards
  lob          VARCHAR(32),
  unit         TEXT
);

CREATE TABLE IF NOT EXISTS observability.error_code_catalog (
  error_code  VARCHAR(64) PRIMARY KEY,
  raw_pattern TEXT,
  category    VARCHAR(32)
);

CREATE TABLE IF NOT EXISTS observability.budget_limits (
  application_id VARCHAR(64),
  environment    VARCHAR(32),
  model_id       VARCHAR(128),
  period         VARCHAR(16),         -- 'daily' | 'monthly'
  max_spend_usd  DECIMAL(10,4),
  alert_at_pct   INT DEFAULT 80,
  PRIMARY KEY (application_id, environment, model_id, period)
);

CREATE TABLE IF NOT EXISTS observability.daily_slo_compliance (
  compliance_date           DATE,
  application_id            VARCHAR(64),
  slo_type                  VARCHAR(64),   -- 'availability' | 'latency_p95' | 'error_rate'
  target_pct                NUMERIC(5,2),
  achieved_pct              NUMERIC(5,2),
  error_budget_consumed_pct NUMERIC(5,2),
  burn_rate_1h              NUMERIC(8,4),
  burn_rate_6h              NUMERIC(8,4),
  breach_flag               BOOLEAN DEFAULT FALSE,
  PRIMARY KEY (compliance_date, application_id, slo_type)
);

CREATE TABLE IF NOT EXISTS observability.alert_threshold (
  metric_name VARCHAR(64),
  comparator  VARCHAR(4),     -- '>' '>=' '<' '<='
  threshold   NUMERIC,
  window      TEXT,
  PRIMARY KEY (metric_name, comparator, window)
);

CREATE TABLE IF NOT EXISTS observability.dashboard_config (
  page   VARCHAR(64),
  widget VARCHAR(64),
  spec   JSONB,
  PRIMARY KEY (page, widget)
);

CREATE TABLE IF NOT EXISTS observability.feedback_case (
  feedback_id        VARCHAR(64) PRIMARY KEY,
  correlation_id     VARCHAR(64),
  rating             INT,
  category           TEXT,
  status             VARCHAR(16) DEFAULT 'open',   -- open | reviewed | fixed
  linked_incident_id TEXT,
  created_at         TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_feedback_corr ON observability.feedback_case (correlation_id);
