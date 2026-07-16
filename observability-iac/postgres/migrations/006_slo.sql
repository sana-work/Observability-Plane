-- SLO definitions + daily compliance. Enrichment stage 8 (SLO Evaluator)
-- computes 1h/6h burn rates per event window and upserts one row per
-- app/SLO/day into daily_slo_compliance.

CREATE TABLE IF NOT EXISTS observability.slo_definitions (
  slo_id            VARCHAR(64) PRIMARY KEY,
  application_id    VARCHAR(64) NOT NULL REFERENCES observability.application_registry (application_id),
  slo_name          TEXT        NOT NULL,
  sli_type          VARCHAR(32) NOT NULL
                    CHECK (sli_type IN ('availability','latency','quality','cost')),
  -- availability: good = status='success'; latency: good = latency_ms <= latency_target_ms
  target_pct        NUMERIC(6,3) NOT NULL CHECK (target_pct > 0 AND target_pct <= 100), -- e.g. 99.5
  latency_target_ms INT,                          -- required when sli_type='latency'
  event_filter      JSONB       NOT NULL DEFAULT '{}',  -- e.g. {"event_type": "REQUEST_COMPLETED"}
  window_days       INT         NOT NULL DEFAULT 30,    -- rolling error-budget window
  enabled           BOOLEAN     NOT NULL DEFAULT true,
  created_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS observability.daily_slo_compliance (
  slo_id                    VARCHAR(64) NOT NULL REFERENCES observability.slo_definitions (slo_id),
  day                       DATE        NOT NULL,
  good_events               BIGINT      NOT NULL DEFAULT 0,
  total_events              BIGINT      NOT NULL DEFAULT 0,
  sli_pct                   NUMERIC(6,3),
  burn_rate_1h              NUMERIC(10,4),   -- worst 1h burn rate observed that day
  burn_rate_6h              NUMERIC(10,4),
  error_budget_consumed_pct NUMERIC(6,2),    -- cumulative over the rolling window
  breached                  BOOLEAN     NOT NULL DEFAULT false,
  computed_at               TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (slo_id, day)
);
CREATE INDEX IF NOT EXISTS ix_slo_compliance_breached
  ON observability.daily_slo_compliance (day) WHERE breached;
