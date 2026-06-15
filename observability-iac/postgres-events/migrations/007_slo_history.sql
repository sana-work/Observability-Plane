-- Long-term SLO history (the firehose copy; daily compliance rows also land in
-- observability.daily_slo_compliance for the control plane). Mirrors future sf_slo.
CREATE TABLE IF NOT EXISTS obs_events.slo_history (
  compliance_date           DATE,
  application_id            VARCHAR(64),
  slo_type                  VARCHAR(64),
  target_pct                NUMERIC(5,2),
  achieved_pct              NUMERIC(5,2),
  error_budget_consumed_pct NUMERIC(5,2),
  burn_rate_1h              NUMERIC(8,4),
  burn_rate_6h              NUMERIC(8,4),
  breach_flag               BOOLEAN,
  event_ts                  TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (compliance_date, application_id, slo_type)
);
CREATE INDEX IF NOT EXISTS ix_slo_app ON obs_events.slo_history (application_id, compliance_date);
