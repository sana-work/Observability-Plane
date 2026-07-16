-- SLO history — long-horizon copy of daily compliance for trend queries
-- (the control-plane observability.daily_slo_compliance stays authoritative;
-- this partitioned copy exists so history survives the swap to Snowflake).

CREATE TABLE IF NOT EXISTS obs_events.slo_history (
  slo_id                    VARCHAR(64) NOT NULL,
  event_ts                  TIMESTAMPTZ NOT NULL,   -- day at midnight UTC
  application_id            VARCHAR(64),
  lob                       VARCHAR(32),
  sli_pct                   NUMERIC(6,3),
  burn_rate_1h              NUMERIC(10,4),
  burn_rate_6h              NUMERIC(10,4),
  error_budget_consumed_pct NUMERIC(6,2),
  breached                  BOOLEAN NOT NULL DEFAULT false,
  PRIMARY KEY (slo_id, event_ts)
) PARTITION BY RANGE (event_ts);

CREATE INDEX IF NOT EXISTS ix_slo_hist_app ON obs_events.slo_history (application_id, event_ts DESC);
