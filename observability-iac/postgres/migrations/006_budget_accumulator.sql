-- INTERIM live spend accumulator (Redis not available yet — replaces INCRBYFLOAT).
-- The Enrichment Consumer's cost stage does an atomic upsert per LLM call and compares
-- the running total to observability.budget_limits to fire BUDGET_THRESHOLD_EXCEEDED.
-- Swap to Redis INCRBYFLOAT when Redis lands (see plan.md §15.7 / memory redis-deferred).
CREATE TABLE IF NOT EXISTS observability.budget_accumulator (
  application_id VARCHAR(64),
  environment    VARCHAR(32),
  model_id       VARCHAR(128),
  period         VARCHAR(16),         -- 'daily' | 'monthly'
  period_key     VARCHAR(16),         -- e.g. '2026-06-17' (daily) or '2026-06' (monthly)
  spend_usd      NUMERIC(14,6) DEFAULT 0,
  updated_at     TIMESTAMPTZ DEFAULT now(),
  PRIMARY KEY (application_id, environment, model_id, period, period_key)
);

-- Atomic increment pattern used by the Enrichment Consumer:
--   INSERT INTO observability.budget_accumulator
--     (application_id, environment, model_id, period, period_key, spend_usd)
--   VALUES ($1,$2,$3,$4,$5,$6)
--   ON CONFLICT (application_id, environment, model_id, period, period_key)
--   DO UPDATE SET spend_usd = budget_accumulator.spend_usd + EXCLUDED.spend_usd,
--                 updated_at = now()
--   RETURNING spend_usd;

GRANT SELECT ON observability.budget_accumulator TO dashboard_ro;
