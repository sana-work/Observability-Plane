-- Catalogs — semantic layers used by enrichment (error mapping) and the
-- chatbot/dashboards (metric definitions).

-- ---------------------------------------------------------------------------
-- error_code_catalog — enrichment stage 5 (Error Code Normaliser) maps raw
-- errors onto this taxonomy. match_pattern is a regex evaluated against
-- "<exception_class>: <message>"; first match by priority wins.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS observability.error_code_catalog (
  error_code      VARCHAR(16) PRIMARY KEY,        -- A0001, T0001, L0001 ...
  category        VARCHAR(32) NOT NULL,           -- agent | tool | llm | rag | guardrail | platform | kafka
  title           TEXT        NOT NULL,
  description     TEXT,
  match_pattern   TEXT        NOT NULL,           -- regex against raw error string
  priority        INT         NOT NULL DEFAULT 100, -- lower = evaluated first
  severity        VARCHAR(16) NOT NULL DEFAULT 'error', -- warning | error | critical
  retryable       BOOLEAN     NOT NULL DEFAULT false,
  runbook_url     TEXT,
  status          VARCHAR(16) NOT NULL DEFAULT 'active',
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_errcat_priority ON observability.error_code_catalog (priority)
  WHERE status = 'active';

-- ---------------------------------------------------------------------------
-- metric_catalog — the chatbot's semantic layer and the KPI dashboard's
-- source of truth: what each metric means, its formula, and where it lives.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS observability.metric_catalog (
  metric_id       VARCHAR(64) PRIMARY KEY,        -- e.g. llm_cost_usd_daily
  metric_name     TEXT        NOT NULL,
  description     TEXT        NOT NULL,           -- chatbot answers quote this
  formula         TEXT        NOT NULL,           -- SQL/PromQL/ES-agg expression
  source_system   VARCHAR(32) NOT NULL
                  CHECK (source_system IN ('obs_events','elasticsearch','prometheus','observability')),
  source_object   TEXT        NOT NULL,           -- table / index pattern / promql metric
  unit            VARCHAR(32) NOT NULL,           -- usd | ms | count | pct | tokens
  aggregation     VARCHAR(16) NOT NULL DEFAULT 'sum', -- sum | avg | p95 | p99 | rate | last
  dimensions      TEXT[]      NOT NULL DEFAULT '{}',  -- allowed group-bys, e.g. {application_id,model_name}
  lob             VARCHAR(32),                    -- NULL = platform-wide
  synonyms        TEXT[]      NOT NULL DEFAULT '{}',  -- NL aliases for the chatbot intent matcher
  owner_team      TEXT,
  status          VARCHAR(16) NOT NULL DEFAULT 'active',
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ---------------------------------------------------------------------------
-- model_pricing — authoritative per-1k-token pricing; enrichment stage 6
-- (Cost Calculator) reads this, overriding the SDK's baked-in estimate table.
-- effective_from allows price changes without losing historical accuracy.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS observability.model_pricing (
  model_name          TEXT         NOT NULL,
  effective_from      DATE         NOT NULL,
  input_usd_per_1k    NUMERIC(12,8) NOT NULL,
  output_usd_per_1k   NUMERIC(12,8) NOT NULL,
  currency            VARCHAR(8)   NOT NULL DEFAULT 'USD',
  provider            TEXT,
  notes               TEXT,
  PRIMARY KEY (model_name, effective_from)
);
