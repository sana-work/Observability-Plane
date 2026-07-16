-- Governance & configuration — budgets, alert rules, dashboard config,
-- feedback workflow.

-- ---------------------------------------------------------------------------
-- budget_limits — 1 row / application / model / period. Enrichment stage 6
-- compares the accumulator against these and emits BUDGET_THRESHOLD_EXCEEDED.
-- model_name = '*' means "all models for this app".
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS observability.budget_limits (
  application_id  VARCHAR(64) NOT NULL REFERENCES observability.application_registry (application_id),
  model_name      TEXT        NOT NULL DEFAULT '*',
  period          VARCHAR(16) NOT NULL DEFAULT 'monthly'
                  CHECK (period IN ('daily','weekly','monthly')),
  max_spend_usd   NUMERIC(12,2) NOT NULL CHECK (max_spend_usd > 0),
  alert_at_pct    INT         NOT NULL DEFAULT 80 CHECK (alert_at_pct BETWEEN 1 AND 100),
  hard_stop       BOOLEAN     NOT NULL DEFAULT false,  -- true → platform may throttle at 100%
  owner_email     TEXT,
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (application_id, model_name, period)
);

-- ---------------------------------------------------------------------------
-- alert_threshold — generic alert rules the dashboard backend evaluates and
-- routes (email/Slack); Prometheus-native alerts live in the helm values.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS observability.alert_threshold (
  alert_id        VARCHAR(64) PRIMARY KEY,
  metric_id       VARCHAR(64) NOT NULL REFERENCES observability.metric_catalog (metric_id),
  comparator      VARCHAR(4)  NOT NULL CHECK (comparator IN ('>','>=','<','<=','==')),
  threshold       NUMERIC     NOT NULL,
  window_minutes  INT         NOT NULL DEFAULT 15,
  scope           JSONB       NOT NULL DEFAULT '{}',   -- {"application_id": "...", "lob": "..."}
  severity        VARCHAR(16) NOT NULL DEFAULT 'warning',
  notify_channel  TEXT        NOT NULL,                -- slack:#ai-obs-alerts | email:team@...
  enabled         BOOLEAN     NOT NULL DEFAULT true,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ---------------------------------------------------------------------------
-- dashboard_config — widget/page definitions for the Custom Dashboard Service
-- (also exported to S3 iac-dashboards/ by CI for versioning).
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS observability.dashboard_config (
  widget_id     VARCHAR(64) PRIMARY KEY,
  page          VARCHAR(64) NOT NULL,          -- platform-overview | cost | kpi | rag-quality | ...
  title         TEXT        NOT NULL,
  widget_type   VARCHAR(32) NOT NULL,          -- timeseries | stat | table | heatmap | trace-tree
  query_def     JSONB       NOT NULL,          -- {source, metric_id | raw query, dimensions, filters}
  layout        JSONB       NOT NULL DEFAULT '{}',   -- {x,y,w,h}
  visibility    VARCHAR(16) NOT NULL DEFAULT 'lob'   -- platform | lob | team
                CHECK (visibility IN ('platform','lob','team')),
  lob           VARCHAR(32),
  owner_team    TEXT,
  updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ---------------------------------------------------------------------------
-- feedback_case — workflow on top of FEEDBACK_SUBMITTED events
-- (open → reviewed → fixed), joined to traces by correlation_id.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS observability.feedback_case (
  case_id         BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  feedback_id     VARCHAR(64) NOT NULL UNIQUE,   -- from the FEEDBACK_SUBMITTED event payload
  correlation_id  VARCHAR(64),
  application_id  VARCHAR(64),
  agent_id        VARCHAR(64),
  rating          SMALLINT CHECK (rating BETWEEN 1 AND 5),
  category        VARCHAR(32),                   -- wrong-answer | slow | tool-failed | irrelevant | unsafe
  status          VARCHAR(16) NOT NULL DEFAULT 'open'
                  CHECK (status IN ('open','reviewed','in_progress','fixed','wont_fix')),
  assigned_to     TEXT,
  linked_incident TEXT,                          -- ServiceNow / Jira reference
  resolution_note TEXT,
  opened_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
  resolved_at     TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS ix_feedback_case_status ON observability.feedback_case (status, opened_at);
CREATE INDEX IF NOT EXISTS ix_feedback_case_corr   ON observability.feedback_case (correlation_id);
