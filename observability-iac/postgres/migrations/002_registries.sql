-- Registries — the Enrichment Consumer (stage 4, Metadata Enricher) joins these
-- in via an in-process TTL cache; dashboards/chatbot use them for ownership,
-- filtering and RBAC. One row per entity (+version where noted).

-- ---------------------------------------------------------------------------
-- application_registry — 1 row / application. Drives per-LOB RBAC everywhere.
-- application_id is what services send as AI_OBS_APPLICATION_ID.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS observability.application_registry (
  application_id   VARCHAR(64) PRIMARY KEY,
  app_name         TEXT        NOT NULL,
  app_type         VARCHAR(32) NOT NULL DEFAULT 'api',      -- api | batch | ui | agent-app
  lob              VARCHAR(32) NOT NULL,                    -- line of business — RBAC + ES index routing
  usecase_id       VARCHAR(64),                             -- business usecase grouping
  csi_id           VARCHAR(32),                             -- inventory/CSI identifier
  owner_team       TEXT        NOT NULL,
  owner_email      TEXT,
  criticality      VARCHAR(16) NOT NULL DEFAULT 'medium',   -- low | medium | high | critical
  environments     TEXT[]      NOT NULL DEFAULT '{dev,staging,prod}',
  status           VARCHAR(16) NOT NULL DEFAULT 'active',   -- active | deprecated | retired
  created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_app_registry_lob ON observability.application_registry (lob);

-- ---------------------------------------------------------------------------
-- service_registry — the 8 fixed platform services (mirror of the frozen
-- contracts/service_names.py enum; conftest.py asserts they stay in sync).
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS observability.service_registry (
  service_name  VARCHAR(64) PRIMARY KEY,      -- must equal a ServiceName enum value
  display_name  TEXT NOT NULL,
  description   TEXT,
  kafka_enabled BOOLEAN NOT NULL DEFAULT false,  -- flips true as Phase 2 onboards each service
  repo_url      TEXT,
  owner_team    TEXT
);

-- ---------------------------------------------------------------------------
-- agent_registry — 1 row / agent / version.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS observability.agent_registry (
  agent_id            VARCHAR(64)  NOT NULL,
  agent_version       VARCHAR(32)  NOT NULL DEFAULT '1',
  agent_name          TEXT         NOT NULL,
  agent_type          VARCHAR(32)  NOT NULL,      -- planner | executor | router | evaluator | custom
  execution_mode      VARCHAR(32)  NOT NULL DEFAULT 'sync',  -- sync | async | event-driven
  application_id      VARCHAR(64)  REFERENCES observability.application_registry (application_id),
  owner_team          TEXT,
  default_model       TEXT,                        -- primary model_name this agent calls
  max_steps           INT,                         -- loop guard, surfaced on Agent dashboard
  status              VARCHAR(16)  NOT NULL DEFAULT 'active',
  created_at          TIMESTAMPTZ  NOT NULL DEFAULT now(),
  PRIMARY KEY (agent_id, agent_version)
);

-- ---------------------------------------------------------------------------
-- tool_registry — 1 row / tool / version. tool_type values match the SDK
-- @trace_tool contract: REST | DB | ServiceNow | RAG | InternalAPI.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS observability.tool_registry (
  tool_id          VARCHAR(64)  NOT NULL,
  tool_version     VARCHAR(32)  NOT NULL DEFAULT '1',
  tool_name        TEXT         NOT NULL,
  tool_type        VARCHAR(32)  NOT NULL
                   CHECK (tool_type IN ('REST','DB','ServiceNow','RAG','InternalAPI')),
  endpoint_url     TEXT,                          -- SLA endpoint for dependency health checks
  sla_latency_ms   INT,                           -- p95 target; Tool dashboard breach marker
  soe_mapping      VARCHAR(64),                   -- system-of-engagement identifier
  owner_team       TEXT,
  status           VARCHAR(16)  NOT NULL DEFAULT 'active',
  created_at       TIMESTAMPTZ  NOT NULL DEFAULT now(),
  PRIMARY KEY (tool_id, tool_version)
);

-- ---------------------------------------------------------------------------
-- rag_registry — 1 row / knowledge base.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS observability.rag_registry (
  rag_id            VARCHAR(64) PRIMARY KEY,
  knowledge_base    TEXT        NOT NULL,
  description       TEXT,
  vector_db_index   TEXT        NOT NULL,          -- pgvector table / index name
  embedding_model   TEXT        NOT NULL,          -- e.g. text-embedding-004
  embedding_dim     INT,
  chunk_strategy    TEXT,                          -- e.g. 'recursive-512-overlap-64'
  lob               VARCHAR(32),
  owner_team        TEXT,
  refresh_schedule  TEXT,                          -- cron of the Consumer Service ingest job
  status            VARCHAR(16) NOT NULL DEFAULT 'active',
  created_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);
