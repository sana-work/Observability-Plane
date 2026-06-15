-- Registries — joined in during enrichment to add lob/owner/model metadata.
CREATE TABLE IF NOT EXISTS observability.application_registry (
  application_id VARCHAR(64) PRIMARY KEY,
  lob           VARCHAR(32),
  soe_id        VARCHAR(64),
  owner_team    VARCHAR(64),
  environment   VARCHAR(16)
);

CREATE TABLE IF NOT EXISTS observability.agent_registry (
  agent_id VARCHAR(64) PRIMARY KEY,
  name     TEXT,
  version  TEXT,
  type     TEXT
);

CREATE TABLE IF NOT EXISTS observability.tool_registry (
  tool_id VARCHAR(64) PRIMARY KEY,
  name    TEXT,
  type    TEXT
);

CREATE TABLE IF NOT EXISTS observability.rag_registry (
  rag_id          VARCHAR(64) PRIMARY KEY,
  knowledge_base  TEXT,
  embedding_model TEXT
);
