-- Seed: the 8 platform services (must mirror contracts/service_names.py — the
-- conftest policy test enforces this) + a dev application matching
-- ai-observability-sdk/.env.example (AI_OBS_APPLICATION_ID=app-1234).

INSERT INTO observability.service_registry (service_name, display_name, description, kafka_enabled) VALUES
  ('agentic-orchestration', 'Agentic Orchestration', 'Multi-agent routing, planning, LLM gate, HIL coordination', true),
  ('agent-executor',        'Agent Executor',        'Stateful multi-step agent execution engine',               true),
  ('gssp-gs',               'GSSP GS — LLM Gateway', 'Generic Generation Service; proxies VertexAI/Claude/Llama', false),
  ('gssp-qs',               'GSSP QS — Query Service','RAG workflow orchestration, guardrails, semantic cache',   false),
  ('gssp-rs',               'GSSP RS — Retrieval',   'Document retrieval and embedding lookup (PGVector/R2D2)',  false),
  ('consumer-service',      'Consumer Service',      'Document ingestion scheduler driving the RAG pipeline',    false),
  ('data-ingestion',        'Data Ingestion',        'REST document ingest, embeddings via SageMaker',           false),
  ('user-feedback',         'User Feedback',         'Feedback capture API linked to traces',                    false)
ON CONFLICT (service_name) DO UPDATE
  SET display_name = EXCLUDED.display_name, description = EXCLUDED.description;

INSERT INTO observability.application_registry
  (application_id, app_name, app_type, lob, usecase_id, owner_team, criticality) VALUES
  ('app-1234', 'SDK Dev Sandbox', 'api', 'wealth', 'uc-dev-sandbox', 'observability-platform', 'low')
ON CONFLICT (application_id) DO NOTHING;

INSERT INTO observability.budget_limits
  (application_id, model_name, period, max_spend_usd, alert_at_pct) VALUES
  ('app-1234', '*', 'monthly', 100.00, 80)
ON CONFLICT DO NOTHING;
