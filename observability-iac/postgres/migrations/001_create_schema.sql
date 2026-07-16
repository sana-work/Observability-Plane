-- Phase 0 / control plane — schema + roles.
-- The `observability` schema holds ONLY authoritative config, registries,
-- catalogs, governance and rollups. The event firehose lives in `obs_events`
-- (see ../postgres-events) so the eventual Snowflake swap is clean.

CREATE SCHEMA IF NOT EXISTS observability;

-- Roles (NOLOGIN group roles; actual service users are GRANTed into these).
DO $$
BEGIN
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'obs_admin') THEN
    CREATE ROLE obs_admin NOLOGIN;              -- migrations, IaC CI
  END IF;
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'obs_enrichment') THEN
    CREATE ROLE obs_enrichment NOLOGIN;         -- enrichment consumer: read registries/catalogs, write budgets+SLO
  END IF;
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'obs_storage') THEN
    CREATE ROLE obs_storage NOLOGIN;            -- storage consumer: write aggregates
  END IF;
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'obs_dashboard') THEN
    CREATE ROLE obs_dashboard NOLOGIN;          -- dashboard/chatbot backend: read all, write dashboard_config/feedback_case/prompt_registry
  END IF;
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'dashboard_ro') THEN
    CREATE ROLE dashboard_ro NOLOGIN;           -- pure read-only (chatbot query planner, BI extracts)
  END IF;
END
$$;

COMMENT ON SCHEMA observability IS
  'Observability Plane control plane: registries, catalogs, governance, SLO, rollups. Event firehose is in obs_events.';
