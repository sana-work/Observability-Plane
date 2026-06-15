-- RBAC — the Custom Dashboard Service reads with a least-privilege role.
DO $$ BEGIN
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'dashboard_ro') THEN
    CREATE ROLE dashboard_ro NOLOGIN;
  END IF;
END $$;

GRANT USAGE ON SCHEMA observability TO dashboard_ro;
GRANT SELECT ON ALL TABLES IN SCHEMA observability TO dashboard_ro;
ALTER DEFAULT PRIVILEGES IN SCHEMA observability GRANT SELECT ON TABLES TO dashboard_ro;
