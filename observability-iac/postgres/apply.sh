#!/usr/bin/env bash
# Migration runner for the control plane (observability.*).
#   PGURL=postgres://user:pass@host:5432/db ./apply.sh [--with-seed]
# Tracks applied files in public.obs_schema_migrations; each migration runs
# in its own transaction; seeds are idempotent upserts and always safe to re-run.
set -euo pipefail
cd "$(dirname "$0")"
PGURL="${PGURL:?set PGURL, e.g. postgres://postgres:obs@localhost:5432/postgres}"

psql "$PGURL" -v ON_ERROR_STOP=1 -q -c "
  CREATE TABLE IF NOT EXISTS public.obs_schema_migrations (
    filename   TEXT PRIMARY KEY,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
  );"

for f in migrations/*.sql; do
  name="$(basename "$f")"
  if [[ -n "$(psql "$PGURL" -tAq -c "SELECT 1 FROM public.obs_schema_migrations WHERE filename='ctl/$name'")" ]]; then
    echo "== skip  $name (already applied)"
    continue
  fi
  echo "== apply $name"
  psql "$PGURL" -v ON_ERROR_STOP=1 -q -1 -f "$f"
  psql "$PGURL" -v ON_ERROR_STOP=1 -q -c \
    "INSERT INTO public.obs_schema_migrations (filename) VALUES ('ctl/$name')"
done

if [[ "${1:-}" == "--with-seed" ]]; then
  for f in seed/*.sql; do
    echo "== seed  $(basename "$f")"
    psql "$PGURL" -v ON_ERROR_STOP=1 -q -1 -f "$f"
  done
fi
echo "== control plane up to date"
