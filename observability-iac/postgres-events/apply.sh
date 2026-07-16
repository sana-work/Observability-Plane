#!/usr/bin/env bash
# Migration runner for the event firehose (obs_events.*).
#   PGURL=postgres://user:pass@host:5432/db ./apply.sh
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
  if [[ -n "$(psql "$PGURL" -tAq -c "SELECT 1 FROM public.obs_schema_migrations WHERE filename='evt/$name'")" ]]; then
    echo "== skip  $name (already applied)"
    continue
  fi
  echo "== apply $name"
  psql "$PGURL" -v ON_ERROR_STOP=1 -q -1 -f "$f"
  psql "$PGURL" -v ON_ERROR_STOP=1 -q -c \
    "INSERT INTO public.obs_schema_migrations (filename) VALUES ('evt/$name')"
done
echo "== obs_events up to date"
