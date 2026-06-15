# observability-iac

Single source of truth for the Observability Plane's contract + infrastructure. Everything
here is applied idempotently by `ci/deploy.yml` on merge to `main` (Phase 0 of `../plan.md`).

## Decisions baked in
- **Kafka Direct Path** — services produce straight to `ai-obs-events-raw`; no OIS / HTTP ingestion.
- **Custom AI-quality layer** — prompt registry (`postgres/migrations/003`), eval scores, Trace Explorer. Langfuse undecided.
- **Snowflake deferred** — `postgres-events/` (`obs_events` schema) is the **interim** firehose; `snowflake/` is written but **not applied** until onboarding. Swap-back: `snowflake/README.md`.
- **50 event types** — `contracts/event_types.py` (reconcile the diagram's "38" label, `plan.md` §15.2).

## Layout
| Dir | What | Applied in Phase 0? |
|---|---|---|
| `contracts/` | `ObsEvent` + `EventType`(50) + `ServiceName`(8) + tests | ✅ (test gate) |
| `kafka/` | topic creation (`create_topics.sh`, `topics.yaml`) | ✅ |
| `postgres/` | `observability` control-plane schema + seed (`metric_catalog`, `error_code_catalog`) | ✅ |
| `postgres-events/` | `obs_events` firehose schema (interim Snowflake stand-in) | ✅ |
| `elasticsearch/` | component + index templates (generated in `apply.sh`) + ILM | ✅ |
| `snowflake/` | target `sf_*` DDL | ❌ deferred |
| `s3/` | payload bucket + lifecycle | ✅ |
| `infra/` | Tempo / kube-prometheus-stack / kminion / Fluent Bit | ✅ (separate cluster workflow) |
| `ci/` | `deploy.yml` | — |

## Run locally
```bash
# contract tests
PYTHONPATH=. pytest contracts/ -q
# apply (needs env/secrets)
KAFKA_BROKERS=... bash kafka/create_topics.sh
for f in postgres/migrations/*.sql postgres/seed/*.sql postgres-events/migrations/*.sql; do psql "$PG_DSN" -f "$f"; done
ES=https://es:9200 bash elasticsearch/apply.sh
ENV=dev KMS_KEY=... bash s3/apply.sh
```

## Phase 0 acceptance
See `../phase-0-foundation.md` — the 9-item Definition of Done gates entry to Phase 1.
