# observability-iac — Phase 0: Foundation, Infrastructure & Contract

Everything the Observability Plane needs to exist **before** any service emits
an event. Idempotent, environment-agnostic, applied by CI (`ci/deploy.yml`).

## Layout & apply order

| # | Directory | What | Apply |
|---|---|---|---|
| 1 | `contracts/` | Frozen wire contract: `ObsEvent` envelope (schema 1.0), 50 `EventType`s, 8 `ServiceName`s | vendored into consumers/SDK; `pytest contracts/tests` |
| 2 | `kafka/` | 3 topics (raw 7d / processed 3d / dead-letter 14d) | `BOOTSTRAP=... ./kafka/create_topics.sh` |
| 3 | `postgres/` | Control plane `observability.*`: registries, prompt registry, catalogs, governance, SLO, aggregates, budget accumulator, grants + seeds | `PGURL=... ./postgres/apply.sh --with-seed` |
| 4 | `postgres-events/` | Interim firehose `obs_events.*` (Snowflake stand-in): partitioned events + domain tables + partition management fns | `PGURL=... ./postgres-events/apply.sh` |
| 5 | `elasticsearch/` | ILM (30d default / 180d compliance), envelope component templates, 11 index-family templates | `ES_URL=... ./elasticsearch/apply.sh` |
| 6 | `s3/` | Archive bucket: SSE-KMS, versioning, lifecycle tiering, 9 prefixes | `BUCKET=... KMS_KEY_ID=... ./s3/apply.sh` |
| 7 | `infra/` | kube-prometheus-stack (**Grafana = internal-only ops console, no ingress**) + alert rules, Tempo (S3), kminion, Fluent Bit | helm / kubectl per file header |
| 8 | `scripts/` | Dead-letter replay | `BOOTSTRAP=... ./scripts/replay_dead_letter.py --dry-run` |

## Compatibility contract with ai-observability-sdk (Phase 1)

Enforced by `tests/test_policy.py` (run in CI on every PR touching either side):

1. `contracts/*.py` byte-identical to `ai-observability-sdk/ai_obs_sdk/contracts/`
2. Topic names = SDK `ObsSettings` defaults (`ai-obs-events-raw` etc.)
3. ES common mappings cover every `ObsEvent` envelope field
4. `service_registry` seed = the frozen `ServiceName` enum
5. `model_pricing` seed = the SDK `cost.py` estimate table
6. All 11 roadmap index families templated; ILM references resolve

Change management: any envelope change bumps `schema_version`, lands here
first, then is re-vendored into the SDK in the same PR.

## Local dev stack

```bash
docker compose -f docker-compose.dev.yml up -d      # kafka + pg + es + kibana + tempo
KAFKA_ENV=dev BOOTSTRAP=localhost:9092 ./kafka/create_topics.sh
PGURL=postgres://postgres:obs@localhost:5432/postgres ./postgres/apply.sh --with-seed
PGURL=postgres://postgres:obs@localhost:5432/postgres ./postgres-events/apply.sh
ES_URL=http://localhost:9200 ./elasticsearch/apply.sh
pytest tests/ contracts/tests -v                     # policy + contract gates
```

Then point any service at it with the SDK's `.env.example` values
(`AI_OBS_KAFKA_BOOTSTRAP_SERVERS=localhost:9092`,
`AI_OBS_OTLP_ENDPOINT=http://localhost:4317`) — the seeded `app-1234`
application and monthly budget make the full loop work out of the box.

## Interim decisions encoded here

- **No Snowflake yet** → `obs_events.*` partitioned Postgres schema, ~90-day
  window (`ensure_month_partitions` / `drop_old_partitions`), archive to S3
  `raw-traces/` before drop.
- **No Redis yet** → `observability.budget_accumulator` + `add_spend()`
  atomic upsert (returns alert/cap crossings exactly once).
- **Grafana = internal-only ops console** (updated 2026-07-15) → deployed
  with kube-prometheus-stack but ClusterIP-only, no ingress, no anonymous
  access; platform team reaches it via
  `kubectl -n observability port-forward svc/kps-grafana 3000:80`.
  Create the admin secret once per cluster before installing the chart:
  `kubectl -n observability create secret generic grafana-admin-credentials
  --from-literal=admin-user=admin --from-literal=admin-password=<generated>`.
  Stakeholder dashboards remain the Custom Dashboard Service, which queries
  Prometheus/ES/Postgres directly (COIN-JWT + per-LOB RBAC). Phase 5 will
  evaluate Grafana's OSS React packages inside the custom dashboard.

## Exit criteria (Phase 0 done)

- [ ] 3 topics live with correct retention/partitions (`kafka-topics --describe`)
- [ ] `observability.*` + `obs_events.*` applied; seeds loaded; grants in place
- [ ] ES: 2 ILM policies, 2 component templates, 11 index templates
- [ ] S3 bucket encrypted, lifecycle applied, prefixes present
- [ ] Tempo answers on :4317; kminion metrics scraped; Fluent Bit shipping
- [ ] Grafana ops console reachable via port-forward only (no ingress/route exists); Prometheus + Tempo datasources work; LOB users have no accounts
- [ ] CI validate job green (contract + policy + SQL dry-run)
