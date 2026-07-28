# Manual Setup Runbook — Dev Environment (no scripts)

Step-by-step instructions to configure everything by hand — for when you're
working through UIs / a SQL client / change tickets instead of running the
`apply.sh` scripts. The files in this repo are still your **source of truth
for values**: you'll paste their contents rather than execute them.

**Order matters.** Follow the sections top to bottom — later steps assume
earlier ones exist.

**What needs NO manual setup:** `contracts/` (that's Python code vendored
into the packages, not infrastructure), `tests/`, `ci/`, `scripts/`,
`snowflake/` (dormant). For a plain dev environment you can also skip most of
`infra/` (see §6).

---

## 0. Prerequisites

You need reachable instances of: **Kafka** (any 3.x), **PostgreSQL 16**,
**Elasticsearch 8.x + Kibana**. If you don't have shared dev instances, the
docker-compose stack gives you all of them
(`docker compose -f docker-compose.dev.yml up -d`) — using it does not
prevent you from configuring everything manually per this runbook.

Throughout: replace `localhost` endpoints with your dev hosts.

---

## 1. Kafka — create the 3 topics

Reference values: [kafka/topics.yaml](kafka/topics.yaml).

### What to create

| Topic | Partitions | Replication | Config overrides |
|---|---|---|---|
| `ai-obs-events-raw` | 12 (dev: 3 is fine) | 3 (dev single broker: **1**) | `retention.ms=604800000` (7d), `compression.type=lz4`, `max.message.bytes=1048576`; prod-only: `min.insync.replicas=2` |
| `ai-obs-events-processed` | 12 (dev: 3) | 3 (dev: 1) | `retention.ms=259200000` (3d), `compression.type=lz4`, `max.message.bytes=1048576`; prod-only: `min.insync.replicas=2` |
| `ai-obs-dead-letter` | 3 | 3 (dev: 1) | `retention.ms=1209600000` (14d), `compression.type=lz4` |

⚠ On a single-broker dev Kafka you MUST use replication 1 and OMIT
`min.insync.replicas` — otherwise producers will fail.

### How — option A: Kafka CLI (one command per topic)

```bash
kafka-topics --bootstrap-server <broker:9092> --create --topic ai-obs-events-raw \
  --partitions 3 --replication-factor 1 \
  --config retention.ms=604800000 --config compression.type=lz4 --config max.message.bytes=1048576
```

```bash
kafka-topics --bootstrap-server <broker:9092> --create --topic ai-obs-events-processed \
  --partitions 3 --replication-factor 1 \
  --config retention.ms=259200000 --config compression.type=lz4 --config max.message.bytes=1048576
```

```bash
kafka-topics --bootstrap-server <broker:9092> --create --topic ai-obs-dead-letter \
  --partitions 3 --replication-factor 1 \
  --config retention.ms=1209600000 --config compression.type=lz4
```

(Inside the dev docker container the binaries live at
`/opt/kafka/bin/kafka-topics.sh`.)

### How — option B: admin UI (AKHQ / Control Center / Conduktor)

Create topic → enter name, partitions, replication from the table → add the
config overrides under "topic configs" exactly as written.

### Verify

```bash
kafka-topics --bootstrap-server <broker:9092> --describe --topic ai-obs-events-raw
```

Expect the partition count and configs you entered. Also confirm
**auto-topic-creation is OFF** on the broker (`auto.create.topics.enable=false`)
so typos don't silently create junk topics.

---

## 2. PostgreSQL — control plane schema (`observability.*`)

Reference values: the files in [postgres/migrations/](postgres/migrations/) —
**they are plain SQL**; "manual" setup means running them one at a time in
your SQL client (psql, pgAdmin, DBeaver) **in filename order**.

### Steps

Connect to the dev database as a superuser/owner, then open and execute each
file's full contents, in this exact order:

| # | File | What it creates | Quick check after running |
|---|---|---|---|
| 1 | `001_create_schema.sql` | `observability` schema + 5 group roles (obs_admin, obs_enrichment, obs_storage, obs_dashboard, dashboard_ro) | `\dn` shows the schema; `\dgS` shows the roles |
| 2 | `002_registries.sql` | application / service / agent / tool / rag registries | `\dt observability.*registry*` → 5 tables |
| 3 | `003_prompt_registry.sql` | prompt_template_registry + activation audit + one-active-version unique index | insert two 'active' rows for one template → 2nd fails |
| 4 | `004_catalogs.sql` | error_code_catalog, metric_catalog, model_pricing | `\dt observability.*catalog*` |
| 5 | `005_governance.sql` | budget_limits, alert_threshold, dashboard_config, feedback_case | — |
| 6 | `006_slo.sql` | slo_definitions, daily_slo_compliance | — |
| 7 | `007_aggregates.sql` | the 7 agg_* rollup tables | `\dt observability.agg_*` → 7 |
| 8 | `008_budget_accumulator.sql` | budget_accumulator + `add_spend()` function | `\df observability.add_spend` |
| 9 | `009_grants.sql` | least-privilege grants per role | `\dp observability.budget_accumulator` |

If a file errors mid-way, fix and re-run it — every statement is
`IF NOT EXISTS`/`OR REPLACE`, so re-running is safe.

### Seed data (required — enrichment reads these)

Run the three files in [postgres/seed/](postgres/seed/) the same way, in order:

1. `001_registries.sql` — the 8 services, dev app **`app-1234`** (must match
   the SDK's `AI_OBS_APPLICATION_ID`), its $100/monthly budget
2. `002_error_code_catalog.sql` — 21 error codes with regex patterns
3. `003_metric_catalog.sql` — 10 metrics + **model_pricing rows** (must match
   the SDK `cost.py` table)

Seeds are upserts — safe to re-run.

### Verify

```sql
SELECT count(*) FROM observability.service_registry;      -- 8
SELECT count(*) FROM observability.error_code_catalog;    -- ~21
SELECT * FROM observability.add_spend('app-1234', 'gemini-1.5-pro', 0.01);
-- one row, alert_crossed=false
```

---

## 3. PostgreSQL — event firehose schema (`obs_events.*`)

Reference: [postgres-events/migrations/](postgres-events/migrations/). Same
procedure: run each file in your SQL client, in order 001 → 008.

| # | File | Creates |
|---|---|---|
| 1 | `001_obs_events_schema.sql` | partitioned `obs_events.events` (full envelope columns incl. raw `user_id`) + `ensure_month_partitions()` / `drop_old_partitions()` functions |
| 2–7 | `002`…`007` | domain tables: llm_events, agent_events, rag_events, feedback_events, quality_scores, slo_history (all partitioned) |
| 8 | `008_partitions_bootstrap.sql` | creates current+2 months of partitions for ALL parents + grants |

**Manual recurring duty** (the script/CronJob normally does this): on the 1st
of each month, run

```sql
SELECT obs_events.ensure_month_partitions(2);
```

and, once data ages past ~90 days (after the S3 archive export exists):

```sql
SELECT obs_events.drop_old_partitions(3);
```

### Verify

```sql
SELECT count(*) FROM pg_tables WHERE schemaname='obs_events';  -- 7 parents + ~21 partitions
INSERT INTO obs_events.events (event_id, event_type, event_ts, service_name, environment, status)
VALUES ('manual-test-1', 'REQUEST_COMPLETED', now(), 'gssp-gs', 'dev', 'success');
SELECT tableoid::regclass FROM obs_events.events WHERE event_id='manual-test-1';
-- shows the current month's partition; then DELETE the test row
```

---

## 4. Elasticsearch — ILM, component templates, index templates

Best manual tool: **Kibana → Dev Tools** (paste-and-run). Order is
mandatory: ILM policies → component templates → index templates (templates
reference the things before them).

### 4.1 ILM policies (2)

In Dev Tools, run — pasting the **full JSON body** from each file in
[elasticsearch/ilm-policies/](elasticsearch/ilm-policies/):

```
PUT _ilm/policy/hot-warm-30d
{ <contents of hot-warm-30d.json> }

PUT _ilm/policy/compliance-180d
{ <contents of compliance-180d.json> }
```

(Alternative UI: Kibana → Stack Management → Index Lifecycle Policies →
Create policy, entering: hot-warm-30d = warm at 2d with force-merge, delete
at 30d; compliance-180d = warm at 7d, delete at 180d.)

### 4.2 Component templates (2)

From [elasticsearch/component-templates/](elasticsearch/component-templates/):

```
PUT _component_template/obs-common-settings
{ <contents of obs-common-settings.json> }

PUT _component_template/obs-common-mappings
{ <contents of obs-common-mappings.json> }
```

`obs-common-mappings` is the envelope — every ObsEvent field with its type
(`user_id` is a `keyword`, raw by design). Don't retype it; paste the file.

### 4.3 Index templates (11 — one PUT each)

From [elasticsearch/index-templates/](elasticsearch/index-templates/), for
**each** of the 11 files:

```
PUT _index_template/<filename-without-.json>
{ <contents of that file> }
```

The 11 names: `ai-obs-requests`, `ai-obs-errors`, `ai-obs-agent-steps`,
`ai-obs-llm-calls`, `ai-obs-tool-calls`, `ai-obs-rag-events`,
`ai-obs-guardrail-events`, `ai-obs-feedback`, `ai-obs-traces`,
`ai-obs-quality-scores`, `ai-obs-anomalies`.

### Verify

```
GET _index_template/ai-obs-*        → 11 templates
POST ai-obs-testlob-requests-2026.07.28/_doc
{ "event_type": "REQUEST_COMPLETED", "timestamp": "2026-07-28T10:00:00Z",
  "latency_ms": 12.5, "user_id": "SOE12345", "status": "success" }
GET ai-obs-testlob-requests-2026.07.28/_mapping
```

Check: `latency_ms` mapped as `double`, `user_id` as `keyword`,
`timestamp` as `date` (i.e., the template applied — not ES guessing).
Then `DELETE ai-obs-testlob-requests-2026.07.28`.

---

## 5. S3 — archive bucket (skippable in dev)

For a laptop/dev environment you can **skip S3 entirely** — run the
enrichment consumer with `OBS_ENRICH_S3_ENABLED=false`. If your dev env has
an AWS account, configure via console:

1. **Create bucket** `ai-obs-archive-dev` in your region.
2. **Block all public access**: ON (all four checkboxes).
3. **Versioning**: Enable.
4. **Default encryption**: SSE-KMS with your key (SSE-S3 acceptable for dev).
5. **Lifecycle rules** (Management tab) — mirror [s3/lifecycle.json](s3/lifecycle.json):
   - `obs-default-tiering` (whole bucket): transition to Standard-IA at 30d,
     Glacier at 180d; abort incomplete multipart uploads after 7d
   - `debug-bundles-expire` (prefix `debug-bundles/`): expire at 90d
   - `audit-evidence-never-expire` (prefix `audit-evidence/`): IA at 30d,
     Glacier at 90d, no expiration
6. **Create the 9 prefixes** (upload an empty `.keep` file into each):
   `redacted-prompts/ redacted-responses/ raw-traces/ rag-contexts/
   uploaded-documents/ audit-evidence/ debug-bundles/ rca-reports/
   iac-dashboards/`

---

## 6. Monitoring infra — mostly optional for dev

| Component | Dev-manual action | Needed in dev? |
|---|---|---|
| **Tempo** (traces) | Easiest: the docker-compose `tempo` service (config in [dev/tempo-local.yaml](dev/tempo-local.yaml)). Manual alternative: run the tempo binary with that yaml. Point the SDK at `http://localhost:4317`, or just set `AI_OBS_TRACING_ENABLED=false` | Optional — SDK works with tracing off |
| **Prometheus + alerts** | Skip in dev, or run a plain Prometheus scraping the consumer's `:9108` and any service `/metrics`. The alert rules in [infra/kube-prometheus-stack-values.yaml](infra/kube-prometheus-stack-values.yaml) are for the cluster | Optional |
| **Grafana** (internal ops console) | Cluster-only decision; nothing to do in dev (Kibana covers browsing) | No |
| **kminion** (consumer lag) | Cluster-only; in dev check lag manually: `kafka-consumer-groups --bootstrap-server <broker> --describe --group obs-enrichment` | No |
| **Fluent Bit** (log shipping) | Cluster-only DaemonSet; in dev read stdout directly | No |

---

## 7. Final verification — the end-to-end loop

After §1–§4 are done (S3/infra optional):

1. Start the enrichment consumer:
   `OBS_ENRICH_KAFKA_BOOTSTRAP_SERVERS=<broker> OBS_ENRICH_PG_DSN=<dsn> OBS_ENRICH_S3_ENABLED=false obs-enrichment`
2. Run the demo SDK service from
   [../obs-enrichment-consumer/README.md](../obs-enrichment-consumer/README.md)
   §"Setup & run", step 4.
3. `curl "localhost:8005/ask" -H "X-User-ID: SOE12345"`
4. Console-consume `ai-obs-events-processed` → the event chain appears with
   `app_owner_team`, control-plane cost, `enriched_at`.
5. Send garbage to the raw topic → it appears on `ai-obs-dead-letter`
   wrapped as `{reason, failed_at, original}`.

## Checklist

- [ ] 3 topics with correct retention/partitions; broker auto-create OFF
- [ ] `observability.*`: 9 migrations + 3 seeds run; `add_spend()` returns a row
- [ ] `obs_events.*`: 8 migrations run; partitions exist for current+2 months
- [ ] ES: 2 ILM + 2 component + 11 index templates; test doc maps correctly
- [ ] (optional) S3 bucket encrypted/versioned/tiered with 9 prefixes
- [ ] End-to-end loop of §7 works

## ⚠ The honest warning

Manual setup drifts: someone fixes a topic config in dev and forgets the
runbook; three months later staging behaves differently and nobody knows why.
This runbook is fine for a dev environment and for learning what each piece
is — but for staging/prod, run the scripts (they apply these exact values
idempotently) or at minimum re-run the verification queries here after every
manual change. The `tests/test_policy.py` gate only protects the *files*,
not what someone typed into a console.
