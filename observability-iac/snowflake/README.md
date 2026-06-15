# Snowflake DDL — DEFERRED (not applied in Phase 0)

**Snowflake is not yet onboarded.** These files are the *target* event-store DDL. In Phase 0
we instead apply the Postgres `obs_events` schema (`../postgres-events/migrations/`) as the
interim firehose. `ci/deploy.yml` intentionally does **not** run this directory.

**Swap-back (when Snowflake onboards) — see `plan.md` §5.9 / §15.6:**
1. Apply `000_warehouse_role.sql` + `001_sf_events.sql … 007_sf_slo.sql`.
2. Point the Storage Consumer's analytics writer at Snowflake (dual-write for a cutover window).
3. Backfill history from `obs_events.*` + S3.
4. Retarget `metric_catalog.source_table` and dashboard/chatbot queries from `obs_events.*` → `sf_*`.
5. Shrink the Postgres firehose window or drop the `obs_events` schema.

`event_id` / `correlation_id` are identical across both, so no contract change.
