-- Phase 0 / Task 0.3 — control-plane schema. Idempotent.
-- This schema is CONTROL PLANE ONLY. The event firehose lives in the separate
-- obs_events schema (postgres-events/migrations), NOT here.
CREATE SCHEMA IF NOT EXISTS observability;
