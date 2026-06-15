#!/usr/bin/env bash
# Phase 0 / Task 0.2 — create the 3 observability topics. Idempotent (--if-not-exists).
set -euo pipefail
B="${KAFKA_BROKERS:?set KAFKA_BROKERS=host:9092[,host:9092]}"

# All 8 services produce all 50 event types here — unvalidated, unredacted.
kafka-topics.sh --bootstrap-server "$B" --create --if-not-exists --topic ai-obs-events-raw \
  --partitions 12 --replication-factor 3 \
  --config retention.ms=604800000 \
  --config compression.type=lz4 \
  --config min.insync.replicas=2

# Enriched, validated, PII-redacted output of the Enrichment Consumer.
kafka-topics.sh --bootstrap-server "$B" --create --if-not-exists --topic ai-obs-events-processed \
  --partitions 12 --replication-factor 3 \
  --config retention.ms=259200000 \
  --config compression.type=lz4 \
  --config min.insync.replicas=2

# Failed validation/enrichment — held 14 days for debugging and replay.
kafka-topics.sh --bootstrap-server "$B" --create --if-not-exists --topic ai-obs-dead-letter \
  --partitions 3 --replication-factor 3 \
  --config retention.ms=1209600000

echo "topics ready:"
kafka-topics.sh --bootstrap-server "$B" --describe --topic ai-obs-events-raw
kafka-topics.sh --bootstrap-server "$B" --describe --topic ai-obs-events-processed
kafka-topics.sh --bootstrap-server "$B" --describe --topic ai-obs-dead-letter
