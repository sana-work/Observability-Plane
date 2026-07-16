#!/usr/bin/env python3
"""Dead-letter replay — re-drive quarantined events back to ai-obs-events-raw.

The Enrichment Consumer wraps failed events as:
  { "reason": "<stage + error>", "failed_at": "<iso>", "original": { ...ObsEvent... } }

Usage:
  BOOTSTRAP=localhost:9092 ./replay_dead_letter.py --dry-run
  BOOTSTRAP=localhost:9092 ./replay_dead_letter.py --reason-filter schema --limit 100
  BOOTSTRAP=localhost:9092 ./replay_dead_letter.py            # replay everything pending

Uses consumer group obs-dlq-replay with manual commit AFTER the re-produce is
delivered, so a crash never loses a quarantined event.
"""
from __future__ import annotations

import argparse
import json
import os
import sys

from confluent_kafka import Consumer, KafkaError, Producer

DLQ_TOPIC = "ai-obs-dead-letter"
RAW_TOPIC = "ai-obs-events-raw"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true", help="print, do not produce/commit")
    ap.add_argument("--reason-filter", default=None, help="substring match on quarantine reason")
    ap.add_argument("--limit", type=int, default=0, help="max events to replay (0 = all pending)")
    ap.add_argument("--idle-timeout", type=float, default=10.0, help="stop after N s without messages")
    args = ap.parse_args()

    bootstrap = os.environ.get("BOOTSTRAP", "localhost:9092")
    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": "obs-dlq-replay",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    producer = Producer({"bootstrap.servers": bootstrap, "enable.idempotence": True})
    consumer.subscribe([DLQ_TOPIC])

    replayed = skipped = 0
    try:
        while args.limit == 0 or replayed < args.limit:
            msg = consumer.poll(args.idle_timeout)
            if msg is None:
                print(f"idle {args.idle_timeout}s — stopping")
                break
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                print(f"consumer error: {msg.error()}", file=sys.stderr)
                return 1

            try:
                wrapper = json.loads(msg.value())
                reason = wrapper.get("reason", "")
                original = wrapper["original"]
            except (json.JSONDecodeError, KeyError) as exc:
                print(f"unparseable DLQ record at offset {msg.offset()}: {exc} — skipping")
                skipped += 1
                if not args.dry_run:
                    consumer.commit(msg)
                continue

            if args.reason_filter and args.reason_filter not in reason:
                skipped += 1
                if not args.dry_run:
                    consumer.commit(msg)
                continue

            key = (original.get("correlation_id") or original.get("event_id", "")).encode()
            print(f"replay event_id={original.get('event_id')} reason={reason!r}")
            if not args.dry_run:
                producer.produce(RAW_TOPIC, key=key, value=json.dumps(original).encode(),
                                 headers=[("dlq_replayed", b"true")])
                producer.flush(10)
                consumer.commit(msg)
            replayed += 1
    finally:
        consumer.close()

    print(f"done: replayed={replayed} skipped={skipped} dry_run={args.dry_run}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
