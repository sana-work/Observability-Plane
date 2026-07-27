"""The consume→enrich→produce→commit loop.

Delivery guarantee: at-least-once with commit-after-produce.
  1. consume a batch from ai-obs-events-raw (manual commit only)
  2. each message: run the pipeline
       ok            -> produce to ai-obs-events-processed (same key + headers)
       DeadLetterError -> produce the wrapped original to ai-obs-dead-letter
  3. flush the producer
  4. commit the batch's offsets ONLY if every produce was delivered.
     Otherwise commit nothing — the whole batch is redelivered. Downstream
     writers dedupe on event_id (ES _id, PG ON CONFLICT), so replays are safe.

A crash between produce and commit re-emits some events; it never loses one.
"""
from __future__ import annotations

import json
import logging
import signal
import sys
import time

from confluent_kafka import Consumer, Producer

from .config import EnrichSettings, get_settings
from .control_plane import ControlPlane
from .dead_letter import wrap
from .deps import Deps
from .errors import DeadLetterError
from .metrics import (
    BATCH_LATENCY,
    EVENTS_DEAD_LETTERED,
    EVENTS_PROCESSED,
    PIPELINE_UP,
    start_metrics_server,
)
from .pipeline import process
from .redactor import build_redactor
from .s3_archiver import S3Archiver
from .slo import SloTracker

logger = logging.getLogger("obs_enrichment.consumer")


class _JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        entry = {
            "timestamp": self.formatTime(record, "%Y-%m-%dT%H:%M:%S%z"),
            "level": record.levelname,
            "logger": record.name,
            "event": record.getMessage(),
        }
        if record.exc_info:
            entry["exception"] = self.formatException(record.exc_info)
        return json.dumps(entry)


def _configure_logging(level: str) -> None:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(_JsonFormatter())
    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(getattr(logging, level.upper(), logging.INFO))


def _kafka_common(settings: EnrichSettings) -> dict:
    conf: dict = {
        "bootstrap.servers": settings.kafka_bootstrap_servers,
        "security.protocol": settings.kafka_security_protocol,
    }
    if settings.kafka_sasl_mechanism:
        conf.update(
            {
                "sasl.mechanism": settings.kafka_sasl_mechanism,
                "sasl.username": settings.kafka_sasl_username,
                "sasl.password": settings.kafka_sasl_password,
            }
        )
    return conf


def build_deps(settings: EnrichSettings) -> Deps:
    return Deps(
        settings=settings,
        control_plane=ControlPlane(settings),
        redactor=build_redactor(settings),
        archiver=S3Archiver(settings) if settings.s3_enabled else None,
        slo_tracker=None,  # filled below — needs the control plane
    )


class EnrichmentLoop:
    def __init__(self, settings: EnrichSettings, deps: Deps, consumer: Consumer, producer: Producer):
        self._settings = settings
        self._deps = deps
        self._consumer = consumer
        self._producer = producer
        self._running = True
        self._delivery_failures = 0

    def stop(self, *_args) -> None:
        logger.info("shutdown requested")
        self._running = False

    def _on_delivery(self, err, _msg) -> None:
        if err is not None:
            self._delivery_failures += 1
            logger.error("produce delivery failed: %s", err)

    def _handle_message(self, msg) -> None:
        try:
            event = process(msg.value(), msg.headers(), self._deps)
        except DeadLetterError as exc:
            stage = exc.reason.split(":", 1)[0]
            self._producer.produce(
                self._settings.topic_dead_letter,
                key=msg.key(),
                value=wrap(msg.value(), exc.reason),
                on_delivery=self._on_delivery,
            )
            EVENTS_DEAD_LETTERED.labels(stage=stage).inc()
            return

        self._producer.produce(
            self._settings.topic_processed,
            key=msg.key() or (event.correlation_id or event.event_id).encode(),
            value=event.model_dump_json().encode(),
            headers=msg.headers(),
            on_delivery=self._on_delivery,
        )
        EVENTS_PROCESSED.labels(event_type=str(event.event_type)).inc()

    def run(self) -> None:
        self._consumer.subscribe([self._settings.topic_raw])
        PIPELINE_UP.set(1)
        logger.info(
            "consuming %s -> %s (group=%s)",
            self._settings.topic_raw, self._settings.topic_processed, self._settings.consumer_group,
        )
        try:
            while self._running:
                msgs = self._consumer.consume(
                    num_messages=self._settings.batch_max_messages,
                    timeout=self._settings.poll_timeout_s,
                )
                if not msgs:
                    self._producer.poll(0)
                    continue

                start = time.perf_counter()
                self._delivery_failures = 0
                had_consume_error = False
                for msg in msgs:
                    if msg.error():
                        logger.error("consume error: %s", msg.error())
                        had_consume_error = True
                        continue
                    self._handle_message(msg)

                remaining = self._producer.flush(self._settings.produce_flush_timeout_s)
                if remaining == 0 and self._delivery_failures == 0 and not had_consume_error:
                    self._consumer.commit(asynchronous=False)
                else:
                    logger.error(
                        "batch NOT committed (undelivered=%s failures=%s) — will be redelivered",
                        remaining, self._delivery_failures,
                    )
                BATCH_LATENCY.observe(time.perf_counter() - start)
        finally:
            PIPELINE_UP.set(0)
            self._consumer.close()
            self._producer.flush(5)
            logger.info("stopped cleanly")


def main() -> None:
    settings = get_settings()
    _configure_logging(settings.log_level)
    start_metrics_server(settings.metrics_port)

    deps = build_deps(settings)
    if settings.slo_enabled:
        deps.slo_tracker = SloTracker(deps.control_plane)

    consumer = Consumer(
        {
            **_kafka_common(settings),
            "group.id": settings.consumer_group,
            "enable.auto.commit": False,          # commit-after-produce, manually
            "auto.offset.reset": "earliest",
            "max.poll.interval.ms": 600_000,      # GLiNER cold batches can be slow
            "client.id": "obs-enrichment-consumer",
        }
    )
    producer = Producer(
        {
            **_kafka_common(settings),
            "enable.idempotence": True,
            "compression.type": "lz4",
            "linger.ms": 20,
            "client.id": "obs-enrichment-producer",
        }
    )

    loop = EnrichmentLoop(settings, deps, consumer, producer)
    signal.signal(signal.SIGTERM, loop.stop)
    signal.signal(signal.SIGINT, loop.stop)
    loop.run()


if __name__ == "__main__":
    main()
