"""Fire-and-forget Kafka event emission.

Design contract with the 8 producing services:
  * emit_event() NEVER raises and NEVER blocks the request path.
  * Delivery failures are logged + counted, not surfaced to callers —
    observability must not take the business path down.
  * Partition key = correlation_id → all events of one request land on one
    partition, so the Enrichment Consumer sees them in order.
  * Envelope validation happens here (fail fast, in tests) via the vendored
    ObsEvent contract; the Enrichment Consumer re-validates authoritatively.
"""
from __future__ import annotations

import atexit
import logging
import threading

from confluent_kafka import KafkaException, Producer

from .config import ObsSettings, get_settings
from .context import get_context
from .contracts import ObsEvent
from .kafka_headers import current_trace_ids, inject_trace_headers

logger = logging.getLogger("ai_obs_sdk.emitter")

_lock = threading.Lock()
_emitter: "KafkaEmitter | None" = None


class KafkaEmitter:
    def __init__(self, settings: ObsSettings):
        self._settings = settings
        conf: dict = {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "security.protocol": settings.kafka_security_protocol,
            "linger.ms": settings.kafka_linger_ms,
            "compression.type": settings.kafka_compression,
            "queue.buffering.max.messages": settings.kafka_queue_max_messages,
            "delivery.timeout.ms": settings.kafka_delivery_timeout_ms,
            "enable.idempotence": True,
            "client.id": f"ai-obs-sdk.{settings.service_name}",
        }
        if settings.kafka_sasl_mechanism:
            conf.update(
                {
                    "sasl.mechanism": settings.kafka_sasl_mechanism,
                    "sasl.username": settings.kafka_sasl_username,
                    "sasl.password": settings.kafka_sasl_password,
                }
            )
        self._producer = Producer(conf)
        self.dropped = 0
        self.delivered = 0
        atexit.register(self.flush)

    # -- delivery report runs on the producer's poll thread --
    def _on_delivery(self, err, msg) -> None:
        if err is not None:
            self.dropped += 1
            logger.warning("obs event delivery failed: %s (topic=%s)", err, msg.topic())
        else:
            self.delivered += 1

    def emit(self, event: ObsEvent) -> None:
        try:
            self._producer.produce(
                topic=self._settings.kafka_topic_raw,
                key=(event.correlation_id or event.event_id).encode(),
                value=event.model_dump_json().encode(),
                headers=inject_trace_headers(event.correlation_id or event.event_id),
                on_delivery=self._on_delivery,
            )
            self._producer.poll(0)  # serve delivery callbacks, non-blocking
        except BufferError:
            # local queue full — drop rather than block the request path
            self.dropped += 1
            logger.warning("obs event dropped: local producer queue full")
        except (KafkaException, Exception):  # noqa: BLE001 — never propagate
            self.dropped += 1
            logger.exception("obs event emit failed")

    def flush(self, timeout: float = 5.0) -> None:
        try:
            self._producer.flush(timeout)
        except Exception:  # noqa: BLE001
            logger.exception("obs producer flush failed")


def get_emitter() -> KafkaEmitter:
    global _emitter
    if _emitter is None:
        with _lock:
            if _emitter is None:
                _emitter = KafkaEmitter(get_settings())
    return _emitter


def emit_event(
    event_type: str,
    *,
    status: str = "success",
    latency_ms: float | None = None,
    error_code: str | None = None,
    http_status: int | None = None,
    payload: dict | None = None,
    component: str | None = None,
) -> None:
    """The one-line producer API used by all 8 services.

    Envelope fields (correlation_id, span ids, service identity, user_id)
    are filled from ObsSettings + the current ObsContext automatically.
    """
    settings = get_settings()
    if not settings.enabled:
        return
    try:
        ctx = get_context()
        trace_id, otel_span_id = current_trace_ids()
        event = ObsEvent(
            event_type=event_type,
            service_name=settings.service_name,
            environment=settings.environment,
            application_id=settings.application_id,
            lob=settings.lob,
            component=component,
            correlation_id=ctx.correlation_id,
            request_id=ctx.request_id,
            trace_id=trace_id or ctx.trace_id,
            span_id=otel_span_id or ctx.span_id,
            parent_span_id=ctx.parent_span_id,
            tenant_id=ctx.tenant_id,
            user_id=ctx.user_id,
            status=status,
            latency_ms=latency_ms,
            error_code=error_code,
            http_status=http_status,
            payload=payload or {},
        )
        get_emitter().emit(event)
    except Exception:  # noqa: BLE001 — validation error, misconfig, anything: log, never raise
        logger.exception("emit_event(%s) failed", event_type)
