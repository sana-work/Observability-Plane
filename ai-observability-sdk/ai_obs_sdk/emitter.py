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
from typing import Any

from confluent_kafka import Producer

from .config import ObsSettings, get_settings
from .context import get_context
from .contracts import ObsEvent
from .kafka_headers import current_trace_ids, inject_trace_headers

logger = logging.getLogger("ai_obs_sdk.emitter")

_lock = threading.Lock()
_emitter: "KafkaEmitter | None" = None

# Events lost *before* reaching the producer (contract validation, bad config).
# Counted separately from KafkaEmitter.dropped so that "we are emitting nothing"
# is always visible in the numbers rather than only in the logs.
_invalid = 0

_JSON_NATIVE = (str, int, float, bool, type(None))


def _coerce_json_safe(value: Any) -> Any:
    """Best-effort conversion of arbitrary payload values to JSON-native ones.

    Teams attach `result.obs_payload` straight from vendor SDKs, so payloads
    routinely contain enums, datetimes, sets and response objects. Rather than
    dropping the whole event when serialization fails, stringify the offenders.
    """
    if isinstance(value, _JSON_NATIVE):
        return value
    if isinstance(value, dict):
        return {str(k): _coerce_json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set, frozenset)):
        return [_coerce_json_safe(v) for v in value]
    return str(value)


def serialize(event: ObsEvent) -> bytes:
    """JSON bytes for the wire, with a coercing retry (see _coerce_json_safe)."""
    try:
        return event.model_dump_json().encode()
    except Exception:  # noqa: BLE001 — pydantic raises on unknown payload types
        safe = event.model_copy(update={"payload": _coerce_json_safe(event.payload)})
        logger.warning(
            "obs event payload was not JSON-serializable; coerced to strings (event_type=%s)",
            event.event_type,
        )
        return safe.model_dump_json().encode()


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
                value=serialize(event),
                headers=inject_trace_headers(event.correlation_id or event.event_id),
                on_delivery=self._on_delivery,
            )
            self._producer.poll(0)  # serve delivery callbacks, non-blocking
        except BufferError:
            # local queue full — drop rather than block the request path
            self.dropped += 1
            logger.warning("obs event dropped: local producer queue full")
        except Exception:  # noqa: BLE001 — never propagate
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

    span_id / parent_span_id ALWAYS come from the ObsContext, never from the
    live OTEL span: the two are different id spaces, and mixing them produced
    parent pointers that matched no emitted span_id (breaking the event trace
    tree whenever tracing was enabled). The OTEL span is still recorded — as
    trace_id on the envelope and payload.otel_span_id — so an event can be
    joined to its Tempo span.
    """
    global _invalid
    try:
        settings = get_settings()
        if not settings.enabled:
            return
        ctx = get_context()
        otel_trace_id, otel_span_id = current_trace_ids()
        event_payload = dict(payload or {})
        if otel_span_id:
            event_payload.setdefault("otel_span_id", otel_span_id)
        event = ObsEvent(
            event_type=event_type,
            service_name=settings.service_name,
            environment=settings.environment,
            application_id=settings.application_id,
            lob=settings.lob,
            component=component,
            correlation_id=ctx.correlation_id,
            request_id=ctx.request_id,
            trace_id=otel_trace_id or ctx.trace_id,
            span_id=ctx.span_id,
            parent_span_id=ctx.parent_span_id,
            tenant_id=ctx.tenant_id,
            user_id=ctx.user_id,
            status=status,
            latency_ms=latency_ms,
            error_code=error_code,
            http_status=http_status,
            payload=event_payload,
        )
        get_emitter().emit(event)
    except Exception:  # noqa: BLE001 — validation error, misconfig, anything: log, never raise
        _invalid += 1
        logger.exception("emit_event(%s) failed — event dropped before produce", event_type)


def emitter_stats() -> dict[str, int]:
    """Counters for health endpoints / Prometheus gauges.

    `invalid` counts events lost before the producer was reached (contract
    violations, bad config) — the class of failure that is otherwise only
    visible in logs.
    """
    em = _emitter
    return {
        "delivered": getattr(em, "delivered", 0),
        "dropped": getattr(em, "dropped", 0),
        "invalid": _invalid,
    }
