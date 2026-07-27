"""The 9-stage enrichment pipeline.

process(raw_bytes, headers, deps) -> enriched ObsEvent
  1 validate        parse JSON + contract validation        (fail -> dead-letter)
  2 trace context   fill trace ids from the traceparent header
  3 pii redact      redact free text in payload (NOT user_id — raw by decision)
  4 metadata        join registry context (owner, lob, criticality)
  5 error map       normalise raw errors onto error_code_catalog
  6 cost            authoritative price + budget accumulator (alert exactly once)
  7 s3 archive      offload oversized payload strings, leave s3:// pointers
  8 slo             sliding-window burn rates -> daily_slo_compliance
  9 quality hook    sample LLM/RAG completions for the eval service

Error policy:
  * stage 1 failure  -> DeadLetterError (the event is wrong; quarantine it)
  * stages 2-9       -> best-effort: an unexpected error logs + counts
    (obs_enrich_stage_errors_total) and the event continues with that
    enrichment missing. A DB/S3 outage must degrade enrichment, not stop
    the pipeline or poison the DLQ with valid events.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone

from .contracts import EventType, ObsEvent
from .deps import Deps
from .errors import DeadLetterError
from .metrics import BUDGET_ALERTS, STAGE_ERRORS
from .s3_archiver import archive_key

logger = logging.getLogger("obs_enrichment.pipeline")

_TERMINAL_REQUEST_EVENTS = {EventType.REQUEST_COMPLETED, EventType.REQUEST_FAILED}
_QUALITY_EVENTS = {EventType.LLM_CALL_COMPLETED, EventType.RAG_RETRIEVAL_COMPLETED}


# ---------------------------------------------------------------- stage 1
def validate(raw_value: bytes) -> ObsEvent:
    try:
        data = json.loads(raw_value)
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise DeadLetterError(f"stage1-validate: not JSON ({exc})") from exc
    try:
        return ObsEvent.model_validate(data)
    except Exception as exc:  # pydantic.ValidationError
        raise DeadLetterError(f"stage1-validate: contract violation ({exc})") from exc


# ---------------------------------------------------------------- stage 2
def trace_context(event: ObsEvent, headers: dict[str, bytes]) -> None:
    tp = headers.get("traceparent")
    if not tp:
        return
    parts = tp.decode(errors="replace").split("-")  # 00-<trace32>-<span16>-<flags>
    if len(parts) == 4 and len(parts[1]) == 32:
        event.trace_id = event.trace_id or parts[1]
        event.parent_span_id = event.parent_span_id or parts[2]


# ---------------------------------------------------------------- stage 3
def pii_redact(event: ObsEvent, deps: Deps) -> None:
    limit = deps.settings.redact_max_field_chars
    for key, value in list(event.payload.items()):
        if isinstance(value, str) and value and len(value) <= limit:
            event.payload[key] = deps.redactor.redact(value)


# ---------------------------------------------------------------- stage 4
def metadata(event: ObsEvent, deps: Deps) -> None:
    if not event.application_id:
        return
    app = deps.control_plane.application(event.application_id)
    if app is None:
        event.payload["app_registered"] = False
        return
    event.lob = event.lob or app.get("lob")
    event.payload.setdefault("usecase_id", app.get("usecase_id"))
    event.payload["app_owner_team"] = app.get("owner_team")
    event.payload["app_criticality"] = app.get("criticality")


# ---------------------------------------------------------------- stage 5
def error_map(event: ObsEvent, deps: Deps) -> None:
    if event.status != "failed" and not event.error_code:
        return
    text = f"{event.error_code or ''}: {event.payload.get('error_message', '')}"
    for rule in deps.control_plane.error_rules():
        if rule.pattern.search(text):
            if event.error_code != rule.error_code:
                event.payload["raw_error_code"] = event.error_code
            event.error_code = rule.error_code
            event.payload["error_category"] = rule.category
            event.payload["error_retryable"] = rule.retryable
            return


# ---------------------------------------------------------------- stage 6
def cost(event: ObsEvent, deps: Deps) -> None:
    if not str(event.event_type).startswith("LLM_"):
        return
    p = event.payload
    model = p.get("model_name")
    itok, otok = p.get("input_tokens"), p.get("output_tokens")
    if model and (itok is not None or otok is not None):
        price = deps.control_plane.model_price(model)
        if price:
            p["estimated_cost_usd"] = round(
                (itok or 0) / 1000 * price[0] + (otok or 0) / 1000 * price[1], 8
            )
            p["cost_source"] = "control_plane"

    cost_usd = p.get("estimated_cost_usd") or 0.0
    if cost_usd > 0 and event.application_id:
        for r in deps.control_plane.add_spend(event.application_id, model or "*", cost_usd):
            if r.alert_crossed:
                BUDGET_ALERTS.labels(kind="alert").inc()
                event.payload["budget_alert"] = {
                    "period": r.period, "spend_usd": r.new_spend_usd, "limit_usd": r.max_spend_usd,
                }
                logger.warning(
                    "budget alert: app=%s model=%s period=%s spend=%.4f limit=%.2f",
                    event.application_id, model, r.period, r.new_spend_usd, r.max_spend_usd,
                )
            if r.cap_crossed:
                BUDGET_ALERTS.labels(kind="cap").inc()
                event.payload["budget_cap_breached"] = True
                logger.error(
                    "BUDGET CAP BREACHED: app=%s model=%s period=%s spend=%.4f limit=%.2f",
                    event.application_id, model, r.period, r.new_spend_usd, r.max_spend_usd,
                )


# ---------------------------------------------------------------- stage 7
def s3_archive(event: ObsEvent, deps: Deps) -> None:
    if deps.archiver is None:
        return
    threshold = deps.settings.archive_threshold_bytes
    for field in deps.settings.archive_fields:
        value = event.payload.get(field)
        if isinstance(value, str) and len(value.encode()) > threshold:
            uri = deps.archiver.put_text(archive_key(field, event.event_id), value)
            event.payload[field] = uri
            event.payload[f"{field}_bytes"] = len(value.encode())


# ---------------------------------------------------------------- stage 8
def slo(event: ObsEvent, deps: Deps) -> None:
    if deps.slo_tracker is None or event.event_type not in _TERMINAL_REQUEST_EVENTS:
        return
    if event.application_id:
        deps.slo_tracker.observe(event.application_id, event.status, event.latency_ms)


# ---------------------------------------------------------------- stage 9
def quality_hook(event: ObsEvent, deps: Deps) -> None:
    if event.event_type in _QUALITY_EVENTS and event.status == "success":
        if deps.rng.uniform(0, 100) < deps.settings.quality_sample_pct:
            event.payload["quality_sample"] = True


# ---------------------------------------------------------------- runner
_BEST_EFFORT = [
    ("stage3-pii", pii_redact),
    ("stage4-metadata", metadata),
    ("stage5-errmap", error_map),
    ("stage6-cost", cost),
    ("stage7-s3", s3_archive),
    ("stage8-slo", slo),
    ("stage9-quality", quality_hook),
]


def process(raw_value: bytes, headers: list[tuple[str, bytes]] | None, deps: Deps) -> ObsEvent:
    event = validate(raw_value)                                  # stage 1 — DLQ on failure
    hdrs = {k: v for k, v in (headers or []) if v is not None}
    try:
        trace_context(event, hdrs)                               # stage 2 — cheap, no I/O
    except Exception:  # noqa: BLE001
        STAGE_ERRORS.labels(stage="stage2-trace").inc()

    for name, fn in _BEST_EFFORT:                                # stages 3-9 — degrade, don't die
        try:
            fn(event, deps)
        except Exception:  # noqa: BLE001
            STAGE_ERRORS.labels(stage=name).inc()
            logger.exception("%s failed for event %s — continuing", name, event.event_id)

    event.payload["enriched_at"] = datetime.now(timezone.utc).isoformat()
    return event
