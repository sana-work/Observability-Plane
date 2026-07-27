"""Prometheus metrics — scraped on :9108 (kminion covers lag; these cover behavior)."""
from prometheus_client import Counter, Gauge, Histogram, start_http_server

EVENTS_PROCESSED = Counter(
    "obs_enrich_events_processed_total", "Events successfully enriched and produced", ["event_type"]
)
EVENTS_DEAD_LETTERED = Counter(
    "obs_enrich_events_dead_lettered_total", "Events quarantined to the dead-letter topic", ["stage"]
)
STAGE_ERRORS = Counter(
    "obs_enrich_stage_errors_total", "Unexpected (non-DLQ) stage errors, skipped-not-fatal", ["stage"]
)
BUDGET_ALERTS = Counter(
    "obs_enrich_budget_threshold_exceeded_total", "Budget alert/cap crossings observed", ["kind"]
)
BATCH_LATENCY = Histogram(
    "obs_enrich_batch_seconds", "Wall time to process one consumed batch",
    buckets=(0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10),
)
PIPELINE_UP = Gauge("obs_enrich_up", "1 while the consume loop is running")


def start_metrics_server(port: int) -> None:
    start_http_server(port)
