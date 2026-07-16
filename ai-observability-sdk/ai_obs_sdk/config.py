"""SDK configuration — every knob is an AI_OBS_* environment variable.

Services construct nothing by hand: `init_observability()` reads this once at
startup. Only `service_name`, `lob`, and `application_id` have no defaults.
"""
from __future__ import annotations

from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class ObsSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="AI_OBS_", env_file=".env", extra="ignore")

    # --- identity (required per service) ---
    service_name: str
    lob: str
    application_id: str
    environment: str = "dev"  # dev | staging | prod

    # --- master switch: flip off to make every SDK call a no-op ---
    enabled: bool = True

    # --- Kafka producer ---
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_topic_raw: str = "ai-obs-events-raw"
    kafka_security_protocol: str = "PLAINTEXT"  # SASL_SSL in prod
    kafka_sasl_mechanism: str | None = None     # e.g. SCRAM-SHA-512
    kafka_sasl_username: str | None = None
    kafka_sasl_password: str | None = None
    kafka_linger_ms: int = 50            # batch window — throughput over latency; emit is async anyway
    kafka_compression: str = "lz4"
    kafka_queue_max_messages: int = 100_000
    kafka_delivery_timeout_ms: int = 10_000

    # --- OTEL tracing → Grafana Tempo ---
    tracing_enabled: bool = True
    otlp_endpoint: str = "http://tempo-distributor.observability.svc:4317"
    trace_sample_ratio: float = 1.0      # head sampling; drop for very hot paths

    # --- logging ---
    log_level: str = "INFO"
    log_json: bool = True                # False → pretty console output for local dev

    # --- prometheus /metrics ---
    metrics_enabled: bool = True

    # --- prompt registry (control-plane API in front of observability.prompt_template_registry) ---
    prompt_registry_url: str | None = None   # e.g. http://obs-dashboard-svc/api/v1/prompts
    prompt_cache_ttl_seconds: int = 300      # in-process TTL cache (Redis not onboarded yet)


@lru_cache(maxsize=1)
def get_settings() -> ObsSettings:
    return ObsSettings()  # type: ignore[call-arg]  # required fields come from env
