"""SDK configuration — every knob is an AI_OBS_* environment variable.

Services construct nothing by hand: `init_observability()` reads this once at
startup. Only `service_name`, `lob`, and `application_id` have no defaults.
"""
from __future__ import annotations

from functools import lru_cache

from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from .contracts import SERVICE_NAME_VALUES

# Paths that produce no telemetry — shared by the middleware (event skipping)
# and OTEL FastAPI instrumentation (span exclusion) so the two cannot drift.
OPERATIONAL_PATHS: tuple[str, ...] = ("/metrics", "/health", "/ready", "/livez")


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


    # --- fail fast at startup, not silently per-event ---------------------
    # An *invalid* service_name (e.g. "gssp_gs") would otherwise pass config
    # and then fail ObsEvent validation on every single emit — which
    # emit_event swallows by design, so the service would run happily and
    # produce zero telemetry forever. Reject it here instead.
    @field_validator("service_name")
    @classmethod
    def known_service_name(cls, v: str) -> str:
        if v not in SERVICE_NAME_VALUES:
            raise ValueError(
                f"AI_OBS_SERVICE_NAME={v!r} is not one of the 8 platform services: "
                f"{sorted(SERVICE_NAME_VALUES)}"
            )
        return v

    # SASL must be configured completely, and over a SASL transport — a half
    # configured producer either fails obscurely at connect time or sends
    # credentials over a plaintext transport.
    @field_validator("kafka_sasl_password")
    @classmethod
    def complete_sasl_config(cls, v: str | None, info) -> str | None:
        data = info.data
        provided = [
            data.get("kafka_sasl_mechanism"),
            data.get("kafka_sasl_username"),
            v,
        ]
        if any(provided) and not all(provided):
            raise ValueError(
                "incomplete SASL config: set AI_OBS_KAFKA_SASL_MECHANISM, "
                "_USERNAME and _PASSWORD together (or none of them)"
            )
        if any(provided) and data.get("kafka_security_protocol") not in ("SASL_SSL", "SASL_PLAINTEXT"):
            raise ValueError(
                "SASL credentials set but AI_OBS_KAFKA_SECURITY_PROTOCOL is "
                f"{data.get('kafka_security_protocol')!r} — use SASL_SSL (or "
                "SASL_PLAINTEXT for a non-production broker)"
            )
        return v


@lru_cache(maxsize=1)
def get_settings() -> ObsSettings:
    return ObsSettings()  # type: ignore[call-arg]  # required fields come from env
