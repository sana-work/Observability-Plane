"""Configuration — every knob is an OBS_ENRICH_* environment variable."""
from __future__ import annotations

from functools import lru_cache

from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class EnrichSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="OBS_ENRICH_", env_file=".env", extra="ignore")

    environment: str = "dev"

    # --- Kafka ---
    kafka_bootstrap_servers: str = "localhost:9092"
    topic_raw: str = "ai-obs-events-raw"
    topic_processed: str = "ai-obs-events-processed"
    topic_dead_letter: str = "ai-obs-dead-letter"
    consumer_group: str = "obs-enrichment"
    kafka_security_protocol: str = "PLAINTEXT"
    kafka_sasl_mechanism: str | None = None
    kafka_sasl_username: str | None = None
    kafka_sasl_password: str | None = None
    batch_max_messages: int = 200          # process/commit unit
    poll_timeout_s: float = 1.0
    produce_flush_timeout_s: float = 10.0

    # --- Postgres control plane (observability.*) ---
    pg_dsn: str = "postgresql://postgres:obs@localhost:5432/postgres"
    pg_pool_min: int = 1
    pg_pool_max: int = 4
    registry_cache_ttl_s: int = 300        # in-process TTL cache (Redis not onboarded)

    # --- PII redaction (stage 3) ---
    gliner_enabled: bool = False           # true in prod once the model image is baked
    gliner_model: str = "urchade/gliner_multi_pii-v1"
    gliner_labels: list[str] = ["person", "email", "phone number", "credit card number", "address"]
    redact_max_field_chars: int = 20_000   # skip NER on absurdly large strings (S3 stage handles them)

    # --- S3 archiver (stage 7) ---
    s3_enabled: bool = True
    s3_bucket: str = "ai-obs-archive-dev"
    archive_threshold_bytes: int = 2048    # payload strings larger than this are offloaded
    archive_fields: list[str] = ["prompt", "response", "rag_context", "raw_response", "trace_json"]

    # --- SLO evaluator (stage 8) ---
    slo_enabled: bool = True

    # --- Quality hook (stage 9) ---
    quality_sample_pct: float = 5.0        # % of LLM/RAG completions flagged for eval

    # --- ops ---
    metrics_port: int = 9108               # prometheus scrape + liveness
    log_level: str = "INFO"


    # Same fail-fast rule as the SDK: a half-configured SASL block either
    # fails obscurely at connect time or ships credentials over a plaintext
    # transport. Catch it at boot instead.
    @field_validator("kafka_sasl_password")
    @classmethod
    def complete_sasl_config(cls, v: str | None, info) -> str | None:
        data = info.data
        provided = [data.get("kafka_sasl_mechanism"), data.get("kafka_sasl_username"), v]
        if any(provided) and not all(provided):
            raise ValueError(
                "incomplete SASL config: set OBS_ENRICH_KAFKA_SASL_MECHANISM, "
                "_USERNAME and _PASSWORD together (or none of them)"
            )
        if any(provided) and data.get("kafka_security_protocol") not in (
            "SASL_SSL",
            "SASL_PLAINTEXT",
        ):
            raise ValueError(
                "SASL credentials set but OBS_ENRICH_KAFKA_SECURITY_PROTOCOL is "
                f"{data.get('kafka_security_protocol')!r} — use SASL_SSL"
            )
        return v


@lru_cache(maxsize=1)
def get_settings() -> EnrichSettings:
    return EnrichSettings()
