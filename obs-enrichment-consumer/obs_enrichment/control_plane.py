"""Read/write access to the Postgres control plane (observability.*), with
in-process TTL caches for the read-mostly registries (the Redis stand-in).

Everything the stages need from Postgres flows through this one class, so
tests can substitute a fake with the same method surface.
"""
from __future__ import annotations

import re
import threading
import time
from dataclasses import dataclass
from typing import Any

from .config import EnrichSettings


@dataclass(frozen=True)
class ErrorRule:
    error_code: str
    category: str
    pattern: re.Pattern
    severity: str
    retryable: bool


@dataclass(frozen=True)
class SpendResult:
    period: str
    new_spend_usd: float
    max_spend_usd: float
    alert_crossed: bool
    cap_crossed: bool


class _TTL:
    def __init__(self, ttl_s: int):
        self._ttl = ttl_s
        self._data: dict[str, tuple[float, Any]] = {}
        self._lock = threading.Lock()

    def get_or_load(self, key: str, loader):
        with self._lock:
            hit = self._data.get(key)
            if hit and time.monotonic() - hit[0] < self._ttl:
                return hit[1]
        value = loader()
        with self._lock:
            self._data[key] = (time.monotonic(), value)
        return value


class ControlPlane:
    """psycopg3 pool wrapper. Instantiated once at startup."""

    def __init__(self, settings: EnrichSettings):
        from psycopg_pool import ConnectionPool  # lazy: tests never import psycopg

        self._pool = ConnectionPool(
            settings.pg_dsn, min_size=settings.pg_pool_min, max_size=settings.pg_pool_max,
            kwargs={"autocommit": True},
        )
        self._cache = _TTL(settings.registry_cache_ttl_s)

    # ---- registries (cached reads) ----

    def application(self, application_id: str) -> dict | None:
        def load():
            with self._pool.connection() as conn:
                row = conn.execute(
                    "SELECT application_id, app_name, lob, usecase_id, owner_team, criticality"
                    " FROM observability.application_registry WHERE application_id = %s",
                    (application_id,),
                ).fetchone()
            if row is None:
                return None
            cols = ("application_id", "app_name", "lob", "usecase_id", "owner_team", "criticality")
            return dict(zip(cols, row))

        return self._cache.get_or_load(f"app:{application_id}", load)

    def error_rules(self) -> list[ErrorRule]:
        def load():
            with self._pool.connection() as conn:
                rows = conn.execute(
                    "SELECT error_code, category, match_pattern, severity, retryable"
                    " FROM observability.error_code_catalog WHERE status = 'active'"
                    " ORDER BY priority ASC"
                ).fetchall()
            rules = []
            for code, cat, pat, sev, retry in rows:
                try:
                    rules.append(ErrorRule(code, cat, re.compile(pat, re.IGNORECASE), sev, retry))
                except re.error:
                    continue  # a bad seed row must not take the pipeline down
            return rules

        return self._cache.get_or_load("error_rules", load)

    def model_price(self, model_name: str) -> tuple[float, float] | None:
        """(input_usd_per_1k, output_usd_per_1k) — latest effective row."""
        def load():
            with self._pool.connection() as conn:
                row = conn.execute(
                    "SELECT input_usd_per_1k, output_usd_per_1k FROM observability.model_pricing"
                    " WHERE model_name = %s AND effective_from <= CURRENT_DATE"
                    " ORDER BY effective_from DESC LIMIT 1",
                    (model_name,),
                ).fetchone()
            return (float(row[0]), float(row[1])) if row else None

        return self._cache.get_or_load(f"price:{model_name}", load)

    def slo_definitions(self, application_id: str) -> list[dict]:
        def load():
            with self._pool.connection() as conn:
                rows = conn.execute(
                    "SELECT slo_id, sli_type, target_pct, latency_target_ms"
                    " FROM observability.slo_definitions"
                    " WHERE application_id = %s AND enabled",
                    (application_id,),
                ).fetchall()
            cols = ("slo_id", "sli_type", "target_pct", "latency_target_ms")
            return [dict(zip(cols, r)) for r in rows]

        return self._cache.get_or_load(f"slo:{application_id}", load)

    # ---- writes (uncached) ----

    def add_spend(self, application_id: str, model_name: str, cost_usd: float) -> list[SpendResult]:
        with self._pool.connection() as conn:
            rows = conn.execute(
                "SELECT period, new_spend_usd, max_spend_usd, alert_crossed, cap_crossed"
                " FROM observability.add_spend(%s, %s, %s)",
                (application_id, model_name, cost_usd),
            ).fetchall()
        return [SpendResult(r[0], float(r[1]), float(r[2]), r[3], r[4]) for r in rows]

    def upsert_slo_compliance(
        self, slo_id: str, day, good: int, total: int,
        burn_1h: float | None, burn_6h: float | None,
    ) -> None:
        with self._pool.connection() as conn:
            conn.execute(
                """
                INSERT INTO observability.daily_slo_compliance
                  (slo_id, day, good_events, total_events, sli_pct, burn_rate_1h, burn_rate_6h, breached)
                VALUES (%(slo)s, %(day)s, %(good)s, %(total)s,
                        CASE WHEN %(total)s > 0 THEN 100.0 * %(good)s / %(total)s END,
                        %(b1)s, %(b6)s, COALESCE(%(b1)s > 14.4, false))
                ON CONFLICT (slo_id, day) DO UPDATE SET
                  good_events  = observability.daily_slo_compliance.good_events + EXCLUDED.good_events,
                  total_events = observability.daily_slo_compliance.total_events + EXCLUDED.total_events,
                  sli_pct = CASE WHEN observability.daily_slo_compliance.total_events + EXCLUDED.total_events > 0
                       THEN 100.0 * (observability.daily_slo_compliance.good_events + EXCLUDED.good_events)
                            / (observability.daily_slo_compliance.total_events + EXCLUDED.total_events) END,
                  burn_rate_1h = GREATEST(COALESCE(observability.daily_slo_compliance.burn_rate_1h, 0), COALESCE(EXCLUDED.burn_rate_1h, 0)),
                  burn_rate_6h = GREATEST(COALESCE(observability.daily_slo_compliance.burn_rate_6h, 0), COALESCE(EXCLUDED.burn_rate_6h, 0)),
                  breached = observability.daily_slo_compliance.breached OR EXCLUDED.breached,
                  computed_at = now()
                """,
                {"slo": slo_id, "day": day, "good": good, "total": total, "b1": burn_1h, "b6": burn_6h},
            )
