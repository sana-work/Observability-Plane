"""Stage-8 SLO burn-rate evaluation (in-memory sliding windows + daily upsert).

Burn rate = (observed bad fraction in a window) / (allowed bad fraction).
Example: target 99.5% → error budget 0.5%. A window with 2% failures burns at
2 / 0.5 = 4x. The classic page threshold is burn_1h > 14.4 (would exhaust a
30-day budget in ~2 days).

Windows are tracked per (application, slo) as per-minute buckets held in
memory. Each replica sees only its partitions' events, so its burn rate is a
per-replica *sample*; the daily_slo_compliance upsert merges good/total counts
across replicas and keeps the MAX burn rate seen — conservative and correct
for alerting. (A future Snowflake/warehouse job can recompute exactly.)
"""
from __future__ import annotations

import time
from collections import defaultdict, deque
from datetime import datetime, timezone


class _Window:
    """Per-minute (good, total) buckets covering the last `minutes` minutes."""

    def __init__(self, minutes: int):
        self.minutes = minutes
        self._buckets: deque[tuple[int, int, int]] = deque()  # (minute_epoch, good, total)

    def add(self, good: bool, now_min: int) -> None:
        if self._buckets and self._buckets[-1][0] == now_min:
            m, g, t = self._buckets[-1]
            self._buckets[-1] = (m, g + int(good), t + 1)
        else:
            self._buckets.append((now_min, int(good), 1))
        cutoff = now_min - self.minutes
        while self._buckets and self._buckets[0][0] <= cutoff:
            self._buckets.popleft()

    def bad_fraction(self) -> float | None:
        good = sum(b[1] for b in self._buckets)
        total = sum(b[2] for b in self._buckets)
        if total == 0:
            return None
        return 1.0 - good / total


class SloTracker:
    def __init__(self, control_plane, clock=time.time):
        self._cp = control_plane
        self._clock = clock
        self._w1h: dict[str, _Window] = defaultdict(lambda: _Window(60))
        self._w6h: dict[str, _Window] = defaultdict(lambda: _Window(360))

    def observe(self, application_id: str, status: str, latency_ms: float | None) -> None:
        """Called for each terminal request event (REQUEST_COMPLETED/FAILED)."""
        for slo in self._cp.slo_definitions(application_id) or []:
            if slo["sli_type"] == "availability":
                good = status == "success"
            elif slo["sli_type"] == "latency":
                target = slo.get("latency_target_ms")
                good = latency_ms is not None and target is not None and latency_ms <= float(target)
            else:
                continue  # quality/cost SLIs are computed downstream, not per-event

            key = str(slo["slo_id"])
            now_min = int(self._clock() // 60)
            self._w1h[key].add(good, now_min)
            self._w6h[key].add(good, now_min)

            budget = 1.0 - float(slo["target_pct"]) / 100.0
            if budget <= 0:
                continue
            bf1, bf6 = self._w1h[key].bad_fraction(), self._w6h[key].bad_fraction()
            burn_1h = (bf1 / budget) if bf1 is not None else None
            burn_6h = (bf6 / budget) if bf6 is not None else None

            day = datetime.fromtimestamp(self._clock(), tz=timezone.utc).date()
            self._cp.upsert_slo_compliance(key, day, int(good), 1, burn_1h, burn_6h)
