"""Burn-rate math on the sliding windows."""
from obs_enrichment.slo import SloTracker
from tests.conftest import FakeControlPlane


def test_burn_rate_math():
    cp = FakeControlPlane()  # slo-avail: availability, target 99% → budget 1%
    now = [1_000_000.0]
    tracker = SloTracker(cp, clock=lambda: now[0])

    # 98 good + 2 bad in the same window → bad fraction 2% → burn 2/1 = 2.0
    for _ in range(98):
        tracker.observe("app-1234", "success", 50.0)
    for _ in range(2):
        tracker.observe("app-1234", "failed", 50.0)

    _slo, _day, good, total, b1, b6 = cp.slo_upserts[-1]
    assert (good, total) == (0, 1)                # last observation was bad
    assert b1 == b6                               # same events in both windows so far
    assert abs(b1 - 2.0) < 1e-6

    # events older than 1h fall out of the 1h window but stay in the 6h window
    now[0] += 2 * 3600
    tracker.observe("app-1234", "success", 50.0)
    *_ignore, b1_new, b6_new = cp.slo_upserts[-1]
    assert b1_new == 0.0                          # fresh 1h window, all good
    assert b6_new > 0.0                           # 6h window still remembers the failures


def test_latency_slo_uses_target():
    cp = FakeControlPlane()
    cp.slos["app-1234"] = [
        {"slo_id": "slo-lat", "sli_type": "latency", "target_pct": 99.0, "latency_target_ms": 100}
    ]
    tracker = SloTracker(cp, clock=lambda: 1_000_000.0)
    tracker.observe("app-1234", "success", 250.0)   # success but SLOW → bad for latency SLI
    _slo, _day, good, _total, b1, _b6 = cp.slo_upserts[-1]
    assert good == 0
    assert abs(b1 - 100.0) < 1e-6


def test_no_slo_defined_no_writes():
    cp = FakeControlPlane()
    cp.slos = {}
    tracker = SloTracker(cp, clock=lambda: 1_000_000.0)
    tracker.observe("app-1234", "success", 10.0)
    assert cp.slo_upserts == []
