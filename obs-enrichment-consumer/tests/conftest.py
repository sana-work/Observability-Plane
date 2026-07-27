import json
import random
import re

import pytest

from obs_enrichment.config import EnrichSettings
from obs_enrichment.control_plane import ErrorRule, SpendResult
from obs_enrichment.deps import Deps
from obs_enrichment.redactor import RegexRedactor


class FakeControlPlane:
    """Same method surface as ControlPlane, backed by dicts."""

    def __init__(self):
        self.apps = {
            "app-1234": {
                "application_id": "app-1234", "app_name": "Sandbox", "lob": "wealth",
                "usecase_id": "uc-1", "owner_team": "obs-platform", "criticality": "low",
            }
        }
        self.rules = [
            ErrorRule("L0001", "llm", re.compile(r"(RateLimit|429)", re.I), "warning", True),
            ErrorRule("T0001", "tool", re.compile(r"(TimeoutError|deadline)", re.I), "error", True),
            ErrorRule("P0999", "platform", re.compile(r".*"), "error", False),
        ]
        self.prices = {"gemini-1.5-pro": (0.00125, 0.005)}
        self.spend_results: list[SpendResult] = []
        self.spend_calls: list[tuple] = []
        self.slos = {"app-1234": [
            {"slo_id": "slo-avail", "sli_type": "availability", "target_pct": 99.0, "latency_target_ms": None},
        ]}
        self.slo_upserts: list[tuple] = []

    def application(self, app_id):
        return self.apps.get(app_id)

    def error_rules(self):
        return self.rules

    def model_price(self, model):
        return self.prices.get(model)

    def add_spend(self, app, model, cost):
        self.spend_calls.append((app, model, cost))
        return self.spend_results

    def slo_definitions(self, app_id):
        return self.slos.get(app_id, [])

    def upsert_slo_compliance(self, slo_id, day, good, total, b1, b6):
        self.slo_upserts.append((slo_id, day, good, total, b1, b6))


class FakeArchiver:
    def __init__(self):
        self.objects: dict[str, str] = {}

    def put_text(self, key, text):
        self.objects[key] = text
        return f"s3://fake-bucket/{key}"


class FakeProducer:
    def __init__(self):
        self.messages: list[dict] = []
        self.fail_next = 0

    def produce(self, topic, key=None, value=None, headers=None, on_delivery=None):
        self.messages.append({"topic": topic, "key": key, "value": value, "headers": headers})
        if on_delivery:
            if self.fail_next > 0:
                self.fail_next -= 1
                on_delivery("delivery-error", None)
            else:
                on_delivery(None, None)

    def flush(self, timeout=None):
        return 0

    def poll(self, timeout=None):
        return 0


@pytest.fixture()
def settings():
    return EnrichSettings(
        pg_dsn="unused", s3_enabled=True, gliner_enabled=False,
        archive_threshold_bytes=100, quality_sample_pct=50.0,
    )


@pytest.fixture()
def cp():
    return FakeControlPlane()


@pytest.fixture()
def deps(settings, cp):
    from obs_enrichment.slo import SloTracker

    d = Deps(
        settings=settings, control_plane=cp, redactor=RegexRedactor(),
        archiver=FakeArchiver(), slo_tracker=SloTracker(cp, clock=lambda: 1_000_000.0),
        rng=random.Random(42),
    )
    return d


def make_raw(event_type="REQUEST_COMPLETED", **overrides) -> bytes:
    base = {
        "event_type": event_type,
        "service_name": "gssp-gs",
        "environment": "dev",
        "status": "success",
        "correlation_id": "corr-1",
        "application_id": "app-1234",
        "user_id": "SOE12345",
        "payload": {},
    }
    base.update(overrides)
    return json.dumps(base).encode()
