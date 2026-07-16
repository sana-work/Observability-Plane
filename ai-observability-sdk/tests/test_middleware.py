import pytest

fastapi = pytest.importorskip("fastapi")

from fastapi import FastAPI
from fastapi.testclient import TestClient

from ai_obs_sdk.middleware import ObservabilityMiddleware


@pytest.fixture()
def app(fake_emitter):
    app = FastAPI()
    app.add_middleware(ObservabilityMiddleware)

    @app.get("/ask")
    def ask():
        return {"ok": True}

    @app.get("/boom")
    def boom():
        raise RuntimeError("kaput")

    return app


def test_request_events_and_correlation_header(app, fake_emitter):
    client = TestClient(app)
    resp = client.get("/ask", headers={"X-Correlation-ID": "corr-abc", "X-SOE-ID": "SOE12345"})

    assert resp.headers["X-Correlation-ID"] == "corr-abc"
    types = [e.event_type for e in fake_emitter.events]
    assert types == ["REQUEST_RECEIVED", "REQUEST_COMPLETED"]
    for e in fake_emitter.events:
        assert e.correlation_id == "corr-abc"
        # user identity is carried raw (unhashed) by platform decision
        assert e.user_id == "SOE12345"


def test_correlation_id_generated_when_absent(app, fake_emitter):
    client = TestClient(app)
    resp = client.get("/ask")
    assert resp.headers["X-Correlation-ID"]
    assert fake_emitter.events[0].correlation_id == resp.headers["X-Correlation-ID"]


def test_unhandled_exception_emits_request_failed(app, fake_emitter):
    client = TestClient(app, raise_server_exceptions=False)
    client.get("/boom")
    types = [e.event_type for e in fake_emitter.events]
    assert types == ["REQUEST_RECEIVED", "REQUEST_FAILED"]
    assert fake_emitter.events[1].error_code == "RuntimeError"


def test_health_and_metrics_paths_skipped(app, fake_emitter):
    client = TestClient(app, raise_server_exceptions=False)
    client.get("/health")
    assert fake_emitter.events == []
