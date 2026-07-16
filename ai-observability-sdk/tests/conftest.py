import os

import pytest

# Deterministic SDK config for every test — no Kafka/OTLP/registry needed.
os.environ.update(
    {
        "AI_OBS_SERVICE_NAME": "gssp-gs",
        "AI_OBS_LOB": "test-lob",
        "AI_OBS_APPLICATION_ID": "app-test-001",
        "AI_OBS_ENVIRONMENT": "dev",
        "AI_OBS_TRACING_ENABLED": "false",
    }
)


class FakeEmitter:
    """Captures ObsEvents instead of producing to Kafka."""

    def __init__(self):
        self.events = []

    def emit(self, event):
        self.events.append(event)

    def flush(self, timeout: float = 0) -> None:
        pass


@pytest.fixture()
def fake_emitter(monkeypatch):
    import ai_obs_sdk.emitter as emitter_mod

    fake = FakeEmitter()
    monkeypatch.setattr(emitter_mod, "_emitter", fake)
    yield fake
    monkeypatch.setattr(emitter_mod, "_emitter", None)
