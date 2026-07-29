"""prompts.py — error contract (M3), cache behaviour (M5)."""
import httpx
import pytest

import ai_obs_sdk.prompts as prompts_mod
from ai_obs_sdk.prompts import Prompt, _TTLCache, get_prompt


@pytest.fixture(autouse=True)
def _reset_cache(monkeypatch):
    monkeypatch.setattr(prompts_mod, "_cache", None)
    yield
    monkeypatch.setattr(prompts_mod, "_cache", None)


def _with_registry(monkeypatch, url="http://registry.test/api/v1/prompts"):
    settings = prompts_mod.get_settings()
    monkeypatch.setattr(settings, "prompt_registry_url", url, raising=False)


# ---------------------------------------------------------------- M3
def test_m3_network_failure_raises_runtime_error(monkeypatch):
    """Docstring promised RuntimeError; httpx errors used to escape, so a
    service catching RuntimeError for its fallback would crash instead."""
    _with_registry(monkeypatch)

    def boom(*_a, **_kw):
        raise httpx.ConnectError("connection refused")

    monkeypatch.setattr(prompts_mod.httpx, "get", boom)
    with pytest.raises(RuntimeError) as exc:
        get_prompt("qa-answer")
    assert isinstance(exc.value.__cause__, httpx.ConnectError)


def test_m3_http_error_status_raises_runtime_error(monkeypatch):
    _with_registry(monkeypatch)

    def not_found(*_a, **_kw):
        return httpx.Response(404, request=httpx.Request("GET", "http://registry.test/x"))

    monkeypatch.setattr(prompts_mod.httpx, "get", not_found)
    with pytest.raises(RuntimeError):
        get_prompt("missing-template")


def test_m3_malformed_body_raises_runtime_error(monkeypatch):
    _with_registry(monkeypatch)

    def bad_body(*_a, **_kw):
        return httpx.Response(
            200, json={"template_id": "x"},  # missing version/text
            request=httpx.Request("GET", "http://registry.test/x"),
        )

    monkeypatch.setattr(prompts_mod.httpx, "get", bad_body)
    with pytest.raises(RuntimeError) as exc:
        get_prompt("x")
    assert "malformed" in str(exc.value)


def test_m3_missing_url_raises_runtime_error(monkeypatch):
    settings = prompts_mod.get_settings()
    monkeypatch.setattr(settings, "prompt_registry_url", None, raising=False)
    with pytest.raises(RuntimeError):
        get_prompt("qa-answer")


def test_fetch_is_cached_until_ttl_expires(monkeypatch):
    _with_registry(monkeypatch)
    calls = {"n": 0}

    def ok(*_a, **_kw):
        calls["n"] += 1
        return httpx.Response(
            200,
            json={"template_id": "qa", "version": "3", "text": "Answer {q}"},
            request=httpx.Request("GET", "http://registry.test/qa"),
        )

    monkeypatch.setattr(prompts_mod.httpx, "get", ok)
    now = [1000.0]
    monkeypatch.setattr(prompts_mod.time, "monotonic", lambda: now[0])

    p1 = get_prompt("qa")
    p2 = get_prompt("qa")
    assert calls["n"] == 1                       # served from cache
    assert p1 is p2
    assert p1.format(q="why?") == "Answer why?"
    assert p1.prompt_hash                        # derived when absent from the body

    now[0] += prompts_mod.get_settings().prompt_cache_ttl_seconds + 1
    get_prompt("qa")
    assert calls["n"] == 2                       # refetched after expiry


# ---------------------------------------------------------------- M5
def test_m5_cache_is_size_bounded():
    cache = _TTLCache(ttl_seconds=3600, max_entries=3)
    for i in range(5):
        cache.put(f"k{i}", Prompt(f"k{i}", "1", "t", "h"))
    assert len(cache._store) == 3
    assert cache.get("k0") is None      # evicted (least recently used)
    assert cache.get("k4") is not None
