"""Prompt registry client — get_prompt() with in-process TTL cache.

Fetches versioned prompt templates from the control-plane API backed by
observability.prompt_template_registry. The TTL cache is the agreed interim
until Redis onboarding is confirmed; swapping to a shared cache later only
changes this module.

To join an LLM event to the prompt version that produced it, pass the returned
values to the decorator explicitly — this module does not touch the context:

    p = get_prompt("qa-answer")
    @trace_llm(model_name=..., prompt_template_id=p.template_id,
               prompt_version=p.version)

All failures raise RuntimeError (network, HTTP status and missing-config
alike), so a service's fallback path only has to catch one exception type.
"""
from __future__ import annotations

import logging
import threading
import time
from collections import OrderedDict
from dataclasses import dataclass

import httpx

from .config import get_settings
from .hashing import prompt_hash

logger = logging.getLogger("ai_obs_sdk.prompts")


@dataclass(frozen=True)
class Prompt:
    template_id: str
    version: str
    text: str
    prompt_hash: str
    ab_bucket: str | None = None  # set when the registry serves an A/B split

    def format(self, **kwargs) -> str:
        return self.text.format(**kwargs)


class _TTLCache:
    """TTL cache with a size bound — entries that are written once and never
    re-read would otherwise never be evicted (expiry is checked on read)."""

    def __init__(self, ttl_seconds: int, max_entries: int = 512):
        self._ttl = ttl_seconds
        self._max = max_entries
        self._store: OrderedDict[str, tuple[float, Prompt]] = OrderedDict()
        self._lock = threading.Lock()

    def get(self, key: str) -> Prompt | None:
        with self._lock:
            hit = self._store.get(key)
            if hit and time.monotonic() - hit[0] < self._ttl:
                self._store.move_to_end(key)
                return hit[1]
            self._store.pop(key, None)
            return None

    def put(self, key: str, value: Prompt) -> None:
        with self._lock:
            self._store[key] = (time.monotonic(), value)
            self._store.move_to_end(key)
            while len(self._store) > self._max:
                self._store.popitem(last=False)  # evict least-recently-used


_cache: _TTLCache | None = None
_cache_lock = threading.Lock()


def _get_cache() -> _TTLCache:
    global _cache
    if _cache is None:
        with _cache_lock:
            if _cache is None:
                _cache = _TTLCache(get_settings().prompt_cache_ttl_seconds)
    return _cache


def get_prompt(template_id: str, version: str = "active") -> Prompt:
    """Fetch a prompt template (cached).

    Raises RuntimeError — and only RuntimeError — when the prompt cannot be
    served: registry URL unset, network failure, HTTP error, or malformed
    response. Services should catch RuntimeError and fall back to a baked-in
    template.
    """
    settings = get_settings()
    if not settings.prompt_registry_url:
        raise RuntimeError("AI_OBS_PROMPT_REGISTRY_URL is not configured")

    key = f"{template_id}:{version}"
    cached = _get_cache().get(key)
    if cached:
        return cached

    try:
        resp = httpx.get(
            f"{settings.prompt_registry_url.rstrip('/')}/{template_id}",
            params={"version": version, "service": settings.service_name},
            timeout=3.0,
        )
        resp.raise_for_status()
        data = resp.json()
    except Exception as exc:  # httpx transport/status errors, bad JSON
        raise RuntimeError(
            f"prompt registry fetch failed for {template_id!r} (version={version!r}): {exc}"
        ) from exc
    try:
        prompt = Prompt(
            template_id=data["template_id"],
            version=data["version"],
            text=data["text"],
            prompt_hash=data.get("prompt_hash") or prompt_hash(data["text"]),
            ab_bucket=data.get("ab_bucket"),
        )
    except (KeyError, TypeError) as exc:
        raise RuntimeError(
            f"prompt registry returned a malformed body for {template_id!r}: {exc}"
        ) from exc
    _get_cache().put(key, prompt)
    return prompt
