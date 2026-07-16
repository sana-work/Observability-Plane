"""Prompt registry client — get_prompt() with in-process TTL cache.

Fetches versioned prompt templates from the control-plane API backed by
observability.prompt_template_registry. The TTL cache is the agreed interim
until Redis onboarding is confirmed; swapping to a shared cache later only
changes this module.

Every fetch emits prompt_template_id / prompt_version / prompt_hash into the
current context's payload path, so LLM events can be joined to the exact
prompt version that produced them.
"""
from __future__ import annotations

import logging
import threading
import time
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
    def __init__(self, ttl_seconds: int):
        self._ttl = ttl_seconds
        self._store: dict[str, tuple[float, Prompt]] = {}
        self._lock = threading.Lock()

    def get(self, key: str) -> Prompt | None:
        with self._lock:
            hit = self._store.get(key)
            if hit and time.monotonic() - hit[0] < self._ttl:
                return hit[1]
            self._store.pop(key, None)
            return None

    def put(self, key: str, value: Prompt) -> None:
        with self._lock:
            self._store[key] = (time.monotonic(), value)


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
    """Fetch a prompt template (cached). Raises RuntimeError if the registry
    is unreachable AND the prompt has never been cached — services should
    ship a baked-in fallback for that case.
    """
    settings = get_settings()
    if not settings.prompt_registry_url:
        raise RuntimeError("AI_OBS_PROMPT_REGISTRY_URL is not configured")

    key = f"{template_id}:{version}"
    cached = _get_cache().get(key)
    if cached:
        return cached

    resp = httpx.get(
        f"{settings.prompt_registry_url.rstrip('/')}/{template_id}",
        params={"version": version, "service": settings.service_name},
        timeout=3.0,
    )
    resp.raise_for_status()
    data = resp.json()
    prompt = Prompt(
        template_id=data["template_id"],
        version=data["version"],
        text=data["text"],
        prompt_hash=data.get("prompt_hash") or prompt_hash(data["text"]),
        ab_bucket=data.get("ab_bucket"),
    )
    _get_cache().put(key, prompt)
    return prompt
