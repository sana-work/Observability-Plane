"""Deterministic hashing helpers for grouping/dedup keys.

Note: user identity (user_id / SOE ID) is carried RAW by platform decision —
there is deliberately no user-hashing helper here. These hashes exist so large
prompt/query texts can be grouped and joined without shipping the full text in
every event.
"""
from __future__ import annotations

import hashlib


def prompt_hash(prompt_text: str) -> str:
    return hashlib.sha256(prompt_text.encode()).hexdigest()[:16]


def query_hash(query_text: str) -> str:
    return hashlib.sha256(query_text.encode()).hexdigest()[:16]
