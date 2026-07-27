"""Dead-letter wrapping — the shape scripts/replay_dead_letter.py expects:
{"reason": ..., "failed_at": ..., "original": <the event, parsed if possible>}
"""
from __future__ import annotations

import json
from datetime import datetime, timezone


def wrap(raw_value: bytes, reason: str) -> bytes:
    try:
        original = json.loads(raw_value)
    except Exception:  # noqa: BLE001 — keep even unparseable garbage, base64-free best effort
        original = {"_unparseable": raw_value.decode(errors="replace")}
    return json.dumps(
        {
            "reason": reason,
            "failed_at": datetime.now(timezone.utc).isoformat(),
            "original": original,
        }
    ).encode()
