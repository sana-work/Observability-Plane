"""Stage-3 PII redaction of free-text payload values.

Two engines, layered:
  * RegexRedactor — always on: emails, card numbers, US-SSN-like, phone numbers.
  * GLiNER NER    — optional (OBS_ENRICH_GLINER_ENABLED=true): catches names,
    addresses and other entities regexes can't. Model loads ONCE at startup
    (~512Mi resident) and runs in-process.

Deliberately NOT redacted: the envelope `user_id` (raw SOE ID by platform
decision) and any non-string payload values.
"""
from __future__ import annotations

import logging
import re

from .config import EnrichSettings

logger = logging.getLogger("obs_enrichment.redactor")

_PATTERNS: list[tuple[str, re.Pattern]] = [
    ("email", re.compile(r"\b[\w.+-]+@[\w-]+\.[\w.-]+\b")),
    ("card", re.compile(r"\b(?:\d[ -]?){13,16}\b")),
    ("ssn", re.compile(r"\b\d{3}-\d{2}-\d{4}\b")),
    ("phone", re.compile(r"\+?\d{1,3}[ -.]?\(?\d{2,4}\)?(?:[ -.]?\d{2,4}){2,3}\b")),
]


class RegexRedactor:
    def redact(self, text: str) -> str:
        for label, pattern in _PATTERNS:
            text = pattern.sub(f"[REDACTED:{label}]", text)
        return text


class GlinerRedactor:
    """Wraps the GLiNER model; falls back to regex-only if the package/model
    is unavailable so a bad image can degrade instead of crash-loop."""

    def __init__(self, settings: EnrichSettings):
        self._regex = RegexRedactor()
        self._labels = settings.gliner_labels
        self._model = None
        try:
            from gliner import GLiNER  # heavy import, optional extra

            self._model = GLiNER.from_pretrained(settings.gliner_model)
            logger.info("GLiNER model loaded: %s", settings.gliner_model)
        except Exception:  # noqa: BLE001
            logger.exception("GLiNER unavailable — falling back to regex-only redaction")

    def redact(self, text: str) -> str:
        text = self._regex.redact(text)
        if self._model is None:
            return text
        try:
            entities = self._model.predict_entities(text, self._labels, threshold=0.5)
            # replace right-to-left so offsets stay valid
            for ent in sorted(entities, key=lambda e: e["start"], reverse=True):
                text = text[: ent["start"]] + f"[REDACTED:{ent['label']}]" + text[ent["end"]:]
        except Exception:  # noqa: BLE001
            logger.exception("GLiNER inference failed; regex-only for this event")
        return text


def build_redactor(settings: EnrichSettings):
    return GlinerRedactor(settings) if settings.gliner_enabled else RegexRedactor()
