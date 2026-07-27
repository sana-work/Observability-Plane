"""Dependency container passed to every stage.

Stages never construct clients — they receive this. Tests build a Deps with
fakes; production builds it once in consumer.main().
"""
from __future__ import annotations

import random
from dataclasses import dataclass, field
from typing import Any

from .config import EnrichSettings


@dataclass
class Deps:
    settings: EnrichSettings
    control_plane: Any        # ControlPlane or a test fake with the same methods
    redactor: Any             # .redact(text) -> text
    archiver: Any | None      # S3Archiver or None when s3_enabled=False
    slo_tracker: Any | None   # SloTracker or None when slo_enabled=False
    rng: random.Random = field(default_factory=random.Random)  # injectable for the sampler
