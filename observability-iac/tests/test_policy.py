"""Policy tests — the Phase 0 CI gate.

These enforce the compatibility contract between observability-iac (Phase 0)
and ai-observability-sdk (Phase 1):
  1. contracts/ is byte-identical to the SDK's vendored copy
  2. topic names in topics.yaml match the SDK's ObsSettings defaults
  3. the ES envelope mapping covers every ObsEvent field
  4. the service_registry seed matches the frozen ServiceName enum
  5. the model_pricing seed stays in sync with the SDK cost table
  6. migrations are uniquely numbered; ES templates reference real ILM policies
"""
from __future__ import annotations

import json
import re
import sys
from pathlib import Path

import yaml

IAC = Path(__file__).resolve().parents[1]
REPO = IAC.parent
SDK = REPO / "ai-observability-sdk"

sys.path.insert(0, str(IAC))
sys.path.insert(0, str(SDK))

from contracts.event_schema import ObsEvent          # noqa: E402
from contracts.event_types import EventType          # noqa: E402
from contracts.service_names import ServiceName      # noqa: E402


# -- 1. contract byte-identity ------------------------------------------------
def test_contracts_identical_to_sdk_vendored_copy():
    for f in ("event_schema.py", "event_types.py", "service_names.py"):
        iac_file = (IAC / "contracts" / f).read_bytes()
        sdk_file = (SDK / "ai_obs_sdk" / "contracts" / f).read_bytes()
        assert iac_file == sdk_file, f"contract drift in {f} — re-vendor into the SDK"


def test_catalog_sizes_frozen():
    assert len(EventType) == 50
    assert len(ServiceName) == 8


# -- 2. kafka topics match SDK defaults ---------------------------------------
def test_topics_match_sdk_defaults_and_roadmap():
    doc = yaml.safe_load((IAC / "kafka" / "topics.yaml").read_text())
    topics = {t["name"]: t for t in doc["topics"]}
    assert set(topics) == {"ai-obs-events-raw", "ai-obs-events-processed", "ai-obs-dead-letter"}

    sdk_config = (SDK / "ai_obs_sdk" / "config.py").read_text()
    assert 'kafka_topic_raw: str = "ai-obs-events-raw"' in sdk_config

    assert topics["ai-obs-events-raw"]["configs"]["retention.ms"] == 7 * 86400_000
    assert topics["ai-obs-events-processed"]["configs"]["retention.ms"] == 3 * 86400_000
    assert topics["ai-obs-dead-letter"]["configs"]["retention.ms"] == 14 * 86400_000


# -- 3. ES envelope mapping covers the ObsEvent model --------------------------
def test_es_common_mappings_cover_envelope():
    mapping = json.loads(
        (IAC / "elasticsearch" / "component-templates" / "obs-common-mappings.json").read_text()
    )
    mapped = set(mapping["template"]["mappings"]["properties"])
    envelope = set(ObsEvent.model_fields) - {"payload"}  # payload mapped per family
    missing = envelope - mapped
    assert not missing, f"envelope fields missing from ES common mappings: {missing}"


def test_es_index_templates_compose_common_and_reference_real_ilm():
    ilm_names = {p.stem for p in (IAC / "elasticsearch" / "ilm-policies").glob("*.json")}
    for tf in (IAC / "elasticsearch" / "index-templates").glob("*.json"):
        doc = json.loads(tf.read_text())
        assert "obs-common-settings" in doc["composed_of"], tf.name
        assert "obs-common-mappings" in doc["composed_of"], tf.name
        override = (
            doc.get("template", {}).get("settings", {}).get("index", {})
            .get("lifecycle", {}).get("name")
        )
        if override:
            assert override in ilm_names, f"{tf.name} references unknown ILM policy {override}"


def test_all_roadmap_index_families_present():
    families = {p.stem for p in (IAC / "elasticsearch" / "index-templates").glob("*.json")}
    expected = {
        "ai-obs-requests", "ai-obs-errors", "ai-obs-agent-steps", "ai-obs-llm-calls",
        "ai-obs-tool-calls", "ai-obs-rag-events", "ai-obs-guardrail-events",
        "ai-obs-feedback", "ai-obs-traces", "ai-obs-quality-scores", "ai-obs-anomalies",
    }
    assert families == expected


# -- 4. seeds match the frozen enums -------------------------------------------
def test_service_registry_seed_matches_service_enum():
    seed = (IAC / "postgres" / "seed" / "001_registries.sql").read_text()
    seeded = set(re.findall(r"^\s*\('([a-z0-9-]+)',", seed, re.MULTILINE))
    enum_values = {s.value for s in ServiceName}
    assert enum_values <= seeded, f"services missing from seed: {enum_values - seeded}"


# -- 5. pricing seed in sync with the SDK estimate table ----------------------
def test_model_pricing_seed_matches_sdk_cost_table():
    from ai_obs_sdk.cost import PRICING  # type: ignore

    seed = (IAC / "postgres" / "seed" / "003_metric_catalog.sql").read_text()
    rows = re.findall(
        r"\('([\w.-]+)',\s*'[\d-]+',\s*([\d.]+),\s*([\d.]+)", seed
    )
    seeded = {name: (float(i), float(o)) for name, i, o in rows}
    for model, (in_p, out_p) in PRICING.items():
        assert model in seeded, f"{model} priced in SDK but missing from model_pricing seed"
        assert seeded[model] == (in_p, out_p), f"{model} price drift SDK vs seed"


# -- 6. migration hygiene -------------------------------------------------------
def test_migrations_uniquely_numbered():
    for d in ("postgres/migrations", "postgres-events/migrations"):
        nums = [f.name[:3] for f in sorted((IAC / d).glob("*.sql"))]
        assert len(nums) == len(set(nums)), f"duplicate migration number in {d}"
        assert all(n.isdigit() for n in nums), f"non-numeric migration prefix in {d}"
