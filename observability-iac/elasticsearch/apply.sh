#!/usr/bin/env bash
# Phase 0 / Task 0.4 — apply ILM policies, component templates, and one composable
# index template per event category. Idempotent (PUT). $ES = https://host:9200
set -euo pipefail
ES="${ES:?set ES=https://host:9200}"
DIR="$(cd "$(dirname "$0")" && pwd)"
H='-H Content-Type:application/json'

# 1) ILM policies
curl -sS -XPUT "$ES/_ilm/policy/hot-warm-30d"   $H -d @"$DIR/ilm-policies/hot-warm-30d.json"
curl -sS -XPUT "$ES/_ilm/policy/compliance-180d" $H -d @"$DIR/ilm-policies/compliance-180d.json"

# 2) Component templates
curl -sS -XPUT "$ES/_component_template/obs-common-settings" $H -d @"$DIR/component-templates/obs-common-settings.json"
curl -sS -XPUT "$ES/_component_template/obs-common-mappings" $H -d @"$DIR/component-templates/obs-common-mappings.json"

# 3) One index template per category (generated — keeps templates in lockstep).
CATEGORIES="requests errors llm-calls rag-events agent-steps tool-calls guardrail-events feedback traces quality-scores anomalies"
for cat in $CATEGORIES; do
  curl -sS -XPUT "$ES/_index_template/ai-obs-$cat" $H -d "{
    \"index_patterns\": [\"ai-obs-*-$cat-*\"],
    \"composed_of\": [\"obs-common-settings\", \"obs-common-mappings\"],
    \"template\": { \"settings\": {
      \"index.lifecycle.name\": \"hot-warm-30d\",
      \"index.lifecycle.rollover_alias\": \"ai-obs-$cat\"
    }},
    \"priority\": 200,
    \"_meta\": { \"category\": \"$cat\" }
  }"
  echo " -> applied ai-obs-$cat"
done

# Regulated LOBs override to compliance-180d via a higher-priority per-LOB template (added in Phase 4).
echo "elasticsearch templates + ILM applied."
