#!/usr/bin/env bash
# Idempotent Elasticsearch IaC apply: ILM policies -> component templates -> index templates.
#   ES_URL=http://localhost:9200 [ES_AUTH=user:pass] ./apply.sh
set -euo pipefail
cd "$(dirname "$0")"
ES_URL="${ES_URL:-http://localhost:9200}"
AUTH=()
[[ -n "${ES_AUTH:-}" ]] && AUTH=(-u "$ES_AUTH")

put() { # put <path> <file>
  local code
  code=$(curl -sS -o /tmp/es_apply_out -w '%{http_code}' "${AUTH[@]}" \
    -X PUT "$ES_URL/$1" -H 'Content-Type: application/json' --data-binary "@$2")
  if [[ "$code" != 2* ]]; then
    echo "!! FAILED $1 ($code):"; cat /tmp/es_apply_out; echo; exit 1
  fi
  echo "ok  $1"
}

echo "== ILM policies"
for f in ilm-policies/*.json; do
  put "_ilm/policy/$(basename "$f" .json)" "$f"
done

echo "== component templates"
for f in component-templates/*.json; do
  put "_component_template/$(basename "$f" .json)" "$f"
done

echo "== index templates"
for f in index-templates/*.json; do
  put "_index_template/$(basename "$f" .json)" "$f"
done

echo "== verify"
curl -sS "${AUTH[@]}" "$ES_URL/_index_template" | python3 -c \
  "import json,sys; ts=json.load(sys.stdin)['index_templates']; print('index templates:', sorted(t['name'] for t in ts if t['name'].startswith('ai-obs')))"
