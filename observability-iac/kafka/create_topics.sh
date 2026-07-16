#!/usr/bin/env bash
# Idempotent topic apply for topics.yaml.
#   BOOTSTRAP=kafka-broker:9092 [KAFKA_ENV=dev] [COMMAND_CONFIG=/path/client.properties] ./create_topics.sh
# KAFKA_ENV=dev  -> replication factor 1, min.insync.replicas dropped (single-broker laptop stack).
# Requires: kafka-topics / kafka-configs on PATH (or set KAFKA_BIN), python3 with PyYAML.
set -euo pipefail

cd "$(dirname "$0")"
BOOTSTRAP="${BOOTSTRAP:-localhost:9092}"
KAFKA_ENV="${KAFKA_ENV:-prod}"
KAFKA_BIN="${KAFKA_BIN:-}"
CMD_CFG=()
[[ -n "${COMMAND_CONFIG:-}" ]] && CMD_CFG=(--command-config "$COMMAND_CONFIG")

kt() { "${KAFKA_BIN}kafka-topics${SH_EXT:-}" --bootstrap-server "$BOOTSTRAP" "${CMD_CFG[@]}" "$@"; }
kc() { "${KAFKA_BIN}kafka-configs${SH_EXT:-}" --bootstrap-server "$BOOTSTRAP" "${CMD_CFG[@]}" "$@"; }

# topics.yaml -> lines of: name|partitions|rf|k=v,k=v
mapfile -t TOPIC_LINES < <(python3 - <<'PY'
import yaml
doc = yaml.safe_load(open("topics.yaml"))
default_rf = doc.get("defaults", {}).get("replication_factor", 3)
for t in doc["topics"]:
    cfgs = ",".join(f"{k}={v}" for k, v in (t.get("configs") or {}).items())
    print(f'{t["name"]}|{t["partitions"]}|{t.get("replication_factor", default_rf)}|{cfgs}')
PY
)

EXISTING="$(kt --list)"

for line in "${TOPIC_LINES[@]}"; do
  IFS='|' read -r name partitions rf configs <<<"$line"
  if [[ "$KAFKA_ENV" == "dev" ]]; then
    rf=1
    configs="$(sed -E 's/(^|,)min\.insync\.replicas=[0-9]+//; s/^,//; s/,,/,/' <<<"$configs")"
  fi

  if grep -qx "$name" <<<"$EXISTING"; then
    echo ">> $name exists — aligning configs"
    [[ -n "$configs" ]] && kc --alter --entity-type topics --entity-name "$name" --add-config "$configs"
    current_parts=$(kt --describe --topic "$name" | grep -o 'PartitionCount: [0-9]*' | awk '{print $2}')
    if (( current_parts < partitions )); then
      echo ">> $name: growing partitions $current_parts -> $partitions"
      kt --alter --topic "$name" --partitions "$partitions"
    fi
  else
    echo ">> creating $name (partitions=$partitions rf=$rf)"
    args=(--create --topic "$name" --partitions "$partitions" --replication-factor "$rf")
    if [[ -n "$configs" ]]; then
      IFS=',' read -ra kvs <<<"$configs"
      for kv in "${kvs[@]}"; do [[ -n "$kv" ]] && args+=(--config "$kv"); done
    fi
    kt "${args[@]}"
  fi
done

echo ">> verify:"
kt --describe --topic ai-obs-events-raw | head -1
kt --describe --topic ai-obs-events-processed | head -1
kt --describe --topic ai-obs-dead-letter | head -1
