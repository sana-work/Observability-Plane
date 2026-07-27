# obs-enrichment-consumer

Phase 3 of the Observability Plane: consumes every event from
`ai-obs-events-raw`, runs the 9-stage enrichment pipeline, and produces to
`ai-obs-events-processed`. Events that fail validation are quarantined on
`ai-obs-dead-letter` in a replayable shape.

## How it works

```
ai-obs-events-raw ──► EnrichmentLoop (consumer.py, group obs-enrichment)
                        │  batch of ≤200 messages, manual commit
                        ▼
                      pipeline.process()          [pipeline.py]
                        1 validate      → DeadLetterError ⇒ ai-obs-dead-letter
                        2 trace ctx     fill trace ids from traceparent header
                        3 pii redact    regex (+ optional GLiNER) on payload text
                        4 metadata      registry join (owner, lob, criticality)
                        5 error map     normalise onto error_code_catalog
                        6 cost          model_pricing + add_spend() budget check
                        7 s3 archive    offload big strings, leave s3:// pointer
                        8 slo           1h/6h burn rates → daily_slo_compliance
                        9 quality hook  sample LLM/RAG events for evals
                        ▼
                      ai-obs-events-processed  (same key + headers)
                        ▼
                      flush → commit offsets ONLY if every produce delivered
```

Guarantees and policies:

- **At-least-once.** Offsets commit only after the batch's produces are
  delivered. A crash re-emits some events; downstream dedupes on `event_id`.
- **Degrade, don't die.** Stages 3–9 are best-effort: a DB or S3 outage logs
  + counts (`obs_enrich_stage_errors_total`) and the event continues with
  that enrichment missing. Only contract violations dead-letter.
- **`user_id` stays raw** (platform decision). Redaction applies to payload
  free text only.
- **No Redis.** Registry/pricing/SLO lookups use an in-process TTL cache;
  budget counting is the atomic Postgres `add_spend()` function (crossings
  fire exactly once platform-wide).
- Budget crossings are surfaced as Prometheus counters
  (`obs_enrich_budget_threshold_exceeded_total{kind=alert|cap}`), log lines,
  and `payload.budget_alert` on the triggering event. (A dedicated
  `BUDGET_THRESHOLD_EXCEEDED` event type is proposed for contract v1.1 —
  the frozen v1.0 enum doesn't include one.)

## Setup & run — full local walkthrough (both repos from zero)

Prereqs: Docker, Python 3.11+.

```bash
# ── 1. Infrastructure (observability-iac) ────────────────────────────────
cd observability-iac
docker compose -f docker-compose.dev.yml up -d          # kafka, postgres, ES, kibana, tempo
KAFKA_ENV=dev BOOTSTRAP=localhost:9092 ./kafka/create_topics.sh
PGURL=postgres://postgres:obs@localhost:5432/postgres ./postgres/apply.sh --with-seed
PGURL=postgres://postgres:obs@localhost:5432/postgres ./postgres-events/apply.sh
ES_URL=http://localhost:9200 ./elasticsearch/apply.sh
cd ..

# ── 2. One venv for everything ───────────────────────────────────────────
python3 -m venv .venv && source .venv/bin/activate
pip install -e "ai-observability-sdk[dev]" -e "obs-enrichment-consumer[dev]"

# ── 3. Run this consumer (terminal A) ────────────────────────────────────
export OBS_ENRICH_KAFKA_BOOTSTRAP_SERVERS=localhost:9092
export OBS_ENRICH_PG_DSN=postgresql://postgres:obs@localhost:5432/postgres
export OBS_ENRICH_S3_ENABLED=false                      # no S3 on a laptop
obs-enrichment                                          # console script

# ── 4. Feed it events (terminal B) — a demo service using the SDK ────────
cat > /tmp/demo.py <<'EOF'
from fastapi import FastAPI
from ai_obs_sdk import init_observability, trace_llm

app = FastAPI()
init_observability(app)

class R:  # fake LLM result
    obs_payload = {"input_tokens": 1000, "output_tokens": 200, "finish_reason": "stop"}

@trace_llm(model_provider="vertexai", model_name="gemini-1.5-pro")
def call_model(q): return R()

@app.get("/ask")
def ask(q: str = "hi"):
    call_model(q)
    return {"ok": True}
EOF
export AI_OBS_SERVICE_NAME=gssp-gs AI_OBS_LOB=wealth AI_OBS_APPLICATION_ID=app-1234
export AI_OBS_ENVIRONMENT=dev AI_OBS_TRACING_ENABLED=false
pip install uvicorn && uvicorn demo:app --app-dir /tmp --port 8005

# ── 5. Trigger and watch (terminal C) ────────────────────────────────────
curl -s "localhost:8005/ask?q=hello" -H "X-User-ID: SOE12345"

docker compose -f observability-iac/docker-compose.dev.yml exec kafka \
  /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic ai-obs-events-processed --from-beginning
# → REQUEST_RECEIVED / LLM_CALL_* / REQUEST_COMPLETED with app_owner_team,
#   recomputed estimated_cost_usd, enriched_at — the pipeline is working.

curl -s localhost:9108/metrics | grep obs_enrich_events_processed_total
```

## Configuration

Everything is an `OBS_ENRICH_*` env var — see [.env.example](.env.example).
Notable: `GLINER_ENABLED` (NER redaction, ~512Mi — bake the model into the
image), `QUALITY_SAMPLE_PCT`, `ARCHIVE_THRESHOLD_BYTES`, `BATCH_MAX_MESSAGES`.

## Tests

```bash
pytest            # 18 tests — no Kafka/Postgres/S3 needed (all faked)
```

## Deploy

- [Dockerfile](Dockerfile) — slim image; `docker build -t obs-enrichment-consumer .`
- [k8s/deployment.yaml](k8s/deployment.yaml) — 3 replicas (≤ partition
  count 12), resources sized for GLiNER, Prometheus scrape on :9108.
  kminion + the `ObsConsumerLagHigh`/`ObsEnrichmentDown` alerts in
  observability-iac watch this service.
