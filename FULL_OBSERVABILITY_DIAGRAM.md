# AI Services Platform — Full Observability Architecture Diagram

> **How to render:**
> 1. Go to [mermaid.live](https://mermaid.live)
> 2. Delete everything in the editor
> 3. Copy **only** the lines between the triple-backtick fences below (start from `flowchart TD`, do NOT include the word `mermaid`)
> 4. Paste into the editor

---

```mermaid
flowchart TD

    subgraph SDK["ai-observability-sdk  |  pip install ai-observability-sdk  |  own repo"]
        direction LR
        sdk_models["models.py\nPydantic ObsEvent envelope"]
        sdk_emit["emitter.py\nKafka fire-and-forget + log fallback"]
        sdk_pii["pii.py\nGLiNER PiiRedactor"]
        sdk_ctx["context.py\ncorrelation_id + traceparent"]
        sdk_mw["middleware.py\nFastAPI auto-inject"]
        sdk_lf["langfuse.py\n@observe helpers + score()"]
    end

    subgraph BIZ["8 Business Services  |  existing repos  |  each adds SDK as pip dependency"]
        direction LR
        svc_ao["Agentic\nOrchestration"]
        svc_ae["Agent Executor\n@observe"]
        svc_gs["GSSP GS\n@observe"]
        svc_qs["GSSP QS\n@observe"]
        svc_rs["GSSP RS"]
        svc_di["Data Ingestion"]
        svc_cs["Consumer Service"]
        svc_uf["User Feedback\nlangfuse.score"]
    end

    BIZ -. "pip install ai-observability-sdk" .-> SDK

    subgraph KAFKA["Kafka Topics"]
        direction LR
        k_raw["ai-obs-events-raw\n7-day retention"]
        k_proc["ai-obs-events-processed\n3-day retention"]
        k_dlq["ai-obs-dead-letter\n14-day retention"]
    end

    BIZ -- "emit_event() via SDK" --> k_raw

    subgraph EC_BOX["Enrichment Consumer  |  obs-pipeline repo"]
        ec["GLiNER re-validate PII\nPydantic schema validate\nMetadata enrich via Redis cache\nToken cost calculate\nRoute to processed or DLQ"]
    end

    subgraph SC_BOX["Storage Consumer  |  obs-pipeline repo"]
        sc["Elasticsearch writer (hot 0-90d)\nSnowflake writer (all events forever)\nS3 archiver (raw payloads)\nPostgreSQL (daily_slo_compliance only)"]
    end

    subgraph AD_BOX["Anomaly Detection Service  |  obs-pipeline repo"]
        ad["Isolation Forest + LSTM\nRedis rolling baselines\nWrites anomaly events to ES"]
    end

    subgraph RCA_BOX["Offline Batch RCA Engine  |  obs-pipeline repo"]
        rca["K8s nightly CronJob\nFailure correlator\nHypothesis ranker\nWeekly digest to Slack and SES"]
    end

    subgraph CDS_BOX["Custom Dashboard Service  |  obs-pipeline repo"]
        cds["FastAPI backend\nReact + Tremor UI\nCOIN JWT auth\n16 dashboard pages"]
    end

    subgraph BOT_BOX["Observability Chatbot  |  obs-pipeline repo"]
        bot["FastAPI /chat endpoint\nIntent classifier via LLM\nMetric semantic layer\nRBAC by LOB\nQuery planner"]
    end

    k_raw --> ec
    ec -- "invalid events" --> k_dlq
    ec --> k_proc
    k_proc --> sc
    k_proc --> ad

    subgraph STORE["Storage Backends"]
        direction LR
        pg[("PostgreSQL\nControl plane only\nregistries + feedback_case\nbudget_limits + daily_slo_compliance")]
        es[("Elasticsearch\nHot events 0-90 days\nper-LOB indices\nanomalies + errors")]
        sf[("Snowflake\nAll events forever\nOn-demand analytics\nBusiness intelligence")]
        s3[("Amazon S3\nRaw payloads\nredacted prompts\nRAG contexts")]
    end

    sc --> pg
    sc --> es
    sc --> sf
    sc --> s3
    ad -- "anomaly events" --> es

    subgraph LANGFUSE["Langfuse  |  self-hosted K8s  |  langfuse.internal:3000"]
        lf_ui["LLM Trace Explorer\nRAG Pipeline Quality\nAgent Step Tree\nPrompt Analytics\nLLM-as-judge Evaluators"]
    end

    svc_ae -- "@observe step loop" --> LANGFUSE
    svc_gs -- "@observe LLM fns" --> LANGFUSE
    svc_qs -- "@observe RAG stages" --> LANGFUSE
    svc_uf -- "langfuse.score()" --> LANGFUSE

    subgraph INFRA["Infrastructure  |  deploy only  |  no custom code"]
        direction LR
        fb["Fluent Bit DaemonSet\nlog collector"]
        km["kminion Helm install\nKafka lag + metrics"]
        prom["Prometheus\nmetrics store"]
        tempo["Grafana Tempo\ntrace backend only"]
    end

    fb -- "structured logs" --> es
    km --> prom

    cds -- "recent ops" --> es
    cds -- "analytics on-demand" --> sf
    cds -- "budget config" --> pg
    cds -. "Kafka metrics" .-> prom

    bot -- "on-demand analytics" --> sf
    bot -- "recent events" --> es
    bot -- "config + workflow" --> pg
    bot -- "reads" --> s3
    bot -- "reads LLM traces" --> LANGFUSE

    rca -- "recent errors" --> es
    rca -- "historical trends" --> sf

    classDef sdk fill:#6366f1,color:#fff,stroke:#4338ca
    classDef biz fill:#16a34a,color:#fff,stroke:#15803d
    classDef kafka fill:#ea580c,color:#fff,stroke:#c2410c
    classDef consumer fill:#0891b2,color:#fff,stroke:#0e7490
    classDef analytics fill:#7c3aed,color:#fff,stroke:#6d28d9
    classDef presentation fill:#0369a1,color:#fff,stroke:#075985
    classDef storage fill:#475569,color:#fff,stroke:#334155
    classDef snowflake fill:#29b5e8,color:#fff,stroke:#0096d6
    classDef langfuse fill:#db2777,color:#fff,stroke:#be185d
    classDef infra fill:#92400e,color:#fff,stroke:#78350f

    class sdk_models,sdk_emit,sdk_pii,sdk_ctx,sdk_mw,sdk_lf sdk
    class svc_ao,svc_ae,svc_gs,svc_qs,svc_rs,svc_di,svc_cs,svc_uf biz
    class k_raw,k_proc,k_dlq kafka
    class ec consumer
    class sc consumer
    class ad analytics
    class rca analytics
    class cds presentation
    class bot presentation
    class pg,es,s3 storage
    class sf snowflake
    class lf_ui langfuse
    class fb,km,prom,tempo infra
```

---

## Colour Legend

| Colour | Layer |
|---|---|
| Indigo | `ai-observability-sdk` — shared pip package |
| Green | 8 Business Services — existing repos, add SDK as dependency |
| Orange | Kafka Topics — raw / processed / dead-letter |
| Cyan | Kafka Consumers — Enrichment + Storage (obs-pipeline) |
| Violet | Analytics Services — Anomaly Detection + RCA Engine (obs-pipeline) |
| Blue | Presentation Services — Custom Dashboard + Chatbot (obs-pipeline) |
| Slate | Storage Backends — PostgreSQL (control plane) + Elasticsearch (hot events) + S3 (payloads) |
| Blue | Snowflake — all events forever, on-demand analytics, BI |
| Pink | Langfuse — self-hosted LLM/RAG/Agent trace UI |
| Brown | Infrastructure — Fluent Bit, kminion, Prometheus, Tempo (deploy only) |

---

## Data Flow Summary (plain text)

```
8 Business Services  (add SDK as pip dependency)
   |
   |-- emit_event() via SDK ---------> ai-obs-events-raw  (Kafka)
   |                                          |
   |                               Enrichment Consumer
   |                               validate + enrich + cost
   |                                  |            |
   |                               invalid      enriched
   |                                  |            |
   |                           dead-letter    ai-obs-events-processed  (Kafka)
   |                                                |
   |                                    +-----------+-----------+
   |                                    |                       |
   |                            Storage Consumer       Anomaly Detection
   |                            ES + Snowflake         Isolation Forest
   |                            + S3 + PG(slo only)
   |                                                           |
   |                                                   ES anomalies index
   |
   |-- @observe  (Agent Executor, GSSP GS, GSSP QS) --> Langfuse
   |-- langfuse.score()  (User Feedback)             --> Langfuse


Presentation reads from storage:
   Custom Dashboard --> PostgreSQL + Elasticsearch + Prometheus (Kafka metrics)
   Chatbot          --> PostgreSQL + Elasticsearch + S3 + Langfuse
   RCA Engine       --> PostgreSQL + Elasticsearch  (nightly, digest to Slack/SES)

Infrastructure (deploy only - no custom code):
   Fluent Bit DaemonSet --> Elasticsearch  (container logs)
   kminion              --> Prometheus     (Kafka metrics)
   Grafana Tempo                           (trace backend, receives OTel traces)
```

---

## Repository Map

| Repo | Type | Contents |
|---|---|---|
| `ai-observability-sdk` | New — pip package | models, emitter, pii, context, middleware, langfuse helpers |
| `obs-pipeline` | New — services | enrichment-consumer, storage-consumer, custom-dashboard, anomaly-detection, rca-engine, obs-chatbot |
| `observability-iac` | New — infra | Kafka topic scripts, PostgreSQL DDL migrations, ES index templates, CI deploy pipeline |
| `agentic-orchestration` | Existing | Add SDK + ObsMiddleware + emit_event() calls |
| `agent-executor` | Existing | Add SDK + @observe on step execution loop |
| `gssp-gs` | Existing | Add SDK + @observe on LLM generator functions |
| `gssp-qs` | Existing | Add SDK + @observe on all 5 RAG pipeline stages |
| `gssp-rs` | Existing | Add SDK + emit_event() on retrieve and embed |
| `data-ingestion` | Existing | Add SDK + emit_event() calls |
| `consumer-service` | Existing | Add SDK + emit_event() calls |
| `user-feedback` | Existing | Add SDK + ObsMiddleware + langfuse.score() |
