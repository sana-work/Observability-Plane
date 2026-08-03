# Observability Plane — Speaker's Document

Everything you need to present both documents fluently and answer anything that comes back.
Written in the voice you'll actually speak in.

**How to use this:** read Part 1 and Part 5 properly — the narrative and the decision log.
Those two are what make you sound like the person who designed this, because they're the
*reasoning*, not the facts. Parts 2 and 3 are the walkthroughs; skim them before you present.
Part 6 is Q&A depth.

---

# Part 1 — The spine of the story

Everything you say should hang off five beats, in this order:

1. **We're flying blind.** Eight-plus AI services, each logging its own way, nothing connects.
2. **It costs us three things.** We can't debug, can't attribute cost, can't produce evidence.
3. **The fix is one contract, not one tool.** Every service describes its work the same way.
4. **It's real.** Here's a request moving through the actual library, live.
5. **Here's exactly where we are, and what I need.**

If you only remember one sentence to fall back on when you lose your thread:

> "The problem was never a missing tool — it was that nothing correlates. Everything I built
> exists to give one request one identity, end to end."

---

# Part 2 — Walking the Roadmap document

## Opening — the hero

*Don't read the screen. Look at the room.*

> "We run more than eight AI services now — agent orchestration, agent execution, an LLM
> gateway, RAG query and retrieval, document ingestion, user feedback. Every one of them logs
> something. None of it connects.
>
> So when a user tells us they got a bad answer, we can't reconstruct what happened. Three
> questions I genuinely could not answer before this work:
>
> **What did that request cost us?** We're spending real money on model calls with no
> attribution per application.
> **Which prompt version produced that answer?** We can't tell whether a change made quality
> better or worse.
> **And for a compliance question — where's the evidence trail?**
>
> This isn't a tooling gap, it's a correlation gap. That distinction drove the whole design."

**The hero numbers, and what each one means** — know these cold:

| Number | What it is | Why it's that number |
|---|---|---|
| **8+** AI services | the producers | grew past the original eight; the design doesn't cap it |
| **50** event types | the controlled vocabulary | every distinct thing worth recording, frozen so it can't sprawl |
| **6** storage layers | ES, Postgres ×2, S3, warehouse, Redis | four live, two future — deliberately staged |
| **9** enrichment stages | central processing | the cross-cutting work services shouldn't carry |
| **8** delivery phases | the plan | foundation → SDK → instrument → enrich → store → quality → dashboards → chatbot |
| **3** phases in progress | honest status | code exists and passes tests; nothing deployed |

## Current state — the gap assessment

> "I didn't guess at the problem. I audited all the services field by field — this table is
> that audit."

Point at **three rows only**:

- **`span_id`, `schema_version`, `lob` — missing everywhere.** "There is literally nothing to
  correlate on. That's the headline finding."
- **`timestamp` — mixed.** "Three different time formats across services."
- **`latency_ms` — strings in some services, seconds in others.** "You can't aggregate that.
  You can't build an SLO on it."

Then the line that lands:

> "Two of the services emit to Kafka at all today. This table is what 'every team does its own
> observability' actually produces — and it's the single strongest argument for a shared
> contract."

## Architecture — the diagram

> "One sentence: **every service emits the same event shape to one Kafka topic, one central
> service enriches everything, and each store serves a different question.**"

Walk the six numbered bands on the diagram — producers, Kafka, enrichment, storage,
presentation, governance. Don't narrate every box; land the shape.

## Event pipeline — the three topics

| Topic | Retention | Why that number |
|---|---|---|
| `ai-obs-events-raw` | 7 days | a week to survive a bad weekend and still replay |
| `ai-obs-events-processed` | 3 days | consumers read within minutes; 3 days covers an outage |
| `ai-obs-dead-letter` | 14 days | humans debug these — give them two weeks |

> "Broken events go to dead-letter — visible and replayable, never silently dropped. If we
> lose data I want it to be loud."

## The 9-stage pipeline — click two stages

**Click stage 3 (PII Redactor):**
> "Redaction happens centrally, once. If I'd pushed this to the services, I'd be trusting
> eight teams to each get PII handling right, and I'd have no way to prove it."

**Click stage 6 (Cost Calculator):**
> "This is where cost governance actually happens. The SDK attaches an estimate so cost is
> visible immediately — but here we recompute from the authoritative pricing table and update
> the budget counter atomically, so a threshold alert fires **exactly once** even with three
> replicas running concurrently. That 'exactly once' is a real problem I had to solve in the
> database, not in application code."

**Then land the error policy — this is a design point worth dwelling on:**
> "One rule governs the whole pipeline. Stage 1 is the only gate: a malformed event is
> genuinely wrong, so it's quarantined. But stages 2 through 9 **degrade rather than die** —
> if Postgres or S3 is down, the event still flows with one enrichment missing.
>
> That's deliberate. An infrastructure hiccup must never poison the dead-letter queue with
> perfectly valid events. Getting that wrong turns a five-minute database blip into a day of
> manual replay."

## Storage — why six layers, not one

> "Each store answers a different question, and the honest reason there are several is that
> no single store answers all of them affordably."

- **Elasticsearch** — "sub-second search: *show me every A0001 error today*. Per-LOB indices,
  which is also how access control works — index permissions, not application logic."
- **Postgres control plane** — "small, authoritative configuration. Registries, budgets, SLO
  definitions. Transactional integrity where it matters."
- **Postgres firehose (`obs_events`)** — "SQL analytics on every event. Monthly partitions,
  90-day window. Dropping a partition is instant; `DELETE` on a billion rows is an outage."
- **S3** — "big and cold. Full prompts, traces, audit evidence. Tiered automatically."
- **Snowflake or Databricks** — "future, and deliberately still open."
- **Redis** — "future. Everything it would do, I'm doing in Postgres today."

## Monitoring — the two panels

> "This is what my team actually looks at."

**Trace panel:** "One request, 1041 milliseconds — retrieval 96, the model 780, the rest
overhead. I don't write any code for this; the SDK auto-instruments the web framework, the
HTTP client and the database driver."

**Health panel:** "Consumer lag and dead-letter rate are the two golden signals. A pipeline
that silently stops is worse than no pipeline — because people trust dashboards that have
quietly stopped updating."

## Delivery plan — the honest status

> "Three phases are in progress — the foundation infrastructure-as-code, the shared SDK, and
> the enrichment consumer. The code exists and passes its tests. What you saw in the simulator
> is the real library, not a mockup.
>
> But none of them is signed off, and nothing is deployed to a shared environment yet.
> I'd rather tell you that plainly than claim a green tick I can't defend."

Then the onboarding box — **this pre-empts the room's real question**:

> "For a service team, adopting this is four steps. Install the package. Add one environment
> block to your Helm values — no code. Add one line at startup. Then decorate your hot paths.
> Steps one to three take about half a day and already give you request events, traces,
> structured logs and a metrics endpoint. Step four is incremental."

---

# Part 3 — Walking the SDK document

> "That was the design. Let me show you what actually happens to a single request."

## The framing

> "The SDK is a small Python library each service installs. It has one job: capture what the
> service did, in a shape everything downstream understands. One line at startup, one decorator
> per interesting call."

## Demo 1 — Happy path

Narrate over the animation rather than reading it:

> "A user calls `/generate`. **The middleware mints the correlation ID** — this is where a
> request's identity is born; every event, log line and span after this inherits it
> automatically.
>
> The decorator wraps the model call, opening a child span so the trace nests properly.
> The model answers. Tokens and cost are computed automatically — the team wrote none of that.
> The response returns with the correlation ID echoed back, so a user's bug report can quote
> the exact ID I need."

**Click an event to expand the JSON:**

> "This is the real payload. Same envelope for every service. Look at `correlation_id` — the
> same value on all four events. That single field is what the entire platform hangs on."

## Demo 2 — Broker down *(the one that wins the room)*

> "Every engineer's first question is: what does this cost my service? So let's kill Kafka
> completely."

> "The user still gets their 200, at identical latency. The events drop with a warning and a
> counter — you can see the counters top-right.
>
> That's the deliberate trade: **telemetry may be lossy, the product may never be slow.**
> `produce()` only appends to an in-memory queue — no network call ever happens on the request
> path. That guarantee is why eight teams will agree to adopt this."

## Demo 3 — Agent Executor *(only if the room is technical and you have time)*

> "This is the one I'm proudest of. The same correlation ID flows from Orchestration into the
> Executor through Kafka message headers — the trace doesn't die at the queue boundary.
> And look at the span tree: agent, then tool, then model, three levels deep. That's what
> 'reconstruct any request' actually means in practice."

## The envelope explorer

Click **`correlation_id`**, then **`payload`**:

> "The envelope is frozen — every event carries these fields. Everything domain-specific goes
> in `payload`. That's why adding a new fact never requires a schema migration: the envelope
> stays stable, the payload flexes."

---

# Part 4 — The numbers to know cold

| Fact | Value |
|---|---|
| Event types in the contract | **50**, across 10 categories |
| Services in the contract today | 8, with the design open to more |
| Kafka partitions (raw/processed) | 12 · keyed by `correlation_id` |
| Producer batching window | 50 ms · lz4 compression |
| Local queue cap | 100,000 messages, then drop-with-warning |
| Enrichment replicas | 3, manual offset commit |
| Postgres firehose window | ~90 days, monthly partitions |
| Elasticsearch retention | 30 days default, 180 for regulated LOBs |
| S3 tiering | Standard → IA at 30d → Glacier at 180d |
| Quality sampling rate | 5% of LLM/RAG events |
| SLO burn-rate page threshold | 1h burn > 14.4× |
| Consumer lag alert | > 1000 sustained 10 minutes |

---

# Part 5 — The decision log

**This is the most important section.** Anyone can describe a design; only the person who made
it can explain what they rejected and why. Each of these is "I considered X, chose Y, and
here's what it costs."

### 1. Kafka-direct, not an ingestion service

**The alternative:** an Observability Ingestion Service — services POST telemetry to an API
that writes to Kafka.
**Why I rejected it:** it adds a synchronous network hop on the request path of every service,
plus a component that can be down. It's a single point of failure in front of something whose
entire job is to still work when things are failing.
**What I chose:** services produce straight to Kafka via the SDK, fire-and-forget.
**The cost:** every service needs Kafka credentials and connectivity. Acceptable — they're all
already in the same cluster.

### 2. A shared library, not "everyone publishes to an agreed format"

**The alternative — and this has been raised:** just define the Kafka message contract and let
each service publish directly, no library to maintain.
**Why I rejected it:** the contract isn't the hard part. The hard part is fire-and-forget
producer semantics, correlation propagation, W3C trace-header injection, partition keying,
UTC timestamp discipline, and validation. Without a shared implementation, eight teams write
those 500 lines eight times, eight ways.
**The proof:** the gap assessment. That's precisely what we have today — three timestamp
formats, latency as strings, no correlation IDs. A contract without a reference implementation
is a PDF nobody follows.
**Important nuance to state:** the library is **not** a mapping layer. There are no
service-specific wrappers in it. It *is* the contract, made executable.

### 3. A frozen 50-type vocabulary, not free-form event names

**The alternative:** let services name their own events.
**Why I rejected it:** event type is a dimension every dashboard groups by. One typo —
`LLM_CALL_COMPLETE` instead of `COMPLETED` — creates a permanent junk dimension and silently
splits every metric.
**What I chose:** a closed enum, validated in the SDK *before the event leaves the process* and
re-validated by the consumer.
**The cost:** adding an event type is a contract change. That friction is the point — it forces
a conscious decision.

### 4. Never block, never raise

**The alternative:** confirm delivery, or surface errors to the caller.
**Why I rejected it:** this code runs inside the request path of production services. If it can
add latency or throw, the first incident gets it ripped out — correctly.
**What I chose:** the producer hands messages to an in-memory queue and returns; a background
thread does all network I/O. Every failure path — broker down, queue full, bad payload —
becomes a log line and a counter.
**The cost:** telemetry can be lost. I accept that explicitly, and I made the loss *visible*
through counters rather than silent.

### 5. Central enrichment, not enrichment at the edge

**The alternative:** each service redacts PII, computes cost, evaluates SLOs itself.
**Why I rejected it:** that's asking eight teams to each implement compliance-sensitive logic
correctly, and to update it in lockstep when pricing or policy changes.
**What I chose:** one consumer, nine stages, one place to fix.
**The cost:** a component that must keep up. Which is why consumer lag is a paging alert.

### 6. Degrade, don't die

**The alternative:** dead-letter anything that fails any stage.
**Why I rejected it:** that conflates "this event is wrong" with "our database is briefly
down". The second would dump millions of perfectly valid events into a queue needing manual
replay.
**What I chose:** stage 1 (validation) is the only gate; stages 2–9 log, count, and continue.

### 7. At-least-once with commit-after-produce

**The alternative:** commit offsets first (at-most-once, loses data on crash), or attempt
exactly-once (complex, expensive).
**What I chose:** produce, flush, *then* commit. A crash re-emits some events; it never loses
one. Duplicates are harmless because every event carries a unique `event_id` and every
downstream write is idempotent — Elasticsearch uses it as the document ID, Postgres does
`ON CONFLICT DO NOTHING`.

### 8. Postgres now, Snowflake or Databricks later

**Why:** the analytics store isn't decided, and I wasn't willing to let that block the pipeline.
So I built the interim in partitioned Postgres with the *same* `event_id`/`correlation_id`
model, in its own schema, separate from the control plane.
**What that buys:** when a warehouse is chosen, the swap touches the storage consumer's writer
and the dashboard queries. Nothing else. The decision stays reversible.

### 9. Redis deferred

**Why:** Redis isn't approved, and everything I need it for has a workable Postgres equivalent
— an atomic function for budget counting, in-process TTL caches for registry lookups.
**The one thing I had to solve:** budget threshold alerts firing exactly once across three
replicas. That lives in a single Postgres function, so concurrency is the database's problem,
not the application's.
**The swap path:** the caching is isolated to one module.

### 10. Grafana as an internal ops console only

**Why not Grafana for stakeholders:** it doesn't generate data — it queries data we already
own. And it can't cleanly model COIN-JWT SSO with per-LOB row-level access.
**What I chose:** Grafana deployed with no ingress, platform team only via port-forward, with
Prometheus and Tempo datasources wired. Stakeholders get the custom dashboard.
**Still open:** reusing Grafana's open-source React components inside our dashboard in Phase 5
— components, not a server.

### 11. A custom AI-quality layer, not Langfuse

**Why:** self-hosting Langfuse pulls in Redis and ClickHouse — both collide with constraints
we already have. The one genuinely attractive piece is its managed LLM-as-judge evaluators.
**What I designed instead:** an eval service that samples 5% of LLM and RAG events and writes
scores into the same `quality_scores` table.
**Kept open:** if we ever want Langfuse, it can run headless behind our pipeline and sync
scores into that same table — the dashboard wouldn't know the difference.

### 12. Hashed user identity

**What:** user identity is hashed before it leaves the service.
**Why:** dashboards can still group and count by user without the platform storing raw
identifiers in every downstream store. Free text inside payloads is separately redacted
centrally, because a user can paste anything into a prompt.

---

# Part 6 — Questions you'll get, with real depth

**"Why not just buy Datadog / New Relic?"**
> "They're excellent at infrastructure telemetry and I'd have no argument if that were the
> problem. But our questions are AI-specific: which prompt version, how many tokens, was the
> retrieval grounded, what did this application spend this month. That's a domain model, not a
> metrics feed. And the interim design deliberately uses infrastructure we already run —
> Kafka, Postgres, Elasticsearch, S3 — so there's no new licensing."

**"What happens when a service sends a malformed event?"**
> "It never reaches Kafka. The SDK validates against the contract when it constructs the
> event, so a bad event type fails in the team's own unit tests. If something invalid does
> arrive, stage 1 quarantines it to the dead-letter topic wrapped with the reason and the
> original payload, and there's a replay script to re-drive it after a fix."

**"How do you know the SDK doesn't slow us down?"**
> "Because no network call happens on the request path. `produce()` appends to an in-memory
> queue and returns in microseconds; a background thread batches and sends. I demonstrate it
> by killing the broker — latency is unchanged."

**"What if the enrichment consumer falls behind?"**
> "Consumer lag is exported per group and pages at over 1000 sustained for ten minutes. It
> scales horizontally up to the partition count — twelve. And because the pipeline is
> at-least-once with idempotent writes, catching up can never duplicate data."

**"How much operational burden is this?"**
> "Two new services — enrichment and storage — three replicas each, plus infrastructure we
> already run. The design choice that matters here is that everything else degrades: if
> Postgres or S3 has a blip, events keep flowing with one enrichment missing."

**"Why one topic instead of a topic per event type?"**
> "Ordering. All events of one request share a partition because I key by correlation ID,
> which means the consumer sees a request's story in order. Split across topics and you lose
> that, and you multiply consumer-group management by fifty."

**"Can we add a new service easily?"**
> "The instrumentation side, yes — install, config, one line. The one place I'd tighten is
> that service names are currently a frozen list in the contract, so a new service needs a
> contract change. I'd move that validation to the registry table so onboarding is a database
> row instead of a release. That's a known improvement, not a surprise."

**"What's the biggest risk?"**
> "Adoption. The platform work is the easy half — the pipeline can't prove value until
> services actually emit. That's why I made integration one line, and why Phase 2 is scoped as
> service-team work with a per-service guide."

## If you genuinely don't know something

Don't invent. This lands better than a guess:

> "I'd have to check that — I don't want to give you a number I can't stand behind. I'll follow
> up today."

---

# Part 7 — Phrases that carry ownership

Use these naturally; they signal you made the calls rather than reporting them.

- "The design decision I made here was…"
- "I considered X, but rejected it because…"
- "What that costs us is… and I accepted it because…"
- "The problem I had to solve was…"
- "I deliberately left that open, because…"
- "That's a known gap, and here's how I'd close it."
- "I'd rather tell you plainly than claim a green tick I can't defend."

And the two closing lines, depending on the room:

**For leadership:**
> "The foundation is built and the design is proven end to end. What I need now is the service
> teams for Phase 2 — that's what turns this from a pipeline into answers."

**For engineers:**
> "Everything you saw is real code with tests behind it. The integration is one line. I'd like
> two teams to onboard first so we can prove the full chain across services."
