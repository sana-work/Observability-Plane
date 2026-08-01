# Presentation Runbook — Observability Plane (12 min + Q&A)

Two artifacts, one story. Keep this open on a second screen.

- **Tab 1** — `observability-roadmap.html` → the *what and why* (stakeholder spine)
- **Tab 2** — `ai-observability-sdk/docs/sdk-request-flow.html` → the *how* (technical proof)

**The one thing to get right:** most architecture reviews are static diagrams. Yours has a
**live simulator**. That is your differentiator — build to it, land it at minute 6, and let it
do the convincing.

---

## Pre-flight (2 minutes before you start)

- [ ] Both files open in separate browser tabs, **Tab 1 active**
- [ ] Roadmap scrolled to the very top (hero visible)
- [ ] SDK page: scenario dropdown reset to **"Happy path"**, press **↺ Reset**
- [ ] Browser zoom ~110% if projecting; close the dev console
- [ ] Know your two demo scenarios cold: **Happy path** and **Kafka broker is DOWN**
- [ ] Have one sentence ready for "how much does this cost us?" (see Q&A)

---

## The 12-minute run sheet

### ⏱ 0:00 – 1:30 · The problem (Tab 1, hero — but mostly talk to the room)

**Do not read the screen.** Look at the audience. The hero is just a backdrop.

> "We run eight AI services. Today, when a user tells us they got a bad answer, we cannot
> reconstruct what happened. Each service logs in its own format, and nothing connects them.
>
> Three things we genuinely cannot answer right now:
> **One** — what did that request cost us? We're spending real money on model calls with no
> attribution per application.
> **Two** — which prompt version produced that answer? We can't tell whether a change made
> quality better or worse.
> **Three** — for a compliance question, we can't produce the evidence trail.
>
> This isn't a tooling gap. It's a *correlation* gap. That's what we set out to fix."

**Highlight:** the five hero stats — 8 services, 50 event types, 5 storage layers, 9 enrichment
stages, 3 phases in progress.

---

### ⏱ 1:30 – 3:00 · Current state, with evidence (Tab 1 → nav **Gap Assessment**)

Click nav **"Gap Assessment"**. Scroll to the field-coverage table.

> "We didn't guess at this. We audited all eight services field by field. Here's what we found."

**Point at exactly three rows — don't read the table:**

| Point at | Say |
|---|---|
| `span_id` / `schema_version` / `lob` — all ✗ | "Universally missing. Nothing to correlate on." |
| `timestamp` — mixed ⚠ | "Three different time formats across services." |
| `latency_ms` — ⚠ strings | "Latency as a string in some services, seconds in others. Un-aggregatable." |

> "Two of eight services emit to Kafka at all. This is what 'every team does their own
> observability' produces — and it's the strongest argument for a shared contract."

---

### ⏱ 3:00 – 5:00 · The architecture (Tab 1 → nav **Event Pipeline**)

> "The design is one sentence: **every service emits the same event shape to one Kafka topic,
> one central service enriches everything, and three stores serve three different questions.**"

Show the **Kafka Topics** table (raw 7d / processed 3d / dead-letter 14d).

> "Three topics. Raw is what services produce. Processed is what's been cleaned. Dead-letter is
> where broken events go — visible and replayable, never silently dropped."

**Then the interactive moment #1** — the 9-stage pipeline:

> "Every event goes through nine stages before it's stored. Cross-cutting work happens once,
> centrally — services never carry it."

👉 **Click stage 3 (PII Redactor)**, then **stage 6 (Cost Calculator)**.

> "Stage 6 is where cost governance actually happens — we recompute from the authoritative
> price table and update the budget counter atomically, so a threshold alert fires exactly
> once even with three replicas running."

**Land the error policy** (bottom panel):

> "One rule that matters: stage 1 is the only gate. A malformed event is quarantined.
> But if Postgres or S3 is down, stages 2 through 9 *degrade* — the event still flows with
> one enrichment missing. An infrastructure hiccup must never poison the queue with valid data."

---

### ⏱ 5:00 – 8:00 · 🎯 THE CENTREPIECE — switch to Tab 2

> "That's the design. Let me show you what actually happens to a single request."

#### Demo 1 — Happy path (~90 seconds)

Scenario: **"Happy path — LLM answer succeeds"** → press **▶ Play**, then narrate over it:

> "A user calls `/generate`. The middleware mints the correlation ID and copies the user ID.
> The decorator wraps the model call — child span, nested under the request.
> Model answers. Tokens and cost computed automatically — the team wrote none of that.
> Response returns, correlation ID echoed back so a bug report can quote it."

👉 **Click one event on the right** to expand its JSON.

> "This is the actual payload landing on Kafka. Same envelope for all eight services.
> One key point — see `correlation_id`? Same value on all four events. That's the join key
> for the entire platform."

#### Demo 2 — Broker down (~60 seconds) ← *the trust-builder*

> "Every engineer's first question is: what does this cost my service? Watch."

Scenario: **"🔌 Kafka broker is DOWN"** → **▶ Play**.

> "Kafka is completely down. The user still gets their 200, at identical latency. Events drop
> with a warning and a counter — see the counters top-right.
> The trade is deliberate: **telemetry may be lossy; the product may never be slow.**
> That's the promise that makes eight teams willing to adopt this."

> *(If the audience is technical and time allows, add:)* Scenario **"Agent Executor"** —
> "Same correlation ID flows from Orchestration into the Executor over Kafka headers.
> Three-level span tree: agent → tool → model. This is what 'reconstruct any request' means."

---

### ⏱ 8:00 – 10:00 · Where we actually are (back to Tab 1 → nav **Delivery Plan**)

**Be precise here. Credibility lives in this section.**

> "Eight phases. Here's the honest status."

👉 Click the **⏳ In progress (3)** filter.

> "Three phases are in progress — not planned, in progress. The code exists:
> the foundation infrastructure-as-code, the shared SDK, and the enrichment consumer.
> **Sixty-six automated tests pass across the three.** What you just saw in the simulator is
> the behaviour of the real library, not a mockup.
>
> But none of them is signed off, and — this matters — **nothing is deployed to a shared
> environment yet.**"

👉 Click **Planned (6)** → **All phases**.

> "Phase 2 — instrumenting the eight services — is the next real milestone, and it's work for
> the service teams, not the platform team. Each service is about three changes: install the
> package, add an env block, add one line at startup. Then decorate the hot paths."

---

### ⏱ 10:00 – 11:30 · What's ahead + the ask

> "Near term: publish the SDK, deploy the foundation to dev, and onboard the first two services
> — Agentic Orchestration and Agent Executor, because they already produce to Kafka and have
> the richest signals.
>
> After that: the storage consumer, which is when Kibana dashboards light up almost for free.
> Then the AI-quality layer — prompt versioning and LLM-as-judge scoring — and the custom
> dashboard.
>
> Two decisions are still open and I'd like input: **Snowflake and Redis are both deferred**;
> we're running production-ready interim stand-ins in Postgres, and the swap path is contained.
> And the AI-quality layer is a build-versus-buy call we haven't closed."

**Close on the ask — pick the one you actually need:**

> "What I need from this group: [a decision on X] / [two service teams committed for Phase 2] /
> [environment access to deploy the foundation]."

---

### ⏱ 11:30+ · Q&A

---

## Anticipated questions — prepared answers

**"Why a shared library? Why not just have each service publish to Kafka in the agreed format?"**
*(This will come up — it's already been raised in Teams.)*

> "We agree on the principle — that IS the design. There are no service-specific wrappers;
> it's one frozen contract, one topic, no transformation layer. The library is the contract
> made executable. Without it, eight teams each reimplement fire-and-forget producing,
> correlation propagation, trace-header injection and validation — eight times, eight ways.
> The gap assessment is the evidence: that's exactly what we have today, and it's why nothing
> correlates. A contract without a reference implementation is a PDF nobody follows."

**"What's the performance overhead on my service?"**
> "Microseconds. `produce()` only appends to an in-memory queue — no network call happens on
> the request path. You saw the broker-down demo: identical latency with Kafka completely gone."

**"Why not Datadog / Langfuse / just use Grafana?"**
> "Grafana is deployed — as an internal ops console, no ingress. It doesn't generate data, it
> queries it, and it can't model our per-LOB access control. Langfuse is a genuine option for
> the eval layer specifically, but self-hosting it needs Redis and ClickHouse, which collides
> with a constraint we already have. Neither replaces the event pipeline."

**"Why is `user_id` stored raw and not hashed?"**
> "Deliberate platform decision. Audit trails and the 'by SOEID' dashboards need the real value.
> Protection is per-LOB access control on the stores plus compliance retention — not hashing.
> Free text inside payloads is still PII-redacted centrally."

**"When can my team onboard?"**
> "As soon as the SDK is published — days, not weeks. Your side is three changes and about half
> a day, plus decorating your hot paths. There's a per-service guide with the exact edits."

**"How much will this cost to run?"**
> "The interim design deliberately avoids new licensed infrastructure: no Snowflake, no Redis,
> no vendor APM. It's Kafka, Postgres, Elasticsearch and S3 — all of which we already run."

---

## If you get squeezed to 5 minutes

Cut to these three beats:
1. **Problem** (60s) — the three questions we can't answer.
2. **Simulator, happy path + broker down** (3 min) — Tab 2 only. This carries the whole story.
3. **Status + ask** (60s) — three phases in progress, 66 tests, nothing deployed, here's what I need.

Skip: gap assessment table, storage layers, event catalog, telemetry schemas.

## If something breaks

- **Simulator won't run** → press ↺ Reset, or use **Step →** manually (more controllable anyway).
- **Projector renders it tiny** → browser zoom to 125–150%; the layout is responsive.
- **Asked for depth you don't have live** → "That's in the implementation guide, I'll follow up"
  — better than improvising. `IMPLEMENTATION.md` has the module-by-module detail.

## Don't do these

- ❌ Read tables aloud — point at 2–3 cells and characterise them
- ❌ Demo more than 2–3 simulator scenarios — you'll blow the clock
- ❌ Say "done" or "built" about any phase — the honest line is *in progress, nothing deployed*
- ❌ Scroll aimlessly while talking — use the nav links, they're one click each
