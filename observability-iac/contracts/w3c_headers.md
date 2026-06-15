# W3C TraceContext — Kafka message header contract

Every message produced to `ai-obs-events-raw` (and forwarded to `-processed`) carries
these headers so a request can be stitched across all 8 services and into the trace tree.

```
traceparent:    00-{32-hex trace-id}-{16-hex parent-id}-{flags}
tracestate:     intentiq={application_id};env={environment}
correlation_id: {correlation_id}
```

- `traceparent` follows the [W3C Trace Context](https://www.w3.org/TR/trace-context/) format
  (`version-traceid-spanid-flags`). Injected by the SDK on produce, extracted by the
  Enrichment Consumer's trace-context stage and propagated downstream.
- `tracestate` carries vendor context (`application_id`, `environment`).
- `correlation_id` is the partition key for all three topics → ordered per-request processing.

Producer/consumer wrapper pseudocode lives in
`2026-06-01_observability-plane-architecture_v2-refined.md` (W3C section).
