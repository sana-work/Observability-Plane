# Changelog

All notable changes to `ai-observability-sdk`.

Versioning while pre-1.0: any change to the vendored contract in
`ai_obs_sdk/contracts/` that the Enrichment Consumer would reject — `EventType`,
the `ObsEvent` envelope, `ServiceName` — is a **minor** bump and needs a
coordinated rollout with the consumer. Everything else is a patch.

Consumers should pin `~=0.1.0`: patches flow automatically, minors do not.

## [0.1.0] — unreleased

First release to the internal index.

### Added
- `init_observability(app)` — one call wires structured logging, OTEL tracing,
  request-context middleware and the `/metrics` endpoint.
- `emit_event()` plus the `@trace_llm` / `@trace_rag` / `@trace_tool` /
  `@trace_agent` decorators, emitting to Kafka `ai-obs-events-raw`
  fire-and-forget (never raises, never blocks the request path).
- `get_prompt()` — versioned, hashed, TTL-cached prompt registry client.
- Vendored event contract (`EventType`, `ObsEvent`, `ServiceName`), kept in
  lockstep with `observability-iac/contracts/` by a CI drift gate.
- `py.typed` — the package ships type information to consumers.

### Packaging
- Version single-sourced from `ai_obs_sdk/__init__.py`; the release job asserts
  the `sdk-v*` git tag matches it.
- `starlette` declared as a direct runtime dependency. It is imported at module
  level by `middleware.py` but was previously only satisfied transitively via
  `prometheus-fastapi-instrumentator`.
- Upper version bounds on all runtime dependencies, so a major release of OTEL
  or pydantic cannot break consumers' fresh installs.
