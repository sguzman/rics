# Roadmap

## Phase 0: Foundation

- [x] Define source TOML schema for fetch/extract/map/date/event/custom parser settings.
- [x] Implement canonical event state storage.
- [x] Implement CLI entry points (`sync`, `build`, `validate`, `harness`).

## Phase 1: Core Pipeline

- [x] HTTP/file/inline source fetcher with retry and pagination support.
- [x] Declarative parsing engine for HTML, JSON, text, and PDF text.
- [x] Merge logic with stable UIDs and revision `SEQUENCE` updates.
- [x] Year-bucketed source calendar export (`<year>.ics`).
- [x] Tracing-based logging through the pipeline.

## Phase 2: Quality and Operability

- [x] Integration test harness with fixture sources and deterministic checks.
- [x] Source validation command.
- [x] README with usage and architecture documentation.
- [ ] Add benchmark fixtures for large-source performance profiling.
- [ ] Add richer diagnostics for mapping failures per field.

## Phase 3: Source Coverage Expansion

- [x] OECD publishing source example config.
- [x] Rough text/ad-hoc source example config.
- [ ] Add sports source family configs (soccer/football/basketball/cricket/olympics).
- [ ] Add prediction market source config family (e.g., Manifold resolution events).
- [ ] Add agency/government and market/economic indicator starter configs.

## Phase 4: Advanced Parsing and Bundles

- [ ] Bundle calendar configs (`data/out/bundles/...`).
- [ ] Full `next_link` pagination traversal.
- [ ] More robust PDF table extraction path.
- [ ] Optional diff report output after each sync.
