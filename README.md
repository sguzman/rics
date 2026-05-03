# rics

`rics` is a Rust calendar ETL and ICS generator that ingests source-specific definitions and emits normalized calendar files.

## Intent

Turn messy event sources such as pages, feeds, PDFs, or rough text into maintainable calendar artifacts without hand-editing `.ics` output.

## Ambition

The config-driven source model and update semantics point toward a reusable calendar-ingestion system that can track recurring sources over time rather than a one-shot converter.

## Current Status

The project already has config, fetch, parser, pipeline, store, and harness modules along with tests and roadmap material. It looks like a functioning ETL pipeline.

## Core Capabilities Or Focus Areas

- Source-specific event extraction.
- Normalization into calendar/event models.
- ICS generation and update semantics for future events.
- Config-driven source definitions.
- Harness/test support for parsing and pipeline behavior.

## Project Layout

- `configs/`: source or runtime configuration definitions.
- `data/`: sample data, working data, or local development artifacts.
- `src/`: Rust source for the main crate or application entrypoint.
- `tests/`: automated tests, fixtures, or parity scenarios.
- `Cargo.toml`: crate or workspace manifest and the first place to check for package structure.

## Setup And Requirements

- Rust toolchain.
- Defined source configs in `configs/` or equivalent.
- Input sources reachable from the local environment.

## Build / Run / Test Commands

```bash
cargo build
cargo test
cargo run -- --help
```

## Notes, Limitations, Or Known Gaps

- Source adapters are the core extensibility point, so source drift is an expected maintenance burden.
- Calendar correctness often depends on normalization policy as much as parsing.

## Next Steps Or Roadmap Hints

- Keep source configurations and fixtures close together so new adapters stay testable.
- Clarify identity/update rules further as more event sources are added.
