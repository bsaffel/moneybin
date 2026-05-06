# Architecture

> **Placeholder.** The full user-facing architecture distillation lands with Wave 2B.

The authoritative architecture document for MoneyBin is being written as the Wave 2B spec `architecture-shared-primitives.md` (planned, not yet committed — will appear in [`docs/specs/`](specs/INDEX.md) when it lands). It will codify the twelve primitives that crystallized through Levels 0–1: `Database` factory, `SecretStore`, service-layer contract, `TableRef`, `ResponseEnvelope`, `@mcp_tool` decorator + privacy middleware, `@tracked` / `track_duration()`, `SanitizedLogFormatter`, `TabularProfile` + `ingest_dataframe()`, `MoneyBinSettings`, SQLMesh layer conventions (`raw` / `prep` / `core` / `app` / `agg` / `reports`), and the scenario fixture YAML format.

In the meantime, the public-facing architecture story is distributed across these existing artifacts:

| Source | What's there |
|---|---|
| [`AGENTS.md`](../AGENTS.md) | Architecture invariants, key abstractions, code standards, the data-layer table (raw / staging / core), and the rules index. The single most important file for understanding how the codebase is shaped. |
| [`CONTRIBUTING.md`](../CONTRIBUTING.md) | Project structure tree (`src/moneybin/` layout), pipeline verification (`make test-scenarios`), and where the strategy lives. |
| [`docs/decisions/`](decisions/) | Architecture Decision Records (ADRs). ADR-000 covers DuckDB as the embedded analytical store; ADR-001 the medallion data layers; ADR-009 encryption key management; etc. Each ADR is the durable record for a load-bearing choice. |
| [`docs/specs/`](specs/INDEX.md) | Per-feature specs. Each spec's `Background` section names the architectural surface it touches. Read the umbrella specs (`mcp-architecture.md`, `matching-overview.md`, `categorization-overview.md`, `smart-import-overview.md`, `testing-overview.md`) for cross-cutting design. |
| [`docs/guides/data-pipeline.md`](guides/data-pipeline.md) | The user-facing pipeline walkthrough: how data moves from source files through dedup, transfer detection, and categorization into `core.fct_transactions`. |

When `architecture-shared-primitives.md` ships at Wave 2B close, this page becomes its one-page distillation: a narrative tour of the primitives, the layer naming conventions, the local/hosted split contract, and the patterns every Wave 3 spec inherits. Until then, the artifacts above are the source of truth.

If you're contributing and need architectural context for a specific change, the heuristic is:

1. **For a single feature:** read its spec in [`docs/specs/`](specs/INDEX.md).
2. **For a load-bearing choice:** find the relevant ADR in [`docs/decisions/`](decisions/).
3. **For "how does this codebase actually fit together":** read [`AGENTS.md`](../AGENTS.md) end-to-end, then [`CONTRIBUTING.md`](../CONTRIBUTING.md) for the directory layout.

Wave 2B will close the loop by writing the missing connective tissue.
