# Spec Index

Single source of truth for spec status. Update this table when a spec's status changes.

## Spec types

| Type | Purpose |
|---|---|
| **Umbrella** | High-level overview that defines the vision, pillars, and child specs for a major initiative. Not directly implementable — decomposes into Feature specs. |
| **Feature** | A single implementable unit of work with concrete schema, API, CLI, and test surface. The primary spec type. |
| **Architecture** | Cross-cutting design decisions, patterns, and contracts that multiple features depend on. |
| **Framework** | Policy and governance specs (privacy, security) that constrain how features are built rather than defining features themselves. |
| **Roadmap** | Strategic sequencing and phasing docs. Not implementable — inform prioritization and ordering of Feature specs. |

## Status definitions

| Status | Meaning |
|---|---|
| `draft` | Design written, not yet reviewed or ready for implementation |
| `ready` | Design reviewed and approved; ready for implementation |
| `in-progress` | Implementation underway |
| `implemented` | Shipped; spec stays in place as living documentation |

## Updating implemented features

| Change size | Action |
|---|---|
| **Bug fix / minor tweak** | No spec needed. Code change speaks for itself. |
| **Enhancement to existing feature** | New spec referencing the original as context. Original stays in place untouched. |
| **Full redesign / replacement** | New spec. Original gets a note at the top pointing to the replacement. Old spec moves to `archived/` since it's been superseded. |

## Smart Import

| Spec | Type | Status | Summary |
|---|---|---|---|
| [Overview](smart-import-overview.md) | Umbrella | ready | Six-pillar initiative: smart tabular detection, PDF, ML categorization, auto-rules, AI-assisted parsing |
| [Tabular Import](smart-import-tabular.md) | Feature | implemented | Universal tabular importer (CSV, TSV, Excel, Parquet, Feather); heuristic detection engine, multi-account support, migration formats (Tiller, Mint, YNAB, Maybe). Supersedes archived `csv-import` spec. |
| [Tabular Cleanup](tabular-import-cleanup.md) | Feature | implemented | Post-ship cleanup: ResolvedMapping dataclass, Literal types, config params, DatabaseKeyError handler, Decimal correctness, N+1 merchant batch optimization, account matching wiring |
| [Smart Import Inbox](smart-import-inbox.md) | Feature | implemented | Watched-folder import UX: drop files in `~/Documents/MoneyBin/<profile>/inbox/`, run `moneybin import inbox` (or `import.inbox_sync` MCP), files move to `processed/YYYY-MM/` or `failed/YYYY-MM/` with YAML error sidecar. Per-profile lockfile + crash-recovery via staging-rename. |
| `smart-import-pdf.md` | Feature | planned | Pillar C: native-text PDF import |
| `smart-import-ai-parsing.md` | Feature | planned | Pillar F: LLM fallback for file parsing |

## Transaction Matching

| Spec | Type | Status | Summary |
|---|---|---|---|
| [Overview](matching-overview.md) | Umbrella | ready | Cross-source dedup, transfer detection, golden-record merge rules; core as gold analytics layer |
| [Same-Record Dedup](matching-same-record-dedup.md) | Feature | implemented | Cross-source dedup + golden-record merge rules (pillars A+C); shared matching engine, `prep.int_transactions__matched`/`__merged`, `meta.fct_transaction_provenance`, `app.match_decisions`, `moneybin matches run/review/history/undo/backfill` CLI |
| [Transfer Detection](matching-transfer-detection.md) | Feature | implemented | Transfer pair detection across accounts (pillar B); shared matching engine (Tier 4), `core.bridge_transfers`, always-review v1, 4-signal scoring |

## Categorization

| Spec | Type | Status | Summary |
|---|---|---|---|
| [Overview](categorization-overview.md) | Umbrella | ready | Auto-rule generation, ML categorization, priority hierarchy, bootstrap strategies. Supersedes archived `transaction-categorization` spec. |
| [Auto-Rule Generation](categorization-auto-rules.md) | Feature | implemented | Auto-generate categorization rules from user edits (pillar E); merchant-first pattern extraction, proposal review queue |
| [Categorize Bulk](categorize-bulk.md) | Feature | implemented | `moneybin categorize bulk` CLI parity for `categorize_bulk` MCP tool; shared Pydantic input model with per-item validation; `BulkRecordingContext` to drop per-item duplicate DB lookups in the bulk loop |
| `categorization-ml.md` | Feature | planned | Pillar D: local ML-powered categorization |
| `merchant-entity-resolution.md` | Feature | planned | Evolve merchants from pattern-to-category cache to first-class entities; multi-pattern matching, automated discovery, query-time resolution |

## Privacy & Security

| Spec | Type | Status | Summary |
|---|---|---|---|
| [Privacy & AI Trust](privacy-and-ai-trust.md) | Framework | ready | AI data flow tiers, consent model, provider profiles, redaction engine, audit log |
| [Data Protection](privacy-data-protection.md) | Feature | implemented | DuckDB encryption at rest (AES-256-GCM), `Database` connection factory, key management, file permissions, PII log sanitization |
| [Privacy & Security Roadmap](privacy-security-roadmap.md) | Roadmap | — | Three-tier data custody model overview |

## MCP

| Spec | Type | Status | Summary |
|---|---|---|---|
| [Architecture & Design](mcp-architecture.md) | Architecture | in-progress | MCP v1 design philosophy, tool taxonomy, privacy integration, CLI symmetry, Apps readiness. Supersedes archived `mcp-read-tools` and `mcp-write-tools` specs. |
| [Tool Surface](mcp-tool-surface.md) | Architecture | in-progress | Concrete tool, prompt, resource, and service layer definitions for MCP v1 (46 tools, 4 prompts, 4 resources) |
| [SQL Schema Discoverability](mcp-sql-discoverability.md) | Feature | implemented | `moneybin://schema` resource exposes curated interface tables (core + select app) with columns, comments, and example queries; eliminates per-session schema reconnaissance |

## Sync

| Spec | Type | Status | Summary |
|---|---|---|---|
| [Overview](sync-overview.md) | Umbrella | draft | Provider-agnostic sync framework: interaction model, SyncClient, CLI/MCP surface, E2E encryption design, provider contract. Supersedes archived `sync-client-integration` spec. |
| [Plaid Provider](sync-plaid.md) | Feature | draft | First sync provider: Plaid Transactions. Raw schemas, staging views, core integration, Plaid Link flow, error codes. |
| `sync-simplefin.md` | Feature | planned | SimpleFIN aggregator provider (alternative to Plaid) |
| `sync-plaid-investments.md` | Feature | planned | Plaid Investments product (gated on `investment-tracking.md`) |

## Testing & Validation

| Spec | Type | Status | Summary |
|---|---|---|---|
| [Overview](testing-overview.md) | Umbrella | ready | Verification infrastructure: synthetic data, assertions, scenarios, format/migration testing |
| [Synthetic Data Generator](testing-synthetic-data.md) | Feature | implemented | Persona-based synthetic financial data: YAML-driven personas/merchants, deterministic seeding, ground-truth labels, Level 2 realism |
| [E2E Testing](e2e-testing.md) | Feature | implemented | Subprocess-based E2E tests: smoke tests (help, no-DB, DB commands), golden-path workflow tests (synthetic, CSV, OFX, lock/unlock, categorization) |
| [Scenario Runner](testing-scenario-runner.md) | Feature | implemented | Whole-pipeline correctness: empty DB → pipeline → assertions/expectations/evaluations against synthetic ground truth and hand-labeled fixtures; scenario suite (`make test-scenarios`); validation primitives reusable for live-data checks |
| [Normalize-Description Fixtures](testing-normalize-description-fixtures.md) | Feature | implemented | YAML golden cases for `normalize_description()`; parametrized exact-equality tests; contributor-facing surface for adding real-world transaction descriptions |
| [Comprehensive Scenario Testing](testing-scenario-comprehensive.md) | Feature | implemented | Five-tier assertion taxonomy, independent-expectations rule, bug-report recipe, relocation of scenarios to `tests/scenarios/`. Architectural authority for all future scenario work. |
| [Anonymized Data Generator](testing-anonymized-data.md) | Feature | planned | Structure-preserving anonymization of real databases for shipping bug-report fixtures and format seeds without leaking PII; statistical similarity guarantees. |
| `testing-csv-fixtures.md` | Feature | planned | Curated bank export samples with expected-result YAML for format detection testing |
| `testing-format-compat.md` | Feature | planned | Extractor verification against fixture files |
| `testing-migration-safety.md` | Feature | planned | Pre/post migration data integrity assertions |

## Infrastructure

| Spec | Type | Status | Summary |
|---|---|---|---|
| [CLI Restructure](cli-restructure.md) | Architecture | implemented | Target CLI command tree: profiles as first-class, `import` as golden path, domain commands top-level. Reference spec for all other specs' CLI sections. |
| [Observability](observability.md) | Feature | implemented | Logging consolidation, `prometheus_client` metrics with DuckDB persistence, instrumentation API (`@tracked`, `track_duration`), log/stats CLI commands |
| [Database Migration](database-migration.md) | Feature | implemented | Dual-path schema migration system: auto-upgrade on first invocation, SQL/Python migrations, rebaseline, SQLMesh version detection |
| `export.md` | Feature | planned | Export analysis results to CSV, Excel, Google Sheets |
| `cli-ux-standards.md` | Architecture | planned | CLI interaction patterns: progressive disclosure, review queues, status commands, output formatting |
| `mcp-ux-standards.md` | Architecture | planned | MCP interaction patterns: tool naming, error surfaces, prompt design, resource conventions |

## Data Quality

| Spec | Type | Status | Summary |
|---|---|---|---|
| [Data Pipeline Reconciliation](data-reconciliation.md) | Feature | draft | Automated pipeline integrity checks: raw→prep→core row accounting, import batch validation, temporal coverage gaps, orphan detection. Complements financial balance reconciliation in `net-worth.md`. |

## Standalone
| [Net Worth & Balance Tracking](net-worth.md) | Feature | draft | Authoritative balance tracking per account, daily carry-forward interpolation, reconciliation deltas, `agg_net_worth` aggregation; cash-only v1. CLI updated by `cli-restructure.md`: `track balance` and `track networth` replace top-level `balance`/`networth`/`reconciliation`. |
| [Asset Tracking](asset-tracking.md) | Feature | draft | Physical asset registry (real estate, vehicles, valuables) with periodic valuations, staleness warnings, liability linking, and net worth integration. CLI namespace: `track asset` per `cli-restructure.md`. |
| [Budget Tracking](budget-tracking.md) | Feature | draft | Monthly budgets with target-vs-actual and rollovers. CLI namespace: `track budget` per `cli-restructure.md`. |
