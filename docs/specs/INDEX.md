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
| `implemented` | Shipped; spec moved to `archived/` |

## Updating implemented features

| Change size | Action |
|---|---|
| **Bug fix / minor tweak** | No spec needed. Code change speaks for itself. |
| **Enhancement to existing feature** | New spec referencing the original as context. Original stays in `archived/` untouched. |
| **Full redesign / replacement** | New spec. Original gets a note at the top pointing to the replacement. |

## Smart Import

| Spec | Type | Status | Summary |
|---|---|---|---|
| [Overview](smart-import-overview.md) | Umbrella | ready | Six-pillar initiative: smart tabular detection, PDF, ML categorization, auto-rules, AI-assisted parsing |
| [Tabular Import](smart-import-tabular.md) | Feature | ready | Universal tabular importer (CSV, TSV, Excel, Parquet, Feather); heuristic detection engine, multi-account support, migration formats (Tiller, Mint, YNAB). Supersedes archived `csv-import` spec. |
| `smart-import-pdf.md` | Feature | planned | Pillar C: native-text PDF import |
| `smart-import-ai-parsing.md` | Feature | planned | Pillar F: LLM fallback for file parsing |

## Transaction Matching

| Spec | Type | Status | Summary |
|---|---|---|---|
| [Overview](matching-overview.md) | Umbrella | ready | Cross-source dedup, transfer detection, golden-record merge rules; core as gold analytics layer |
| [Same-Record Dedup](matching-same-record-dedup.md) | Feature | ready | Cross-source dedup + golden-record merge rules (pillars A+C) |
| [Transfer Detection](matching-transfer-detection.md) | Feature | ready | Transfer pair detection across accounts (pillar B); shared matching engine, bridge table, always-review v1 |

## Categorization

| Spec | Type | Status | Summary |
|---|---|---|---|
| [Overview](categorization-overview.md) | Umbrella | ready | Auto-rule generation, ML categorization, priority hierarchy, bootstrap strategies. Supersedes archived `transaction-categorization` spec. |
| [Auto-Rule Generation](categorization-auto-rules.md) | Feature | ready | Auto-generate categorization rules from user edits (pillar E); merchant-first pattern extraction, proposal review queue |
| `categorization-ml.md` | Feature | planned | Pillar D: local ML-powered categorization |
| `merchant-entity-resolution.md` | Feature | planned | Evolve merchants from pattern-to-category cache to first-class entities; multi-pattern matching, automated discovery, query-time resolution |

## Privacy & Security

| Spec | Type | Status | Summary |
|---|---|---|---|
| [Privacy & AI Trust](privacy-and-ai-trust.md) | Framework | ready | AI data flow tiers, consent model, provider profiles, redaction engine, audit log |
| [Data Protection](privacy-data-protection.md) | Feature | ready | DuckDB encryption at rest (AES-256-GCM), `Database` connection factory, key management, file permissions, PII log sanitization |
| [Privacy & Security Roadmap](privacy-security-roadmap.md) | Roadmap | — | Three-tier data custody model overview |

## MCP

| Spec | Type | Status | Summary |
|---|---|---|---|
| [Architecture & Design](mcp-architecture.md) | Architecture | ready | MCP v1 design philosophy, tool taxonomy, privacy integration, CLI symmetry, Apps readiness. Supersedes archived `mcp-read-tools` and `mcp-write-tools` specs. |
| [Tool Surface](mcp-tool-surface.md) | Architecture | ready | Concrete tool, prompt, resource, and service layer definitions for MCP v1 (46 tools, 4 prompts, 4 resources) |

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
| [Synthetic Data Generator](testing-synthetic-data.md) | Feature | ready | Persona-based synthetic financial data: YAML-driven personas/merchants, deterministic seeding, ground-truth labels, Level 2 realism |
| `testing-anonymized-data.md` | Feature | planned | Structure-preserving anonymization of real databases with statistical similarity guarantees |
| `testing-csv-fixtures.md` | Feature | planned | Curated bank export samples with expected-result JSON for format detection testing |
| `testing-format-compat.md` | Feature | planned | Extractor verification against fixture files |
| `testing-migration-safety.md` | Feature | planned | Pre/post migration data integrity assertions |

## Standalone

| Spec | Type | Status | Summary |
|---|---|---|---|
| [Database Migration](database-migration.md) | Feature | ready | Dual-path schema migration system: auto-upgrade on first invocation, SQL/Python migrations, rebaseline, SQLMesh version detection |
| [Net Worth & Balance Tracking](net-worth.md) | Feature | draft | Authoritative balance tracking per account, daily carry-forward interpolation, reconciliation deltas, `agg_net_worth` aggregation; cash-only v1 |
| [Budget Tracking](budget-tracking.md) | Feature | draft | Monthly budgets with target-vs-actual and rollovers |
