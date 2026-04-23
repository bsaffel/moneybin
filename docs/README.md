# MoneyBin Documentation

## Feature Guide

Detailed per-feature documentation with usage examples and reference material.

| Guide | Description |
|-------|-------------|
| [Data Import](guides/data-import.md) | OFX/QFX, CSV, TSV, Excel, Parquet, Feather, W-2 PDF |
| [Data Pipeline](guides/data-pipeline.md) | Three-layer architecture, SQLMesh transforms |
| [Categorization](guides/categorization.md) | Rule engine, merchants, bulk operations |
| [Database & Security](guides/database-security.md) | Encryption, key management, migrations |
| [Multi-Profile Support](guides/profiles.md) | Isolation boundaries, profile lifecycle |
| [MCP Server](guides/mcp-server.md) | AI integration — tools, prompts, client setup |
| [CLI Reference](guides/cli-reference.md) | Complete command tree |
| [Observability](guides/observability.md) | Logging, metrics, instrumentation |
| [Synthetic Data](guides/synthetic-data.md) | Test data generation — personas, ground truth |
| [Direct SQL Access](guides/sql-access.md) | DuckDB shell, UI, key tables, example queries |

## Decisions (ADRs)

Architecture Decision Records.

| Document | Description |
|----------|-------------|
| [ADR-001: Medallion Data Layers](decisions/001-medallion-data-layers.md) | Raw/prep/core layer design |
| [ADR-002: Privacy Tiers](decisions/002-privacy-tiers.md) | Local Only / Encrypted Sync / Managed custody models |
| [ADR-003: MCP Primary Interface](decisions/003-mcp-primary-interface.md) | MCP server as main consumer interface |
| [ADR-004: E2E Encryption](decisions/004-e2e-encryption.md) | Encryption design for Encrypted Sync tier (proposed) |
| [ADR-005: Security Tradeoffs](decisions/005-security-tradeoffs.md) | Threat model and honest security analysis (proposed) |
| [ADR-006: SQLMesh Replaces dbt](decisions/006-sqlmesh-replaces-dbt.md) | Transformation engine choice |
| [ADR-007: JSON Over Parquet for Sync](decisions/007-json-over-parquet-for-sync.md) | Sync payload format |

## Feature Specs

Self-contained documents for driving feature development. See the [Spec Index](specs/INDEX.md) for full status tracking.

## Reference

Lookup material — not specs, not decisions.

| Document | Description |
|----------|-------------|
| [System Overview](reference/system-overview.md) | Consolidated system architecture, tech stack, directory structure |
| [Data Model](reference/data-model.md) | Schema definitions, ER diagram, example queries |
| [Data Sources](reference/data-sources.md) | Data source roadmap and priorities |
| [MCP Prompts](reference/prompts/README.md) | 9 prompt templates for guided financial workflows |

## Coding Standards

Coding rules live in `.claude/rules/` and `CLAUDE.md`, not in docs. See:

- `CLAUDE.md` — Package manager, linting, type checking, architecture, security
- `.claude/rules/cli.md` — CLI development patterns
- `.claude/rules/testing.md` — Testing standards
- `.claude/rules/mcp-server.md` — MCP server rules
- `.claude/rules/database.md` — DuckDB and SQL standards
- `.claude/rules/data-extraction.md` — Data extraction patterns
