# MoneyBin Documentation

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

Self-contained documents for driving feature development. Follow the [spec template](specs/_template.md).

### Ready to Build

| Spec | Description |
|------|-------------|
| [CSV Import](specs/csv-import.md) | Bank-specific CSV parsers with generic fallback |
| [MCP Tier 1 Tools](specs/mcp-tier1-tools.md) | 11 analytical tools buildable with existing data model |

### Draft

| Spec | Description |
|------|-------------|
| [Transaction Categorization](specs/transaction-categorization.md) | Category hierarchy, rules engine, bulk operations |
| [Budget Tracking](specs/budget-tracking.md) | Budget definitions, rollover, status tracking |
| [Plaid Integration](specs/sync-plaid.md) | Plaid API + E2E encryption for Encrypted Sync tier |
| [Privacy & Security Roadmap](specs/privacy-security-roadmap.md) | Future privacy tiers (Encrypted Sync, Managed) and security controls |


### Implemented (Pattern Reference)

| Spec | Description |
|------|-------------|
| [OFX Import](specs/archived/ofx-import.md) | OFX/QFX bank file import |
| [W-2 Extraction](specs/archived/w2-extraction.md) | W-2 PDF extraction with dual strategy |
| [MCP Read Tools](specs/archived/mcp-read-tools.md) | 8 read-only MCP tools, 5 resources, 5 prompts |
| [MCP Write Tools](specs/archived/mcp-write-tools.md) | Import, categorization, budgets, analytics tools |

## Reference

Lookup material -- not specs, not decisions.

| Document | Description |
|----------|-------------|
| [System Overview](reference/system-overview.md) | Consolidated system architecture, tech stack, directory structure |
| [Data Model](reference/data-model.md) | Schema definitions, ER diagram, example queries |
| [Data Sources](reference/data-sources.md) | Data source roadmap and priorities |
| [MCP Prompts](reference/prompts/README.md) | 9 prompt templates for guided financial workflows |

## Coding Standards

Coding rules live in `.claude/rules/` and `CLAUDE.md`, not in docs. See:

- `CLAUDE.md` -- Package manager, linting, type checking, architecture, security
- `.claude/rules/cli.md` -- CLI development patterns
- `.claude/rules/testing.md` -- Testing standards
- `.claude/rules/mcp-server.md` -- MCP server rules
- `.claude/rules/duckdb-sql.md` -- DuckDB and SQL standards
- `.claude/rules/data-extraction.md` -- Data extraction patterns
