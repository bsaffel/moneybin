# MoneyBin Documentation

## Architecture

Architecture Decision Records and system design.

| Document | Description |
|----------|-------------|
| [System Overview](architecture/system-overview.md) | Consolidated system architecture, tech stack, directory structure |
| [ADR-001: Medallion Data Layers](architecture/001-medallion-data-layers.md) | Raw/prep/core layer design |
| [ADR-002: Privacy Tiers](architecture/002-privacy-tiers.md) | Local Only / Encrypted Sync / Managed custody models |
| [ADR-003: MCP Primary Interface](architecture/003-mcp-primary-interface.md) | MCP server as main consumer interface |
| [ADR-004: E2E Encryption](architecture/004-e2e-encryption.md) | Encryption design for Encrypted Sync tier (proposed) |
| [ADR-005: Security Tradeoffs](architecture/005-security-tradeoffs.md) | Threat model and honest security analysis (proposed) |

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
| [Plaid Integration](specs/plaid-integration.md) | Plaid API + E2E encryption for Encrypted Sync tier |
| [Privacy & Security Roadmap](specs/privacy-security-roadmap.md) | Future privacy tiers (Encrypted Sync, Managed) and security controls |
| [Distribution Roadmap](specs/distribution-roadmap.md) | What needs to change before pip distribution: path defaults, SQLMesh packaging, first-run UX |

### Implemented (Pattern Reference)

| Spec | Description |
|------|-------------|
| [OFX Import](specs/implemented/ofx-import.md) | OFX/QFX bank file import |
| [W-2 Extraction](specs/implemented/w2-extraction.md) | W-2 PDF extraction with dual strategy |
| [MCP Read Tools](specs/implemented/mcp-read-tools.md) | 8 read-only MCP tools, 5 resources, 5 prompts |
| [MCP Write Tools](specs/implemented/mcp-write-tools.md) | Import, categorization, budgets, analytics tools |

## Reference

Lookup material -- not specs, not decisions.

| Document | Description |
|----------|-------------|
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
