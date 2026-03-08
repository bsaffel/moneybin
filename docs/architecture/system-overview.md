# System Overview

MoneyBin is an open-source, local-first personal financial analysis platform. Financial data flows from source files through extractors into DuckDB, is transformed by SQLMesh into canonical analytical tables, and is accessible through two parallel interfaces.

## Architecture

```text
                                                    +---------------------------+
                                                    |       MCP Server          |
                                                    |  (AI-assisted analysis)   |
                                                    +-------------+-------------+
                                                                  |
Source Files --> Extractors --> Raw Tables --> SQLMesh --> Core Tables -+
                                                                      |
                                                    +-------------+---+---------+
                                                    |      Data Toolkit         |
                                                    |  DuckDB / SQLMesh / Jupyter|
                                                    +---------------------------+
```

### Primary interface: MCP server

The MCP server gives AI assistants (Claude, Cursor, etc.) secure access to financial data via tools across 11 domains. It runs locally via stdio -- not a remote service. See [ADR-003](003-mcp-primary-interface.md).

### Data toolkit

The same DuckDB database is directly accessible with standard data tools:
- **DuckDB** -- `moneybin db shell` or any DuckDB client
- **SQLMesh** -- `moneybin transform apply` for staging and core models
- **Jupyter** -- `make jupyter` for ad-hoc exploration
- **Streamlit** -- Interactive dashboards (templates planned)
- **Dagster** -- Optional orchestration for scheduled pipelines

## Data architecture

Data flows through three layers ([ADR-001](001-medallion-data-layers.md)):

| Layer | Schema | Materialized | Purpose |
|-------|--------|-------------|---------|
| Raw | `raw` | Table | Source-specific tables preserved exactly as extracted |
| Staging | `prep` | View | Light cleaning, type casting, column renaming (SQLMesh) |
| Core | `core` | Table | Canonical fact and dimension tables unifying all sources |

See [Data Model](../reference/data-model.md) for schema definitions.

## Data sources

| Source | Status | Raw Tables | Import |
|--------|--------|------------|--------|
| OFX/QFX files | Implemented | `raw.ofx_*` | `moneybin extract ofx` |
| W-2 PDF forms | Implemented | `raw.w2_forms` | `moneybin extract w2` |
| CSV files | Planned | `raw.csv_*` | `moneybin extract csv` |
| Plaid API | Planned (Encrypted Sync) | `raw.plaid_*` | Automatic sync |

See [Data Sources](../reference/data-sources.md) for the full roadmap.

## Technology stack

| Component | Technology | Purpose |
|-----------|-----------|---------|
| Database | DuckDB 1.4+ | Local analytical database |
| MCP Server | FastMCP (mcp[cli]) | AI assistant integration |
| Transformations | SQLMesh | Data modeling and transformation |
| CLI | Typer | Command line interface |
| Data Processing | Polars | DataFrame operations |
| PDF Extraction | pdfplumber + pytesseract | W-2 and statement parsing |
| File Parsing | ofxparse | OFX/QFX file parsing |
| Validation | Pydantic | Data validation and settings |
| Type Checking | Pyright | Static type analysis |
| Linting | Ruff | Code formatting and linting |

## Privacy tiers

The architecture supports three data custody models ([ADR-002](002-privacy-tiers.md)):

| Tier | Data Location | Bank Sync | Status |
|------|--------------|-----------|--------|
| Local Only | Local DuckDB | Manual import | Implemented |
| Encrypted Sync | Local + encrypted cloud | Plaid (E2E encrypted) | Proposed |
| Managed | Cloud | Plaid (server-readable) | Future |

## Directory structure

```text
moneybin/
  src/moneybin/
    mcp/                    # MCP server (primary interface)
    cli/                    # Command line interface (Typer)
    extractors/             # Source file parsers (OFX, PDF)
    loaders/                # DuckDB data loaders
    connectors/             # External API integrations
    services/               # Business logic layer
    sql/schema/             # DDL definitions
    utils/                  # Shared utilities
    config.py               # Centralized configuration
  sqlmesh/                  # SQLMesh project
    models/                 # Transformation models (prep + core)
  data/{profile}/           # Profile-isolated data storage
  tests/                    # Test suite
  docs/                     # Documentation
```

## Configuration

### Profile system

Each profile gets isolated storage: `data/{profile}/moneybin.duckdb`, `data/{profile}/raw/`, `logs/{profile}/moneybin.log`.

Resolution priority: CLI flag (`--profile=alice`) > env var (`MONEYBIN_PROFILE`) > saved default > interactive prompt.

### MCP server configuration

- `MONEYBIN_MCP_MAX_ROWS` -- Maximum rows per query result (default: 1000)
- `MONEYBIN_MCP_MAX_CHARS` -- Maximum characters per result (default: 50000)
- `MONEYBIN_MCP_ALLOWED_TABLES` -- Optional table allowlist (comma-separated)

## Related ADRs

- [ADR-001: Medallion Data Layers](001-medallion-data-layers.md)
- [ADR-002: Privacy Tiers](002-privacy-tiers.md)
- [ADR-003: MCP Primary Interface](003-mcp-primary-interface.md)
- [ADR-004: E2E Encryption](004-e2e-encryption.md)
- [ADR-005: Security Tradeoffs](005-security-tradeoffs.md)
