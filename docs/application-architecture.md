# MoneyBin Application Architecture

## System Overview

MoneyBin is an open-source, local-first personal financial analysis platform. Financial data flows from source files through extractors into DuckDB, is transformed by dbt into canonical analytical tables, and is then accessible through two parallel interfaces:

```text
                                                    ┌─────────────────────────┐
                                                    │     MCP Server          │
                                                    │  (AI-assisted analysis) │
                                                    │  25 tools, 8 resources  │
                                                    └────────────┬────────────┘
                                                                 │
Source Files ──→ Extractors ──→ Raw Tables ──→ dbt ──→ Core Tables
                                                                 │
                                                    ┌────────────┴────────────┐
                                                    │     Data Toolkit        │
                                                    │  DuckDB SQL / Jupyter   │
                                                    │  dbt / Streamlit        │
                                                    └─────────────────────────┘
```

### Primary Interface: MCP Server

The MCP server gives AI assistants (Cursor, Claude Desktop, etc.) secure, read-only access to your financial data via 25 tools across 11 domains. See [`mcp-server-design.md`](mcp-server-design.md) for the complete specification.

### Data Toolkit

The same DuckDB database is directly accessible with standard data tools:

- **DuckDB** -- Query your data with SQL via `moneybin db shell` or any DuckDB client
- **dbt** -- Transform raw data into analytical models with `moneybin transform run`
- **Jupyter** -- Launch notebooks for ad-hoc exploration with `make jupyter`
- **Streamlit** -- Build interactive dashboards on top of your data

### Optional: Dagster Orchestration

For users who want scheduled data refresh pipelines, [Dagster](https://dagster.io/) is available as an orchestration layer. This is most relevant for the Encrypted Sync tier where Plaid data needs periodic refreshes.

## Data Architecture

### Data Layers

Data flows through three layers:

```text
Raw ──→ Staging ──→ Core
```

- **Raw**: Source-specific tables preserved exactly as extracted (`raw.*`)
- **Staging**: dbt views with light transformations -- renaming, type casting, trimming (`prep.*`)
- **Core**: Canonical fact and dimension tables that unify all sources (`core.*`)

See [`duckdb-data-model.md`](duckdb-data-model.md) for complete schema definitions and [`duckdb-er-diagram.md`](duckdb-er-diagram.md) for the ER diagram.

### Current Data Sources

| Source | Extractor | Raw Tables | Status |
|--------|-----------|------------|--------|
| OFX/QFX files | `ofx_extractor.py` | `raw.ofx_institutions`, `raw.ofx_accounts`, `raw.ofx_transactions`, `raw.ofx_balances` | Implemented |
| W-2 PDF forms | `w2_extractor.py` | `raw.w2_forms` | Implemented |
| CSV files | (planned) | `raw.csv_transactions`, `raw.csv_accounts` | Planned |
| Plaid API | (planned) | `raw.plaid_*` | Planned (Encrypted Sync tier) |

### Core Tables

| Table | Description |
|-------|-------------|
| `core.dim_accounts` | Deduplicated account records from all sources |
| `core.fct_transactions` | Standardized transactions with amounts normalized (negative = expense) |

## Directory Structure

```text
moneybin/
├── src/moneybin/               # Main application package
│   ├── mcp/                    # MCP server (primary interface)
│   │   ├── server.py           # FastMCP server + DuckDB lifecycle
│   │   ├── tools.py            # Tool implementations (25 tools)
│   │   ├── resources.py        # Resource endpoints (8 resources)
│   │   ├── prompts.py          # Prompt templates (8 prompts)
│   │   └── privacy.py          # Security controls
│   ├── cli/                    # Command line interface (Typer)
│   │   ├── main.py             # CLI entry point
│   │   └── commands/           # Command modules
│   │       ├── mcp.py          # MCP server commands
│   │       ├── extract.py      # Data extraction commands
│   │       ├── load.py         # Data loading commands
│   │       ├── transform.py    # dbt transformation commands
│   │       ├── db.py           # Database exploration commands
│   │       ├── config.py       # Configuration commands
│   │       ├── credentials.py  # Credential management
│   │       └── sync.py         # Sync service commands
│   ├── extractors/             # Source file parsers
│   │   ├── ofx_extractor.py    # OFX/QFX file parsing
│   │   └── w2_extractor.py     # W-2 PDF extraction
│   ├── loaders/                # DuckDB data loaders
│   │   ├── ofx_loader.py       # OFX data loading
│   │   ├── w2_loader.py        # W-2 data loading
│   │   └── parquet_loader.py   # Parquet file loading
│   ├── connectors/             # External API integrations
│   │   └── plaid_sync.py       # Plaid API (future)
│   ├── sql/schema/             # DDL definitions for raw + core tables
│   ├── logging/config.py       # Structured logging configuration
│   ├── utils/                  # Shared utilities
│   │   ├── user_config.py      # Profile-based configuration
│   │   ├── secrets_manager.py  # Credential management
│   │   └── file.py             # File utilities
│   └── config.py               # Centralized configuration
├── src/moneybin_server/        # Encrypted Sync server (future)
├── dbt/                        # dbt transformation project
│   ├── models/
│   │   ├── ofx/                # OFX staging models (views)
│   │   └── core/               # Core fact + dimension tables
│   ├── profiles.yml            # dbt connection profiles
│   └── dbt_project.yml         # dbt project configuration
├── pipelines/                  # Dagster orchestration (optional)
├── data/{profile}/             # Profile-isolated data storage
│   ├── raw/                    # Raw extracted files
│   ├── processed/              # Intermediate data
│   └── moneybin.duckdb         # Profile-specific database
├── tests/                      # Test suite
│   ├── moneybin/               # Client tests
│   │   └── test_mcp/           # MCP server tests
│   └── moneybin_server/        # Server tests
└── docs/                       # Documentation
```

## Technology Stack

| Component | Technology | Purpose |
|-----------|-----------|---------|
| Database | DuckDB 1.4+ | Local analytical database |
| MCP Server | FastMCP (mcp[cli]) | AI assistant integration |
| Transformations | dbt-duckdb | Data modeling and transformation |
| CLI | Typer | Command line interface |
| Data Processing | Polars | DataFrame operations (preferred over Pandas) |
| PDF Extraction | pdfplumber + pytesseract | W-2 and statement parsing |
| File Parsing | ofxparse | OFX/QFX file parsing |
| Validation | Pydantic | Data validation and settings |
| Orchestration | Dagster (optional) | Scheduled pipelines |
| Notebooks | Jupyter | Ad-hoc analysis |
| Type Checking | Pyright | Static type analysis |
| Linting | Ruff | Code formatting and linting |

## Configuration

### Profile System

MoneyBin uses profiles to isolate data for different users or environments. Each profile gets its own DuckDB database and configuration:

```bash
moneybin --profile=alice extract ofx bank-files/*.qfx
moneybin --profile=bob extract ofx other-files/*.qfx
```

### dbt Configuration

```yaml
# dbt/profiles.yml
moneybin:
  outputs:
    dev:
      type: duckdb
      path: 'dbt/dev.duckdb'
      threads: 4
  target: dev
```

Staging models materialize as views, core models as tables. A custom `generate_schema_name` macro ensures schema names are not duplicated.

### MCP Server Configuration

The MCP server is configured via environment variables:

- `MONEYBIN_MCP_MAX_ROWS` -- Maximum rows per query result (default: 1000)
- `MONEYBIN_MCP_MAX_CHARS` -- Maximum characters per result (default: 50000)
- `MONEYBIN_MCP_ALLOWED_TABLES` -- Optional table allowlist (comma-separated)

## Privacy Tiers

The architecture supports three data custody models. See [`privacy-tiers-architecture.md`](privacy-tiers-architecture.md) for full details.

| Tier | Data Location | Bank Sync | MCP Server | Status |
|------|--------------|-----------|------------|--------|
| Local Only | Local DuckDB | Manual import (OFX/CSV) | Local stdio | Implemented |
| Encrypted Sync | Local + encrypted cloud | Plaid (E2E encrypted) | Local stdio | Planned |
| Managed | Cloud | Plaid (server-readable) | Remote HTTP | Future |
