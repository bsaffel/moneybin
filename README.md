<!-- markdownlint-disable MD033 MD041 -->
<div align="center">
  <img src="docs/assets/moneybin-icon.png" alt="MoneyBin Icon" width="400">
</div>
<!-- markdownlint-enable MD033 MD041 -->

# MoneyBin

An open-source, local-first personal financial analysis platform. MoneyBin gives you complete ownership of your financial data with two ways to interact with it:

1. **MCP Server** -- Ask your AI assistant about your finances. MoneyBin exposes tools across 11 domains so Claude, Cursor, or any MCP-compatible assistant can query your accounts, transactions, spending, taxes, and more.

2. **Data Toolkit** -- Query your DuckDB database directly with SQL, build dbt transformation models, explore data in Jupyter notebooks, or create Streamlit dashboards. Your data, your tools.

All data stays on your machine. Nothing is sent to any external service.

## How It Works

```text
Bank Files (OFX/CSV/PDF) ──→ Extractors ──→ Raw Tables ──→ dbt ──→ Core Tables
                                                                        │
                                                         ┌──────────────┼──────────────┐
                                                         ▼              ▼              ▼
                                                    MCP Server     DuckDB SQL      Jupyter
                                                   (AI assistants) (direct query)  (notebooks)
```

Import your financial data from local files, transform it with dbt into a clean analytical model, then interact with it through AI assistants or hands-on data tools.

## Quick Start

### 1. Install

```bash
# Clone and set up
git clone https://github.com/yourusername/moneybin.git
cd moneybin
make setup
```

Requires Python 3.11+ and [uv](https://docs.astral.sh/uv/). The `make setup` command will install uv automatically if needed.

### 2. Import Your Data

```bash
# Import OFX/QFX files from your bank
moneybin extract ofx path/to/downloads/*.qfx

# Extract W-2 tax forms from PDF
moneybin extract w2 path/to/w2.pdf

# Run dbt to build the core analytical model
moneybin transform run
```

### 3. Connect Your AI Assistant

Add MoneyBin to your AI tool's MCP configuration:

**Cursor** (`.cursor/mcp.json`):

```json
{
  "mcpServers": {
    "moneybin": {
      "command": "uv",
      "args": ["run", "--directory", "/path/to/moneybin", "moneybin", "mcp", "serve"]
    }
  }
}
```

**Claude Desktop** (`claude_desktop_config.json`):

```json
{
  "mcpServers": {
    "moneybin": {
      "command": "uv",
      "args": ["run", "--directory", "/path/to/moneybin", "moneybin", "mcp", "serve"]
    }
  }
}
```

Then ask your assistant things like:

- "What are my account balances?"
- "Show my spending by category for the last 3 months"
- "Find all recurring subscriptions and their annual cost"
- "How much did I pay in taxes last year?"
- "Compare my spending this month vs last month"

## MCP Server

MoneyBin's MCP server exposes **25 tools** across 11 financial domains, plus resources and prompt templates.

### Tool Domains

| Namespace | Tools | Description |
|-----------|-------|-------------|
| `schema.*` | 2 | Database discovery -- list tables, describe columns |
| `accounts.*` | 4 | Account listing, balances, activity, balance history |
| `transactions.*` | 3 | Search, find recurring charges, find large outliers |
| `spending.*` | 4 | By category, monthly summary, period comparison, top merchants |
| `cashflow.*` | 2 | Net cash flow summary, income source identification |
| `tax.*` | 2 | W-2 summary, comprehensive tax summary |
| `overview.*` | 2 | Net worth, data status (what's loaded and how fresh) |
| `investments.*` | 2 | Holdings and performance (requires Plaid data) |
| `liabilities.*` | 1 | Debt summary (requires Plaid data) |
| `institutions.*` | 1 | Connected financial institutions |
| `sql.*` | 1 | Execute arbitrary read-only SQL queries |

All tools are **read-only**. The MCP server opens DuckDB in read-only mode and validates all queries to reject write operations. See [`docs/mcp-server-design.md`](docs/mcp-server-design.md) for the complete specification.

### Resources & Prompts

- **8 Resources**: Schema info, account summaries, recent transactions, W-2 data, data status
- **8 Prompts**: Spending analysis, anomaly detection, tax preparation, financial health check, subscription audit, year-in-review, and more

## Data Toolkit

MoneyBin stores all your financial data in a local [DuckDB](https://duckdb.org/) database that you can access directly with any tool in the modern data stack.

### DuckDB

Query your data directly with SQL -- no MCP server needed:

```bash
# Interactive SQL shell
moneybin db shell

# One-off queries
moneybin db query "SELECT * FROM core.fct_transactions WHERE amount < -500"

# Web UI for visual exploration
moneybin db ui
```

### dbt

MoneyBin uses [dbt](https://www.getdbt.com/) to transform raw imported data into a clean analytical model through three layers (raw -> staging -> core):

```bash
# Run all transformations
moneybin transform run

# Run dbt tests for data quality
moneybin transform test

# Generate and serve dbt docs
cd dbt && dbt docs generate && dbt docs serve
```

### Jupyter

Launch Jupyter for ad-hoc exploration and analysis:

```bash
make jupyter
```

Connect directly to your DuckDB database for exploratory analysis, visualizations, and custom reports.

### Streamlit

Build interactive dashboards on top of your DuckDB data. (Dashboard templates coming soon.)

### Dagster (Optional)

For users who want scheduled data refresh pipelines, [Dagster](https://dagster.io/) is available as an orchestration layer:

```bash
make dagster-dev
```

## Privacy & Security

MoneyBin follows a [three-tier data custody model](docs/privacy-tiers-architecture.md) that makes trust boundaries explicit:

### Local Only (Default)

> "Nothing leaves this machine."

- All data stored locally in DuckDB
- Manual imports only (OFX, CSV, PDF)
- Fully usable offline
- Maximum privacy -- no cloud, no sync, no third-party access

### Encrypted Sync (Future)

> "We store it, but we can't read it."

- E2E encrypted cloud backup and multi-device sync
- Bank sync via Plaid with immediate encryption
- Server stores only opaque ciphertext
- You hold the encryption keys

See [`docs/architecture/e2e-encryption.md`](docs/architecture/e2e-encryption.md) for the encryption design.

### Managed (Future)

> "We manage the data so everything just works."

- Traditional SaaS-style experience
- Server-readable data for rich analytics
- Fastest onboarding

### Security Controls

- **Read-only MCP**: DuckDB opened in read-only mode; write operations rejected
- **Result limits**: Configurable row and character limits on query results
- **Table allowlist**: Optional restriction on which tables the MCP server can access
- **Profile isolation**: Each user profile has its own database and credentials
- **No credential exposure**: Credentials never passed on command line

## Data Sources

### Currently Supported

| Source | Format | Import Command |
|--------|--------|----------------|
| Bank statements | OFX/QFX | `moneybin extract ofx <file>` |
| W-2 tax forms | PDF | `moneybin extract w2 <file>` |

### Planned

| Source | Format | Status |
|--------|--------|--------|
| Bank transactions | CSV | Coming soon |
| Bank transactions | Plaid API | Encrypted Sync tier |
| 1099 forms | PDF | Planned |
| Investment statements | PDF/CSV | Planned |

See [`docs/data-sources-strategy.md`](docs/data-sources-strategy.md) for the full data source roadmap.

## CLI Reference

```bash
# Main help
moneybin --help

# Import data
moneybin extract ofx <file>         # Import OFX/QFX bank files
moneybin extract w2 <file>          # Extract W-2 from PDF

# Transform data
moneybin transform run              # Run dbt transformations
moneybin transform test             # Run dbt data quality tests

# Explore data
moneybin db shell                   # Interactive SQL shell
moneybin db ui                      # Web UI for data exploration
moneybin db query "SELECT ..."      # Run a SQL query

# MCP server
moneybin mcp serve                  # Start MCP server (stdio)
moneybin mcp serve --transport sse  # Start with SSE transport

# Profile management
moneybin --profile=alice extract ofx <file>  # Use a specific profile
moneybin -p alice transform run              # Short flag
```

## Project Structure

```text
moneybin/
├── src/moneybin/
│   ├── mcp/                # MCP server (primary interface)
│   │   ├── server.py       # FastMCP server + DuckDB lifecycle
│   │   ├── tools.py        # Tool implementations
│   │   ├── resources.py    # Resource endpoints
│   │   ├── prompts.py      # Prompt templates
│   │   └── privacy.py      # Security controls
│   ├── cli/                # Command line interface
│   ├── extractors/         # File parsers (OFX, PDF, CSV)
│   ├── loaders/            # DuckDB data loaders
│   ├── connectors/         # External API integrations
│   └── utils/              # Shared utilities
├── dbt/                    # dbt transformation models
├── data/{profile}/         # Profile-isolated data storage
├── tests/                  # Test suite
└── docs/                   # Documentation
```

## Setup Details

### Prerequisites

- Python 3.11+
- [uv](https://docs.astral.sh/uv/) package manager
- Git

### Installing uv

```bash
# macOS / Linux
curl -LsSf https://astral.sh/uv/install.sh | sh

# macOS (Homebrew)
brew install uv

# Windows
powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"
```

### Profile System

MoneyBin uses profiles to isolate data for different users or environments:

```bash
# Each profile gets its own DuckDB database
moneybin --profile=alice extract ofx bank-files/*.qfx
moneybin --profile=bob extract ofx other-files/*.qfx

# Set a default profile via environment variable
export MONEYBIN_PROFILE=alice
```

Profiles support individual family members, personal vs business separation, or dev vs production environments.

## Contributing

```bash
# Setup development environment
make setup

# Run code quality checks
make check          # format + lint + type-check

# Run tests
make test           # all tests
make test-unit      # unit tests only
make test-cov       # with coverage report
```

See `.cursor/rules/` for coding standards and conventions.

## Documentation

- [`docs/mcp-server-design.md`](docs/mcp-server-design.md) -- Complete MCP server specification
- [`docs/privacy-tiers-architecture.md`](docs/privacy-tiers-architecture.md) -- Privacy tier design
- [`docs/duckdb-er-diagram.md`](docs/duckdb-er-diagram.md) -- Data model ER diagram
- [`docs/application-architecture.md`](docs/application-architecture.md) -- System architecture
- [`docs/ofx-import-guide.md`](docs/ofx-import-guide.md) -- OFX import guide
- [`docs/architecture/e2e-encryption.md`](docs/architecture/e2e-encryption.md) -- Encryption design

## License

[To be determined]
