<!-- markdownlint-disable MD033 MD041 -->
<div align="center">
  <img src="docs/assets/moneybin-icon.png" alt="MoneyBin Icon" width="400">
</div>
<!-- markdownlint-enable MD033 MD041 -->

# MoneyBin - Personal Financial Data App

A self-hosted personal financial data aggregation and analysis toolkit built on DuckDB that provides functionality similar to Empower or Monarch Money while maintaining complete data ownership and control.

## Overview

MoneyBin allows you to:

- **Free Tier**: Extract structured financial data from local files (CSV, Excel, OFX, PDF statements)
- **Paid Tier (Optional)**: Automatically sync transaction data from bank accounts via MoneyBin Sync service
- Store everything in a local DuckDB database
- Query your financial data with SQL for insights like:
  - "How much did I pay in taxes last year?"
  - "What was my total spending by category?"
  - "What are my monthly recurring expenses?"

## Architecture

MoneyBin is split into two distinct components:

- **Client (Free & Local)**: Local-first data processing with DuckDB, dbt, file importers, and Jupyter
- **Server (Optional Paid Sync)**: Hosted sync service for automatic bank connections via Plaid/Yodlee

### Security Model

All sensitive financial data stays on your local machine. The optional sync service implements **end-to-end encryption with honest security**:

- 🔐 **Client authenticates via OAuth** (Auth0) - never sends Plaid tokens
- 🔒 **Server encrypts immediately** - converts Plaid data to encrypted Parquet
- 🔑 **Only you can decrypt** - encryption keys derived from your master password
- 🛡️ **Server can't read stored data** - encrypted at rest, only you have the key
- ⚠️ **Honest disclosure** - server sees plaintext briefly while encrypting for you

**What this means:**

- ✅ Your stored data is encrypted - we can't decrypt it later
- ✅ Database breach → only encrypted data compromised
- ✅ Better than most financial services (which store plaintext)
- ⚠️ Requires trusting server during active processing (like email with PGP)
- ✅ **Free tier alternative** - use local-only mode for complete control

> **Future Feature**: E2E encryption will be implemented in Phase 2. See [`docs/architecture/e2e-encryption.md`](docs/architecture/e2e-encryption.md) for complete design and [`docs/architecture/security-tradeoffs.md`](docs/architecture/security-tradeoffs.md) for honest security analysis.

## Key Benefits

- ✅ **Data Ownership**: Your financial data stays under your control
- ✅ **No Expiration**: Data doesn't disappear when subscriptions end
- ✅ **Privacy First**: No third-party access to your sensitive information
- 🔐 **Zero-Knowledge Security**: E2E encryption means even the sync server can't read your data (future)
- ✅ **Customizable**: Build exactly the analysis you need
- ✅ **Cost Effective**: No recurring subscription fees for local use

## Quick Start

### Prerequisites

- Python 3.11+
- uv package manager (recommended) or pip
- Git

#### Installing uv Package Manager

The project uses [uv](https://docs.astral.sh/uv/) for fast, reliable dependency management. If you don't have uv installed, the `make setup` command will automatically install it for you. However, you can also install it manually:

**macOS and Linux:**

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

**Windows:**

```bash
powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"
```

**Alternative installation methods:**

```bash
# Using pip
pip install uv

# Using homebrew (macOS)
brew install uv

# Using pipx
pipx install uv
```

**Verify installation:**

```bash
uv --version
```

### Fastest Setup (Recommended)

The easiest way to get started is using the provided Makefile:

```bash
# Complete development environment setup
make setup

# See all available commands
make help

# Check environment status
make status
```

This single command will:

- Check Python installation
- Automatically install uv package manager if not present
- Create virtual environment with correct Python version
- Install all development dependencies using uv
- Set up pre-commit hooks for code quality

### Alternative Setup Options

#### Option 1: Simple Manual Approach

1. Export CSV files from your bank accounts
2. Use Python scripts to extract data from tax PDFs
3. Import everything into DuckDB
4. Run SQL queries for analysis

#### Option 2: Automated with APIs

1. Set up Plaid API for automatic bank transaction sync
2. Use OCR services or Python libraries for PDF processing
3. Build automated data pipeline with scheduled updates
4. Create dashboards for ongoing monitoring

## Technical Architecture

The system follows a simple data flow:

```text
Tax PDFs + Bank Data → Processing Scripts → CSV Files → DuckDB → SQL Analysis
```

See the specialized strategy documents for detailed implementation approaches.

## Project Structure

The project follows a modern data engineering architecture with clear separation of concerns:

```text
moneybin/
├── data/                    # All data storage (profile-based isolation)
│   ├── {profile}/          # Profile-specific data (alice, bob, household)
│   │   ├── raw/            # Raw extracted data (plaid, csv, excel, ofx, pdf)
│   │   ├── processed/      # Intermediate processed data
│   │   └── duckdb/         # Profile-specific DuckDB database
├── dbt/                     # dbt transformations and models
├── pipelines/               # Dagster orchestration
├── src/
│   ├── moneybin/           # Client package (local-first)
│   │   ├── cli/            # Command line interface
│   │   ├── extractors/     # Local file parsers (CSV, Excel, OFX, PDF)
│   │   ├── connectors/     # Sync service integrations (Plaid Sync)
│   │   ├── loaders/        # DuckDB data loaders
│   │   └── utils/          # Shared utilities and configuration
│   └── moneybin_server/    # Server package (hosted sync)
│       ├── connectors/     # External API integrations (Plaid, Yodlee)
│       ├── api/            # FastAPI server (future)
│       └── config.py       # Server-side configuration
├── tests/
│   ├── moneybin/           # Client tests
│   └── moneybin_server/    # Server tests
└── docs/                    # Technical documentation
```

**📁 For the complete directory structure** with all subdirectories and files, see [Application Architecture → Directory Structure](docs/application-architecture.md#directory-structure).

## Detailed Setup Instructions

### Manual Setup (Alternative)

If you prefer to set up manually or need to understand the individual steps:

#### 1. Python Version Management

The project includes a `.python-version` file for automatic Python version management:

```bash
# Install pyenv if not already installed (optional but recommended)
# macOS: brew install pyenv
# Linux: Follow https://github.com/pyenv/pyenv#installation

# If using pyenv, it will automatically use the correct Python version
pyenv install --skip-existing
pyenv local
```

#### 2. Create Virtual Environment

```bash
# Create virtual environment (this is done automatically by 'make setup')
make venv

# Or manually with uv:
uv venv .venv --python 3.11
```

#### 3. Install Dependencies

The project uses uv for faster, more reliable dependency management:

```bash
# Sync dependencies from lockfile (recommended - fastest and most reproducible)
make sync

# Sync production dependencies only (no dev tools)
make sync-prod

# Update all dependencies to latest versions
make update-deps

# Generate/update lockfile without installing
make lock
```

#### 4. Framework Status

The project already has the following frameworks initialized:

##### Git Repository

- Already initialized and ready to use

##### Dagster Project

- Pipeline definitions are in the `pipelines/` directory
- Ready for development and deployment

##### dbt Core Project

- Located in the `dbt/` subdirectory
- Already configured with `dbt_project.yml`
- Ready for model development

#### 5. Configure Database Connection

##### DuckDB Setup

The project uses DuckDB as the analytical database. The dbt profile is already configured:

```yaml
# dbt/profiles.yml (already configured)
moneybin:
  outputs:
    dev:
      type: duckdb
      path: 'dbt/dev.duckdb'
      threads: 4
  target: dev
```

**Optional: DuckDB CLI for Data Exploration**

MoneyBin includes convenient `db` commands that wrap the DuckDB CLI. To use these commands, install the DuckDB CLI separately:

```bash
# macOS
brew install duckdb

# Linux (download from duckdb.org)
wget https://github.com/duckdb/duckdb/releases/latest/download/duckdb_cli-linux-amd64.zip
unzip duckdb_cli-linux-amd64.zip
sudo mv duckdb /usr/local/bin/

# Windows (download from duckdb.org)
# https://duckdb.org/docs/installation/
```

Once installed, you can use:

```bash
moneybin db ui              # Open web UI to explore your data
moneybin db shell           # Interactive SQL shell
moneybin db query "..."     # Run one-off queries
```

#### 6. Environment Variables & Profile Configuration

MoneyBin uses a **profile-based configuration system** to manage financial data for different users:

##### Profile System

- **User-based profiles**: Each person has their own profile (e.g., `alice`, `bob`, `household`)
- **Separate credentials**: Each profile has its own Plaid credentials and transaction data
- **Use cases**: Individual family members, personal vs business, testing vs production
- **Default**: Uses `default` profile unless explicitly overridden

##### Setup Configuration Files

```bash
# Create configuration files for each user/profile
cp .env.dev.example .env.alice     # Alice's personal accounts
cp .env.prod.example .env.bob      # Bob's personal accounts
cp .env.prod.example .env.household # Shared household accounts

# Edit each file with that user's Plaid credentials
```

##### Example: User Profile Configuration (.env.alice)

```bash
# .env.alice - Alice's personal financial accounts
PLAID_CLIENT_ID=alice_plaid_client_id
PLAID_SECRET=alice_plaid_secret
PLAID_ENV=production  # or sandbox for testing

# Optional: Separate database per user
# DUCKDB_PATH=data/duckdb/moneybin_alice.duckdb
# MONEYBIN_PLAID__DAYS_LOOKBACK=365
```

##### Example: Shared Profile (.env.household)

```bash
# .env.household - Shared household accounts
PLAID_CLIENT_ID=household_plaid_client_id
PLAID_SECRET=household_plaid_secret
PLAID_ENV=production

# Shared household database
# DUCKDB_PATH=data/duckdb/moneybin_household.duckdb
```

##### Using Profiles with CLI

```bash
# Default profile
moneybin sync plaid

# Alice's accounts
moneybin --profile=alice sync plaid
moneybin --profile=alice load parquet
moneybin --profile=alice transform run

# Bob's accounts
moneybin --profile=bob sync plaid

# Shared household account
moneybin --profile=household sync plaid

# Short flag
moneybin -p alice sync plaid

# Via environment variable
export MONEYBIN_PROFILE=alice
moneybin sync plaid
```

##### Testing vs Production

You can also use profiles for environment separation:

```bash
# For development/testing
moneybin --profile=dev sync plaid    # Uses .env.dev (sandbox)

# For production data
moneybin --profile=prod sync plaid   # Uses .env.prod (real accounts)
```

#### 7. Verify Installation

```bash
# Check environment status
make status

# Test Dagster (starts development server)
make dagster-dev

# Test dbt (navigate to dbt directory first)
cd dbt && dbt debug
```

The Dagster UI will be available at `http://localhost:3000` when running the development server.

## Development Workflow

### MoneyBin CLI Commands

The project provides a unified CLI interface using modern Typer framework:

```bash
# Main help - see all available commands
moneybin --help

# Profile-based commands (different users)
moneybin --profile=alice sync plaid      # Sync Alice's bank accounts
moneybin --profile=bob sync plaid        # Sync Bob's bank accounts
moneybin -p household load parquet       # Load shared household data

# Data sync commands (external services - optional paid tier)
moneybin sync --help
moneybin sync plaid                      # Sync from Plaid (via MoneyBin Sync service)
moneybin sync plaid --verbose            # With debug logging
moneybin sync plaid --force              # Force full sync (bypass incremental)
moneybin sync all                        # Sync from all configured services

# Data extraction commands (local files - free tier)
moneybin extract --help
moneybin extract csv <file>              # Extract from CSV file
moneybin extract excel <file>            # Extract from Excel file
moneybin extract ofx <file>              # Extract from OFX/QFX file
moneybin extract pdf <file>              # Extract from PDF statement

# Data loading commands
moneybin load --help
moneybin load parquet                    # Load Parquet files into DuckDB
moneybin load status                     # Check database loading status

# Data transformation commands
moneybin transform --help
moneybin transform run                   # Run all dbt transformations
moneybin transform run -m core           # Run specific model selection
moneybin transform test                  # Run dbt tests

# Database exploration commands
moneybin db --help
moneybin db ui                           # Open DuckDB web UI to explore data
moneybin db query "SELECT * FROM ..."   # Execute SQL query
moneybin db query "SELECT ..." --format csv  # Export query results as CSV
moneybin db shell                        # Open interactive SQL shell

# Credential management commands
moneybin credentials --help
moneybin credentials setup               # Set up .env file
moneybin credentials setup --force       # Overwrite existing .env
moneybin credentials validate            # Validate all credentials
moneybin credentials validate-plaid      # Validate Plaid specifically
moneybin credentials list-services       # Show supported services
```

### Available Makefile Commands

#### Setup & Installation Commands

```bash
make setup          # Complete development environment setup (recommended)
make sync           # Sync dependencies from lockfile (modern, reproducible)
make sync-prod      # Sync production dependencies only
make update-deps    # Update all dependencies to latest versions
make lock           # Generate/update lockfile without installing
make venv           # Create virtual environment only
make pre-commit     # Install pre-commit hooks
make check-python   # Verify Python installation
```

#### Development Commands

```bash
make test           # Run all tests (requires dev dependencies via sync)
make test-cov       # Run tests with coverage report
make test-unit      # Run unit tests only
make test-integration # Run integration tests only
make format         # Format code with ruff and fix issues
make lint           # Lint code with ruff
make type-check     # Type check with pyright
make check          # Run all code quality checks (format + lint + type-check)
make jupyter        # Start Jupyter notebook server
make dagster-dev    # Start Dagster development server
```

#### Utility Commands

```bash
make status         # Show development environment status
make help           # Show all available commands with descriptions
make clean          # Clean all generated files and virtual environment
make clean-cache    # Clean Python cache files only
make clean-venv     # Remove virtual environment only
make activate       # Show how to activate virtual environment
```

### Framework-Specific Commands

#### Dagster Development

```bash
# Start Dagster development server (preferred method)
make dagster-dev

# Manual commands (if needed)
source .venv/bin/activate
cd pipelines && dagster dev
```

#### dbt Commands

```bash
# Run all models (from project root)
dbt run

# Test data quality
dbt test

# Generate and serve documentation
dbt docs generate
dbt docs serve

# Run specific model or tag
dbt run -m staging

# Check dbt configuration
dbt debug
```

### Recommended Development Workflow

```bash
# 1. Initial setup (one time) - installs everything you need
make setup

# 2. Check environment status
make status

# 3. Start development server
make dagster-dev

# 4. In another terminal, run code quality checks and tests
make check
make test

# Note: 'make setup' installs both development tools AND testing framework
# All dependencies are managed through uv sync
```

## Data Sources Supported

### Tax Documents

- Form 1040 (Individual Tax Returns)
- W-2 (Wage and Tax Statements)
- 1099 Forms (Various types)
- State tax returns
- Tax preparation software exports

### Bank Transactions

- Checking accounts
- Savings accounts
- Credit cards
- Investment accounts
- Loans and mortgages

### Supported Formats

- **Input**: PDF, CSV, API connections
- **Processing**: Python, SQL
- **Storage**: DuckDB, CSV
- **Output**: SQL queries, CSV exports, dashboards

## Example Queries

Once your data is in DuckDB, you can run queries like:

```sql
-- Total taxes paid last year
SELECT SUM(amount) as total_taxes
FROM transactions
WHERE category = 'Tax Payment'
AND date BETWEEN '2023-01-01' AND '2023-12-31';

-- Monthly spending by category
SELECT
    strftime('%Y-%m', date) as month,
    category,
    SUM(amount) as total_spent
FROM transactions
WHERE amount < 0  -- Expenses are negative
GROUP BY month, category
ORDER BY month DESC, total_spent;

-- Compare spending year over year
SELECT
    strftime('%Y', date) as year,
    category,
    SUM(ABS(amount)) as total_spent
FROM transactions
WHERE amount < 0
GROUP BY year, category
ORDER BY year DESC, total_spent DESC;
```

## Security & Privacy

### Data Protection

- **Local Storage**: All financial data stored locally or in user-controlled environments
- **Encryption**: Disk encryption recommended (FileVault, BitLocker, etc.)
- **No Third-Party Storage**: External APIs used only for data extraction, not storage
- **Privacy First**: No data sharing with third parties

### Credential Security

- **Profile-Based Separation**: Dev/prod credentials kept in separate files
- **Gitignored Files**: `.env.dev` and `.env.prod` never committed to version control
- **CLI Security**: Credentials never passed on command line (prevents shell history logging)
- **Environment Variables**: Secure loading via Pydantic Settings
- **Default Safety**: Always defaults to dev profile to prevent accidental production access

### Best Practices

1. **Never commit** `.env.dev`, `.env.prod`, or `.env` files
2. **Use disk encryption** on machines with financial data
3. **Rotate credentials** immediately if compromised
4. **Monitor API usage** via Plaid dashboard for unusual activity
5. **Separate databases** for dev and prod if desired (via `DUCKDB_PATH`)
6. **Test with sandbox first** before connecting real bank accounts

## Contributing

This is a personal project focused on individual financial data management. See `.cursor/` for Cursor-integrated development standards.

## License

[To be determined based on your preferences]

## Next Steps

1. **Verify Setup**: Run `make status` to ensure everything is configured correctly
2. **Review Documentation**: See `docs/` folder for technical details
3. **Configure Data Sources**: Set up Plaid API keys and bank connections in `.env` file
4. **Start Development**: Run `make dagster-dev` to start the development server
5. **Run Quality Checks**: Use `make check` before committing code
6. **Set up Scheduling**: Configure Dagster schedules for automated runs

For detailed implementation guidance, see the documentation in the `docs/` folder.
