<!-- markdownlint-disable MD033 MD041 -->
<div align="center">
  <img src="docs/assets/moneybin-icon.png" alt="MoneyBin Icon" width="400">
</div>
<!-- markdownlint-enable MD033 MD041 -->

# MoneyBin - Personal Financial Data App

A self-hosted personal financial data aggregation and analysis toolkit built on DuckDB that provides functionality similar to Empower or Monarch Money while maintaining complete data ownership and control.

## Overview

MoneyBin allows you to:

- Extract structured financial data from CSVs, APIs, and tax PDFs (1040, W-2, 1099, etc.)
- Aggregate transaction data from all your bank accounts
- Store everything in a local DuckDB database
- Query your financial data with SQL for insights like:
  - "How much did I pay in taxes last year?"
  - "What was my total spending by category?"
  - "What are my monthly recurring expenses?"

## Key Benefits

- ✅ **Data Ownership**: Your financial data stays under your control
- ✅ **No Expiration**: Data doesn't disappear when subscriptions end
- ✅ **Privacy First**: No third-party access to your sensitive information
- ✅ **Customizable**: Build exactly the analysis you need
- ✅ **Cost Effective**: No recurring subscription fees

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
├── data/                    # All data storage (raw, processed, databases)
├── dbt/                     # dbt transformations and models
├── pipelines/               # Dagster orchestration
├── src/moneybin/            # Python application code
│   ├── cli/                 # Command line interface
│   ├── extractors/          # Data extraction (Plaid, PDF, CSV)
│   ├── processors/          # Data processing utilities
│   └── utils/               # Shared utilities and configuration
├── tests/                   # Unit and integration tests
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
moneybin extract plaid

# Alice's accounts
moneybin --profile=alice extract plaid
moneybin --profile=alice load parquet
moneybin --profile=alice transform run

# Bob's accounts
moneybin --profile=bob extract plaid

# Shared household account
moneybin --profile=household extract plaid

# Short flag
moneybin -p alice extract plaid

# Via environment variable
export MONEYBIN_PROFILE=alice
moneybin extract plaid
```

##### Testing vs Production

You can also use profiles for environment separation:

```bash
# For development/testing
moneybin --profile=dev extract plaid    # Uses .env.dev (sandbox)

# For production data
moneybin --profile=prod extract plaid   # Uses .env.prod (real accounts)
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
moneybin --profile=alice extract plaid    # Alice's accounts
moneybin --profile=bob extract plaid      # Bob's accounts
moneybin -p household load parquet        # Shared household account

# Data extraction commands
moneybin extract --help
moneybin extract plaid                    # Extract from Plaid API (dev profile by default)
moneybin extract plaid --verbose          # With debug logging
moneybin extract plaid --force            # Force full extraction (bypass incremental)
moneybin extract all                      # Extract from all sources

# Data loading commands
moneybin load --help
moneybin load parquet                     # Load Parquet files into DuckDB
moneybin load status                      # Check database loading status

# Data transformation commands
moneybin transform --help
moneybin transform run                    # Run all dbt transformations
moneybin transform run -m core            # Run specific model selection
moneybin transform test                   # Run dbt tests

# Credential management commands
moneybin credentials --help
moneybin credentials setup                # Set up .env file
moneybin credentials setup --force        # Overwrite existing .env
moneybin credentials validate             # Validate all credentials
moneybin credentials validate-plaid       # Validate Plaid specifically
moneybin credentials list-services        # Show supported services
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
