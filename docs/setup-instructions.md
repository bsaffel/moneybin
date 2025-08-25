# Setup Instructions

## Prerequisites

- Python 3.11+
- uv package manager (recommended) or pip
- Git

## Initial Setup

### 1. Python Version Management (Recommended)

#### Option A: Using pyenv (Recommended)

```bash
# Install pyenv if not already installed
# macOS: brew install pyenv
# Linux: Follow https://github.com/pyenv/pyenv#installation

# The project includes a .python-version file that will automatically
# use Python 3.11+ when pyenv is available
pyenv install --skip-existing
pyenv local
```

#### Option B: System Python

Ensure you have Python 3.11+ installed on your system.

### 2. Create Virtual Environment

```bash
# Create virtual environment using uv (recommended)
uv venv .venv

# Activate virtual environment
# On macOS/Linux:
source .venv/bin/activate

# On Windows:
# .venv\Scripts\activate

# Verify activation (should show .venv path)
which python
```

### 3. Install Dependencies

#### Option A: Using uv (Recommended - Faster)

```bash
# Install uv if not already installed
# macOS: brew install uv
# Linux: curl -LsSf https://astral.sh/uv/install.sh | sh

# Install project in editable mode with development dependencies
uv pip install -e ".[dev]"

# Or install just the main dependencies
uv pip install -e .
```

#### Option B: Using pip (fallback)

```bash
# Install the project in editable mode with development dependencies
pip install -e ".[dev]"

# Or install just the main dependencies
pip install -e .
```

### 4. Framework Status

The project already has the following frameworks initialized:

#### Git Repository
- Already initialized and ready to use

#### Dagster Project
- Pipeline definitions are in the `pipelines/` directory
- Ready for development and deployment

#### dbt Core Project
- Located in the `dbt/` subdirectory
- Already configured with `dbt_project.yml`
- Ready for model development

### 5. Configure Database Connection

#### DuckDB Setup

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

### 6. Environment Variables

Create a `.env` file for sensitive configuration:

```bash
# .env (do not commit to git)
PLAID_CLIENT_ID=your_plaid_client_id
PLAID_SECRET=your_plaid_secret
PLAID_ENV=development  # or production

# QuickBooks (if using)
QUICKBOOKS_CLIENT_ID=your_qb_client_id
QUICKBOOKS_CLIENT_SECRET=your_qb_client_secret

# Database
DUCKDB_PATH=dbt/dev.duckdb
```

### 7. Verify Installation

#### Test Dagster

```bash
# From project root
make dagster-dev
```

Or manually:
```bash
dagster dev
```

Access the Dagster UI at `http://localhost:3000`

#### Test dbt Core

```bash
dbt debug
```

Should show successful connection to DuckDB.

## Quick Setup with Makefile

The easiest way to get started is using the provided Makefile:

```bash
# Complete development environment setup
make setup

# See all available commands
make help

# Check environment status
make status
```

## Next Steps

1. **Review Documentation**: See `docs/` folder for technical details
2. **Configure Data Sources**: Set up Plaid API keys and bank connections
3. **Start Development**: Begin with basic data extraction pipelines
4. **Set up Scheduling**: Configure Dagster schedules for automated runs

## Current Directory Structure

```text
moneybin/
├── .cursor/                 # AI development standards
├── .venv/                   # Python virtual environment
├── config/                  # Configuration files
├── data/                    # Data storage
├── dbt/                     # dbt transformations (already initialized)
│   ├── models/             # SQL transformation models
│   ├── macros/             # Reusable SQL macros
│   ├── tests/              # Data quality tests
│   ├── seeds/              # Reference data
│   └── dbt_project.yml     # Project configuration
├── docs/                    # Documentation
├── logs/                    # Application logs
├── notebooks/               # Jupyter notebooks for analysis
├── pipelines/               # Dagster pipeline definitions
├── src/                     # Custom Python modules
├── tests/                   # Unit and integration tests
├── .git/                    # Git repository (already initialized)
├── .env                     # Environment variables (create manually)
├── .python-version          # Python version pin
├── dbt_project.yml          # DBT project configuration
├── Makefile                 # Development automation
├── pyproject.toml           # Python project configuration
└── uv.lock                  # UV dependency lock file
```

## Framework-Specific Commands

### Dagster Commands

```bash
# Start development server
make dagster-dev
# or
dagster dev

# Run specific job
dagster job execute -j daily_data_pipeline

# View asset lineage
dagster asset list
```

### dbt Core Commands

```bash
# Navigate to dbt directory
cd dbt

# Run all models
dbt run

# Test data quality
dbt test

# Generate documentation
dbt docs generate
dbt docs serve

# Run specific model
dbt run -m staging
```

### Combined Workflow

```bash
# Typical development cycle
make dagster-dev &           # Start orchestration server
cd dbt && dbt run           # Run transformations
cd ../pipelines && dagster job execute -j daily_data_pipeline
```
