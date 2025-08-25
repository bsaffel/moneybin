# MoneyBin - Personal Financial Data Aggregation

A self-hosted personal financial data aggregation and analysis system that provides functionality similar to Empower or Monarch Money while maintaining complete data ownership and control.

## Overview

MoneyBin allows you to:

- Extract structured data from tax PDFs (1040, W-2, 1099, etc.)
- Aggregate transaction data from all your bank accounts
- Store everything in a local DuckDB database
- Query your financial data with SQL for insights like:
  - "How much did I pay in taxes last year?"
  - "What was my total spending by category?"
  - "What are my monthly recurring expenses?"

## Key Benefits

✅ **Data Ownership**: Your financial data stays under your control
✅ **No Expiration**: Data doesn't disappear when subscriptions end
✅ **Privacy First**: No third-party access to your sensitive information
✅ **Customizable**: Build exactly the analysis you need
✅ **Cost Effective**: No recurring subscription fees

## Quick Start Options

### Option 1: Simple Manual Approach

1. Export CSV files from your bank accounts
2. Use Python scripts to extract data from tax PDFs
3. Import everything into DuckDB
4. Run SQL queries for analysis

### Option 2: Automated with APIs

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

```text
moneybin/
├── .cursor/                 # Cursor-integrated project rules
├── .venv/                   # Python virtual environment
├── config/                  # Configuration files
├── data/                    # Data storage
├── dbt/                     # DBT Core project (already initialized)
│   ├── models/             # DBT data models
│   ├── analyses/           # DBT analyses
│   ├── macros/             # DBT macros
│   ├── seeds/              # DBT seed files
│   ├── snapshots/          # DBT snapshots
│   └── tests/              # DBT tests
├── docs/                    # Technical documentation
├── logs/                    # Application logs
├── notebooks/               # Jupyter notebooks for analysis
├── pipelines/               # Dagster pipeline definitions
├── src/                     # Python source code
│   ├── extractors/          # Data extraction modules
│   ├── processors/          # Data processing utilities
│   ├── validators/          # Data validation
│   └── utils/               # Shared utilities
├── tests/                   # Unit and integration tests
├── .gitignore               # Git ignore patterns
├── .python-version          # Python version pin for pyenv/uv
├── dbt_project.yml          # DBT project configuration
├── Makefile                 # Development automation
├── pyproject.toml           # Python project configuration
└── uv.lock                  # UV dependency lock file
```

## Getting Started

### Quick Setup with Makefile

The easiest way to get started is using the provided Makefile:

```bash
# Complete development environment setup
make setup

# See all available commands
make help

# Check environment status
make status
```

### Manual Setup Steps

1. **Python Version**: Use pyenv with `.python-version` file (recommended) or ensure Python 3.11+
2. **Setup Environment**: Run `make setup` or manually:
   - `uv venv .venv && source .venv/bin/activate`
   - `uv pip install -e ".[dev]"`
3. **Frameworks**: The project already has:
   - Git repository initialized
   - DBT Core project in `dbt/` subdirectory
   - Dagster pipeline structure in `pipelines/` directory
4. **Review Documentation**: See `docs/` folder for technical details
5. **Check Development Rules**: See `.cursor/` for AI-integrated development standards

### Development Commands

```bash
# Code quality
make format          # Format code with ruff
make lint           # Lint code with ruff
make type-check     # Type check with pyright
make check          # Run all quality checks

# Testing
make test           # Run all tests
make test-cov       # Run tests with coverage
make test-unit      # Run unit tests only

# Development servers
make jupyter        # Start Jupyter notebook
make dagster-dev    # Start Dagster development server
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

- All data stored locally or in user-controlled environments
- Encryption at rest and in transit
- Secure API key management
- No data sharing with third parties
- Compliance with financial data handling standards

## Contributing

This is a personal project focused on individual financial data management. See `.cursor/` for Cursor-integrated development standards.

## License

[To be determined based on your preferences]

## Next Steps

1. Set up your development environment with `make setup`
2. Choose your initial data sources
3. Implement basic PDF extraction or CSV processing
4. Set up DuckDB and run your first queries
5. Gradually add automation and advanced features

For detailed implementation guidance, see the documentation in the `docs/` folder.
