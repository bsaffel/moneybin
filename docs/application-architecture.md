# MoneyBin Application Architecture

## System Overview

```text
┌─────────────────┐    ┌──────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Data Sources  │───▶│   Dagster    │───▶│   dbt Models    │───▶│   DuckDB        │
│                 │    │ Orchestration│    │ Transformations │    │   Analytics     │
└─────────────────┘    └──────────────┘    └─────────────────┘    └─────────────────┘
        │                       │                       │                      │
        ▼                       ▼                       ▼                      ▼
┌─────────────────┐    ┌──────────────┐    ┌─────────────────┐    ┌─────────────────┐
│ • Plaid API     │    │ • Scheduling │    │ • Data Cleaning │    │ • SQL Queries   │
│ • Bank APIs     │    │ • Monitoring │    │ • Categorization│    │ • Aggregations  │
│ • CSV Files     │    │ • Retries    │    │ • Deduplication │    │ • Reports       │
│ • PDF OCR       │    │ • Logging    │    │ • Validation    │    │ • Dashboards    │
└─────────────────┘    └──────────────┘    └─────────────────┘    └─────────────────┘
```

## Directory Structure

```text
moneybin/
├── .cursor/
│   └── rules/                    # Cursor AI integration rules
├── pipelines/                     # Dagster pipeline definitions
│   ├── assets.py                 # Data assets definitions
│   ├── definitions.py            # Pipeline definitions
│   └── __init__.py               # Package initialization
├── dbt/                          # dbt transformations
│   ├── models/                   # SQL transformation models
│   │   ├── staging/             # Raw data staging
│   │   ├── intermediate/        # Business logic
│   │   └── marts/               # Final analytics tables
│   ├── macros/                  # Reusable SQL macros
│   ├── tests/                   # Data quality tests
│   └── seeds/                   # Reference data
├── src/                         # Python source code
│   ├── extractors/              # Data extraction modules
│   │   ├── plaid_extractor.py  # Plaid API integration
│   │   ├── pdf_extractor.py    # PDF processing
│   │   ├── csv_processor.py    # CSV standardization
│   │   └── quickbooks_api.py   # QuickBooks integration
│   ├── processors/              # Data processing utilities
│   ├── validators/              # Data validation
│   └── utils/                   # Shared utilities
├── data/                        # Data storage
│   ├── raw/                     # Raw extracted data
│   ├── processed/               # Cleaned/standardized data
│   ├── duckdb/                  # DuckDB database files
│   └── temp/                    # Temporary processing files
├── config/                      # Configuration files
│   ├── dagster.yaml            # Dagster configuration
│   ├── dbt_profiles.yml        # dbt profiles
│   └── app_config.yaml         # Application settings
├── notebooks/                   # Jupyter notebooks for analysis
├── tests/                       # Unit and integration tests
├── docs/                        # Documentation
└── pyproject.toml              # Python project configuration
```

## Technology Stack

### Core Components

- **Orchestration**: Dagster 1.5+ (latest stable)
- **Transformations**: dbt-duckdb 1.6+
- **Database**: DuckDB 0.9+ (embedded analytics)
- **Language**: Python 3.11+
- **PDF Processing**: pdfplumber 0.9+

### Key Dependencies

All project dependencies are managed in `pyproject.toml`. See the `[project.dependencies]` section for core runtime dependencies and `[project.optional-dependencies]` for development, testing, and documentation dependencies.

## Dagster Asset Architecture

### Data Assets Flow

```python
# pipelines/assets.py
from dagster import asset, AssetIn
import polars as pl
import duckdb

@asset(group_name="raw_data")
def plaid_transactions() -> pd.DataFrame:
    """Extract transactions from Plaid API"""
    from src.extractors.plaid_extractor import PlaidExtractor
    extractor = PlaidExtractor()
    return extractor.get_all_transactions()

@asset(group_name="raw_data")
def manual_csv_transactions() -> pd.DataFrame:
    """Process manually uploaded CSV files"""
    from src.extractors.csv_processor import CSVProcessor
    processor = CSVProcessor()
    return processor.process_all_csv_files()

@asset(group_name="raw_data")
def tax_pdf_data() -> pd.DataFrame:
    """Extract data from tax PDF documents"""
    from src.extractors.pdf_extractor import TaxPDFExtractor
    extractor = TaxPDFExtractor()
    return extractor.extract_all_tax_forms()

@asset(group_name="staging", deps=[plaid_transactions, manual_csv_transactions])
def raw_transactions_combined() -> None:
    """Combine all transaction sources into DuckDB staging table"""
    # Load data into DuckDB raw tables
    conn = duckdb.connect('data/duckdb/financial.db')

    # Load Plaid data
    plaid_df = plaid_transactions()
    conn.execute("CREATE TABLE IF NOT EXISTS raw_plaid_transactions AS SELECT * FROM plaid_df")

    # Load CSV data
    csv_df = manual_csv_transactions()
    conn.execute("CREATE TABLE IF NOT EXISTS raw_csv_transactions AS SELECT * FROM csv_df")

    conn.close()

@asset(group_name="analytics", deps=[raw_transactions_combined])
def dbt_models() -> None:
    """Run dbt transformations"""
    import subprocess
    result = subprocess.run(["dbt", "run", "--project-dir", "dbt"],
                          capture_output=True, text=True)
    if result.returncode != 0:
        raise Exception(f"dbt run failed: {result.stderr}")
```

### Job Definitions

```python
# pipelines/definitions.py
from dagster import job, op, Config

@job
def daily_data_pipeline():
    """Daily job to extract and process financial data"""
    raw_transactions_combined()
    dbt_models()

@job
def weekly_pdf_processing():
    """Weekly job to process new PDF documents"""
    tax_pdf_data()
    dbt_models()
```

### Scheduling

```python
# pipelines/schedules.py (future)
from dagster import schedule, ScheduleEvaluationContext

@schedule(
    job=daily_data_pipeline,
    cron_schedule="0 6 * * *"  # 6 AM daily
)
def daily_schedule(context: ScheduleEvaluationContext):
    return {}

@schedule(
    job=weekly_pdf_processing,
    cron_schedule="0 8 * * 0"  # 8 AM Sundays
)
def weekly_schedule(context: ScheduleEvaluationContext):
    return {}
```

## dbt Model Architecture

### Staging Models

```sql
-- dbt/models/staging/stg_plaid_transactions.sql
{{ config(materialized='table') }}

SELECT
    account_id,
    transaction_id,
    date,
    amount,
    description,
    category,
    'plaid' as source_system,
    CURRENT_TIMESTAMP as loaded_at
FROM {{ source('raw', 'plaid_transactions') }}
WHERE date >= '2020-01-01'
```

```sql
-- dbt/models/staging/stg_csv_transactions.sql
{{ config(materialized='table') }}

SELECT
    account_id,
    transaction_id,
    date,
    amount,
    description,
    category,
    'csv_upload' as source_system,
    CURRENT_TIMESTAMP as loaded_at
FROM {{ source('raw', 'csv_transactions') }}
WHERE date >= '2020-01-01'
```

### Intermediate Models

```sql
-- dbt/models/intermediate/int_transactions_unified.sql
{{ config(materialized='table') }}

WITH all_transactions AS (
    SELECT * FROM {{ ref('stg_plaid_transactions') }}
    UNION ALL
    SELECT * FROM {{ ref('stg_csv_transactions') }}
),

deduplicated AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY account_id, date, amount, description
            ORDER BY loaded_at DESC
        ) as rn
    FROM all_transactions
)

SELECT
    account_id,
    transaction_id,
    date,
    amount,
    description,
    category,
    source_system,
    loaded_at
FROM deduplicated
WHERE rn = 1
```

### Mart Models

```sql
-- dbt/models/marts/fct_transactions.sql
{{ config(materialized='table') }}

SELECT
    transaction_id,
    account_id,
    date,
    amount,
    description,
    {{ categorize_transaction('description') }} as category_clean,
    source_system,
    loaded_at,
    YEAR(date) as year,
    MONTH(date) as month,
    CASE WHEN amount > 0 THEN 'Income' ELSE 'Expense' END as transaction_type
FROM {{ ref('int_transactions_unified') }}
```

```sql
-- dbt/models/marts/dim_accounts.sql
{{ config(materialized='table') }}

SELECT DISTINCT
    account_id,
    FIRST_VALUE(account_name) OVER (PARTITION BY account_id ORDER BY loaded_at DESC) as account_name,
    FIRST_VALUE(account_type) OVER (PARTITION BY account_id ORDER BY loaded_at DESC) as account_type,
    FIRST_VALUE(institution) OVER (PARTITION BY account_id ORDER BY loaded_at DESC) as institution
FROM {{ ref('int_transactions_unified') }}
```

## Configuration Management

### Dagster Configuration

```yaml
# config/dagster.yaml
storage:
  filesystem:
    base_dir: "data/dagster_storage"

run_launcher:
  module: dagster.core.launcher.sync_in_memory_run_launcher
  class: SyncInMemoryRunLauncher

compute_logs:
  module: dagster.core.storage.noop_compute_log_manager
  class: NoOpComputeLogManager
```

### dbt Configuration

```yaml
# dbt/dbt_project.yml
name: 'moneybin'
version: '1.0.0'
config-version: 2

profile: 'moneybin'

model-paths: ["models"]
analysis-paths: ["analysis"]
test-paths: ["tests"]
seed-paths: ["seeds"]
macro-paths: ["macros"]

target-path: "target"
clean-targets:
  - "target"
  - "dbt_packages"

models:
  moneybin:
    staging:
      +materialized: table
    intermediate:
      +materialized: table
    marts:
      +materialized: table
```

```yaml
# config/dbt_profiles.yml
moneybin:
  outputs:
    dev:
      type: duckdb
      path: 'data/duckdb/financial.db'
      threads: 4
    prod:
      type: duckdb
      path: 'data/duckdb/financial_prod.db'
      threads: 8
  target: dev
```

## Data Validation & Quality

### dbt Tests

```sql
-- dbt/tests/assert_no_duplicate_transactions.sql
SELECT
    account_id,
    date,
    amount,
    description,
    COUNT(*) as duplicate_count
FROM {{ ref('fct_transactions') }}
GROUP BY account_id, date, amount, description
HAVING COUNT(*) > 1
```

### Dagster Data Quality

```python
@asset
def transaction_quality_report() -> pd.DataFrame:
    """Generate data quality metrics"""
    conn = duckdb.connect('data/duckdb/financial.db')

    quality_checks = conn.execute("""
        SELECT
            'duplicate_transactions' as check_name,
            COUNT(*) as issues_found
        FROM (
            SELECT account_id, date, amount, description, COUNT(*) as cnt
            FROM fct_transactions
            GROUP BY account_id, date, amount, description
            HAVING COUNT(*) > 1
        )
        UNION ALL
        SELECT
            'missing_categories',
            COUNT(*)
        FROM fct_transactions
        WHERE category_clean IS NULL
    """).fetchdf()

    return quality_checks
```

## Deployment & Operations

### Local Development

```bash
# Start Dagster development server
dagster dev

# Run dbt models
cd dbt && dbt run

# Run tests
pytest tests/
```

### Production Considerations

- **Database**: Upgrade to DuckDB with persistent storage
- **Scheduling**: Use Dagster's built-in scheduler or external cron
- **Monitoring**: Dagster UI for pipeline monitoring
- **Backups**: Regular DuckDB file backups to cloud storage
- **Security**: Environment variable management for API keys

This architecture provides a robust, scalable foundation for financial data processing with modern data engineering best practices.
