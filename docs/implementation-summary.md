# MoneyBin Implementation Summary

## ✅ Completed Setup

### 1. Project Rules Cleanup

- **Streamlined PROJECT_RULES.md**: Converted to high-level overview
- **Cursor Integration**: Created `.cursor/rules/*.mdc` files for AI integration
- **DuckDB Standards**: Added explicit DuckDB function usage rules

### 2. Comprehensive Architecture Design

- **Data Pipeline**: Dagster → dbt → DuckDB architecture defined
- **Directory Structure**: Complete application structure planned
- **Technology Stack**: Modern Python 3.11+ with latest libraries

### 3. Framework Initialization ✅

- **Git Repository**: Already initialized and ready
- **Dagster Project**: Pipeline structure in `pipelines/` directory
- **dbt Core Project**: Located in `dbt/` subdirectory with `dbt_project.yml`
- **Python Environment**: Virtual environment setup with `uv` package manager

### 4. Data Sources Strategy (Priority Order)

#### Priority 1: Plaid API ✅

- **Wells Fargo**: Full support
- **Chase**: Full support
- **Capital One**: Full support
- **Fidelity**: Investment accounts supported
- **E*TRADE**: Brokerage accounts supported

#### Priority 2: Direct Bank APIs / Yodlee ⚠️

- **Goldman Sachs**: Limited to Marcus savings
- **QuickBooks**: Separate API integration required

#### Priority 3: Manual CSV Processing ✅

- **All Banks**: Fallback CSV export processing
- **Standardization**: Bank-specific parsers defined

#### Priority 4: PDF OCR Processing ✅

- **Primary Tool**: pdfplumber for modern OCR
- **Tax Forms**: IRS.gov and Georgia Tax Center support
- **Bank Statements**: All institutions as final fallback

### 5. Modern OCR Strategy

- **pdfplumber**: Primary tool for 2024
- **Tax Forms**: Form 1040, W-2, 1099 extraction
- **Bank Statements**: Multi-bank PDF processing
- **Investment Accounts**: Fidelity, E*TRADE statement processing

### 6. Modern CLI Interface ✅

- **Unified Entry Point**: Single `moneybin` command for all operations
- **Typer Framework**: Type-safe CLI with automatic help generation
- **Command Groups**: Organized into logical groups (extract, credentials)
- **Rich Documentation**: Automatic help formatting and validation
- **Console Scripts**: Proper entry points in pyproject.toml

## 🏗️ Application Architecture

### Core Components

```text
Dagster Orchestration (pipelines/)
    ↓
Raw Data Assets (Plaid, CSV, PDF)
    ↓
DuckDB Staging Tables
    ↓
dbt Transformations (dbt/)
    ↓
Analytics-Ready Data
```

### Current Directory Structure

The project follows a modern data engineering architecture with Dagster orchestration, dbt transformations, and DuckDB analytics.

**📁 For the complete directory structure**, see [Application Architecture → Directory Structure](application-architecture.md#directory-structure).

## 🎯 Institution-Specific Approaches

| Institution | Plaid | Direct API | CSV | PDF |
|-------------|-------|------------|-----|-----|
| Wells Fargo | ✅ Primary | ❌ Business Only | ✅ Fallback | ✅ Final |
| Chase | ✅ Primary | ❌ Business Only | ✅ Fallback | ✅ Final |
| Capital One | ✅ Primary | ❌ Limited | ✅ Fallback | ✅ Final |
| Fidelity | ✅ Primary | ❌ No API | ✅ Fallback | ✅ Final |
| E*TRADE | ✅ Primary | ❌ No API | ✅ Fallback | ✅ Final |
| Goldman Sachs | ❌ Not Supported | ⚠️ Marcus Only | ⚠️ Limited | ✅ Primary |
| QuickBooks | ❌ Separate API | ✅ Full Access | ✅ Export | ❌ N/A |
| IRS.gov | ❌ No API | ❌ No API | ❌ No Export | ✅ Only Option |
| Georgia Tax | ❌ No API | ❌ No API | ❌ No Export | ✅ Only Option |

## 🛠️ Technology Stack

### Core Dependencies

- **Dagster 1.8+**: Workflow orchestration
- **dbt-duckdb 1.8+**: SQL transformations
- **DuckDB 1.1+**: Analytics database
- **pdfplumber 0.11+**: PDF processing
- **plaid-python 15.0+**: Bank API integration

### Development Tools

- **Python 3.11+**: Modern Python features
- **uv**: Fast Python package manager
- **Cursor AI**: Integrated development rules
- **pytest**: Testing framework
- **polars 0.22+**: High-performance data manipulation when needed

## 🚀 Next Steps

### Phase 1: Foundation ✅ (Completed)

1. ✅ Set up Dagster + dbt + DuckDB environment
2. ✅ Initialize project structure and dependencies
3. ✅ Configure development environment with Makefile

### Phase 2: Core Implementation ✅ (In Progress)

1. ✅ **Plaid API Integration**: Modern Typer CLI with unified interface
   - `moneybin extract plaid` - Extract from all institutions
   - `moneybin credentials validate` - Validate API credentials
   - Secure credential management with environment variables
2. Create CSV processing pipeline for manual uploads
3. Build basic PDF extraction for tax forms
4. Develop data transformation models in dbt

### Phase 3: Advanced Features

1. Add QuickBooks API integration
2. Implement Goldman Sachs PDF processing
3. Create automated scheduling and monitoring
4. Build data quality validation and testing

### Phase 4: Analytics & Optimization

1. Create financial analysis dashboards
2. Implement advanced categorization
3. Add trend analysis and budgeting features
4. Optimize performance for large datasets

## 📚 Documentation Structure

- **[data-sources-strategy.md](data-sources-strategy.md)**: Institution-specific approaches
- **[application-architecture.md](application-architecture.md)**: Technical architecture details
- **[modern-ocr-strategy.md](modern-ocr-strategy.md)**: PDF processing implementation
- **[setup-instructions.md](setup-instructions.md)**: Development environment setup
- **`.cursor/`**: AI-integrated development standards

## ⚡ Key Benefits Achieved

✅ **Modern Architecture**: Dagster + dbt + DuckDB for scalable data processing
✅ **Framework Ready**: All frameworks initialized and configured
✅ **Development Environment**: Complete setup with Makefile automation
✅ **Comprehensive Coverage**: All target institutions have extraction strategies
✅ **Prioritized Approach**: API-first with intelligent fallbacks
✅ **AI Integration**: Cursor rules for consistent development
✅ **Local Control**: Complete data ownership and privacy
✅ **Extensible Design**: Easy to add new data sources and features

The MoneyBin project now has a complete, modern architecture with all frameworks initialized and clear implementation paths for all target financial institutions and tax processing requirements.

## 🔐 Configuration Management & Profile System

MoneyBin implements a comprehensive profile-based system for complete data isolation, enabling multi-user support and environment separation (dev/test/prod).

### Profile System Architecture

**Core Implementation**: `src/moneybin/config.py`, `src/moneybin/utils/user_config.py`

Each profile maintains completely isolated:
- **Database**: Separate DuckDB file per profile
- **Data**: Isolated raw, processed, and temp directories
- **Logs**: Profile-specific log files
- **Extractors**: All data extraction respects profile boundaries

### Profile Directory Structure

```
data/
  {profile}/                    # Each profile has its own data directory
    moneybin.duckdb            # Profile-specific database
    raw/                       # Raw extracted data
      ofx/                     # OFX/QFX bank files
      w2/                      # W2 tax forms
      plaid/                   # Plaid API data
    processed/                 # Processed data
    temp/                      # Temporary files

logs/
  {profile}/                    # Profile-specific logs
    moneybin.log               # Application log file
```

### User Configuration Management

**File**: `~/.moneybin/config.yaml`

Stores persistent user preferences:
```yaml
default_profile: john-smith
```

**Profile Name Normalization**:
- User-friendly input: "John Smith", "alice_work", "Bob-Personal"
- Normalized output: "john-smith", "alice-work", "bob-personal"
- Rules: lowercase, spaces/underscores → hyphens, special characters removed

**First-Run Experience**:
```bash
$ moneybin sync plaid
👋 Welcome to MoneyBin!

No default profile found. Let's set one up.

What is your first name? John
✅ Created profile: john
✅ Set john as your default profile

Configuration saved to: ~/.moneybin/config.yaml
```

### Profile Resolution Priority

```
1. CLI flag: --profile=alice       (highest priority)
2. Environment variable: MONEYBIN_PROFILE=alice
3. Saved default: ~/.moneybin/config.yaml
4. Interactive prompt: (first run only)
```

### CLI Profile Management

```bash
# View current configuration
moneybin config show
moneybin config path                    # Show config file location

# Manage default profile
moneybin config get-default-profile
moneybin config set-default-profile john

# Use different profiles
moneybin --profile=alice sync plaid     # Alice's data
moneybin --profile=bob extract ofx file.qfx    # Bob's data
moneybin -p work load parquet           # Work profile (short flag)

# Environment variable
export MONEYBIN_PROFILE=alice
moneybin extract plaid                  # Uses alice profile
```

### Profile-Aware Components

#### Extractors (All Respect Profiles)

**OFX Extractor** (`src/moneybin/extractors/ofx_extractor.py`):
```python
# Automatically uses: data/{profile}/raw/ofx/
extractor = OFXExtractor()  # Uses current profile
```

**W2 Extractor** (`src/moneybin/extractors/w2_extractor.py`):
```python
# Automatically uses: data/{profile}/raw/w2/
extractor = W2Extractor()  # Uses current profile
```

**Plaid Extractor** (`src/moneybin_server/connectors/plaid/extractor.py`):
```python
# Must provide profile-aware path explicitly
config = PlaidExtractionConfig(
    raw_data_path=settings.data.raw_data_path / "plaid"
)
```

#### Logging System

**Implementation**: `src/moneybin/logging/config.py`

Logs automatically go to `logs/{profile}/moneybin.log`:
```python
# Setup happens in CLI main callback with profile context
setup_logging(cli_mode=True, verbose=verbose, profile=profile)

# Log files created per profile:
# logs/alice/moneybin.log
# logs/bob/moneybin.log
# logs/test/moneybin.log
```

### Multi-User Example

```bash
# Alice's workflow
moneybin --profile=alice sync plaid
moneybin --profile=alice load parquet
# Data: data/alice/raw/plaid/
# Database: data/alice/moneybin.duckdb
# Logs: logs/alice/moneybin.log

# Bob's workflow
moneybin --profile=bob extract ofx statement.qfx
moneybin --profile=bob load parquet
# Data: data/bob/raw/ofx/
# Database: data/bob/moneybin.duckdb
# Logs: logs/bob/moneybin.log

# No data pollution - completely isolated!
```

### Test Isolation

**Implementation**: `tests/moneybin/conftest.py`

Automatic test cleanup via `autouse=True` fixture:
```python
@pytest.fixture(autouse=True)
def clean_profile_state():
    """Clean up profile state before and after each test."""
    # Before test: clean state
    clear_settings_cache()
    set_current_profile("test")

    yield  # Run test

    # After test: cleanup
    clear_settings_cache()
    set_current_profile("test")
    cleanup_test_profile_directories()  # Remove test profiles
```

**Cleaned Up Profiles**:
- `alice`, `bob`, `dev`, `prod`
- `alice-work`, `bob-personal`
- `invalid`, `testprofile`
- Any profile created during tests

**Persistent Profiles**:
- `test` (test fixture profile)
- User profiles (e.g., `john-smith`, `brandon`)

### Security Features

- **Data Isolation**: Each user/environment has completely separate data
- **No Cross-Contamination**: Tests cannot affect production data
- **Clean State**: Automatic cleanup ensures reproducible tests
- **Type Safety**: Pydantic validation of all configuration values
- **Path Validation**: Profile names validated and normalized for safety

## 📊 Data Warehouse Architecture

### Core Data Models (dbt)

**Implementation**: `dbt/models/core/`

#### Unified Transactions Fact Table

**File**: `dbt/models/core/fct_transactions.sql`

The primary fact table for all transaction-level analysis:

**Design Philosophy**:

- **Multi-Source Ready**: Supports transactions from any source (Plaid, CSV, cryptocurrency, etc.)
- **Standardized Schema**: Consistent data types and field names across all sources
- **Kimball Methodology**: Follows dimensional modeling best practices
- **Type Safety**: Proper data type conversions from raw data

**Key Features**:

1. **Source System Tracking**

   ```sql
   source_system VARCHAR  -- 'plaid', 'csv', 'crypto', etc.
   ```

   Enables multi-source transaction consolidation and data lineage

2. **Standardized Amounts**

   ```sql
   amount DECIMAL(18,2)           -- negative = expense, positive = income
   amount_absolute DECIMAL(18,2)  -- always positive
   transaction_direction VARCHAR  -- 'expense', 'income', 'zero'
   ```

   Normalized convention (opposite of Plaid's) aligns with accounting standards

3. **Rich Time Dimensions**

   ```sql
   transaction_date DATE
   transaction_year INTEGER
   transaction_month INTEGER
   transaction_year_month VARCHAR  -- 'YYYY-MM'
   transaction_year_quarter VARCHAR  -- 'YYYY-QN'
   ```

   Pre-computed for fast time-based analysis

4. **Location Data**

   ```sql
   location_city, location_region, location_country
   location_latitude, location_longitude
   ```

   Enables geographic analysis of spending

5. **Category Hierarchy**

   ```sql
   category VARCHAR           -- Primary category
   subcategory VARCHAR        -- Detailed subcategory
   ```

   Two-level categorization for flexible analysis

6. **Data Quality** (via dbt tests in `schema.yml`):

- Transaction ID uniqueness
- Required field validation (not null)
- Valid transaction directions
- Valid source systems
- Referential integrity

#### Future Core Tables (Planned)

- `fct_account_balances`: Daily balance snapshots
- `fct_investments`: Investment transactions and positions
- `dim_accounts`: Account master data
- `dim_institutions`: Financial institution metadata

### Analytics Marts (Future)

**Location**: `dbt/models/marts/`

Future analytical models built on `fct_transactions`:

- Monthly spending analysis by category
- Cash flow projections
- Budget tracking and variance
- Net worth over time

## 🧪 Testing Infrastructure

### Test Coverage

**Location**: `tests/`

#### Configuration Tests

**File**: `tests/test_config_profiles.py`

Comprehensive profile system testing:

- Profile loading and validation
- Environment file selection (`.env.dev`, `.env.prod`)
- Profile caching and reloading
- Legacy environment variable support
- Settings validation

#### CLI Profile Tests

**File**: `tests/test_cli_profiles.py`

CLI integration testing:

- Profile flag parsing (`--profile`, `-p`)
- Default profile behavior (dev)
- Invalid profile handling
- Profile propagation to commands
- Profile indicator output validation
- Environment variable overrides

#### Data Pipeline Tests

- `test_plaid_extractor.py`: Plaid API integration
- `test_parquet_loader.py`: Database loading
- `test_extract_commands.py`: CLI extraction commands
- `test_load_commands.py`: CLI loading commands
- `test_transform_commands.py`: CLI transformation commands

### Running Tests

```bash
# All tests
make test

# With coverage
make test-cov

# Specific test files
pytest tests/test_config_profiles.py -v
pytest tests/test_cli_profiles.py -v

# Specific test
pytest tests/test_cli_profiles.py::TestCLIProfileHandling::test_explicit_prod_profile -v
```

## 📋 Next Implementation Steps

### Immediate (To Complete This Feature)

1. **Configure Production Environment**

   ```bash
   cp .env.prod.example .env.prod
   # Edit .env.prod with real Plaid production credentials
   ```

2. **Extract Production Data**

   ```bash
   moneybin --profile=prod extract plaid
   ```

3. **Load and Transform**

   ```bash
   moneybin --profile=prod load parquet
   moneybin --profile=prod transform run
   ```

4. **Verify Unified Transactions**

   ```bash
   moneybin --profile=prod load status
   # Query fct_transactions table to verify data
   ```

5. **Compare with Other Apps**
   - Export transactions from existing finance apps
   - Compare against `fct_transactions` to validate accuracy

### Short-Term Enhancements

1. **Additional Marts**
   - `monthly_spending.sql`: Monthly aggregations by category
   - `recurring_transactions.sql`: Identify recurring charges
   - `cash_flow.sql`: Income vs expenses analysis

2. **Dimension Tables**
   - `dim_accounts`: Account master data with metadata
   - `dim_categories`: Category hierarchy and mappings

3. **Data Quality**
   - Additional dbt tests for business logic
   - Data freshness checks
   - Volume anomaly detection

### Future Enhancements

1. **Additional Data Sources**
   - Manual CSV uploads with web interface
   - Cryptocurrency exchange APIs
   - Investment portfolio tracking
   - Additional bank API integrations

2. **Advanced Analytics**
   - Budget vs actual tracking
   - Spending trend analysis
   - Anomaly detection
   - Forecasting and projections

3. **Automation**
   - Dagster scheduled runs
   - Email/SMS alerts
   - Data quality monitoring
   - Automated reconciliation

4. **Visualization**
   - Dashboard integration (Metabase, Superset)
   - Custom reports and exports
   - Real-time transaction monitoring
