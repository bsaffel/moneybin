# Plaid API Integration Guide

## Overview

This guide covers the secure implementation of Plaid API integration for MoneyBin, including setup, configuration, data extraction, and security best practices.

## Quick Start

### 1. Environment Setup

1. **Copy the environment template:**

   ```bash
   cp .env.example .env
   ```

2. **Configure Plaid credentials in `.env`:**

   ```bash
   # Plaid API Configuration
   PLAID_CLIENT_ID=your_plaid_client_id_here
   PLAID_SECRET=your_plaid_secret_here
   PLAID_ENV=sandbox  # sandbox, development, or production
   ```

3. **Get your Plaid credentials:**
   - Visit [Plaid Dashboard](https://dashboard.plaid.com/team/keys)
   - Create a new application or use existing one
   - Copy Client ID and Secret key

### 2. Link Financial Institutions

Use Plaid Link to connect your accounts and get access tokens:

```python
from moneybin.extractors import PlaidExtractor, PlaidConnectionManager

# Initialize the extractor
extractor = PlaidExtractor()

# After using Plaid Link to connect an account, you'll get an access token
# Add it to your .env file:
# PLAID_TOKEN_WELLS_FARGO=access-sandbox-xxx
# PLAID_TOKEN_CHASE=access-sandbox-yyy
```

### 3. Extract Data

#### Using the CLI (Recommended)

```bash
# Extract from all configured institutions
moneybin extract plaid

# Extract with verbose logging for debugging
moneybin extract plaid --verbose
```

#### Using Python API Directly

```python
# Extract from a single institution
access_token = "access-sandbox-example-token"
data = extractor.extract_all_data(access_token, "Wells Fargo")

# Extract from all configured institutions
manager = PlaidConnectionManager()
all_data = manager.extract_all_institutions()
```

### 4. Run the Extraction

```bash
# Set up credentials (first time only)
moneybin credentials setup

# Validate credentials
moneybin credentials validate

# Extract data from Plaid
moneybin extract plaid

# Extract with verbose logging
moneybin extract plaid --verbose

# Create .env template without extraction
moneybin extract plaid --setup-env

# Or integrate with Dagster pipeline
make dagster-dev
```

## Supported Institutions

### ✅ Full Plaid Support

- **Wells Fargo**: Checking, savings, credit cards
- **Chase**: All account types
- **Capital One**: Banking and credit products
- **Fidelity**: Investment accounts and 401(k)
- **E*TRADE**: Brokerage and retirement accounts

### ⚠️ Limited Support

- **Goldman Sachs**: Marcus savings only (wealth management requires PDF processing)
- **QuickBooks**: Separate API integration (not through Plaid)

## Data Types Extracted

### Core Banking Data

- **Accounts**: Account details, balances, metadata
- **Transactions**: All transaction history with categorization
- **Balances**: Current and available balances

### Investment Data (when available)

- **Holdings**: Current investment positions
- **Securities**: Security master data (stocks, bonds, funds)
- **Investment Transactions**: Buy/sell/dividend transactions

### Credit & Loan Data (when available)

- **Credit Cards**: Balances, APRs, payment due dates
- **Student Loans**: Loan details, servicer info, PSLF status
- **Mortgages**: Loan terms, payment schedules, property info

### Identity Data (requires additional permissions)

- **Account Holders**: Names, addresses, contact information

## Security Features

### Credential Management

- Environment variable storage for API keys
- Secure credential validation
- No hardcoded secrets in source code
- Future encrypted token storage support

### API Security

- Automatic retry with exponential backoff
- Rate limit handling
- Comprehensive error classification
- Secure HTTPS connections only

### Data Protection

- Local data storage only
- Encrypted data at rest (when configured)
- Audit logging for all operations
- Data validation and quality checks

## Error Handling

### Error Types and Resolution

#### Authentication Errors

```text
PlaidAuthError: Invalid credentials or expired tokens
Resolution: Check API keys or relink account
```

#### Item Errors

```text
PlaidItemError: Account connection issues
Resolution: User needs to relink account through Plaid Link
```

#### Rate Limit Errors

```text
PlaidRateLimitError: API rate limits exceeded
Resolution: Wait and retry, or reduce extraction frequency
```

#### API Errors

```text
PlaidAPIError: General API issues
Resolution: Check Plaid service status
```

### Retry Logic

The extractor automatically retries failed requests with:

- **Exponential backoff**: 1s, 2s, 4s delays
- **Smart retry**: Only retries temporary errors
- **Max attempts**: 3 retries per request
- **Rate limit handling**: Automatic delay for rate limits

## Data Validation

### Automatic Quality Checks

1. **Required Fields**: Ensures all critical fields are present
2. **Duplicate Detection**: Identifies duplicate records
3. **Amount Validation**: Checks for reasonable transaction amounts
4. **Date Validation**: Validates date formats and ranges
5. **Reference Integrity**: Ensures foreign key relationships

### Custom Validation

```python
from moneybin.extractors.plaid_schemas import validate_transaction_data

# Validate extracted transaction data
transactions = extractor.get_transactions(access_token)
quality_checks = validate_transaction_data(transactions.to_dicts())

for check in quality_checks:
    if not check.passed:
        print(f"Quality issue: {check.check_name}")
        print(f"Errors: {check.error_details}")
```

## Configuration Options

### PlaidExtractionConfig

```python
from moneybin.extractors import PlaidExtractionConfig
from pathlib import Path

config = PlaidExtractionConfig(
    days_lookback=365,  # How far back to extract transactions
    batch_size=500,     # Transactions per API request (max 500)
    max_retries=3,      # Maximum retry attempts
    retry_delay=1.0,    # Base delay between retries
    output_format="polars",  # Data format (polars or pandas)
    save_raw_data=True, # Save raw API responses
    raw_data_path=Path("data/raw/plaid")  # Where to save raw data
)

extractor = PlaidExtractor(config)
```

## Integration with MoneyBin Pipeline

### Dagster Asset Integration

```python
# pipelines/assets.py
from dagster import asset
from moneybin.extractors import PlaidConnectionManager

@asset(group_name="raw_data")
def plaid_raw_data():
    """Extract all Plaid data and save to raw storage."""
    manager = PlaidConnectionManager()
    return manager.extract_all_institutions()

@asset(group_name="staging", deps=[plaid_raw_data])
def plaid_staging_tables():
    """Load Plaid data into DuckDB staging tables."""
    # Implementation to load into DuckDB
    pass
```

### dbt Model Integration

The extracted data feeds into dbt staging models:

- `stg_plaid_accounts`: Standardized account data
- `stg_plaid_transactions`: Cleaned transaction data
- `stg_plaid_investments`: Investment holdings and transactions
- `stg_plaid_liabilities`: Credit and loan data

## Monitoring and Logging

### Extraction Job Tracking

Each extraction creates a job record with:

- Unique job ID for tracking
- Start/end times
- Extraction metrics (counts by data type)
- Error messages and status
- Configuration used

### Log Levels

- **INFO**: Normal operation progress
- **WARNING**: Non-critical issues (missing optional data)
- **ERROR**: Critical failures requiring attention
- **DEBUG**: Detailed API request/response info (development only)

### Log Files

- `logs/plaid_extraction.log`: All extraction activity
- `logs/dbt.log`: dbt transformation logs
- `logs/dagster.log`: Pipeline orchestration logs

## Troubleshooting

### Common Issues

#### "No Plaid tokens found"

**Solution**: Add access tokens to environment variables

```bash
PLAID_TOKEN_WELLS_FARGO=access-sandbox-xxx
PLAID_TOKEN_CHASE=access-sandbox-yyy
```

#### "Authentication error"

**Solutions**:

1. Verify Client ID and Secret in `.env`
2. Check Plaid environment setting (sandbox/development/production)
3. Ensure API keys match the environment

#### "Item error - may need relink"

**Solution**: User needs to relink account through Plaid Link interface

#### "Rate limit exceeded"

**Solutions**:

1. Wait for rate limit reset (usually 1 hour)
2. Reduce extraction frequency
3. Contact Plaid for higher rate limits

#### "Investment data not available"

**Explanation**: Not all institutions support investment data through Plaid
**Solution**: Use PDF processing or CSV import as fallback

### Debugging Steps

1. **Check credentials:**

   ```bash
   # Quick validation using CLI
   moneybin credentials validate

   # Or check Plaid specifically
   moneybin credentials validate-plaid
   ```

   ```python
   # Or using Python API directly
   from moneybin.utils import SecretsManager
   manager = SecretsManager()
   validation = manager.validate_all_credentials()
   print(validation)
   ```

2. **Test single institution:**

   ```python
   extractor = PlaidExtractor()
   data = extractor.extract_all_data("your-access-token", "Test Institution")
   ```

3. **Enable debug logging:**

   ```bash
   # Using CLI with verbose flag
   moneybin extract plaid --verbose
   ```

   ```python
   # Or programmatically
   import logging
   logging.getLogger().setLevel(logging.DEBUG)
   ```

## Security Best Practices

### ✅ Implemented

- Environment variable credential storage
- No secrets in source code or version control
- Secure HTTPS API connections
- Local data storage only
- Comprehensive error handling
- Audit logging

### 🔄 Recommended Enhancements

- Encrypted token storage using keyring or similar
- Webhook signature verification for Plaid notifications
- Database encryption at rest
- API request signing for additional security
- Regular credential rotation

### ❌ Never Do

- Hardcode API keys in source code
- Store credentials in version control
- Use HTTP (unencrypted) connections
- Log sensitive data (tokens, account numbers)
- Store data on third-party services without encryption

## Performance Optimization

### Batch Processing

- Transactions extracted in 500-record batches
- Parallel processing for multiple institutions
- Efficient Polars DataFrames for large datasets

### Incremental Updates

```python
# Extract only recent transactions
from datetime import datetime, timedelta

recent_start = datetime.now() - timedelta(days=7)
transactions = extractor.get_transactions(
    access_token,
    start_date=recent_start
)
```

### Data Storage

- Raw data saved as Parquet files for efficiency
- DuckDB for high-performance analytics
- Automatic data compression

## Next Steps

1. **Set up Plaid Link**: Implement web interface for account linking
2. **Webhook Integration**: Handle real-time transaction notifications
3. **Incremental Updates**: Implement daily incremental data extraction
4. **Data Monitoring**: Set up alerts for extraction failures
5. **Advanced Analytics**: Build financial insights and reporting

## Support and Resources

- [Plaid API Documentation](https://plaid.com/docs/api/)
- [Plaid Python SDK](https://github.com/plaid/plaid-python)
- [DuckDB Documentation](https://duckdb.org/docs/)
- [MoneyBin Architecture Guide](application-architecture.md)
- [Data Sources Strategy](data-sources-strategy.md)
