# DuckDB Data Model for Plaid API Integration

## Overview

This document defines the comprehensive data model for storing all Plaid API data in DuckDB. The schema is designed to handle data from all supported financial institutions while maintaining data integrity, performance, and analytical capabilities.

## Core Design Principles

1. **Data Ownership**: All data stored locally in user-controlled DuckDB instance
2. **Security**: Sensitive data encrypted at rest with proper access controls
3. **Completeness**: Capture all available Plaid API data fields
4. **Performance**: Optimized for analytical queries and aggregations
5. **Auditability**: Full data lineage and extraction timestamps
6. **Flexibility**: Support for multiple institutions and account types

## Institution Coverage

### Supported via Plaid API

- ✅ Wells Fargo (checking, savings, credit cards)
- ✅ Chase (all account types)
- ✅ Capital One (banking and credit products)
- ✅ Fidelity (investment accounts and 401k)
- ✅ E*TRADE (brokerage and retirement accounts)

### Future Extensions

- QuickBooks (separate API integration)
- Manual CSV imports (all institutions)
- PDF statement processing (Goldman Sachs, tax forms)

## Core Schema Tables

### 1. Institutions Table

Stores information about financial institutions.

```sql
CREATE TABLE institutions (
    institution_id VARCHAR PRIMARY KEY,
    institution_name VARCHAR NOT NULL,
    country_codes VARCHAR[], -- Array of supported countries
    products VARCHAR[], -- Array of supported Plaid products
    routing_numbers VARCHAR[], -- Array of institution routing numbers
    oauth BOOLEAN DEFAULT FALSE,
    status VARCHAR, -- Institution status in Plaid
    primary_color VARCHAR, -- Brand color for UI
    logo_url VARCHAR, -- Institution logo URL
    url VARCHAR, -- Institution website
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### 2. Items Table

Represents Plaid Items (connections to institutions).

```sql
CREATE TABLE items (
    item_id VARCHAR PRIMARY KEY,
    institution_id VARCHAR NOT NULL,
    webhook_url VARCHAR,
    error_code VARCHAR, -- Current error code if any
    error_type VARCHAR, -- Type of error
    available_products VARCHAR[], -- Array of available products
    billed_products VARCHAR[], -- Array of billed products
    consent_expiration_time TIMESTAMP,
    update_type VARCHAR, -- Type of last update
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    FOREIGN KEY (institution_id) REFERENCES institutions(institution_id)
);
```

### 3. Accounts Table

Core account information for all account types.

```sql
CREATE TABLE accounts (
    account_id VARCHAR PRIMARY KEY,
    item_id VARCHAR NOT NULL,
    institution_id VARCHAR NOT NULL,
    account_name VARCHAR NOT NULL,
    official_name VARCHAR,
    account_type VARCHAR NOT NULL, -- depository, credit, loan, investment, other
    account_subtype VARCHAR, -- checking, savings, credit card, etc.
    mask VARCHAR, -- Last 4 digits
    persistent_account_id VARCHAR, -- Stable across reconnections

    -- Balance information
    balance_available DECIMAL(15,2),
    balance_current DECIMAL(15,2),
    balance_limit DECIMAL(15,2),
    balance_iso_currency_code VARCHAR DEFAULT 'USD',
    balance_unofficial_currency_code VARCHAR,
    balance_last_updated_datetime TIMESTAMP,

    -- Metadata
    verification_status VARCHAR,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    extracted_at TIMESTAMP NOT NULL,

    FOREIGN KEY (item_id) REFERENCES items(item_id),
    FOREIGN KEY (institution_id) REFERENCES institutions(institution_id)
);
```

### 4. Transactions Table

Core transaction data from all account types.

```sql
CREATE TABLE transactions (
    transaction_id VARCHAR PRIMARY KEY,
    account_id VARCHAR NOT NULL,
    pending_transaction_id VARCHAR, -- Links pending to posted transactions

    -- Transaction details
    amount DECIMAL(15,2) NOT NULL,
    iso_currency_code VARCHAR DEFAULT 'USD',
    unofficial_currency_code VARCHAR,

    -- Dates
    date DATE NOT NULL,
    authorized_date DATE,
    authorized_datetime TIMESTAMP,
    datetime TIMESTAMP,

    -- Description and categorization
    name VARCHAR, -- Primary transaction description
    merchant_name VARCHAR,
    original_description VARCHAR, -- Raw bank description
    account_owner VARCHAR, -- Which account owner made the transaction

    -- Transaction classification
    category_primary VARCHAR, -- Top-level category
    category_detailed VARCHAR, -- Full category hierarchy
    category_confidence_level VARCHAR, -- Plaid's confidence in categorization
    payment_channel VARCHAR, -- online, in store, atm, other
    transaction_type VARCHAR, -- special, place, digital, etc.
    transaction_code VARCHAR, -- Bank-specific transaction code

    -- Location data
    location_address VARCHAR,
    location_city VARCHAR,
    location_region VARCHAR,
    location_postal_code VARCHAR,
    location_country VARCHAR,
    location_lat DECIMAL(10,8),
    location_lon DECIMAL(11,8),
    location_store_number VARCHAR,

    -- Status and metadata
    pending BOOLEAN DEFAULT FALSE,
    personal_finance_category VARCHAR, -- Enhanced categorization
    personal_finance_category_confidence_level VARCHAR,
    website VARCHAR, -- Merchant website
    logo_url VARCHAR, -- Merchant logo

    -- Audit fields
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    extracted_at TIMESTAMP NOT NULL,

    FOREIGN KEY (account_id) REFERENCES accounts(account_id)
);
```

### 5. Investment Holdings Table

Current investment positions.

```sql
CREATE TABLE investment_holdings (
    holding_id VARCHAR PRIMARY KEY, -- Generated: account_id + security_id + date
    account_id VARCHAR NOT NULL,
    security_id VARCHAR NOT NULL,

    -- Position details
    institution_price DECIMAL(15,4),
    institution_price_as_of DATE,
    institution_price_datetime TIMESTAMP,
    institution_value DECIMAL(15,2),
    cost_basis DECIMAL(15,2),
    quantity DECIMAL(15,6),

    -- Currency
    iso_currency_code VARCHAR DEFAULT 'USD',
    unofficial_currency_code VARCHAR,

    -- Vesting information (for 401k, employee stock plans)
    vested_quantity DECIMAL(15,6),
    vested_value DECIMAL(15,2),

    -- Audit fields
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    extracted_at TIMESTAMP NOT NULL,

    FOREIGN KEY (account_id) REFERENCES accounts(account_id),
    FOREIGN KEY (security_id) REFERENCES securities(security_id)
);
```

### 6. Securities Table

Investment security master data.

```sql
CREATE TABLE securities (
    security_id VARCHAR PRIMARY KEY,

    -- Security identifiers
    isin VARCHAR, -- International Securities Identification Number
    cusip VARCHAR, -- Committee on Uniform Securities Identification Procedures
    sedol VARCHAR, -- Stock Exchange Daily Official List
    institution_security_id VARCHAR, -- Institution-specific ID
    institution_id VARCHAR,
    proxy_security_id VARCHAR,

    -- Security details
    name VARCHAR NOT NULL,
    ticker_symbol VARCHAR,
    is_cash_equivalent BOOLEAN DEFAULT FALSE,
    type VARCHAR, -- equity, derivative, mutual fund, etc.

    -- Pricing information
    close_price DECIMAL(15,4),
    close_price_as_of DATE,
    iso_currency_code VARCHAR DEFAULT 'USD',
    unofficial_currency_code VARCHAR,

    -- Additional metadata
    market_identifier_code VARCHAR,
    option_contract STRUCT(
        contract_type VARCHAR,
        expiration_date DATE,
        strike_price DECIMAL(15,4),
        underlying_security_id VARCHAR
    ),

    -- Audit fields
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    extracted_at TIMESTAMP NOT NULL,

    FOREIGN KEY (institution_id) REFERENCES institutions(institution_id)
);
```

### 7. Investment Transactions Table

Investment-specific transactions (buys, sells, dividends, etc.).

```sql
CREATE TABLE investment_transactions (
    investment_transaction_id VARCHAR PRIMARY KEY,
    account_id VARCHAR NOT NULL,
    security_id VARCHAR,
    cancel_transaction_id VARCHAR, -- For cancelled transactions

    -- Transaction details
    date DATE NOT NULL,
    name VARCHAR NOT NULL,
    quantity DECIMAL(15,6),
    amount DECIMAL(15,2) NOT NULL,
    price DECIMAL(15,4),
    fees DECIMAL(15,2),

    -- Transaction classification
    type VARCHAR NOT NULL, -- buy, sell, dividend, etc.
    subtype VARCHAR, -- More specific classification

    -- Currency
    iso_currency_code VARCHAR DEFAULT 'USD',
    unofficial_currency_code VARCHAR,

    -- Audit fields
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    extracted_at TIMESTAMP NOT NULL,

    FOREIGN KEY (account_id) REFERENCES accounts(account_id),
    FOREIGN KEY (security_id) REFERENCES securities(security_id)
);
```

### 8. Liabilities Table

Credit cards, loans, and other liabilities.

```sql
CREATE TABLE liabilities (
    liability_id VARCHAR PRIMARY KEY, -- Generated: account_id + liability_type
    account_id VARCHAR NOT NULL,
    liability_type VARCHAR NOT NULL, -- credit, student, mortgage

    -- Credit card specific fields
    last_payment_amount DECIMAL(15,2),
    last_payment_date DATE,
    last_statement_balance DECIMAL(15,2),
    last_statement_issue_date DATE,
    minimum_payment_amount DECIMAL(15,2),
    next_payment_due_date DATE,

    -- APR information
    apr_percentage DECIMAL(5,2),
    apr_type VARCHAR,
    balance_transfer_fee DECIMAL(15,2),
    cash_advance_fee DECIMAL(15,2),
    foreign_transaction_fee DECIMAL(15,2),

    -- Student loan specific fields
    loan_name VARCHAR,
    loan_status_type VARCHAR,
    loan_status_end_date DATE,
    payment_reference_number VARCHAR,
    pslf_status_estimated_eligibility_date DATE,
    repayment_plan_description VARCHAR,
    repayment_plan_type VARCHAR,
    sequence_number VARCHAR,

    -- Servicer information
    servicer_address_city VARCHAR,
    servicer_address_country VARCHAR,
    servicer_address_postal_code VARCHAR,
    servicer_address_region VARCHAR,
    servicer_address_street VARCHAR,

    -- Mortgage specific fields
    account_number VARCHAR,
    current_late_fee DECIMAL(15,2),
    escrow_balance DECIMAL(15,2),
    has_pmi BOOLEAN,
    has_prepayment_penalty BOOLEAN,
    interest_rate_percentage DECIMAL(5,2),
    interest_rate_type VARCHAR,
    loan_type_description VARCHAR,
    loan_term VARCHAR,
    maturity_date DATE,
    next_monthly_payment DECIMAL(15,2),
    origination_date DATE,
    origination_principal_amount DECIMAL(15,2),
    past_due_amount DECIMAL(15,2),

    -- Property information
    property_address_city VARCHAR,
    property_address_country VARCHAR,
    property_address_postal_code VARCHAR,
    property_address_region VARCHAR,
    property_address_street VARCHAR,

    -- Year-to-date information
    ytd_interest_paid DECIMAL(15,2),
    ytd_principal_paid DECIMAL(15,2),

    -- Audit fields
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    extracted_at TIMESTAMP NOT NULL,

    FOREIGN KEY (account_id) REFERENCES accounts(account_id)
);
```

### 9. Identity Table

Account holder identity information.

```sql
CREATE TABLE identity (
    identity_id VARCHAR PRIMARY KEY, -- Generated UUID
    account_id VARCHAR NOT NULL,

    -- Personal information
    owner_names VARCHAR[], -- Array of names
    phone_numbers VARCHAR[], -- Array of phone numbers
    emails VARCHAR[], -- Array of email addresses

    -- Address information (stored as JSON for flexibility)
    addresses JSON, -- Array of address objects with full details

    -- Audit fields
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    extracted_at TIMESTAMP NOT NULL,

    FOREIGN KEY (account_id) REFERENCES accounts(account_id)
);
```

### 10. Balance History Table

Historical balance snapshots for trend analysis.

```sql
CREATE TABLE balance_history (
    balance_history_id VARCHAR PRIMARY KEY, -- Generated: account_id + date
    account_id VARCHAR NOT NULL,

    -- Balance snapshot
    available_balance DECIMAL(15,2),
    current_balance DECIMAL(15,2),
    limit_balance DECIMAL(15,2),
    iso_currency_code VARCHAR DEFAULT 'USD',
    unofficial_currency_code VARCHAR,

    -- Snapshot metadata
    snapshot_date DATE NOT NULL,
    snapshot_time TIMESTAMP NOT NULL,

    -- Audit fields
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    extracted_at TIMESTAMP NOT NULL,

    FOREIGN KEY (account_id) REFERENCES accounts(account_id)
);
```

## Data Extraction Tracking Tables

### 11. Extraction Jobs Table

Track data extraction operations for monitoring and debugging.

```sql
CREATE TABLE extraction_jobs (
    job_id VARCHAR PRIMARY KEY, -- Generated UUID
    job_type VARCHAR NOT NULL, -- full_extract, incremental_update, etc.
    institution_id VARCHAR,
    item_id VARCHAR,

    -- Job execution details
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP,
    status VARCHAR NOT NULL, -- running, completed, failed, cancelled
    error_message TEXT,

    -- Data extraction metrics
    accounts_extracted INTEGER DEFAULT 0,
    transactions_extracted INTEGER DEFAULT 0,
    holdings_extracted INTEGER DEFAULT 0,
    investment_transactions_extracted INTEGER DEFAULT 0,

    -- Configuration
    extraction_config JSON, -- Store extraction parameters

    -- Audit fields
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### 12. Data Quality Metrics Table

Track data quality and validation results.

```sql
CREATE TABLE data_quality_metrics (
    metric_id VARCHAR PRIMARY KEY, -- Generated UUID
    job_id VARCHAR NOT NULL,
    table_name VARCHAR NOT NULL,

    -- Quality metrics
    total_records INTEGER NOT NULL,
    duplicate_records INTEGER DEFAULT 0,
    null_required_fields INTEGER DEFAULT 0,
    invalid_amounts INTEGER DEFAULT 0,
    future_dates INTEGER DEFAULT 0,

    -- Validation results
    validation_status VARCHAR NOT NULL, -- passed, failed, warning
    validation_errors JSON, -- Array of validation error details

    -- Audit fields
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    FOREIGN KEY (job_id) REFERENCES extraction_jobs(job_id)
);
```

## Indexes for Performance

```sql
-- Primary performance indexes
CREATE INDEX idx_transactions_account_date ON transactions(account_id, date DESC);
CREATE INDEX idx_transactions_date ON transactions(date DESC);
CREATE INDEX idx_transactions_amount ON transactions(amount);
CREATE INDEX idx_transactions_category ON transactions(category_primary);
CREATE INDEX idx_transactions_merchant ON transactions(merchant_name);

-- Investment indexes
CREATE INDEX idx_holdings_account_security ON investment_holdings(account_id, security_id);
CREATE INDEX idx_investment_txns_account_date ON investment_transactions(account_id, date DESC);
CREATE INDEX idx_securities_ticker ON securities(ticker_symbol);

-- Balance history indexes
CREATE INDEX idx_balance_history_account_date ON balance_history(account_id, snapshot_date DESC);

-- Audit and monitoring indexes
CREATE INDEX idx_extraction_jobs_status_time ON extraction_jobs(status, start_time DESC);
CREATE INDEX idx_accounts_institution ON accounts(institution_id);
```

## Views for Common Queries

### Current Account Balances View

```sql
CREATE VIEW current_account_balances AS
SELECT
    a.account_id,
    a.account_name,
    a.account_type,
    a.account_subtype,
    i.institution_name,
    a.balance_current,
    a.balance_available,
    a.balance_limit,
    a.balance_iso_currency_code,
    a.updated_at as last_updated
FROM accounts a
JOIN institutions i ON a.institution_id = i.institution_id
WHERE a.balance_current IS NOT NULL;
```

### Recent Transactions View

```sql
CREATE VIEW recent_transactions AS
SELECT
    t.transaction_id,
    t.account_id,
    a.account_name,
    i.institution_name,
    t.date,
    t.amount,
    t.name,
    t.merchant_name,
    t.category_primary,
    t.category_detailed,
    t.payment_channel,
    t.pending
FROM transactions t
JOIN accounts a ON t.account_id = a.account_id
JOIN institutions i ON a.institution_id = i.institution_id
WHERE t.date >= CURRENT_DATE - INTERVAL '30 days'
ORDER BY t.date DESC, t.datetime DESC;
```

### Investment Portfolio Summary View

```sql
CREATE VIEW investment_portfolio_summary AS
SELECT
    h.account_id,
    a.account_name,
    i.institution_name,
    s.name as security_name,
    s.ticker_symbol,
    s.type as security_type,
    h.quantity,
    h.institution_price,
    h.institution_value,
    h.cost_basis,
    CASE
        WHEN h.cost_basis > 0 THEN (h.institution_value - h.cost_basis) / h.cost_basis * 100
        ELSE NULL
    END as gain_loss_percentage,
    h.extracted_at
FROM investment_holdings h
JOIN accounts a ON h.account_id = a.account_id
JOIN institutions i ON a.institution_id = i.institution_id
JOIN securities s ON h.security_id = s.security_id
WHERE h.quantity > 0;
```

## Data Types and Constraints

### Currency Handling

- All monetary amounts stored as `DECIMAL(15,2)` for precision
- Default currency is USD with support for international currencies
- Separate fields for ISO currency codes and unofficial currencies (cryptocurrencies)

### Date Handling

- All dates stored in ISO format (YYYY-MM-DD)
- Timestamps include timezone information where available
- Separate tracking of authorized vs. posted dates for transactions

### Text Data

- `VARCHAR` without length limits for flexibility
- Arrays stored as DuckDB native arrays for efficient querying
- Complex nested data (addresses, options) stored as JSON/STRUCT

### Null Handling

- Required fields marked as `NOT NULL`
- Optional fields allow NULL values
- Default values provided where appropriate

## Data Retention and Archiving

### Retention Policies

```sql
-- Example: Archive transactions older than 7 years
CREATE TABLE transactions_archive AS
SELECT * FROM transactions
WHERE date < CURRENT_DATE - INTERVAL '7 years';

DELETE FROM transactions
WHERE date < CURRENT_DATE - INTERVAL '7 years';
```

### Backup Strategy

- Daily incremental backups of the entire database
- Weekly full database exports to encrypted storage
- Monthly data validation and integrity checks

## Security Considerations

### Data Encryption

- Database file encryption using DuckDB encryption features
- Sensitive fields (account numbers, SSNs) hashed where possible
- API tokens stored in secure environment variables only

### Access Control

- Application-level access control for different user roles
- Audit logging for all data access and modifications
- Secure API endpoint authentication

### Privacy Compliance

- PII data handling following financial data regulations
- Data anonymization capabilities for analytics
- User consent tracking for data processing

## Integration with dbt Models

### Staging Models

The raw Plaid data will be processed through dbt staging models:

- `stg_plaid_accounts`: Clean and standardize account data
- `stg_plaid_transactions`: Clean and categorize transaction data
- `stg_plaid_investments`: Process investment holdings and transactions
- `stg_plaid_liabilities`: Standardize liability data

### Intermediate Models

- `int_accounts_unified`: Combine Plaid accounts with other sources
- `int_transactions_unified`: Deduplicate and merge all transaction sources
- `int_investment_positions`: Calculate current portfolio positions
- `int_cash_flow`: Monthly cash flow analysis

### Mart Models

- `fct_transactions`: Final transaction fact table
- `fct_investment_performance`: Investment performance metrics
- `dim_accounts`: Account dimension table
- `dim_merchants`: Merchant dimension table
- `dim_categories`: Transaction category hierarchy

## Sample Queries

### Monthly Spending by Category

```sql
SELECT
    DATE_TRUNC('month', date) as month,
    category_primary,
    SUM(CASE WHEN amount > 0 THEN amount ELSE 0 END) as total_spent,
    COUNT(*) as transaction_count
FROM transactions
WHERE amount > 0
    AND date >= CURRENT_DATE - INTERVAL '12 months'
GROUP BY 1, 2
ORDER BY 1 DESC, 3 DESC;
```

### Net Worth Calculation

```sql
WITH account_balances AS (
    SELECT
        SUM(CASE WHEN account_type IN ('depository', 'investment') THEN balance_current ELSE 0 END) as assets,
        SUM(CASE WHEN account_type IN ('credit', 'loan') THEN balance_current ELSE 0 END) as liabilities
    FROM accounts
    WHERE balance_current IS NOT NULL
)
SELECT
    assets,
    liabilities,
    assets - liabilities as net_worth
FROM account_balances;
```

### Investment Performance

```sql
SELECT
    s.ticker_symbol,
    s.name,
    h.quantity,
    h.cost_basis,
    h.institution_value,
    h.institution_value - h.cost_basis as unrealized_gain_loss,
    CASE
        WHEN h.cost_basis > 0 THEN (h.institution_value - h.cost_basis) / h.cost_basis * 100
        ELSE NULL
    END as return_percentage
FROM investment_holdings h
JOIN securities s ON h.security_id = s.security_id
WHERE h.quantity > 0
ORDER BY unrealized_gain_loss DESC;
```

This comprehensive data model provides the foundation for storing and analyzing all financial data extracted from Plaid API, ensuring data integrity, performance, and security while supporting advanced analytics and reporting capabilities.
