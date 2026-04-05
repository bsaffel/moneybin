/* Canonical Transactions Fact Table */ /* Consolidates financial transactions from all data sources into a single, */ /* standardized format. Sign convention: negative = expense, positive = income. */
MODEL (
  name core.fct_transactions,
  kind VIEW,
  grain transaction_id
);

WITH ofx_transactions AS (
  SELECT
    transaction_id,
    account_id,
    posted_date AS transaction_date,
    NULL::DATE AS authorized_date,
    amount::DECIMAL(18, 2) AS amount,
    payee AS description,
    NULL::TEXT AS merchant_name,
    memo,
    NULL::TEXT AS category,
    NULL::TEXT AS subcategory,
    NULL::TEXT AS payment_channel,
    transaction_type,
    check_number,
    FALSE AS is_pending,
    NULL::TEXT AS pending_transaction_id,
    NULL::TEXT AS location_address,
    NULL::TEXT AS location_city,
    NULL::TEXT AS location_region,
    NULL::TEXT AS location_postal_code,
    NULL::TEXT AS location_country,
    NULL::DOUBLE AS location_latitude,
    NULL::DOUBLE AS location_longitude,
    'USD' AS currency_code,
    'ofx' AS source_system,
    extracted_at::TIMESTAMP AS source_extracted_at,
    CURRENT_TIMESTAMP AS loaded_at
  FROM prep.stg_ofx__transactions
), csv_transactions AS (
  SELECT
    transaction_id,
    account_id,
    transaction_date AS transaction_date,
    post_date AS authorized_date,
    amount::DECIMAL(18, 2) AS amount,
    description,
    NULL::TEXT AS merchant_name,
    memo,
    category,
    subcategory,
    NULL::TEXT AS payment_channel,
    transaction_type,
    check_number,
    FALSE AS is_pending,
    NULL::TEXT AS pending_transaction_id,
    NULL::TEXT AS location_address,
    NULL::TEXT AS location_city,
    NULL::TEXT AS location_region,
    NULL::TEXT AS location_postal_code,
    NULL::TEXT AS location_country,
    NULL::DOUBLE AS location_latitude,
    NULL::DOUBLE AS location_longitude,
    'USD' AS currency_code,
    'csv' AS source_system,
    extracted_at::TIMESTAMP AS source_extracted_at,
    CURRENT_TIMESTAMP AS loaded_at
  FROM prep.stg_csv__transactions
), all_transactions AS (
  SELECT
    *
  FROM ofx_transactions
  UNION ALL
  SELECT
    *
  FROM csv_transactions
), standardized AS (
  SELECT
    t.transaction_id,
    t.account_id,
    t.transaction_date,
    t.authorized_date,
    t.amount,
    ABS(t.amount) AS amount_absolute,
    CASE WHEN t.amount < 0 THEN 'expense' WHEN t.amount > 0 THEN 'income' ELSE 'zero' END AS transaction_direction,
    t.description,
    COALESCE(m.canonical_name, t.merchant_name) AS merchant_name,
    t.memo,
    COALESCE(c.category, t.category) AS category,
    COALESCE(c.subcategory, t.subcategory) AS subcategory,
    c.categorized_by,
    t.payment_channel,
    t.transaction_type,
    t.check_number,
    t.is_pending,
    t.pending_transaction_id,
    t.location_address,
    t.location_city,
    t.location_region,
    t.location_postal_code,
    t.location_country,
    t.location_latitude,
    t.location_longitude,
    t.currency_code,
    t.source_system,
    t.source_extracted_at,
    t.loaded_at,
    DATE_PART('year', t.transaction_date) AS transaction_year,
    DATE_PART('month', t.transaction_date) AS transaction_month,
    DATE_PART('day', t.transaction_date) AS transaction_day,
    DATE_PART('dayofweek', t.transaction_date) AS transaction_day_of_week,
    STRFTIME(t.transaction_date, '%Y-%m') AS transaction_year_month,
    STRFTIME(t.transaction_date, '%Y') || '-Q' || QUARTER(t.transaction_date) AS transaction_year_quarter
  FROM all_transactions AS t
  LEFT JOIN app.transaction_categories AS c
    ON t.transaction_id = c.transaction_id
  LEFT JOIN app.merchants AS m
    ON c.merchant_id = m.merchant_id
)
SELECT
  transaction_id, -- Unique transaction identifier; stable across re-imports from the same source
  account_id, -- Foreign key to core.dim_accounts
  transaction_date, -- Date the transaction posted or settled
  authorized_date, -- Date the transaction was authorized; NULL for OFX and CSV sources
  amount, -- Transaction amount; negative = expense, positive = income
  amount_absolute, -- Absolute value of amount; avoids sign handling in aggregations
  transaction_direction, -- Derived from amount sign: expense, income, or zero
  description, -- Raw payee or merchant description from the source
  merchant_name, -- Normalized merchant name from app.merchants; falls back to source value
  memo, -- Additional notes from the source, e.g. OFX memo field
  category, -- Spending category; from app.transaction_categories when categorized, else source value
  subcategory, -- Spending subcategory; from app.transaction_categories when categorized, else source value
  categorized_by, -- How the category was assigned: rule, ai, user, or NULL if uncategorized
  payment_channel, -- Payment channel (online, in store, other); NULL for OFX and CSV sources
  transaction_type, -- Source-specific transaction type code, e.g. DEBIT, CREDIT, CHECK
  check_number, -- Check number for check transactions; NULL otherwise
  is_pending, -- True if the transaction has not yet settled; always FALSE for OFX and CSV sources
  pending_transaction_id, -- ID of the pending transaction this record resolved; NULL for OFX and CSV sources
  location_address, -- Merchant street address; NULL for OFX and CSV sources
  location_city, -- Merchant city; NULL for OFX and CSV sources
  location_region, -- Merchant state or region; NULL for OFX and CSV sources
  location_postal_code, -- Merchant postal code; NULL for OFX and CSV sources
  location_country, -- Merchant country code; NULL for OFX and CSV sources
  location_latitude, -- Merchant latitude coordinate; NULL for OFX and CSV sources
  location_longitude, -- Merchant longitude coordinate; NULL for OFX and CSV sources
  currency_code, -- ISO 4217 currency code; hardcoded USD for OFX and CSV sources
  source_system, -- Origin of the record: ofx or csv
  source_extracted_at, -- When the data was parsed from the source file
  loaded_at, -- When this record was last written to the core view
  transaction_year, -- Calendar year extracted from transaction_date
  transaction_month, -- Calendar month (1-12) extracted from transaction_date
  transaction_day, -- Calendar day (1-31) extracted from transaction_date
  transaction_day_of_week, -- Day of week where 0 = Sunday and 6 = Saturday
  transaction_year_month, -- Year-month in YYYY-MM format for period grouping
  transaction_year_quarter -- Year-quarter in YYYY-QN format for period grouping
FROM standardized
