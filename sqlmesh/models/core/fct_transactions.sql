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
  *
FROM standardized
