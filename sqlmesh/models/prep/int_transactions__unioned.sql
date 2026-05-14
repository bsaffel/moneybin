MODEL (
  name prep.int_transactions__unioned,
  kind VIEW
);

WITH ofx AS (
  SELECT
    source_transaction_id,
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
    source_type,
    source_origin,
    source_file,
    extracted_at::TIMESTAMP AS source_extracted_at,
    loaded_at
  FROM prep.stg_ofx__transactions
), manual AS (
  SELECT
    source_transaction_id,
    account_id,
    transaction_date,
    NULL::DATE AS authorized_date,
    amount::DECIMAL(18, 2) AS amount,
    description,
    merchant_name,
    memo,
    category,
    subcategory,
    payment_channel,
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
    COALESCE(currency_code, 'USD') AS currency_code,
    source_type,
    source_origin,
    NULL::TEXT AS source_file,
    created_at::TIMESTAMP AS source_extracted_at,
    created_at::TIMESTAMP AS loaded_at
  FROM prep.stg_manual__transactions
), tabular AS (
  SELECT
    transaction_id AS source_transaction_id,
    account_id,
    transaction_date,
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
    COALESCE(currency, 'USD') AS currency_code,
    source_type,
    source_origin,
    source_file,
    extracted_at::TIMESTAMP AS source_extracted_at,
    loaded_at
  FROM prep.stg_tabular__transactions
), plaid AS (
  SELECT
    transaction_id AS source_transaction_id,
    account_id,
    posted_date AS transaction_date,
    NULL::DATE AS authorized_date,
    amount::DECIMAL(18, 2) AS amount,
    description,
    merchant_name,
    NULL::TEXT AS memo,
    plaid_category AS category,
    NULL::TEXT AS subcategory,
    NULL::TEXT AS payment_channel,
    NULL::TEXT AS transaction_type,
    NULL::TEXT AS check_number,
    is_pending,
    NULL::TEXT AS pending_transaction_id,
    NULL::TEXT AS location_address,
    NULL::TEXT AS location_city,
    NULL::TEXT AS location_region,
    NULL::TEXT AS location_postal_code,
    NULL::TEXT AS location_country,
    NULL::DOUBLE AS location_latitude,
    NULL::DOUBLE AS location_longitude,
    'USD' AS currency_code,
    source_type,
    source_origin,
    source_file,
    extracted_at::TIMESTAMP AS source_extracted_at,
    loaded_at
  FROM prep.stg_plaid__transactions
)
SELECT
  *
FROM ofx
UNION ALL
SELECT
  *
FROM tabular
UNION ALL
SELECT
  *
FROM manual
UNION ALL
SELECT
  *
FROM plaid
