-- Canonical Transactions Fact Table
--
-- Consolidates financial transactions from all data sources into a single,
-- standardized format. Sign convention: negative = expense, positive = income.

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
        CAST(NULL AS DATE) AS authorized_date,
        CAST(amount AS DECIMAL(18, 2)) AS amount,
        payee AS description,
        CAST(NULL AS VARCHAR) AS merchant_name,
        memo,
        CAST(NULL AS VARCHAR) AS category,
        CAST(NULL AS VARCHAR) AS subcategory,
        CAST(NULL AS VARCHAR) AS payment_channel,
        transaction_type,
        check_number,
        FALSE AS is_pending,
        CAST(NULL AS VARCHAR) AS pending_transaction_id,
        CAST(NULL AS VARCHAR) AS location_address,
        CAST(NULL AS VARCHAR) AS location_city,
        CAST(NULL AS VARCHAR) AS location_region,
        CAST(NULL AS VARCHAR) AS location_postal_code,
        CAST(NULL AS VARCHAR) AS location_country,
        CAST(NULL AS DOUBLE) AS location_latitude,
        CAST(NULL AS DOUBLE) AS location_longitude,
        'USD' AS currency_code,
        'ofx' AS source_system,
        CAST(extracted_at AS TIMESTAMP) AS source_extracted_at,
        CURRENT_TIMESTAMP AS loaded_at
    FROM prep.stg_ofx__transactions
),

all_transactions AS (
    SELECT * FROM ofx_transactions
),

standardized AS (
    SELECT
        t.transaction_id,
        t.account_id,
        t.transaction_date,
        t.authorized_date,
        t.amount,
        ABS(t.amount) AS amount_absolute,
        CASE
            WHEN t.amount < 0 THEN 'expense'
            WHEN t.amount > 0 THEN 'income'
            ELSE 'zero'
        END AS transaction_direction,
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
        DATE_PART('dayofweek', t.transaction_date)
            AS transaction_day_of_week,
        STRFTIME(t.transaction_date, '%Y-%m')
            AS transaction_year_month,
        STRFTIME(t.transaction_date, '%Y') || '-Q'
            || QUARTER(t.transaction_date)
            AS transaction_year_quarter
    FROM all_transactions t
    LEFT JOIN app.transaction_categories c
        ON t.transaction_id = c.transaction_id
    LEFT JOIN app.merchants m
        ON c.merchant_id = m.merchant_id
)

SELECT * FROM standardized
