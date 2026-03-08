-- Canonical Transactions Fact Table
--
-- Consolidates financial transactions from all data sources into a single,
-- standardized format. Sign convention: negative = expense, positive = income.

MODEL (
    name core.fct_transactions,
    kind FULL,
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
        transaction_id,
        account_id,
        transaction_date,
        authorized_date,
        amount,
        ABS(amount) AS amount_absolute,
        CASE
            WHEN amount < 0 THEN 'expense'
            WHEN amount > 0 THEN 'income'
            ELSE 'zero'
        END AS transaction_direction,
        description,
        merchant_name,
        memo,
        category,
        subcategory,
        payment_channel,
        transaction_type,
        check_number,
        is_pending,
        pending_transaction_id,
        location_address,
        location_city,
        location_region,
        location_postal_code,
        location_country,
        location_latitude,
        location_longitude,
        currency_code,
        source_system,
        source_extracted_at,
        loaded_at,
        DATE_PART('year', transaction_date) AS transaction_year,
        DATE_PART('month', transaction_date) AS transaction_month,
        DATE_PART('day', transaction_date) AS transaction_day,
        DATE_PART('dayofweek', transaction_date) AS transaction_day_of_week,
        STRFTIME(transaction_date, '%Y-%m') AS transaction_year_month,
        STRFTIME(transaction_date, '%Y') || '-Q' || QUARTER(transaction_date) AS transaction_year_quarter
    FROM all_transactions
)

SELECT * FROM standardized
