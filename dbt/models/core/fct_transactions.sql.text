-- Unified Transactions Fact Table
--
-- This fact table consolidates financial transactions from all data sources
-- into a single, standardized format for downstream analysis and reporting.
--
-- Data Sources:
-- - Plaid API (current)
-- - Future sources: manual CSV uploads, additional bank APIs, etc.
--
-- Design Decisions:
-- - Source system tracking for multi-source support
-- - Standardized data types (DATE, DECIMAL)
-- - Normalized transaction amounts (positive = income, negative = expense)
-- - Pending transaction handling
-- - Future-proof schema for extensibility

-- dbt configuration
{{ config(materialized='table', tags=['core', 'transactions']) }}

with plaid_transactions as (

    select
        -- Primary identifiers
        transaction_id,
        account_id,

        -- Transaction details
        cast(date as DATE) as transaction_date,
        cast(authorized_date as DATE) as authorized_date,
        cast(amount as DECIMAL(18, 2)) as amount,
        name as description,
        merchant_name,

        -- Transaction classification
        category_primary as category,
        category_detailed as subcategory,
        payment_channel,
        transaction_type,

        -- Transaction status
        pending as is_pending,
        pending_transaction_id,

        -- Location information
        location_address,
        location_city,
        location_region,
        location_postal_code,
        location_country,
        location_lat as location_latitude,
        location_lon as location_longitude,

        -- Currency information
        coalesce(iso_currency_code, 'USD') as currency_code,

        -- Metadata
        'plaid' as source_system,
        cast(extracted_at as TIMESTAMP) as source_extracted_at,
        current_timestamp as dbt_loaded_at

    from {{ source('raw', 'raw_plaid_transactions') }}

),

standardized_transactions as (

    select
        -- Primary identifiers
        transaction_id,
        account_id,

        -- Transaction timing
        transaction_date,
        authorized_date,

        -- Transaction amount (standardized)
        -- Plaid: positive = expense, negative = income
        -- Standardize to: negative = expense, positive = income
        -amount as amount,
        abs(amount) as amount_absolute,
        case
            when amount > 0 then 'expense'
            when amount < 0 then 'income'
            else 'zero'
        end as transaction_direction,

        -- Transaction description
        description,
        merchant_name,

        -- Classification
        category,
        subcategory,
        payment_channel,
        transaction_type,

        -- Transaction status
        is_pending,
        pending_transaction_id,

        -- Location
        location_address,
        location_city,
        location_region,
        location_postal_code,
        location_country,
        location_latitude,
        location_longitude,

        -- Currency
        currency_code,

        -- Source tracking
        source_system,
        source_extracted_at,
        dbt_loaded_at,

        -- Derived time dimensions
        date_part('year', transaction_date) as transaction_year,
        date_part('month', transaction_date) as transaction_month,
        date_part('day', transaction_date) as transaction_day,
        date_part('dayofweek', transaction_date)
            as transaction_day_of_week,
        strftime(transaction_date, '%Y-%m')
            as transaction_year_month,
        strftime(transaction_date, '%Y-Q%q')
            as transaction_year_quarter

    from plaid_transactions

)

select * from standardized_transactions
