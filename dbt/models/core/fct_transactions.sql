-- Canonical Transactions Fact Table
--
-- This fact table consolidates financial transactions from all data sources
-- into a single, standardized format for downstream analysis and reporting.
-- It serves as the "gold" / core layer — the single source of truth for
-- transaction data.
--
-- Data Sources:
-- - OFX/QFX file imports (current)
-- - Plaid API (future — staged but not yet active)
-- - Manual CSV uploads (future)
--
-- Design Decisions:
-- - Source system tracking for multi-source provenance
-- - Standardized amount sign convention: negative = expense, positive = income
-- - Standardized data types (DATE, DECIMAL)
-- - Derived time dimensions for efficient analytical queries
-- - Future-proof schema with nullable source-specific columns

{{ config(materialized='table', tags=['core', 'transactions']) }}

with ofx_transactions as (

    select
        -- Primary identifiers
        transaction_id,
        account_id,

        -- Transaction timing
        posted_date as transaction_date,
        cast(null as date) as authorized_date,

        -- Transaction amount
        -- OFX convention: negative = debit/expense, positive = credit/income
        -- This matches our standardized convention, no sign flip needed
        cast(amount as decimal(18, 2)) as amount,

        -- Description
        payee as description,
        cast(null as varchar) as merchant_name,
        memo,

        -- Classification
        cast(null as varchar) as category,
        cast(null as varchar) as subcategory,
        cast(null as varchar) as payment_channel,
        transaction_type,
        check_number,

        -- Status
        false as is_pending,
        cast(null as varchar) as pending_transaction_id,

        -- Location (not available in OFX)
        cast(null as varchar) as location_address,
        cast(null as varchar) as location_city,
        cast(null as varchar) as location_region,
        cast(null as varchar) as location_postal_code,
        cast(null as varchar) as location_country,
        cast(null as double) as location_latitude,
        cast(null as double) as location_longitude,

        -- Currency
        'USD' as currency_code,

        -- Metadata
        'ofx' as source_system,
        cast(extracted_at as timestamp) as source_extracted_at,
        current_timestamp as dbt_loaded_at

    from {{ ref('stg_ofx__transactions') }}

),

-- Future: Plaid transactions
-- plaid_transactions as (
--     select
--         transaction_id,
--         account_id,
--         cast(date as date) as transaction_date,
--         cast(authorized_date as date) as authorized_date,
--         -- Plaid: positive = expense, negative = income → flip sign
--         cast(-amount as decimal(18, 2)) as amount,
--         name as description,
--         merchant_name,
--         cast(null as varchar) as memo,
--         category_primary as category,
--         category_detailed as subcategory,
--         payment_channel,
--         transaction_type,
--         cast(null as varchar) as check_number,
--         pending as is_pending,
--         pending_transaction_id,
--         location_address,
--         location_city,
--         location_region,
--         location_postal_code,
--         location_country,
--         location_lat as location_latitude,
--         location_lon as location_longitude,
--         coalesce(iso_currency_code, 'USD') as currency_code,
--         'plaid' as source_system,
--         cast(extracted_at as timestamp) as source_extracted_at,
--         current_timestamp as dbt_loaded_at
--     from {{ source('raw', 'raw_plaid_transactions') }}
-- ),

all_transactions as (

    select * from ofx_transactions
    -- UNION ALL select * from plaid_transactions

),

standardized as (

    select
        -- Primary identifiers
        transaction_id,
        account_id,

        -- Transaction timing
        transaction_date,
        authorized_date,

        -- Standardized amounts
        amount,
        abs(amount) as amount_absolute,
        case
            when amount < 0 then 'expense'
            when amount > 0 then 'income'
            else 'zero'
        end as transaction_direction,

        -- Description
        description,
        merchant_name,
        memo,

        -- Classification
        category,
        subcategory,
        payment_channel,
        transaction_type,
        check_number,

        -- Status
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

    from all_transactions

)

select * from standardized
