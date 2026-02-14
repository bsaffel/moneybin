-- Canonical Accounts Dimension Table
--
-- This dimension table consolidates financial accounts from all data sources
-- into a single, deduplicated table. It serves as the "gold" / core layer
-- and the single source of truth for account information.
--
-- Data Sources:
-- - OFX/QFX file imports (current)
-- - Plaid API (future)
-- - Manual entries (future)
--
-- Deduplication:
-- Accounts appearing across multiple imports or sources are deduplicated
-- by account_id, keeping the most recently extracted record per source.
-- Cross-source deduplication (e.g. same account from OFX and Plaid) will
-- be handled via account mapping when multiple sources are available.

{{ config(materialized='table', tags=['core', 'accounts']) }}

with ofx_accounts as (

    select
        -- Identifiers
        account_id,
        routing_number,

        -- Classification
        account_type,

        -- Institution
        institution_org as institution_name,
        institution_fid,

        -- Source tracking
        'ofx' as source_system,
        source_file,
        extracted_at,
        loaded_at

    from {{ ref('stg_ofx__accounts') }}

),

-- Future: Plaid accounts
-- plaid_accounts as (
--     select
--         account_id,
--         null as routing_number,
--         type || '.' || subtype as account_type,
--         institution_name,
--         institution_id as institution_fid,
--         'plaid' as source_system,
--         null as source_file,
--         extracted_at,
--         loaded_at
--     from {{ ref('stg_plaid__accounts') }}
-- ),

all_accounts as (

    select * from ofx_accounts
    -- UNION ALL select * from plaid_accounts

),

deduplicated as (

    select
        *,
        row_number() over (
            partition by account_id
            order by extracted_at desc
        ) as _row_num

    from all_accounts

)

select
    -- Identifiers
    account_id,
    routing_number,

    -- Classification
    account_type,

    -- Institution
    institution_name,
    institution_fid,

    -- Source tracking
    source_system,
    source_file,
    extracted_at,
    loaded_at,

    -- dbt metadata
    current_timestamp as dbt_updated_at

from deduplicated
where _row_num = 1
