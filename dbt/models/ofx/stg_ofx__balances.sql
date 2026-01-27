-- Staging model for OFX account balances
--
-- Light transformations:
-- - Standardize column names
-- - Convert timestamps to DATE type
-- - No business logic (save for marts/facts)

select
    -- Account identifier
    account_id,

    -- Statement period
    cast(statement_start_date as date) as statement_start_date,
    cast(statement_end_date as date) as statement_end_date,

    -- Balance details
    ledger_balance,
    cast(ledger_balance_date as date) as ledger_balance_date,
    available_balance,

    -- Metadata
    source_file,
    extracted_at,
    loaded_at

from {{ source('raw', 'ofx_balances') }}
