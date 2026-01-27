-- Staging model for OFX transactions
--
-- Light transformations:
-- - Standardize column names
-- - Convert date_posted to DATE type
-- - No business logic (save for marts/facts)

select
    -- Transaction identifiers
    transaction_id,
    account_id,

    -- Transaction details
    transaction_type,
    cast(date_posted as date) as posted_date,
    amount,
    trim(payee) as payee,
    trim(memo) as memo,
    check_number,

    -- Metadata
    source_file,
    extracted_at,
    loaded_at

from {{ source('raw', 'ofx_transactions') }}
