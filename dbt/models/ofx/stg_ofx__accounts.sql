-- Staging model for OFX account details
--
-- Light transformations:
-- - Standardize column names
-- - No business logic (save for marts/facts)

select
    -- Account identifiers
    account_id,
    routing_number,
    account_type,

    -- Institution relationship
    institution_org,
    institution_fid,

    -- Metadata
    source_file,
    extracted_at,
    loaded_at

from {{ source('raw', 'ofx_accounts') }}
