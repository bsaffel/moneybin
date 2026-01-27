-- Staging model for OFX financial institutions
--
-- Light transformations:
-- - Standardize column names
-- - No business logic (save for marts/facts)

select
    -- Institution identifiers
    organization as institution_name,
    fid as institution_fid,

    -- Metadata
    source_file,
    extracted_at,
    loaded_at

from {{ source('raw', 'ofx_institutions') }}
