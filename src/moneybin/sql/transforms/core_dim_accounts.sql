-- Inline transform: core.dim_accounts
-- Extracted from dbt/models/core/dim_accounts.sql
-- Replaces {{ ref() }} with direct raw table references
CREATE OR REPLACE TABLE core.dim_accounts AS (
    WITH ofx_accounts AS (
        SELECT
            account_id,
            routing_number,
            account_type,
            institution_org AS institution_name,
            institution_fid,
            'ofx' AS source_system,
            source_file,
            extracted_at,
            loaded_at
        FROM raw.ofx_accounts
    ),

    all_accounts AS (
        SELECT * FROM ofx_accounts
    ),

    deduplicated AS (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY account_id
                ORDER BY extracted_at DESC
            ) AS _row_num
        FROM all_accounts
    )

    SELECT
        account_id,
        routing_number,
        account_type,
        institution_name,
        institution_fid,
        source_system,
        source_file,
        extracted_at,
        loaded_at,
        CURRENT_TIMESTAMP AS dbt_updated_at
    FROM deduplicated
    WHERE _row_num = 1
);
