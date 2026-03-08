-- Canonical Accounts Dimension Table
--
-- Consolidates financial accounts from all data sources into a single,
-- deduplicated table. Accounts appearing across multiple imports or sources
-- are deduplicated by account_id, keeping the most recently extracted record.

MODEL (
    name core.dim_accounts,
    kind FULL,
    grain account_id
);

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
    FROM prep.stg_ofx__accounts
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
    CURRENT_TIMESTAMP AS updated_at
FROM deduplicated
WHERE _row_num = 1
