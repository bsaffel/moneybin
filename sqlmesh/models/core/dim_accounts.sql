/* Canonical accounts dimension; deduplicated accounts from all sources, keeping the most recently extracted record per account_id */
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
    'ofx' AS source_type,
    source_file,
    extracted_at,
    loaded_at
  FROM prep.stg_ofx__accounts
), tabular_accounts AS (
  SELECT
    account_id,
    routing_number,
    account_type,
    institution_name,
    institution_fid,
    source_type,
    source_file,
    extracted_at,
    loaded_at
  FROM prep.stg_tabular__accounts
), all_accounts AS (
  SELECT
    *
  FROM ofx_accounts
  UNION ALL
  SELECT
    *
  FROM tabular_accounts
), deduplicated AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY extracted_at DESC) AS _row_num
  FROM all_accounts
)
SELECT
  account_id, /* Unique account identifier; stable across imports; foreign key in fct_transactions */
  routing_number, /* ABA bank routing number; NULL when not provided by source */
  account_type, /* Account classification from source, e.g. CHECKING, SAVINGS, CREDITLINE */
  institution_name, /* Human-readable name of the financial institution */
  institution_fid, /* OFX financial institution identifier; NULL for tabular sources */
  source_type, /* Origin of the winning record after deduplication: ofx, csv, tsv, excel, etc. */
  source_file, /* Path to the source file from which this record was loaded */
  extracted_at, /* When the data was parsed from the source file */
  loaded_at, /* When the record was written to the raw table */
  CURRENT_TIMESTAMP AS updated_at /* When this core record was last refreshed by SQLMesh */
FROM deduplicated
WHERE
  _row_num = 1
