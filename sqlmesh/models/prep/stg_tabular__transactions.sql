MODEL (
  name prep.stg_tabular__transactions,
  kind VIEW
);

WITH ranked AS (
  SELECT
    transaction_id,
    account_id,
    transaction_date,
    post_date,
    amount,
    original_amount,
    original_date_str,
    TRIM(description) AS description,
    TRIM(memo) AS memo,
    category,
    subcategory,
    transaction_type,
    status,
    check_number,
    source_transaction_id,
    reference_number,
    balance,
    currency,
    member_name,
    source_file,
    source_type,
    source_origin,
    import_id,
    row_number,
    extracted_at,
    loaded_at,
    ROW_NUMBER() OVER (PARTITION BY transaction_id, account_id ORDER BY loaded_at DESC) AS _row_num
  FROM raw.tabular_transactions
)
SELECT
  transaction_id, /* Deterministic ID: source-provided or SHA-256 hash */
  account_id, /* Source-system account identifier */
  transaction_date, /* Parsed date from source */
  post_date, /* Settlement date when available */
  amount, /* Normalized: negative = expense, positive = income */
  original_amount, /* Raw amount string for audit */
  original_date_str, /* Raw date string for audit */
  description, /* Trimmed transaction description */
  memo, /* Trimmed supplementary details */
  category, /* Source-provided category (for migration bootstrap) */
  subcategory, /* Source-provided subcategory */
  transaction_type, /* Source-provided type code */
  status, /* Source-provided status */
  check_number, /* Check number when applicable */
  source_transaction_id, /* Institution-assigned unique ID */
  reference_number, /* Institution reference number */
  balance, /* Running balance after this transaction */
  currency, /* ISO 4217 currency code */
  member_name, /* Account holder name */
  source_file, /* Path to source file */
  source_type, /* Import pathway: csv, tsv, excel, parquet, feather, pipe */
  source_origin, /* Institution/format that produced this data */
  import_id, /* UUID linking to import batch */
  row_number, /* 1-based source file row number */
  extracted_at, /* When data was parsed from source */
  loaded_at /* When record was loaded into database */
FROM ranked
WHERE
  _row_num = 1
