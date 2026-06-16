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
    deleted_from_source_at,
    ROW_NUMBER() OVER (PARTITION BY transaction_id, account_id ORDER BY loaded_at DESC) AS _row_num
  FROM raw.tabular_transactions
  /* Exclude soft-deleted rows BEFORE ranking: a soft-deleted row with a newer
     loaded_at would rank #1 and then be dropped by the outer filter, while a valid
     same-key row at #2 is also excluded — silently losing the transaction.
     Filtering pre-rank lets the valid row take #1. */
  WHERE
    deleted_from_source_at IS NULL
)
SELECT
  COALESCE(links.account_id, ranked.account_id) AS account_id, /* canonical via the import-time resolver link; source-native only if unresolved */
  ranked.account_id AS source_account_key,
  ranked.transaction_id,
  ranked.transaction_date,
  ranked.post_date,
  ranked.amount,
  ranked.original_amount,
  ranked.original_date_str,
  ranked.description,
  ranked.memo,
  ranked.category,
  ranked.subcategory,
  ranked.transaction_type,
  ranked.status,
  ranked.check_number,
  ranked.source_transaction_id,
  ranked.reference_number,
  ranked.balance,
  ranked.currency,
  ranked.member_name,
  ranked.source_file,
  ranked.source_type,
  ranked.source_origin,
  ranked.import_id,
  ranked.row_number,
  ranked.extracted_at,
  ranked.loaded_at
FROM ranked
LEFT JOIN app.account_links AS links
  ON links.status = 'accepted'
  AND links.ref_kind = 'source_native'
  AND links.source_type = ranked.source_type
  AND links.source_origin = ranked.source_origin
  AND links.ref_value = ranked.account_id
WHERE
  ranked._row_num = 1
