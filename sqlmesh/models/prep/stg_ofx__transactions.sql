MODEL (
  name prep.stg_ofx__transactions,
  kind VIEW
);

WITH ranked AS (
  SELECT
    t.source_transaction_id,
    t.account_id,
    t.transaction_type,
    t.date_posted::DATE AS posted_date,
    t.amount,
    TRIM(t.payee) AS payee,
    TRIM(t.memo) AS memo,
    t.check_number,
    t.source_file,
    t.extracted_at,
    t.loaded_at,
    t.import_id,
    'ofx' AS source_type,
    t.source_origin,
    ROW_NUMBER() OVER (PARTITION BY t.source_transaction_id, t.account_id ORDER BY t.loaded_at DESC) AS _row_num
  FROM raw.ofx_transactions AS t
)
SELECT
  COALESCE(links.account_id, ranked.account_id) AS account_id, /* canonical via the import-time resolver link; source-native only if unresolved */
  ranked.account_id AS source_account_key,
  ranked.source_transaction_id,
  ranked.transaction_type,
  ranked.posted_date,
  ranked.amount,
  ranked.payee,
  ranked.memo,
  ranked.check_number,
  ranked.source_file,
  ranked.extracted_at,
  ranked.loaded_at,
  ranked.import_id,
  ranked.source_type,
  ranked.source_origin
FROM ranked
LEFT JOIN app.account_links AS links
  ON links.status = 'accepted'
  AND links.ref_kind = 'source_native'
  AND links.source_type = ranked.source_type
  AND links.source_origin = ranked.source_origin
  AND links.ref_value = ranked.account_id
WHERE
  ranked._row_num = 1
