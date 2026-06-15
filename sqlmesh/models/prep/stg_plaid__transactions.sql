MODEL (
  name prep.stg_plaid__transactions,
  kind VIEW
);

SELECT
  COALESCE(links.account_id, t.account_id) AS account_id, /* canonical when linked, else source-native (transient until B7 backfill) */
  t.account_id AS source_account_key,
  t.transaction_id,
  t.transaction_date AS posted_date,
  -1 * t.amount AS amount, /* Flip Plaid (positive = expense) → MoneyBin (negative = expense) */
  TRIM(t.description) AS description,
  TRIM(t.merchant_name) AS merchant_name,
  t.category AS plaid_category,
  t.pending AS is_pending,
  t.source_file,
  t.source_type,
  t.source_origin,
  t.extracted_at,
  t.loaded_at
FROM raw.plaid_transactions AS t
LEFT JOIN app.account_links AS links
  ON links.status = 'accepted'
  AND links.ref_kind = 'source_native'
  AND links.source_type = t.source_type
  AND links.source_origin = t.source_origin
  AND links.ref_value = t.account_id
