MODEL (
  name prep.stg_manual__transactions,
  kind VIEW
);

SELECT
  COALESCE(links.account_id, t.account_id) AS account_id, /* canonical via the import-time resolver link; source-native only if unresolved */
  t.account_id AS source_account_key,
  t.source_transaction_id,
  t.source_type,
  t.source_origin,
  t.import_id,
  t.transaction_date::DATE AS transaction_date,
  t.amount::DECIMAL(18, 2) AS amount,
  t.description,
  t.merchant_name,
  t.memo,
  t.category,
  t.subcategory,
  t.payment_channel,
  t.transaction_type,
  t.check_number,
  COALESCE(t.currency_code, 'USD') AS currency_code,
  t.created_at,
  t.created_by
FROM raw.manual_transactions AS t
LEFT JOIN app.account_links AS links
  ON links.status = 'accepted'
  AND links.ref_kind = 'source_native'
  AND links.source_type = t.source_type
  AND links.source_origin = t.source_origin
  AND links.ref_value = t.account_id
