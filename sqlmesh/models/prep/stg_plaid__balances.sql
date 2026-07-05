MODEL (
  name prep.stg_plaid__balances,
  kind VIEW
);

SELECT
  COALESCE(links.account_id, b.account_id) AS account_id, /* canonical via the import-time resolver link; source-native only if unresolved */
  b.account_id AS source_account_key,
  b.balance_date,
  b.current_balance,
  b.available_balance,
  b.source_file,
  b.source_type,
  b.source_origin,
  b.extracted_at,
  b.loaded_at
FROM raw.plaid_balances AS b
LEFT JOIN app.account_links AS links
  ON links.status = 'accepted'
  AND links.ref_kind = 'source_native'
  AND links.source_type = b.source_type
  AND links.source_origin = b.source_origin
  AND links.ref_value = b.account_id
