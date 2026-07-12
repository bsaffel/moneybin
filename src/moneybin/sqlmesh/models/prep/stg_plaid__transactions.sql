MODEL (
  name prep.stg_plaid__transactions,
  kind VIEW
);

SELECT
  COALESCE(links.account_id, t.account_id) AS account_id, /* canonical via the import-time resolver link; source-native only if unresolved */
  t.account_id AS source_account_key,
  t.transaction_id,
  t.transaction_date AS posted_date,
  -1 * t.amount AS amount, /* Flip Plaid (positive = expense) → MoneyBin (negative = expense) */
  TRIM(t.description) AS description,
  TRIM(t.merchant_name) AS merchant_name,
  t.category AS plaid_category,
  TRIM(t.original_description) AS original_description,
  t.iso_currency_code,
  t.authorized_date,
  t.pending_transaction_id,
  t.payment_channel,
  t.check_number,
  t.merchant_entity_id,
  TRIM(t.location_address) AS location_address,
  TRIM(t.location_city) AS location_city,
  TRIM(t.location_region) AS location_region,
  TRIM(t.location_postal_code) AS location_postal_code,
  TRIM(t.location_country) AS location_country,
  t.location_latitude,
  t.location_longitude,
  t.category_detailed,
  t.category_confidence,
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
