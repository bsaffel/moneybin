MODEL (
  name prep.stg_manual__transactions,
  kind VIEW
);

/* category and subcategory are wrapped in NULLIF(TRIM(...), '') for the reason
   stg_plaid__accounts states for its own free-text columns: '' passes a NULL
   check while rendering as a malformed label. Here the NULL check that matters
   is core.uncategorized_queue's `category IS NULL` — a category of spaces hides
   a transaction nobody ever categorized from the queue built to surface it. */
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
  NULLIF(TRIM(t.category, ' ' || CHR(9) || CHR(10) || CHR(13)), '') AS category,
  NULLIF(TRIM(t.subcategory, ' ' || CHR(9) || CHR(10) || CHR(13)), '') AS subcategory,
  t.payment_channel,
  t.transaction_type,
  t.check_number,
  t.currency_code,
  t.created_at,
  t.created_by
FROM raw.manual_transactions AS t
LEFT JOIN app.account_links AS links
  ON links.status = 'accepted'
  AND links.ref_kind = 'source_native'
  AND links.source_type = t.source_type
  AND links.source_origin = t.source_origin
  AND links.ref_value = t.account_id
