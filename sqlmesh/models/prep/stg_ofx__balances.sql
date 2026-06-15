MODEL (
  name prep.stg_ofx__balances,
  kind VIEW
);

SELECT
  COALESCE(links.account_id, b.account_id) AS account_id, /* canonical when linked, else source-native (transient until B7 backfill) */
  b.account_id AS source_account_key,
  b.statement_start_date::DATE AS statement_start_date,
  b.statement_end_date::DATE AS statement_end_date,
  b.ledger_balance,
  b.ledger_balance_date::DATE AS ledger_balance_date,
  b.available_balance,
  b.source_file,
  b.extracted_at,
  b.loaded_at,
  b.import_id,
  b.source_type,
  b.source_origin
FROM raw.ofx_balances AS b
LEFT JOIN app.account_links AS links
  ON links.status = 'accepted'
  AND links.ref_kind = 'source_native'
  AND links.source_type = b.source_type
  AND links.source_origin = b.source_origin
  AND links.ref_value = b.account_id
