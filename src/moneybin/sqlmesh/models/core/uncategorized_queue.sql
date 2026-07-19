/* Uncategorized transactions ranked by curator-impact (large + old first).
   Consumers re-rank as needed; the priority_score column is exposed for
   default-sort convenience. */
MODEL (
  name core.uncategorized_queue,
  kind VIEW
);

SELECT
  t.transaction_id, /* Joinable to core.fct_transactions */
  t.account_id, /* Owning account */
  a.display_name AS account_name, /* Account display name */
  t.transaction_date AS txn_date, /* Transaction date */
  t.amount, /* Signed amount */
  t.description, /* Original description */
  t.merchant_id, /* Foreign key to core.dim_merchants.merchant_id; NULL when no canonical merchant was resolved */
  t.merchant_name AS merchant_normalized, /* Normalized merchant string (display) */
  CAST(CURRENT_DATE - t.transaction_date AS INT) AS age_days, /* Days since txn_date */
  ABS(t.amount) * CAST(CURRENT_DATE - t.transaction_date AS INT) AS priority_score, /* Default sort key: amount * age */
  t.source_type, /* Source system that contributed this transaction */
  NULL::TEXT AS source_id /* Provenance reference within source; placeholder pending source_id surfacing on fct_transactions */
FROM core.fct_transactions AS t
INNER JOIN core.dim_accounts AS a
  ON t.account_id = a.account_id
WHERE
  t.category IS NULL AND NOT t.is_transfer AND NOT a.archived
