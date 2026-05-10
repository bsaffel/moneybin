/* Uncategorized transactions ranked by curator-impact (large + old first).
   Consumers re-rank as needed; the priority_score column is exposed for
   default-sort convenience. */
MODEL (
  name reports.uncategorized_queue,
  kind VIEW
);

SELECT
  t.transaction_id, /* Joinable to core.fct_transactions */
  t.account_id, /* Owning account */
  a.display_name AS account_name, /* Account display name */
  t.transaction_date AS txn_date, /* Transaction date */
  t.amount, /* Signed amount */
  t.description, /* Original description */
  t.merchant_name AS merchant_normalized, /* Normalized merchant string */
  CAST(current_date - t.transaction_date AS INTEGER) AS age_days, /* Days since txn_date */
  ABS(t.amount) * CAST(current_date - t.transaction_date AS INTEGER) AS priority_score, /* Default sort key: amount * age */
  t.source_type, /* Source system that contributed this transaction */
  CAST(NULL AS VARCHAR) AS source_id /* Provenance reference within source; placeholder pending source_id surfacing on fct_transactions */
FROM core.fct_transactions AS t
INNER JOIN core.dim_accounts AS a ON t.account_id = a.account_id
WHERE t.category IS NULL
  AND NOT t.is_transfer
