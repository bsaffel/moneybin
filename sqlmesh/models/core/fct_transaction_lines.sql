/* Split-expanded transaction grain: one row per unsplit transaction, N rows per
   split transaction. Reads from core.fct_transactions (not app.transaction_splits
   directly), preserving the rule that consumers don't touch app.* — if it's not
   resolved in the fact, it didn't happen.
   VIEW kind despite fct_ prefix: derived state, not materialized. The underlying
   fact already incrementalizes; materializing here adds invalidation complexity
   without benefit. */
MODEL (
  name core.fct_transaction_lines,
  kind VIEW,
  grain (transaction_id, line_id)
);

SELECT
  t.transaction_id, /* Foreign key to core.fct_transactions */
  COALESCE(s.split_id, 'whole') AS line_id, /* 'whole' for unsplit transactions; split_id for split children */
  COALESCE(s.amount, t.amount) AS line_amount, /* Per-line amount; equals parent.amount for unsplit rows */
  COALESCE(s.category, t.category) AS line_category, /* Per-line category; falls through to parent for unsplit rows */
  COALESCE(s.subcategory, t.subcategory) AS line_subcategory, /* Per-line subcategory; falls through to parent for unsplit rows */
  s.note AS line_note, /* NULL on unsplit rows; per-split note when present */
  CASE WHEN s.split_id IS NULL THEN 'whole' ELSE 'split' END AS line_kind, /* 'whole' for unsplit transactions, 'split' for split children */
  t.account_id, /* Foreign key to core.dim_accounts */
  t.transaction_date, /* Date the transaction posted or settled */
  t.merchant_name, /* Normalized merchant name from the parent fact row */
  t.description, /* Payee or merchant description from the parent fact row */
  t.is_pending, /* TRUE if any contributing source row is pending */
  t.transfer_pair_id, /* FK to core.bridge_transfers.transfer_id; NULL if not a transfer */
  t.is_transfer, /* TRUE if this transaction is part of a confirmed transfer pair */
  t.source_type, /* Canonical source type from the parent fact row */
  t.source_count, /* Number of contributing source rows on the parent */
  t.transaction_year, /* Calendar year */
  t.transaction_month, /* Calendar month (1-12) */
  t.transaction_year_month, /* YYYY-MM period grouping */
  t.transaction_year_quarter /* YYYY-QN period grouping */
FROM core.fct_transactions AS t
LEFT JOIN UNNEST(t.splits) AS u (s)
  ON TRUE
WHERE NOT t.has_splits OR NOT s.split_id IS NULL
