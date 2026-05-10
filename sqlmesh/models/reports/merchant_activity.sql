/* Per-merchant lifetime aggregations. Subsumes top_merchants — top-N is
   ORDER BY total_spend DESC LIMIT N. NULL merchants bucketed into a single
   '(unknown)' row to keep the view consumable. */
MODEL (
  name reports.merchant_activity,
  kind VIEW
);

WITH normalized AS (
  SELECT
    COALESCE(merchant_name, '(unknown)') AS merchant_normalized,
    amount,
    category,
    transaction_date,
    account_id
  FROM core.fct_transactions
  WHERE NOT is_transfer
)
SELECT
  merchant_normalized, /* Normalized merchant string; '(unknown)' when source merchant is NULL */
  SUM(CASE WHEN amount < 0 THEN ABS(amount) ELSE 0 END) AS total_spend, /* Lifetime absolute outflow */
  SUM(CASE WHEN amount > 0 THEN amount ELSE 0 END) AS total_inflow, /* Lifetime sum of positive amounts */
  SUM(CASE WHEN amount < 0 THEN amount ELSE 0 END) AS total_outflow, /* Lifetime sum of negative amounts (kept negative) */
  COUNT(*) AS txn_count, /* Total transaction count */
  AVG(amount) AS avg_amount, /* Mean signed amount */
  MEDIAN(amount) AS median_amount, /* Median signed amount */
  MIN(transaction_date) AS first_seen, /* Earliest transaction */
  MAX(transaction_date) AS last_seen, /* Most recent transaction */
  COUNT(DISTINCT date_trunc('month', transaction_date)) AS active_months, /* Distinct year-month count */
  MODE() WITHIN GROUP (ORDER BY category) AS top_category, /* Modal category text; NULL if all uncategorized */
  COUNT(DISTINCT account_id) AS account_count /* Distinct accounts on which this merchant appears */
FROM normalized
GROUP BY merchant_normalized
