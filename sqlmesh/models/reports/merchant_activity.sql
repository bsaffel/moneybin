/* Per-merchant lifetime aggregations. Subsumes top_merchants — top-N is
   ORDER BY total_spend DESC LIMIT N. Aggregates on merchant_id (FK to
   core.dim_merchants); transactions without a canonical merchant
   (merchant_id IS NULL) collapse into a single '(uncategorized)'
   bucket. */
MODEL (
  name reports.merchant_activity,
  kind VIEW
);

WITH normalized AS (
  SELECT
    t.merchant_id,
    CASE
      WHEN t.merchant_id IS NULL
      THEN '(uncategorized)'
      ELSE COALESCE(t.merchant_name, '(uncategorized)')
    END AS merchant_normalized,
    t.amount,
    t.category,
    t.transaction_date,
    t.account_id
  FROM core.fct_transactions AS t
  INNER JOIN core.dim_accounts AS a
    ON t.account_id = a.account_id
  WHERE
    NOT t.is_transfer AND NOT a.archived
)
SELECT
  merchant_id, /* Foreign key to core.dim_merchants.merchant_id; NULL for the '(uncategorized)' bucket aggregating transactions without a canonical merchant */
  merchant_normalized, /* Display label: dim_merchants.canonical_name for resolved merchants; '(uncategorized)' when merchant_id IS NULL */
  SUM(CASE WHEN amount < 0 THEN ABS(amount) ELSE 0 END) AS total_spend, /* Lifetime absolute outflow */
  SUM(CASE WHEN amount > 0 THEN amount ELSE 0 END) AS total_inflow, /* Lifetime sum of positive amounts */
  SUM(CASE WHEN amount < 0 THEN amount ELSE 0 END) AS total_outflow, /* Lifetime sum of negative amounts (kept negative) */
  COUNT(*) AS txn_count, /* Total transaction count */
  AVG(amount) AS avg_amount, /* Mean signed amount */
  MEDIAN(amount) AS median_amount, /* Median signed amount */
  MIN(transaction_date) AS first_seen, /* Earliest transaction */
  MAX(transaction_date) AS last_seen, /* Most recent transaction */
  COUNT(DISTINCT DATE_TRUNC('MONTH', transaction_date)) AS active_months, /* Distinct year-month count */
  MODE(
  ORDER BY
    category) AS top_category, /* Modal category text; NULL if all uncategorized */
  COUNT(DISTINCT account_id) AS account_count /* Distinct accounts on which this merchant appears */
FROM normalized
GROUP BY
  merchant_id,
  merchant_normalized
