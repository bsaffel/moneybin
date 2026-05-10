/* Anomaly-flavored view of all non-transfer transactions, with z-scores against
   account and category baselines. Consumers filter by their own definition of
   "large" — top-N by amount, |z|>2.5, etc. Z-scores use median + MAD (more
   outlier-robust than mean + stddev). */
MODEL (
  name reports.large_transactions,
  kind VIEW
);

WITH base AS (
  SELECT
    t.transaction_id,
    t.account_id,
    a.display_name AS account_name,
    t.transaction_date AS txn_date,
    t.amount,
    t.description,
    t.merchant_name AS merchant_normalized,
    t.category
  FROM core.fct_transactions AS t
  INNER JOIN core.dim_accounts AS a ON t.account_id = a.account_id
  WHERE NOT t.is_transfer
    AND NOT a.archived
), per_account_median AS (
  SELECT
    account_id,
    MEDIAN(ABS(amount)) AS median_abs
  FROM base
  GROUP BY account_id
), per_account AS (
  SELECT
    b.account_id,
    pam.median_abs,
    MEDIAN(ABS(ABS(b.amount) - pam.median_abs)) AS mad
  FROM base AS b
  INNER JOIN per_account_median AS pam USING (account_id)
  GROUP BY b.account_id, pam.median_abs
), per_category_median AS (
  SELECT
    category,
    MEDIAN(ABS(amount)) AS median_abs,
    COUNT(*) AS n
  FROM base
  GROUP BY category
), per_category AS (
  SELECT
    b.category,
    pcm.median_abs,
    pcm.n,
    MEDIAN(ABS(ABS(b.amount) - pcm.median_abs)) AS mad
  FROM base AS b
  INNER JOIN per_category_median AS pcm USING (category)
  GROUP BY b.category, pcm.median_abs, pcm.n
), top_n AS (
  SELECT transaction_id
  FROM base
  ORDER BY ABS(amount) DESC
  LIMIT 100
)
SELECT
  b.transaction_id, /* Joinable to core.fct_transactions */
  b.account_id, /* Owning account */
  b.account_name, /* Account display name */
  b.txn_date, /* Transaction date */
  b.amount, /* Signed amount */
  b.description, /* Original description */
  b.merchant_normalized, /* Normalized merchant string */
  b.category, /* Spending category text; NULL if uncategorized */
  CASE
    WHEN pa.mad > 0 THEN (ABS(b.amount) - pa.median_abs) / (1.4826 * pa.mad)
    ELSE NULL
  END AS amount_zscore_account, /* Modified z-score relative to account median + MAD */
  CASE
    WHEN pc.n >= 5 AND pc.mad > 0 THEN (ABS(b.amount) - pc.median_abs) / (1.4826 * pc.mad)
    ELSE NULL
  END AS amount_zscore_category, /* Modified z-score relative to category median + MAD; NULL if category < 5 txns */
  (b.transaction_id IN (SELECT transaction_id FROM top_n)) AS is_top_100 /* TRUE if in the top 100 by ABS(amount) overall */
FROM base AS b
LEFT JOIN per_account AS pa USING (account_id)
LEFT JOIN per_category AS pc USING (category)
