/* Monthly spending per category with MoM, YoY, and trailing-3mo comparisons.
   Outflow-only; consumers comparing income trends use reports.cash_flow.
   Trust core.fct_transactions.is_transfer (canonical signal). */
MODEL (
  name reports.spending_trend,
  kind VIEW
);

WITH monthly AS (
  SELECT
    date_trunc('month', t.transaction_date) AS year_month,
    t.category,
    SUM(ABS(t.amount)) AS total_spend,
    COUNT(*) AS txn_count
  FROM core.fct_transactions AS t
  INNER JOIN core.dim_accounts AS a ON t.account_id = a.account_id
  WHERE t.amount < 0
    AND NOT t.is_transfer
    AND NOT a.archived
  GROUP BY date_trunc('month', t.transaction_date), t.category
)
SELECT
  m.year_month, /* First-of-month */
  m.category, /* Spending category text; NULL for uncategorized */
  m.total_spend, /* Sum of absolute outflow this month in this category */
  m.txn_count, /* Outflow transaction count */
  LAG(m.total_spend, 1) OVER (PARTITION BY m.category ORDER BY m.year_month) AS prev_month_spend, /* Spend in the previous calendar month, same category */
  m.total_spend - LAG(m.total_spend, 1) OVER (PARTITION BY m.category ORDER BY m.year_month) AS mom_delta, /* total_spend - prev_month_spend */
  CASE
    WHEN LAG(m.total_spend, 1) OVER (PARTITION BY m.category ORDER BY m.year_month) > 0
      THEN (m.total_spend - LAG(m.total_spend, 1) OVER (PARTITION BY m.category ORDER BY m.year_month))
        / LAG(m.total_spend, 1) OVER (PARTITION BY m.category ORDER BY m.year_month)
    ELSE NULL
  END AS mom_pct, /* mom_delta / prev_month_spend; NULL when prev = 0 */
  LAG(m.total_spend, 12) OVER (PARTITION BY m.category ORDER BY m.year_month) AS prev_year_spend, /* Spend in the same calendar month one year prior */
  m.total_spend - LAG(m.total_spend, 12) OVER (PARTITION BY m.category ORDER BY m.year_month) AS yoy_delta, /* total_spend - prev_year_spend */
  CASE
    WHEN LAG(m.total_spend, 12) OVER (PARTITION BY m.category ORDER BY m.year_month) > 0
      THEN (m.total_spend - LAG(m.total_spend, 12) OVER (PARTITION BY m.category ORDER BY m.year_month))
        / LAG(m.total_spend, 12) OVER (PARTITION BY m.category ORDER BY m.year_month)
    ELSE NULL
  END AS yoy_pct, /* yoy_delta / prev_year_spend; NULL when prev_year = 0 */
  AVG(m.total_spend) OVER (
    PARTITION BY m.category ORDER BY m.year_month
    ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
  ) AS trailing_3mo_avg /* Rolling 3-month average ending this month, same category */
FROM monthly AS m
