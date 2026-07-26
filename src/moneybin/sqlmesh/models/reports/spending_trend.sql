/* Monthly spending per category with MoM, YoY, and trailing-3mo comparisons.
   Outflow-only; consumers comparing income trends use reports.cash_flow.
   Trust core.fct_transactions.is_transfer (canonical signal). */
MODEL (
  name reports.spending_trend,
  kind VIEW
);

WITH monthly AS (
  SELECT
    DATE_TRUNC('MONTH', t.transaction_date)::DATE AS month_date,
    t.category,
    t.currency_code,
    SUM(ABS(t.amount)) AS total_spend,
    COUNT(*) AS txn_count
  FROM core.fct_transactions AS t
  INNER JOIN core.dim_accounts AS a
    ON t.account_id = a.account_id
  WHERE
    t.amount < 0 AND NOT t.is_transfer AND NOT a.archived
  GROUP BY
    DATE_TRUNC('MONTH', t.transaction_date),
    t.category,
    t.currency_code
), bounds AS (
  SELECT
    MIN(month_date) AS first_month,
    MAX(month_date) AS last_month
  FROM monthly
), categories AS (
  /* Observed (category, currency) pairs, not the cross product of the two:
     densifying every category against every currency would invent zero-spend
     (Rent, EUR) months for a user who only ever paid rent in dollars. */
  SELECT DISTINCT
    category,
    currency_code
  FROM monthly
), calendar AS (
  SELECT
    UNNEST(GENERATE_SERIES(first_month, last_month, INTERVAL '1' MONTH))::DATE AS month_date
  FROM bounds
), dense_monthly AS (
  SELECT
    calendar.month_date,
    categories.category,
    categories.currency_code,
    COALESCE(monthly.total_spend, 0::DECIMAL(18, 2)) AS total_spend,
    COALESCE(monthly.txn_count, 0) AS txn_count
  FROM calendar
  CROSS JOIN categories
  LEFT JOIN monthly
    ON calendar.month_date = monthly.month_date
    AND categories.category IS NOT DISTINCT FROM monthly.category
    AND categories.currency_code IS NOT DISTINCT FROM monthly.currency_code
), comparisons AS (
  SELECT
    month_date,
    category,
    currency_code,
    total_spend,
    txn_count,
    LAG(total_spend, 1) OVER (PARTITION BY category, currency_code ORDER BY month_date) AS prev_month_spend,
    LAG(total_spend, 12) OVER (PARTITION BY category, currency_code ORDER BY month_date) AS prev_year_spend,
    AVG(total_spend) OVER (
      PARTITION BY category, currency_code
      ORDER BY month_date
      ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ) AS trailing_3mo_avg
  FROM dense_monthly
)
SELECT
  STRFTIME(month_date, '%Y-%m') AS year_month, /* Calendar month as 'YYYY-MM'; every observed category-currency pair has one row per month in the eligible data window */
  category, /* Spending category text; NULL for uncategorized */
  currency_code, /* ISO 4217 currency this row's spend is denominated in; NULL is the unknown-currency segment, never resolved to the home currency (multi-currency.md Requirement 5) */
  total_spend, /* Sum of absolute outflow this month in this category; zero when the category has no outflow this month */
  txn_count, /* Outflow transaction count; zero when the category has no outflow this month */
  prev_month_spend, /* Spend in the immediately previous calendar month, same category and currency */
  total_spend - prev_month_spend AS mom_delta, /* total_spend - prev_month_spend */
  CASE
    WHEN prev_month_spend > 0
    THEN (
      total_spend - prev_month_spend
    ) / prev_month_spend
    ELSE NULL
  END AS mom_pct, /* mom_delta / prev_month_spend; NULL when prev = 0 */
  prev_year_spend, /* Spend in the same calendar month one year prior, same category and currency */
  total_spend - prev_year_spend AS yoy_delta, /* total_spend - prev_year_spend */
  CASE
    WHEN prev_year_spend > 0
    THEN (
      total_spend - prev_year_spend
    ) / prev_year_spend
    ELSE NULL
  END AS yoy_pct, /* yoy_delta / prev_year_spend; NULL when prev_year = 0 */
  trailing_3mo_avg /* Rolling average of up to three calendar months ending this month, same category and currency; missing months contribute zero */
FROM comparisons
