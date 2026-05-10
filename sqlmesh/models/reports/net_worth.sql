/* Cross-account daily aggregation of net worth.
   Excludes archived accounts and accounts with include_in_net_worth=FALSE.
   Reads from the resolved view in core.dim_accounts (per the canonical-dim
   rule in .claude/rules/database.md). */
MODEL (
  name reports.net_worth,
  kind VIEW
);

SELECT
  d.balance_date, /* Calendar date */
  SUM(d.balance) AS net_worth, /* Total balance across all included accounts */
  COUNT(DISTINCT d.account_id) AS account_count, /* Number of accounts contributing on this date */
  SUM(CASE WHEN d.balance > 0 THEN d.balance ELSE 0 END) AS total_assets, /* Sum of positive balances */
  SUM(CASE WHEN d.balance < 0 THEN d.balance ELSE 0 END) AS total_liabilities /* Sum of negative balances (kept negative) */
FROM core.fct_balances_daily AS d
INNER JOIN core.dim_accounts AS a ON d.account_id = a.account_id
WHERE a.include_in_net_worth AND NOT a.archived
GROUP BY d.balance_date
