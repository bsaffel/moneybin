/* Cross-account daily aggregation of net worth.
   Excludes archived accounts and accounts with include_in_net_worth=FALSE.
   Reads from the resolved view in core.dim_accounts (per the canonical-dim
   rule in .claude/rules/database.md). */
MODEL (
  name reports.net_worth,
  kind VIEW
);

SELECT
  d.currency_code, /* ISO 4217 currency this row's totals are denominated in; NULL is the unknown-currency segment, never resolved to the home currency (multi-currency.md Requirement 5). Rows sharing NULL pool into one segment and are summed: unknown is one bucket, not one bucket per real currency, so two accounts in genuinely different currencies that both lack one are added together. That is why an unknown currency is a `system doctor` FAILURE rather than a warning — the remedy is `accounts set --currency`, not a total MoneyBin could compute. Splitting the bucket is impossible by construction: nothing distinguishes two unknowns. Every other money-summing reports.* model pools the same way. */
  d.balance_date, /* Calendar date */
  COUNT(DISTINCT d.account_id) AS account_count, /* Number of accounts contributing on this date in this currency */
  SUM(CASE WHEN d.balance > 0 THEN d.balance ELSE 0 END) AS total_assets, /* Sum of positive balances */
  SUM(CASE WHEN d.balance < 0 THEN d.balance ELSE 0 END) AS total_liabilities, /* Sum of negative balances (kept negative) */
  SUM(d.balance) AS net_worth /* Total balance across included accounts denominated in currency_code */
FROM core.fct_balances_daily AS d
INNER JOIN core.dim_accounts AS a
  ON d.account_id = a.account_id
WHERE
  a.include_in_net_worth AND NOT a.archived
GROUP BY
  d.balance_date,
  d.currency_code
