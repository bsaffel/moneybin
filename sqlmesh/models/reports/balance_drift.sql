/* Per-(account, assertion_date) reconciliation deltas: asserted vs computed
   balance. Feeds moneybin doctor (next spec). Threshold defaults are
   intentional v1 hardcodes; future iterations may move them to settings. */
MODEL (
  name reports.balance_drift,
  kind VIEW
);

SELECT
  ba.account_id, /* Joinable to core.dim_accounts */
  a.display_name AS account_name, /* Account display name */
  ba.assertion_date, /* User-asserted balance date */
  ba.balance AS asserted_balance, /* User-entered balance for this date */
  fbd.balance AS computed_balance, /* Carried-forward balance from core.fct_balances_daily; NULL if missing */
  ba.balance - fbd.balance AS drift, /* asserted_balance - computed_balance */
  ABS(ba.balance - fbd.balance) AS drift_abs, /* For default sort */
  CASE
    WHEN ba.balance != 0 THEN (ba.balance - fbd.balance) / ba.balance
    ELSE NULL
  END AS drift_pct, /* drift / asserted_balance */
  CAST(current_date - ba.assertion_date AS INTEGER) AS days_since_assertion, /* today - assertion_date */
  CASE
    WHEN fbd.balance IS NULL THEN 'no-data'
    WHEN ABS(ba.balance - fbd.balance) < 1.00 THEN 'clean'
    WHEN ABS(ba.balance - fbd.balance) < 10.00 THEN 'warning'
    ELSE 'drift'
  END AS status /* clean (<$1) | warning (<$10) | drift (>=$10) | no-data (computed_balance NULL) */
FROM app.balance_assertions AS ba
INNER JOIN core.dim_accounts AS a ON ba.account_id = a.account_id
LEFT JOIN core.fct_balances_daily AS fbd
  ON ba.account_id = fbd.account_id
  AND ba.assertion_date = fbd.balance_date
