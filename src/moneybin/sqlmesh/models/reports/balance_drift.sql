/* Per-(account, assertion_date) reconciliation deltas: asserted vs computed
   balance. Feeds moneybin doctor (next spec). Threshold defaults are
   intentional v1 hardcodes; future iterations may move them to settings. */
MODEL (
  name reports.balance_drift,
  kind VIEW
);

WITH positions AS (
  SELECT
    ba.account_id,
    a.display_name AS account_name,
    ba.assertion_date,
    ba.balance AS asserted_balance,
    fbd.balance - fbd.reconciliation_delta AS computed_balance
  FROM app.balance_assertions AS ba
  INNER JOIN core.dim_accounts AS a
    ON ba.account_id = a.account_id
  LEFT JOIN core.fct_balances_daily AS fbd
    ON ba.account_id = fbd.account_id AND ba.assertion_date = fbd.balance_date
  WHERE
    NOT a.archived
), deltas AS (
  SELECT
    account_id,
    account_name,
    assertion_date,
    asserted_balance,
    computed_balance,
    asserted_balance - computed_balance AS drift
  FROM positions
)
SELECT
  account_id, /* Joinable to core.dim_accounts */
  account_name, /* Account display name */
  assertion_date, /* User-asserted balance date */
  asserted_balance, /* User-entered balance for this date */
  computed_balance, /* Independent transaction-derived position: daily winning balance minus reconciliation_delta; NULL without a prior anchor */
  drift, /* asserted_balance - computed_balance */
  ABS(drift) AS drift_abs, /* For default sort */
  CASE WHEN asserted_balance <> 0 THEN drift / asserted_balance ELSE NULL END AS drift_pct, /* drift / asserted_balance */
  CAST(CURRENT_DATE - assertion_date AS INT) AS days_since_assertion, /* today - assertion_date */
  CASE
    WHEN computed_balance IS NULL
    THEN 'no-data'
    WHEN ABS(drift) < 1.00
    THEN 'clean'
    WHEN ABS(drift) < 10.00
    THEN 'warning'
    ELSE 'drift'
  END AS status /* clean (<$1) | warning (<$10) | drift (>=$10) | no-data (computed_balance NULL) */
FROM deltas
