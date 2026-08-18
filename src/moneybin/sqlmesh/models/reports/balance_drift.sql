/* Per-(account, assertion_date) reconciliation deltas: asserted vs computed
   balance. Feeds moneybin doctor (next spec). Threshold defaults are
   intentional v1 hardcodes; future iterations may move them to settings. They
   are absolute amounts in each row's own currency, never converted. */
MODEL (
  name reports.balance_drift,
  kind VIEW
);

WITH positions AS (
  /* currency_mismatch: an asserted balance carries no currency of its own, so
     its unit is the account's; the computed side comes from the observation,
     which states its own (core.fct_balances prefers it over the account's).
     Those diverge on an ordinary path — an observation states EUR, `system
     doctor` flags the account unknown, and the user runs the `accounts set
     --currency` fix it recommends with a different code. Subtracting them then
     yields a number in no unit at all, labelled with whichever one won. Only a
     mismatch between two *known* currencies is withheld: when either side is
     unknown there is nothing to contradict, and withholding would blank the
     report for every account whose currency nobody has assigned yet. */
  SELECT
    ba.account_id,
    a.display_name AS account_name,
    a.currency_code,
    ba.assertion_date,
    ba.balance AS asserted_balance,
    NOT fbd.currency_code IS NULL
    AND NOT a.currency_code IS NULL
    AND fbd.currency_code <> a.currency_code AS currency_mismatch,
    CASE
      WHEN NOT fbd.currency_code IS NULL
      AND NOT a.currency_code IS NULL
      AND fbd.currency_code <> a.currency_code
      THEN NULL
      WHEN NOT fbd.is_observed
      THEN fbd.balance
      WHEN NOT fbd.reconciliation_delta IS NULL
      THEN fbd.balance - fbd.reconciliation_delta
      ELSE NULL
    END AS computed_balance
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
    currency_code,
    currency_mismatch,
    assertion_date,
    asserted_balance,
    computed_balance,
    asserted_balance - computed_balance AS drift
  FROM positions
)
SELECT
  account_id, /* Joinable to core.dim_accounts */
  account_name, /* Account display name */
  currency_code, /* ISO 4217 currency the account is denominated in; both balances and the drift between them share it, so this row never blends currencies (multi-currency.md Requirement 5) */
  assertion_date, /* User-asserted balance date */
  asserted_balance, /* User-entered balance for this date */
  computed_balance, /* Interpolated daily balance or observed balance minus its adjustment; NULL for a missing row or first observation */
  drift, /* asserted_balance - computed_balance */
  ABS(drift) AS drift_abs, /* For default sort */
  CASE WHEN asserted_balance <> 0 THEN drift / asserted_balance ELSE NULL END AS drift_pct, /* drift / asserted_balance */
  CAST(CURRENT_DATE - assertion_date AS INT) AS days_since_assertion, /* today - assertion_date */
  CASE
    WHEN currency_mismatch
    THEN 'currency-mismatch'
    WHEN computed_balance IS NULL
    THEN 'no-data'
    WHEN ABS(drift) < 1.00
    THEN 'clean'
    WHEN ABS(drift) < 10.00
    THEN 'warning'
    ELSE 'drift'
  END AS status /* clean (<1) | warning (<10) | drift (>=10) | no-data (computed_balance NULL) | currency-mismatch (the account's currency and the observation's disagree, so no drift is computable). The clean/warning thresholds are absolute amounts in the row's own currency_code. A display-converted read re-buckets them against the converted drift, in `reports/definitions/balance_drift.py::_rebucket_status` — SQL cannot read that module's `_CLEAN_BELOW` / `_WARNING_BELOW`, so changing 1.00 or 10.00 here means changing them there in the same edit. */
FROM deltas
