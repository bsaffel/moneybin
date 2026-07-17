/* Union of all balance observation sources: OFX statement balances,
   tabular running balances, user-entered assertions, and Plaid sync snapshots.
   One row per observation. Consumed by core.fct_balances_daily for
   carry-forward + reconciliation. */
MODEL (
  name core.fct_balances,
  kind VIEW
);

WITH ofx_balances AS (
  SELECT
    account_id,
    ledger_balance_date AS balance_date,
    ledger_balance AS balance,
    'ofx' AS source_type,
    source_file AS source_ref,
    loaded_at AS updated_at,
    currency_code
  FROM prep.stg_ofx__balances
), tabular_balances AS (
  SELECT
    account_id,
    transaction_date AS balance_date,
    balance,
    'tabular' AS source_type,
    source_file AS source_ref,
    loaded_at AS updated_at,
    currency AS currency_code
  FROM prep.stg_tabular__transactions
  WHERE
    NOT balance IS NULL
), user_assertions AS (
  SELECT
    account_id,
    assertion_date AS balance_date,
    balance,
    'assertion' AS source_type,
    'user' AS source_ref,
    updated_at,
    NULL::TEXT AS currency_code
  FROM app.balance_assertions
), plaid_balances AS (
  /* Plaid reports credit/loan balances as a positive amount owed, core.fct_balances
     and reports.net_worth treat liabilities as negative (net_worth sums balances,
     positive = asset, negative = liability), so negate them via the account type.
     Rows with no current_balance are dropped, not anchored at 0 by
     fct_balances_daily — available_balance has different semantics per account
     type (spendable vs. credit headroom) and is not a safe fallback. A row whose
     account_type is unresolvable (NULL from Plaid, or an unmatched join) is also
     dropped: without the type we can't sign it, and defaulting to the positive
     ELSE branch would silently book a liability as an asset. */
  SELECT
    b.account_id,
    b.balance_date,
    CASE
      WHEN a.account_type IN ('credit', 'loan')
      THEN -1 * b.current_balance
      ELSE b.current_balance
    END AS balance,
    'plaid' AS source_type,
    b.source_origin AS source_ref,
    b.loaded_at AS updated_at,
    COALESCE(b.iso_currency_code, b.unofficial_currency_code) AS currency_code
  FROM prep.stg_plaid__balances AS b
  LEFT JOIN prep.stg_plaid__accounts AS a
    ON a.source_account_key = b.source_account_key AND a.source_origin = b.source_origin
  WHERE
    NOT b.current_balance IS NULL AND NOT a.account_type IS NULL
), unioned AS (
  SELECT
    *
  FROM ofx_balances
  UNION ALL
  SELECT
    *
  FROM tabular_balances
  UNION ALL
  SELECT
    *
  FROM user_assertions
  UNION ALL
  SELECT
    *
  FROM plaid_balances
)
SELECT
  u.account_id, /* Source-system account identifier */
  u.balance_date, /* Date the balance was observed */
  u.balance, /* Observed balance amount */
  u.source_type, /* Observation source: ofx, tabular, assertion, or plaid */
  u.source_ref, /* Source file path (ofx/tabular), 'user' for assertions, or the Plaid item id */
  u.updated_at, /* Latest of all per-row input timestamps contributing to this row's current values. From the contributing observation's loaded_at (OFX/tabular) or created_at (user assertion). See docs/specs/core-updated-at-convention.md. */
  COALESCE(u.currency_code, a.currency_code) AS currency_code /* the observation's own captured currency, else inherited from core.dim_accounts.currency_code (multi-currency.md Requirement 3) */
FROM unioned AS u
LEFT JOIN core.dim_accounts AS a
  ON u.account_id = a.account_id
