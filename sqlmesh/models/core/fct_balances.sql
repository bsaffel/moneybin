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
    loaded_at AS updated_at
  FROM prep.stg_ofx__balances
), tabular_balances AS (
  SELECT
    account_id,
    transaction_date AS balance_date,
    balance,
    'tabular' AS source_type,
    source_file AS source_ref,
    loaded_at AS updated_at
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
    updated_at
  FROM app.balance_assertions
), plaid_balances AS (
  SELECT
    account_id,
    balance_date,
    current_balance AS balance,
    'plaid' AS source_type,
    source_origin AS source_ref,
    loaded_at AS updated_at
  FROM prep.stg_plaid__balances
)
SELECT
  account_id, /* Source-system account identifier */
  balance_date, /* Date the balance was observed */
  balance, /* Observed balance amount */
  source_type, /* Observation source: ofx, tabular, assertion, or plaid */
  source_ref, /* Source file path (ofx/tabular), 'user' for assertions, or the Plaid item id */
  updated_at /* Latest of all per-row input timestamps contributing to this row's current values. From the contributing observation's loaded_at (OFX/tabular) or created_at (user assertion). See docs/specs/core-updated-at-convention.md. */
FROM ofx_balances
UNION ALL
SELECT
  account_id,
  balance_date,
  balance,
  source_type,
  source_ref,
  updated_at
FROM tabular_balances
UNION ALL
SELECT
  account_id,
  balance_date,
  balance,
  source_type,
  source_ref,
  updated_at
FROM user_assertions
UNION ALL
SELECT
  account_id,
  balance_date,
  balance,
  source_type,
  source_ref,
  updated_at
FROM plaid_balances
