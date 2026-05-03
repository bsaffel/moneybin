/* Union of all balance observation sources: OFX statement balances,
   tabular running balances, and user-entered assertions.
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
    source_file AS source_ref
  FROM prep.stg_ofx__balances
), tabular_balances AS (
  SELECT
    account_id,
    transaction_date AS balance_date,
    balance,
    'tabular' AS source_type,
    source_file AS source_ref
  FROM prep.stg_tabular__transactions
  WHERE balance IS NOT NULL
), user_assertions AS (
  SELECT
    account_id,
    assertion_date AS balance_date,
    balance,
    'assertion' AS source_type,
    'user' AS source_ref
  FROM app.balance_assertions
)
SELECT
  account_id, /* Source-system account identifier */
  balance_date, /* Date the balance was observed */
  balance, /* Observed balance amount */
  source_type, /* Observation source: ofx, tabular, or assertion */
  source_ref /* Source file path or 'user' for assertions */
FROM ofx_balances
UNION ALL
SELECT account_id, balance_date, balance, source_type, source_ref FROM tabular_balances
UNION ALL
SELECT account_id, balance_date, balance, source_type, source_ref FROM user_assertions
