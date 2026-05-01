MODEL (
  name prep.stg_ofx__balances,
  kind VIEW
);

SELECT
  account_id,
  statement_start_date::DATE AS statement_start_date,
  statement_end_date::DATE AS statement_end_date,
  ledger_balance,
  ledger_balance_date::DATE AS ledger_balance_date,
  available_balance,
  source_file,
  extracted_at,
  loaded_at,
  import_id,
  'ofx' AS source_type
FROM raw.ofx_balances
