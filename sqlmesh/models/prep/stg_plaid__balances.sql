MODEL (
  name prep.stg_plaid__balances,
  kind VIEW
);

SELECT
  account_id,
  balance_date,
  current_balance,
  available_balance,
  source_file,
  source_type,
  source_origin,
  extracted_at,
  loaded_at
FROM raw.plaid_balances
