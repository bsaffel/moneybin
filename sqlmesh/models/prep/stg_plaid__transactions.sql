MODEL (
  name prep.stg_plaid__transactions,
  kind VIEW
);

SELECT
  transaction_id,
  account_id,
  transaction_date AS posted_date,
  -1 * amount AS amount, -- Flip Plaid (positive = expense) → MoneyBin (negative = expense)
  TRIM(description) AS description,
  TRIM(merchant_name) AS merchant_name,
  category AS plaid_category,
  pending AS is_pending,
  source_file,
  source_type,
  source_origin,
  extracted_at,
  loaded_at
FROM raw.plaid_transactions
