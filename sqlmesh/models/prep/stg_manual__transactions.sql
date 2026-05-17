MODEL (
  name prep.stg_manual__transactions,
  kind VIEW
);

SELECT
  source_transaction_id,
  source_type,
  source_origin,
  import_id,
  account_id,
  transaction_date::DATE AS transaction_date,
  amount::DECIMAL(18, 2) AS amount,
  description,
  merchant_name,
  memo,
  category,
  subcategory,
  payment_channel,
  transaction_type,
  check_number,
  COALESCE(currency_code, 'USD') AS currency_code,
  created_at,
  created_by
FROM raw.manual_transactions
