MODEL (
  name prep.stg_ofx__transactions,
  kind VIEW
);

SELECT
  transaction_id,
  account_id,
  transaction_type,
  date_posted::DATE AS posted_date,
  amount,
  TRIM(payee) AS payee,
  TRIM(memo) AS memo,
  check_number,
  source_file,
  extracted_at,
  loaded_at
FROM raw.ofx_transactions
