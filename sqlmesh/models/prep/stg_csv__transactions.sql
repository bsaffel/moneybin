MODEL (
  name prep.stg_csv__transactions,
  kind VIEW
);

WITH ranked AS (
  SELECT
    transaction_id,
    account_id,
    transaction_date,
    post_date,
    amount,
    TRIM(description) AS description,
    TRIM(memo) AS memo,
    category,
    subcategory,
    transaction_type,
    transaction_status,
    check_number,
    reference_number,
    balance,
    member_name,
    source_file,
    extracted_at,
    loaded_at,
    ROW_NUMBER() OVER (PARTITION BY transaction_id, account_id ORDER BY loaded_at DESC) AS _row_num
  FROM raw.csv_transactions
)
SELECT
  transaction_id,
  account_id,
  transaction_date,
  post_date,
  amount,
  description,
  memo,
  category,
  subcategory,
  transaction_type,
  transaction_status,
  check_number,
  reference_number,
  balance,
  member_name,
  source_file,
  extracted_at,
  loaded_at
FROM ranked
WHERE
  _row_num = 1
