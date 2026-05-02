MODEL (
  name prep.stg_ofx__transactions,
  kind VIEW
);

WITH ranked AS (
  SELECT
    t.source_transaction_id,
    t.account_id,
    t.transaction_type,
    t.date_posted::DATE AS posted_date,
    t.amount,
    TRIM(t.payee) AS payee,
    TRIM(t.memo) AS memo,
    t.check_number,
    t.source_file,
    t.extracted_at,
    t.loaded_at,
    t.import_id,
    'ofx' AS source_type,
    COALESCE(t.source_origin, a.institution_org, 'ofx_unknown') AS source_origin,
    ROW_NUMBER() OVER (PARTITION BY t.source_transaction_id, t.account_id ORDER BY t.loaded_at DESC) AS _row_num
  FROM raw.ofx_transactions AS t
  LEFT JOIN raw.ofx_accounts AS a
    ON t.account_id = a.account_id
)
SELECT
  source_transaction_id,
  account_id,
  transaction_type,
  posted_date,
  amount,
  payee,
  memo,
  check_number,
  source_file,
  extracted_at,
  loaded_at,
  import_id,
  source_type,
  source_origin
FROM ranked
WHERE
  _row_num = 1
