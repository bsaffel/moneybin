MODEL (
  name prep.stg_csv__accounts,
  kind VIEW
);

SELECT
  account_id,
  account_type,
  institution_name,
  NULL::TEXT AS routing_number,
  NULL::TEXT AS institution_fid,
  source_file,
  extracted_at,
  loaded_at
FROM raw.csv_accounts
