MODEL (
  name prep.stg_plaid__accounts,
  kind VIEW
);

SELECT
  account_id,
  NULL::VARCHAR AS routing_number,
  account_type,
  institution_name,
  NULL::VARCHAR AS institution_fid,
  official_name,
  mask,
  account_subtype,
  source_file,
  source_type,
  source_origin,
  extracted_at,
  loaded_at
FROM raw.plaid_accounts
