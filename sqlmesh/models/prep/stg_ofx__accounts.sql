MODEL (
  name prep.stg_ofx__accounts,
  kind VIEW
);

SELECT
  account_id,
  routing_number,
  account_type,
  institution_org,
  institution_fid,
  source_file,
  extracted_at,
  loaded_at
FROM raw.ofx_accounts
