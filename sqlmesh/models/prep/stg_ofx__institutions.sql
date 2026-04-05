MODEL (
  name prep.stg_ofx__institutions,
  kind VIEW
);

SELECT
  organization AS institution_name,
  fid AS institution_fid,
  source_file,
  extracted_at,
  loaded_at
FROM raw.ofx_institutions
