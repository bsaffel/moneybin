MODEL (
  name prep.stg_tabular__accounts,
  kind VIEW
);

SELECT
  links.account_id,
  a.account_id AS source_account_key,
  a.account_name,
  a.account_number,
  a.account_number_masked,
  a.account_type,
  a.institution_name,
  a.currency,
  NULL::TEXT AS routing_number,
  NULL::TEXT AS institution_fid,
  a.source_file,
  a.source_type,
  a.source_origin,
  a.import_id,
  a.extracted_at,
  a.loaded_at
FROM raw.tabular_accounts AS a
LEFT JOIN app.account_links AS links
  ON links.status = 'accepted'
  AND links.ref_kind = 'source_native'
  AND links.source_type = a.source_type
  AND links.source_origin = a.source_origin
  AND links.ref_value = a.account_id
