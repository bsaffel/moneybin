MODEL (
  name prep.stg_ofx__accounts,
  kind VIEW
);

SELECT
  COALESCE(links.account_id, a.account_id) AS account_id, /* canonical via the import-time resolver link; source-native only if unresolved */
  a.account_id AS source_account_key,
  a.routing_number,
  m.account_type,
  COALESCE(m.account_subtype, LOWER(a.account_type)) AS account_subtype,
  a.institution_org,
  a.institution_fid,
  a.source_file,
  a.source_type,
  a.source_origin,
  a.extracted_at,
  a.loaded_at,
  a.import_id
FROM raw.ofx_accounts AS a
LEFT JOIN app.account_links AS links
  ON links.status = 'accepted'
  AND links.ref_kind = 'source_native'
  AND links.source_type = a.source_type
  AND links.source_origin = a.source_origin
  AND links.ref_value = a.account_id
LEFT JOIN seeds.account_type_map AS m
  ON m.alias = UPPER(a.account_type)
