MODEL (
  name prep.stg_tabular__accounts,
  kind VIEW
);

SELECT
  COALESCE(links.account_id, a.account_id) AS account_id, /* canonical via the import-time resolver link; source-native only if unresolved */
  a.account_id AS source_account_key,
  a.account_name,
  a.account_number,
  a.account_number_masked,
  m.account_type,
  CASE
    WHEN NOT m.alias IS NULL
    THEN m.account_subtype
    ELSE LOWER(NULLIF(TRIM(a.account_type), ''))
  END AS account_subtype,
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
LEFT JOIN seeds.account_type_map AS m
  ON m.alias = UPPER(NULLIF(TRIM(a.account_type), ''))
