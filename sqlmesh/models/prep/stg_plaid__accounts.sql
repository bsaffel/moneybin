MODEL (
  name prep.stg_plaid__accounts,
  kind VIEW
);

SELECT
  COALESCE(links.account_id, a.account_id) AS account_id, /* canonical via the import-time resolver link; source-native only if unresolved */
  a.account_id AS source_account_key,
  NULL::TEXT AS routing_number,
  a.account_type,
  a.institution_name,
  NULL::TEXT AS institution_fid,
  a.official_name,
  a.mask,
  a.account_subtype,
  a.source_file,
  a.source_type,
  a.source_origin,
  a.extracted_at,
  a.loaded_at
FROM raw.plaid_accounts AS a
LEFT JOIN app.account_links AS links
  ON links.status = 'accepted'
  AND links.ref_kind = 'source_native'
  AND links.source_type = a.source_type
  AND links.source_origin = a.source_origin
  AND links.ref_value = a.account_id
