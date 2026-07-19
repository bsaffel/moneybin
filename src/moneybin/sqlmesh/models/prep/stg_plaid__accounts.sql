MODEL (
  name prep.stg_plaid__accounts,
  kind VIEW
);

/* account_type normalizes through seeds.account_type_map like every other
   source, so the vocabulary is decided in one place. account_subtype is the
   exception: Plaid's own subtype is finer than the registry's (401k, money
   market, mortgage), so it wins and the registry only fills in when Plaid
   sent none. */
SELECT
  COALESCE(links.account_id, a.account_id) AS account_id, /* canonical via the import-time resolver link; source-native only if unresolved */
  a.account_id AS source_account_key,
  NULL::TEXT AS routing_number,
  m.account_type,
  a.institution_name,
  NULL::TEXT AS institution_fid,
  a.official_name,
  a.mask,
  COALESCE(a.account_subtype, m.account_subtype, LOWER(a.account_type)) AS account_subtype,
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
LEFT JOIN seeds.account_type_map AS m
  ON m.alias = UPPER(a.account_type)
