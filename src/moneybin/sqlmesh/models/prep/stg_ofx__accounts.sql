MODEL (
  name prep.stg_ofx__accounts,
  kind VIEW
);

/* account_type normalizes to the canonical vocabulary via seeds.account_type_map;
   the source spelling is preserved as account_subtype when the registry has no
   finer distinction for it.

   The NULLIF(TRIM(...), '') is not defensive padding. The extractor now writes
   NULL for an absent <ACCTTYPE>, but rows imported before that fix hold the ''
   that ofxparse's Account constructor produced, and those rows are still on
   disk. Without this, '' misses every registry alias and then falls through the
   subtype COALESCE as LOWER('') — relocating the empty string into
   account_subtype instead of removing it. */
SELECT
  COALESCE(links.account_id, a.account_id) AS account_id, /* canonical via the import-time resolver link; source-native only if unresolved */
  a.account_id AS source_account_key,
  a.routing_number,
  m.account_type,
  CASE
    WHEN NOT m.alias IS NULL
    THEN m.account_subtype
    ELSE LOWER(NULLIF(TRIM(a.account_type), ''))
  END AS account_subtype,
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
  ON m.alias = UPPER(NULLIF(TRIM(a.account_type), ''))
