MODEL (
  name prep.stg_plaid__accounts,
  kind VIEW
);

/* account_type normalizes through seeds.account_type_map like every other
   source, so the vocabulary is decided in one place. An unmapped alias resolves
   to NULL here exactly as it does for the other two sources.

   Every free-text column here is wrapped in NULLIF(TRIM(...), '') for the same
   reason: dim_accounts merges these with FILTER(WHERE NOT ... IS NULL) and
   concatenates them into display_name, and '' passes a NULL check while
   rendering as a malformed label. Plaid's SyncAccount declares them `str | None`
   and nothing upstream promises blank arrives as NULL rather than ''.

   Resist "fixing" the account_type NULL. core.fct_balances drops Plaid balances whose
   account_type is unresolvable, and that is deliberate (see the comment on its
   plaid_balances CTE, guarded by test_plaid_null_account_type_dropped): without
   a type the balance cannot be signed, and any non-NULL default falls to the
   positive ELSE branch — booking a possible liability as an asset and
   overstating net worth by twice the balance. Dropping understates instead,
   which is the safe direction. The unmapped source spelling still survives in
   account_subtype, and the real remedy is adding the alias to the registry.

   account_subtype is the other exception: Plaid's own subtype is finer than the
   registry's (401k, money market, mortgage), so it wins and the registry only
   fills in when Plaid sent none. */
SELECT
  COALESCE(links.account_id, a.account_id) AS account_id, /* canonical via the import-time resolver link; source-native only if unresolved */
  a.account_id AS source_account_key,
  NULL::TEXT AS routing_number,
  m.account_type,
  NULLIF(TRIM(a.institution_name), '') AS institution_name,
  NULL::TEXT AS institution_fid,
  NULLIF(TRIM(a.official_name), '') AS official_name,
  a.mask,
  COALESCE(
    NULLIF(TRIM(a.account_subtype), ''),
    CASE
      WHEN NOT m.alias IS NULL
      THEN m.account_subtype
      ELSE LOWER(NULLIF(TRIM(a.account_type), ''))
    END
  ) AS account_subtype,
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
  ON m.alias = UPPER(NULLIF(TRIM(a.account_type), ''))
