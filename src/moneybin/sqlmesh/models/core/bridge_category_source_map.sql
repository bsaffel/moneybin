/* Resolved category-source → canonical-category bridge: seeds.category_source_map
   (provider rows, blank source_origin) plus app.category_source_map, user rows
   winning per (source_type, source_origin, source_category_code,
   source_subcategory_code). Exactly one row per key; reverse lookup prefers
   code_level='detailed' then 'primary'. A user row with a NULL category_id
   ignores its term, and so switches off the seed row it overrides. */
MODEL (
  name core.bridge_category_source_map,
  kind VIEW
);

SELECT
  s.source_type, /* The row's own source_type: plaid for provider rows; csv, tsv, excel, parquet, feather, pdf, or manual for imported mappings */
  '' AS source_origin, /* Seed rows are provider-wide: blank origin; an imported mapping carries its row's source_origin */
  s.source_category_code, /* Source category text, verbatim */
  COALESCE(s.source_subcategory_code, '') AS source_subcategory_code, /* Second half of the source key; '' means "no subcategory" (never a distinct real value) */
  s.code_level, /* 'detailed' or 'primary'; detailed wins in reverse lookup */
  s.category_id, /* FK to core.dim_categories.category_id; NULL only on a user row, marking the term ignored (known, categorizes nothing) */
  s.source_taxonomy_version, /* Provider taxonomy revision curated against */
  TRUE AS is_default /* TRUE for seeded rows, FALSE for user overrides */
FROM seeds.category_source_map AS s
WHERE
  NOT EXISTS(
    SELECT
      1
    FROM app.category_source_map AS a
    WHERE
      a.source_type = s.source_type
      AND a.source_origin = ''
      AND a.source_category_code = s.source_category_code
      AND a.source_subcategory_code = COALESCE(s.source_subcategory_code, '')
  )
UNION ALL
SELECT
  source_type,
  source_origin,
  source_category_code,
  source_subcategory_code,
  code_level,
  category_id,
  source_taxonomy_version,
  FALSE AS is_default
FROM app.category_source_map
