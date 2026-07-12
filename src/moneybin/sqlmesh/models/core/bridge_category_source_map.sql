/* Resolved provider-code → canonical-category bridge: seeds.category_source_map
   plus app.category_source_map, user rows winning per (source_type,
   source_category_code). Exactly one row per code; reverse lookup prefers
   code_level='detailed' then 'primary'. */
MODEL (
  name core.bridge_category_source_map,
  kind VIEW
);

SELECT
  s.source_type, /* Provider provenance: plaid, mx, simplefin */
  s.source_category_code, /* Provider category code, verbatim */
  s.code_level, /* 'detailed' or 'primary'; detailed wins in reverse lookup */
  s.category_id, /* FK to core.dim_categories.category_id */
  s.source_taxonomy_version, /* Provider taxonomy revision curated against */
  TRUE AS is_default /* TRUE for seeded rows, FALSE for user overrides */
FROM seeds.category_source_map AS s
WHERE
  NOT EXISTS(
    SELECT
      1
    FROM app.category_source_map AS a
    WHERE
      a.source_type = s.source_type AND a.source_category_code = s.source_category_code
  )
UNION ALL
SELECT
  source_type,
  source_category_code,
  code_level,
  category_id,
  source_taxonomy_version,
  FALSE AS is_default
FROM app.category_source_map
