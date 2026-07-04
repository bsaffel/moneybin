/* Curated provider category-code → canonical MoneyBin category (seed defaults).
   Plaid Personal Finance Category v2 codes, verified against Plaid's published
   taxonomy. User overrides live in app.category_source_map; the two are unioned
   by core.bridge_category_source_map. Edit the CSV to change defaults; SQLMesh
   detects changes automatically. */
MODEL (
  name seeds.category_source_map,
  kind SEED (
    path 'category_source_map.csv'
  ),
  columns (
    source_type TEXT,
    source_category_code TEXT,
    code_level TEXT,
    category_id TEXT,
    source_taxonomy_version TEXT
  ),
  grain (source_type, source_category_code)
)
