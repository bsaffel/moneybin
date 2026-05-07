/* Seed merchant mappings for universal/global brands (Netflix, Spotify, Amazon Prime, etc.).
   Edit the CSV to add/modify entries; SQLMesh detects changes automatically. */
MODEL (
  name seeds.merchants_global,
  kind SEED (
    path 'merchants_global.csv'
  ),
  columns (
    merchant_id TEXT,
    raw_pattern TEXT,
    match_type TEXT,
    canonical_name TEXT,
    category TEXT,
    subcategory TEXT,
    country TEXT
  ),
  grain merchant_id
)
