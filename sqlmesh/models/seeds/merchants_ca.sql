/* Seed merchant mappings for the CA region. Curated separately from global to allow
   per-region overrides. */
MODEL (
  name seeds.merchants_ca,
  kind SEED (
    path 'merchants_ca.csv'
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
