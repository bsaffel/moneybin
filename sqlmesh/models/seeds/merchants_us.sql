/* Seed merchant mappings for the US region. Curated separately from global to allow
   per-region overrides (e.g., Costco, Tim Hortons regional differences). */
MODEL (
  name seeds.merchants_us,
  kind SEED (
    path 'merchants_us.csv'
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
