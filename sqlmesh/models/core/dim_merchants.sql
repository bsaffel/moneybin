/* Resolved merchant dimension: unifies app.user_merchants with regional seed
   merchant tables (global, US, CA) and applies app.merchant_overrides.
   User merchants take precedence on overlap. Replaces the Python-built
   app.merchants view (retired with reports-recipe-library.md). */
MODEL (
  name core.dim_merchants,
  kind VIEW
);

SELECT
  merchant_id, /* User-assigned UUID hex */
  raw_pattern, /* Pattern matched against transaction descriptions */
  match_type, /* contains | exact | regex */
  canonical_name, /* Normalized merchant display name */
  category, /* Default category for matched transactions */
  subcategory, /* Default subcategory */
  created_by, /* 'user', 'ai', 'rule' for user_merchants; 'seed' for seeds */
  created_at, /* Timestamp of user creation; NULL for seeds */
  is_user /* TRUE for user-created, FALSE for seeded */
FROM (
  -- User merchants first (user wins on overlap with seeds)
  SELECT
    merchant_id,
    raw_pattern,
    match_type,
    canonical_name,
    category,
    subcategory,
    created_by,
    created_at,
    TRUE AS is_user
  FROM app.user_merchants
  UNION ALL
  -- Global seeds
  SELECT
    s.merchant_id,
    s.raw_pattern,
    s.match_type,
    s.canonical_name,
    COALESCE(o.category, s.category) AS category,
    COALESCE(o.subcategory, s.subcategory) AS subcategory,
    'seed' AS created_by,
    NULL::TIMESTAMP AS created_at,
    FALSE AS is_user
  FROM seeds.merchants_global AS s
  LEFT JOIN app.merchant_overrides AS o USING (merchant_id)
  WHERE COALESCE(o.is_active, TRUE)
  UNION ALL
  -- US seeds
  SELECT
    s.merchant_id,
    s.raw_pattern,
    s.match_type,
    s.canonical_name,
    COALESCE(o.category, s.category) AS category,
    COALESCE(o.subcategory, s.subcategory) AS subcategory,
    'seed' AS created_by,
    NULL::TIMESTAMP AS created_at,
    FALSE AS is_user
  FROM seeds.merchants_us AS s
  LEFT JOIN app.merchant_overrides AS o USING (merchant_id)
  WHERE COALESCE(o.is_active, TRUE)
  UNION ALL
  -- CA seeds
  SELECT
    s.merchant_id,
    s.raw_pattern,
    s.match_type,
    s.canonical_name,
    COALESCE(o.category, s.category) AS category,
    COALESCE(o.subcategory, s.subcategory) AS subcategory,
    'seed' AS created_by,
    NULL::TIMESTAMP AS created_at,
    FALSE AS is_user
  FROM seeds.merchants_ca AS s
  LEFT JOIN app.merchant_overrides AS o USING (merchant_id)
  WHERE COALESCE(o.is_active, TRUE)
)
