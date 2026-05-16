/* Resolved merchant dimension: unifies app.user_merchants with regional seed
   merchant tables (global, US, CA) and applies app.merchant_overrides to the
   seed rows only. User merchants are not joined to overrides — they are
   already mutable, so corrections happen on the source row. User merchants
   take precedence on overlap. */
MODEL (
  name core.dim_merchants,
  kind VIEW
);

SELECT
  merchant_id, /* User-assigned UUID hex */
  raw_pattern, /* Pattern matched against transaction descriptions; NULL for exemplar-only merchants (match_type='oneOf') */
  match_type, /* contains | exact | regex | oneOf */
  canonical_name, /* Normalized merchant display name */
  category, /* Default category for matched transactions */
  subcategory, /* Default subcategory */
  created_by, /* 'user', 'ai', 'rule' for user_merchants; 'seed' for seeds */
  exemplars, /* Exact match_text values for oneOf set-membership lookup; empty for seeds */
  created_at, /* Timestamp of user creation; NULL for seeds */
  updated_at, /* Latest of all per-row input timestamps contributing to this row's current values. NULL for pure-seed rows; query meta.model_freshness for seed model freshness. */
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
    exemplars,
    created_at,
    updated_at,
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
    CAST([] AS VARCHAR[]) AS exemplars,
    NULL::TIMESTAMP AS created_at,
    o.updated_at AS updated_at,
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
    CAST([] AS VARCHAR[]) AS exemplars,
    NULL::TIMESTAMP AS created_at,
    o.updated_at AS updated_at,
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
    CAST([] AS VARCHAR[]) AS exemplars,
    NULL::TIMESTAMP AS created_at,
    o.updated_at AS updated_at,
    FALSE AS is_user
  FROM seeds.merchants_ca AS s
  LEFT JOIN app.merchant_overrides AS o USING (merchant_id)
  WHERE COALESCE(o.is_active, TRUE)
)
