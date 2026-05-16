/* Resolved category dimension: unifies seeds.categories with
   app.user_categories and applies app.category_overrides. UNION (not UNION ALL)
   collapses any accidental category_id collision between a seed row and a
   user_categories row into a single output row. */
MODEL (
  name core.dim_categories,
  kind VIEW
);

SELECT
  s.category_id, /* Stable ID; user_categories assign 12-char UUID hex, seeds carry their own */
  s.category, /* Top-level spending category name */
  s.subcategory, /* Subcategory; NULL for top-level-only entries */
  s.description, /* Human-readable category description */
  s.plaid_detailed, /* Plaid PFC detailed mapping; NULL for user-defined categories */
  TRUE AS is_default, /* TRUE for seeded defaults, FALSE for user-created */
  COALESCE(o.is_active, TRUE) AS is_active, /* FALSE if user has soft-deleted this default */
  NULL::TIMESTAMP AS created_at, /* NULL for seeded categories; populated for user_categories below */
  o.updated_at AS updated_at /* Latest of all per-row input timestamps contributing to this row's current values. NULL for pure-seed rows; query meta.model_freshness for seed model freshness. */
FROM seeds.categories AS s
LEFT JOIN app.category_overrides AS o USING (category_id)
UNION
SELECT
  category_id,
  category,
  subcategory,
  description,
  NULL AS plaid_detailed,
  FALSE AS is_default,
  is_active,
  created_at,
  updated_at
FROM app.user_categories
