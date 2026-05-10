/* Resolved category dimension: unifies seeds.categories with
   app.user_categories and applies app.category_overrides.
   Replaces the Python-built app.categories view (retired with
   reports-recipe-library.md). */
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
  NULL::TIMESTAMP AS created_at /* NULL for seeded categories; populated for user_categories below */
FROM seeds.categories AS s
LEFT JOIN app.category_overrides AS o USING (category_id)
UNION ALL
SELECT
  category_id,
  category,
  subcategory,
  description,
  NULL AS plaid_detailed,
  FALSE AS is_default,
  is_active,
  created_at
FROM app.user_categories
