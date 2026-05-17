/* Resolved merchant dimension: thin view over app.user_merchants. All
   merchants are user-created or system-created on the user's behalf
   (created_by: 'user' | 'ai' | 'rule' | 'plaid' | 'migration'). MoneyBin
   does not ship a curated seed merchant catalog; the LLM-assist and
   auto-rule pipeline learns merchants from the user's data. */
MODEL (
  name core.dim_merchants,
  kind VIEW
);

SELECT
  um.merchant_id, /* UUID hex from uuid.uuid4().hex[:12] */
  um.raw_pattern, /* Pattern matched against transaction descriptions; NULL for exemplar-only merchants (match_type='oneOf') */
  um.match_type, /* contains | exact | regex | oneOf */
  um.canonical_name, /* Normalized merchant display name */
  um.category_id, /* Foreign key to core.dim_categories.category_id; NULL for merchants without a default category */
  COALESCE(dc.category, um.category) AS category, /* Default category resolved via category_id FK; falls back to app.user_merchants.category snapshot for orphaned rows */
  COALESCE(dc.subcategory, um.subcategory) AS subcategory, /* Default subcategory resolved via FK; same fallback as category */
  um.created_by, /* 'user' | 'ai' | 'rule' | 'plaid' | 'migration' */
  um.exemplars, /* Exact match_text values for oneOf set-membership lookup */
  um.created_at, /* Timestamp of creation */
  um.updated_at /* Latest of all per-row input timestamps contributing to this row's current values. Set on UPDATE by service writes. */
FROM app.user_merchants AS um
LEFT JOIN core.dim_categories AS dc
  ON um.category_id = dc.category_id
