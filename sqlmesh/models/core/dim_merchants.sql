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
  merchant_id, /* UUID hex from uuid.uuid4().hex[:12] */
  raw_pattern, /* Pattern matched against transaction descriptions; NULL for exemplar-only merchants (match_type='oneOf') */
  match_type, /* contains | exact | regex | oneOf */
  canonical_name, /* Normalized merchant display name */
  category, /* Default category for matched transactions */
  subcategory, /* Default subcategory */
  created_by, /* 'user' | 'ai' | 'rule' | 'plaid' | 'migration' */
  exemplars, /* Exact match_text values for oneOf set-membership lookup */
  created_at, /* Timestamp of creation */
  updated_at /* Latest of all per-row input timestamps contributing to this row's current values. Set on UPDATE by service writes. */
FROM app.user_merchants
