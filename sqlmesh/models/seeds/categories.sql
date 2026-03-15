-- Default category taxonomy based on Plaid Personal Finance Category v2
--
-- 16 primary categories with ~100 subcategories. Used to seed user.categories.
-- Edit the CSV to add/modify defaults; SQLMesh detects changes automatically.

MODEL (
    name seeds.categories,
    kind SEED (
        path 'categories.csv'
    ),
    columns (
        category_id VARCHAR,
        category VARCHAR,
        subcategory VARCHAR,
        description VARCHAR,
        plaid_detailed VARCHAR
    ),
    grain category_id
);
