/* Monthly spending budget targets by category; each record defines a spending limit for a category over a date range */
CREATE TABLE IF NOT EXISTS app.budgets (
    budget_id VARCHAR PRIMARY KEY, -- Unique identifier for this budget record
    category VARCHAR NOT NULL, -- DEPRECATED in V014 (Phase 1 dual-write): display snapshot; category_id is the canonical reference
    category_id VARCHAR, -- Foreign key to core.dim_categories.category_id (top-level row, subcategory IS NULL); NULL only for orphaned legacy rows
    monthly_amount DECIMAL(18, 2) NOT NULL, -- Target monthly spending limit for this category in USD
    start_month VARCHAR NOT NULL, -- First month this budget is active, in YYYY-MM format
    end_month VARCHAR, -- Last month this budget is active, in YYYY-MM format; NULL means no end date
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, -- Timestamp when this budget was created
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP -- Timestamp when this budget was last modified
);
