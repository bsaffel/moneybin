/* Monthly spending budget targets by category; each record defines a spending limit for a category over a date range */
CREATE TABLE IF NOT EXISTS app.budgets (
    budget_id VARCHAR PRIMARY KEY, -- Unique identifier for this budget record
    category VARCHAR NOT NULL, -- Spending category this budget applies to; matches app.categories.category
    monthly_amount DECIMAL(18, 2) NOT NULL, -- Target monthly spending limit for this category in USD
    start_month VARCHAR NOT NULL, -- First month this budget is active, in YYYY-MM format
    end_month VARCHAR, -- Last month this budget is active, in YYYY-MM format; NULL means no end date
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, -- Timestamp when this budget was created
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP -- Timestamp when this budget was last modified
);
