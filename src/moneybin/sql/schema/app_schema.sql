-- App schema for application-managed data
-- These tables are written to by MCP tools (categorization, budgeting, notes)
-- Separated from raw/prep/core which hold externally-imported data
CREATE SCHEMA IF NOT EXISTS app;

-- Transaction categories assigned by the user, rules, Plaid, or AI
CREATE TABLE IF NOT EXISTS app.transaction_categories (
    transaction_id VARCHAR PRIMARY KEY,
    category VARCHAR NOT NULL,
    subcategory VARCHAR,
    categorized_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    categorized_by VARCHAR DEFAULT 'ai',
    merchant_id VARCHAR,
    confidence DECIMAL(3, 2),
    rule_id VARCHAR
);

-- Monthly budget targets by category
CREATE TABLE IF NOT EXISTS app.budgets (
    budget_id VARCHAR PRIMARY KEY,
    category VARCHAR NOT NULL,
    monthly_amount DECIMAL(18, 2) NOT NULL,
    start_month VARCHAR NOT NULL,
    end_month VARCHAR,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Free-form notes on transactions
CREATE TABLE IF NOT EXISTS app.transaction_notes (
    transaction_id VARCHAR PRIMARY KEY,
    note VARCHAR NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
