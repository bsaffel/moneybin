-- User schema for AI-managed data
-- These tables are written to by MCP tools (categorization, budgeting, notes)
CREATE SCHEMA IF NOT EXISTS "user";

-- Transaction categories assigned by the user or AI
CREATE TABLE IF NOT EXISTS "user".transaction_categories (
    transaction_id VARCHAR PRIMARY KEY,
    category VARCHAR NOT NULL,
    subcategory VARCHAR,
    categorized_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    categorized_by VARCHAR DEFAULT 'ai'
);

-- Monthly budget targets by category
CREATE TABLE IF NOT EXISTS "user".budgets (
    budget_id VARCHAR PRIMARY KEY,
    category VARCHAR NOT NULL,
    monthly_amount DECIMAL(18, 2) NOT NULL,
    start_month VARCHAR NOT NULL,
    end_month VARCHAR,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Free-form notes on transactions
CREATE TABLE IF NOT EXISTS "user".transaction_notes (
    transaction_id VARCHAR PRIMARY KEY,
    note VARCHAR NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
