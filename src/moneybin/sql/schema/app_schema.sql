-- App schema for application-managed data
-- These tables are written to by MCP tools (categorization, budgeting, notes)
-- Separated from raw/prep/core which hold externally-imported data
CREATE SCHEMA IF NOT EXISTS app;

-- Transaction categories assigned by the user, rules, Plaid, or AI
CREATE TABLE IF NOT EXISTS app.transaction_categories (
    transaction_id VARCHAR PRIMARY KEY, -- Foreign key to core.fct_transactions; one categorization record per transaction
    category VARCHAR NOT NULL, -- Assigned spending category
    subcategory VARCHAR, -- Assigned spending subcategory; NULL if categorized at top level only
    categorized_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, -- Timestamp when this categorization was last written
    categorized_by VARCHAR DEFAULT 'ai', -- How the category was assigned: rule (categorization rule match), ai (LLM), or user (manual)
    merchant_id VARCHAR, -- Foreign key to app.merchants if this transaction was matched to a known merchant; NULL otherwise
    confidence DECIMAL(3, 2), -- Model confidence score for AI-assigned categories, 0.00-1.00; NULL for rule or user assignments
    rule_id VARCHAR -- Foreign key to app.categorization_rules if assigned by a rule; NULL otherwise
);

-- Monthly budget targets by category
CREATE TABLE IF NOT EXISTS app.budgets (
    budget_id VARCHAR PRIMARY KEY, -- Unique identifier for this budget record
    category VARCHAR NOT NULL, -- Spending category this budget applies to; matches app.categories.category
    monthly_amount DECIMAL(18, 2) NOT NULL, -- Target monthly spending limit for this category in USD
    start_month VARCHAR NOT NULL, -- First month this budget is active, in YYYY-MM format
    end_month VARCHAR, -- Last month this budget is active, in YYYY-MM format; NULL means no end date
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, -- Timestamp when this budget was created
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP -- Timestamp when this budget was last modified
);

-- Free-form notes on transactions
CREATE TABLE IF NOT EXISTS app.transaction_notes (
    transaction_id VARCHAR PRIMARY KEY, -- Foreign key to core.fct_transactions; one note per transaction
    note VARCHAR NOT NULL, -- Free-form text note added by the user about this transaction
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP -- Timestamp when this note was created
);
