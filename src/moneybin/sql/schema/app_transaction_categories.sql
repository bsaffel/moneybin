/* Category assignments for transactions; written by the rules engine, AI model, or user; one record per transaction */
CREATE TABLE IF NOT EXISTS app.transaction_categories (
    transaction_id VARCHAR PRIMARY KEY, -- Foreign key to core.fct_transactions; one categorization record per transaction
    category VARCHAR NOT NULL, -- Assigned spending category
    subcategory VARCHAR, -- Assigned spending subcategory; NULL if categorized at top level only
    categorized_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, -- Timestamp when this categorization was last written
    categorized_by VARCHAR DEFAULT 'ai', -- How the category was assigned: rule (categorization rule match), ai (LLM), or user (manual)
    merchant_id VARCHAR, -- Foreign key to core.dim_merchants if this transaction was matched to a known merchant; NULL otherwise
    confidence DECIMAL(3, 2), -- Model confidence score for AI-assigned categories, 0.00-1.00; NULL for rule or user assignments
    rule_id VARCHAR -- Foreign key to app.categorization_rules if assigned by a rule; NULL otherwise
);
