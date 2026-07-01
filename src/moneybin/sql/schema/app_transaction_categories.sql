/* Category assignments for transactions; written by the rules engine, AI model, or user; one record per transaction */
CREATE TABLE IF NOT EXISTS app.transaction_categories (
    transaction_id VARCHAR PRIMARY KEY, -- Foreign key to core.fct_transactions; one categorization record per transaction
    category VARCHAR NOT NULL, -- DEPRECATED in V014 (Phase 1 dual-write): display snapshot; category_id is the canonical reference
    subcategory VARCHAR, -- DEPRECATED in V014 (Phase 1 dual-write): display snapshot; category_id is the canonical reference
    category_id VARCHAR, -- Foreign key to core.dim_categories.category_id; NULL only for orphaned legacy rows
    categorized_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, -- Timestamp when this categorization was last written
    categorized_by VARCHAR DEFAULT 'ai', -- Assignment METHOD: user | rule | auto_rule | migration | ml | provider_native | ai
    merchant_id VARCHAR, -- Foreign key to core.dim_merchants if this transaction was matched to a known merchant; NULL otherwise
    confidence DECIMAL(3, 2), -- Model confidence score for AI-assigned categories, 0.00-1.00; NULL for rule or user assignments
    rule_id VARCHAR, -- Foreign key to app.categorization_rules if assigned by a rule; NULL otherwise
    source_type VARCHAR NOT NULL DEFAULT 'internal' -- Origin aggregator for provider_native writes (plaid | mx | ...); 'internal' for all other methods
);
