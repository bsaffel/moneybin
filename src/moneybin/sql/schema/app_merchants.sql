/* Merchant normalization patterns matched against transaction descriptions to canonicalize merchant names and cache category assignments */
-- Query examples for the LLM: see src/moneybin/services/schema_catalog.py (EXAMPLES dict)
CREATE TABLE IF NOT EXISTS app.merchants (
    merchant_id VARCHAR PRIMARY KEY, -- Unique identifier for this merchant record
    raw_pattern VARCHAR NOT NULL, -- Pattern matched against fct_transactions.description to identify this merchant
    match_type VARCHAR DEFAULT 'contains', -- How raw_pattern is applied: contains, exact, or regex
    canonical_name VARCHAR NOT NULL, -- Normalized display name used in fct_transactions.merchant_name
    category VARCHAR, -- Default category to apply when this merchant is matched; may be overridden by categorization rules
    subcategory VARCHAR, -- Default subcategory to apply when this merchant is matched; may be overridden by categorization rules
    created_by VARCHAR DEFAULT 'ai', -- Who created the record: ai (LLM-suggested) or user (manually entered)
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, -- Timestamp when this merchant record was created
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP -- Timestamp when this merchant record was last modified
);
