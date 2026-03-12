-- Auto-categorization rules with conditions
-- Rules are evaluated in priority order (lower number = higher priority)
CREATE TABLE IF NOT EXISTS app.categorization_rules (
    rule_id VARCHAR PRIMARY KEY,
    name VARCHAR NOT NULL,
    merchant_pattern VARCHAR NOT NULL,
    match_type VARCHAR DEFAULT 'contains',
    min_amount DECIMAL(18, 2),
    max_amount DECIMAL(18, 2),
    account_id VARCHAR,
    category VARCHAR NOT NULL,
    subcategory VARCHAR,
    priority INTEGER DEFAULT 100,
    is_active BOOLEAN DEFAULT true,
    created_by VARCHAR DEFAULT 'user',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
