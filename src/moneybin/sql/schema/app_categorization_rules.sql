/* Auto-categorization rules evaluated in priority order; assign categories based on description patterns, amount bounds, and account filters */
CREATE TABLE IF NOT EXISTS app.categorization_rules (
    rule_id VARCHAR PRIMARY KEY, -- Unique identifier for this rule
    name VARCHAR NOT NULL, -- Human-readable label for this rule, e.g. Starbucks coffee purchases
    merchant_pattern VARCHAR NOT NULL, -- Pattern matched against fct_transactions.description to trigger this rule
    match_type VARCHAR DEFAULT 'contains', -- How merchant_pattern is applied: contains, exact, or regex
    min_amount DECIMAL(18, 2), -- Lower bound (absolute value) for amount matching; NULL means no lower bound
    max_amount DECIMAL(18, 2), -- Upper bound (absolute value) for amount matching; NULL means no upper bound
    account_id VARCHAR, -- Restricts rule to a specific account; NULL means rule applies to all accounts
    category VARCHAR NOT NULL, -- Category to assign when this rule matches
    subcategory VARCHAR, -- Subcategory to assign when this rule matches; NULL if no subcategory applies
    priority INTEGER DEFAULT 100, -- Evaluation order; lower number = higher priority. Default 100; use lower values for more specific rules
    is_active BOOLEAN DEFAULT true, -- False for rules that have been disabled without being deleted
    created_by VARCHAR DEFAULT 'user', -- Who created the rule: user (manually entered) or ai (LLM-suggested)
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, -- Timestamp when this rule was created
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP -- Timestamp when this rule was last modified
);
