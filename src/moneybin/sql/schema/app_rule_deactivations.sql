/* Audit trail for categorization rule deactivations; append-only history of why each rule was disabled */
CREATE TABLE IF NOT EXISTS app.rule_deactivations (
    deactivation_id VARCHAR PRIMARY KEY, -- 12-char truncated UUID4 hex identifier
    rule_id VARCHAR NOT NULL, -- Soft reference to categorization_rules.rule_id (no foreign key)
    reason VARCHAR NOT NULL, -- Why the rule was deactivated: override_threshold, manual, etc.
    override_count INTEGER, -- Number of user/AI overrides that triggered deactivation; NULL when not override-driven
    new_category VARCHAR, -- Category the override population converged on; NULL when not override-driven
    new_subcategory VARCHAR, -- Subcategory the override population converged on; NULL when none
    deactivated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP -- When the rule was deactivated
);

CREATE INDEX IF NOT EXISTS idx_rule_deactivations_rule_id
    ON app.rule_deactivations (rule_id);
