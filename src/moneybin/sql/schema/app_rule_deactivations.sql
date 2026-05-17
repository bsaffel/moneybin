/* Audit trail for categorization rule deactivations; append-only history of why each rule was disabled */
CREATE TABLE IF NOT EXISTS app.rule_deactivations (
    deactivation_id VARCHAR PRIMARY KEY, -- 12-char truncated UUID4 hex identifier
    rule_id VARCHAR NOT NULL, -- Soft reference to categorization_rules.rule_id (no foreign key)
    reason VARCHAR NOT NULL, -- Why the rule was deactivated: override_threshold, manual, etc.
    override_count INTEGER, -- Number of user/AI overrides that triggered deactivation; NULL when not override-driven
    new_category VARCHAR, -- DEPRECATED in V014 (Phase 1 dual-write): display snapshot; new_category_id is the canonical reference
    new_subcategory VARCHAR, -- DEPRECATED in V014 (Phase 1 dual-write): display snapshot; new_category_id is the canonical reference
    new_category_id VARCHAR, -- Foreign key to core.dim_categories.category_id (converged category at deactivation); NULL only for orphaned legacy rows or non-override deactivations
    deactivated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP -- When the rule was deactivated
);

CREATE INDEX IF NOT EXISTS idx_rule_deactivations_rule_id
    ON app.rule_deactivations (rule_id);
