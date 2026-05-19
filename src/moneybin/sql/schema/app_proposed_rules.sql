/* Auto-rule proposals generated from user categorization patterns; staged for review before activation */
CREATE TABLE IF NOT EXISTS app.proposed_rules (
    proposed_rule_id VARCHAR PRIMARY KEY, -- 12-char truncated UUID4 hex identifier
    merchant_pattern VARCHAR NOT NULL, -- Pattern to match: canonical merchant name or cleaned description
    match_type VARCHAR DEFAULT 'contains', -- How merchant_pattern is matched: contains, exact, or regex
    category VARCHAR NOT NULL, -- DEPRECATED in V014 (Phase 1 dual-write): display snapshot; category_id is the canonical reference. NOT NULL retained until Phase 2 drops the column.
    subcategory VARCHAR, -- DEPRECATED in V014 (Phase 1 dual-write): display snapshot; category_id is the canonical reference
    category_id VARCHAR, -- Foreign key to core.dim_categories.category_id; NULL only for orphaned legacy rows
    rule_id VARCHAR, -- Foreign key to app.categorization_rules.rule_id; set when status='approved'. NULL until approval. Replaces the text-keyed proposal->rule linkage that V016 retired.
    status VARCHAR DEFAULT 'pending', -- Lifecycle state: tracking (sub-threshold accumulation), pending (awaiting decision), approved, rejected, or superseded
    trigger_count INTEGER DEFAULT 1, -- Number of categorizations that triggered or reinforced this proposal
    source VARCHAR DEFAULT 'pattern_detection', -- How proposal was generated: pattern_detection or ml
    sample_txn_ids VARCHAR[], -- Up to 5 transaction_ids that triggered this proposal
    proposed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, -- When the proposal was first created
    decided_at TIMESTAMP, -- When the user approved or rejected; NULL while pending
    decided_by VARCHAR -- Who decided: 'user' or NULL while pending
);

CREATE INDEX IF NOT EXISTS idx_proposed_rules_pattern_status
    ON app.proposed_rules (merchant_pattern, status);

CREATE INDEX IF NOT EXISTS idx_proposed_rules_rule_id
    ON app.proposed_rules (rule_id);
