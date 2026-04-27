/* Auto-rule proposals generated from user categorization patterns; staged for review before activation */
CREATE TABLE IF NOT EXISTS app.proposed_rules (
    proposed_rule_id VARCHAR PRIMARY KEY, -- 12-char truncated UUID4 hex identifier
    merchant_pattern VARCHAR NOT NULL, -- Pattern to match: canonical merchant name or cleaned description
    match_type VARCHAR DEFAULT 'contains', -- How merchant_pattern is matched: contains, exact, or regex
    category VARCHAR NOT NULL, -- Proposed category to assign on approval
    subcategory VARCHAR, -- Proposed subcategory; NULL when no subcategory applies
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
