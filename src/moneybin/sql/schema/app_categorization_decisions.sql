/* Transaction-level categorization review decisions for the normalized review surface */
CREATE TABLE IF NOT EXISTS app.categorization_decisions (
    decision_id VARCHAR PRIMARY KEY, -- Deterministic cat_<sha256[:16]>[_aN] attempt identifier
    transaction_id VARCHAR NOT NULL, -- Canonical core.fct_transactions transaction under review
    attempt_number INTEGER NOT NULL CHECK (attempt_number >= 1), -- Monotonic proposal attempt for this transaction
    status VARCHAR NOT NULL CHECK (status IN ('pending', 'accepted', 'rejected', 'superseded')), -- Explicit proposal-attempt lifecycle state
    category_id VARCHAR, -- Accepted canonical category target; NULL while pending or rejected
    merchant_id VARCHAR, -- Accepted canonical merchant target; NULL when no merchant was assigned
    category VARCHAR, -- Immutable accepted category display snapshot
    subcategory VARCHAR, -- Immutable accepted subcategory display snapshot
    categorized_by VARCHAR, -- Immutable assignment-method snapshot
    confidence DECIMAL(3, 2), -- Immutable accepted confidence snapshot
    rule_id VARCHAR, -- Immutable accepted categorization-rule snapshot
    source_type VARCHAR, -- Immutable accepted source snapshot
    category_revision BIGINT NOT NULL CHECK (category_revision >= 0), -- Category audit revision observed by this attempt
    proposed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, -- When the pending proposal was materialized
    decided_at TIMESTAMP, -- When accepted or rejected; NULL while pending
    decided_by VARCHAR CHECK (decided_by IS NULL OR decided_by IN ('user', 'system')), -- User for MCP/CLI decisions; system for migration backfill
    reversed_at TIMESTAMP, -- When audit undo reversed the terminal outcome without deleting history
    reversed_by VARCHAR, -- Actor that reversed the terminal outcome
    UNIQUE (transaction_id, attempt_number),
    CHECK (
        (status = 'pending' AND decided_at IS NULL AND decided_by IS NULL)
        OR (status IN ('accepted', 'rejected', 'superseded') AND decided_at IS NOT NULL AND decided_by IS NOT NULL)
    ),
    CHECK (
        status != 'accepted'
        OR (category_id IS NOT NULL AND category IS NOT NULL)
    ),
    CHECK (
        status = 'accepted'
        OR (
            category_id IS NULL
            AND merchant_id IS NULL
            AND category IS NULL
            AND subcategory IS NULL
            AND categorized_by IS NULL
            AND confidence IS NULL
            AND rule_id IS NULL
            AND source_type IS NULL
        )
    ),
    CHECK (
        (reversed_at IS NULL AND reversed_by IS NULL)
        OR (
            reversed_at IS NOT NULL
            AND reversed_by IS NOT NULL
            AND status IN ('accepted', 'rejected')
        )
    )
);
