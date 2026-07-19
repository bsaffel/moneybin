/* Transaction-level categorization review decisions for the normalized review surface */
CREATE TABLE IF NOT EXISTS app.categorization_decisions (
    decision_id VARCHAR PRIMARY KEY, -- Deterministic cat_<sha256[:16]> identifier bound to transaction_id
    transaction_id VARCHAR NOT NULL UNIQUE, -- Canonical core.fct_transactions transaction under review
    status VARCHAR NOT NULL CHECK (status IN ('pending', 'accepted', 'rejected')), -- Explicit proposal lifecycle state
    category_id VARCHAR, -- Accepted canonical category target; NULL while pending or rejected
    merchant_id VARCHAR, -- Accepted canonical merchant target; NULL when no merchant was assigned
    proposed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, -- When the pending proposal was materialized
    decided_at TIMESTAMP, -- When accepted or rejected; NULL while pending
    decided_by VARCHAR CHECK (decided_by IS NULL OR decided_by IN ('user', 'system')), -- User for MCP/CLI decisions; system for migration backfill
    CHECK (
        (status = 'pending' AND decided_at IS NULL AND decided_by IS NULL)
        OR (status IN ('accepted', 'rejected') AND decided_at IS NOT NULL AND decided_by IS NOT NULL)
    ),
    CHECK (status != 'rejected' OR (category_id IS NULL AND merchant_id IS NULL))
);
