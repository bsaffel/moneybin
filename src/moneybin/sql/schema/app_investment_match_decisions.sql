/* Durable whole-investment-event Proposals; planner writes never alter Golden membership. */
CREATE TABLE IF NOT EXISTS app.investment_match_decisions (
    proposal_id VARCHAR PRIMARY KEY, -- Opaque review lifecycle identity
    algorithm_version VARCHAR NOT NULL, -- Exact planning contract version
    relationship_fingerprint VARCHAR NOT NULL, -- Exact members and canonical dependencies
    candidate_graph_fingerprint VARCHAR NOT NULL, -- Connected alternatives and rejection constraints
    confidence_band VARCHAR NOT NULL CHECK (confidence_band IN ('native', 'exact', 'fuzzy')), -- Weakest member-pair evidence
    status VARCHAR NOT NULL CHECK (status IN ('pending', 'accepted', 'rejected', 'stale', 'reversed')), -- Human transitions unavailable during review-only rollout
    auto_eligible BOOLEAN NOT NULL, -- Immutable measurement only; no mutation authority
    is_competing BOOLEAN NOT NULL, -- Derived by the persisted constrained optimal solve
    proposal JSON NOT NULL, -- Exact legs, evidence, conflicts, alternatives and supersession topology
    actor VARCHAR NOT NULL, -- Surface requesting the planning operation
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, -- First planning time
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, -- Last planner lifecycle change
    decided_at TIMESTAMP, -- First human decision time; NULL for planner writes
    accepted_at TIMESTAMP -- Ratification time; NULL throughout review-only rollout
);
