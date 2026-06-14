/* Merge-proposal review queue for account identity (M1S, account-identity-resolution.md Decision 2).
   match_decisions-shaped: one row per (provisional account, candidate account) proposal, so candidates are
   relational rows -- queryable, not JSON. This is the ONLY place pending/ambiguous account-link state lives;
   the review queue reads the pending rows. Written only through AccountLinkDecisionsRepo, which pairs every
   mutation with an app.audit_log row (Invariant 10). */
CREATE TABLE IF NOT EXISTS app.account_link_decisions (
    decision_id VARCHAR NOT NULL,              -- uuid4[:12] primary key for this proposal
    provisional_account_id VARCHAR NOT NULL,   -- the just-minted source account under review
    candidate_account_id VARCHAR NOT NULL,     -- an existing canonical account proposed as the same
    confidence_score DECIMAL(5, 4),            -- weak-signal confidence 0.0000 to 1.0000
    match_signals JSON,                        -- which weak signal fired + its value (institution_last4 / name)
    status VARCHAR NOT NULL                    -- pending, accepted, rejected, reversed
        CHECK (status IN ('pending', 'accepted', 'rejected', 'reversed')),
    decided_by VARCHAR NOT NULL                -- domain actor: auto or user (human OR agent ratification)
        CHECK (decided_by IN ('auto', 'user')),
    match_reason VARCHAR,                      -- human-readable explanation of why this pairing was proposed
    decided_at TIMESTAMP NOT NULL,             -- when the decision was made (or the proposal created)
    reversed_at TIMESTAMP,                     -- when a prior decision was undone; NULL otherwise
    reversed_by VARCHAR,                       -- domain actor who reversed; NULL otherwise
    PRIMARY KEY (decision_id)
);
