/* Fuzzy-match review queue for security identity (sync-plaid-investments.md).
   MERGE semantics: the provisional security already exists by review time (rung-3
   mint-now/merge-later), so accept rebinds the provider ref to candidate_security_id,
   deletes the provisional created_by='plaid' catalog row, and migrates
   app.lot_selections; reject keeps the minted security and records the declined
   pairing so the resolver never re-proposes it. Pending rows surface via the
   domain-neutral review sweep as security_links_pending. Written only through
   SecurityLinkDecisionsRepo (Invariant 10). */
CREATE TABLE IF NOT EXISTS app.security_link_decisions (
    decision_id VARCHAR NOT NULL,              -- uuid4[:12]
    ref_kind VARCHAR NOT NULL                  -- provider-ref kind under review
        CHECK (ref_kind IN ('plaid_security_id', 'institution_security_id')),
    ref_value VARCHAR NOT NULL,                -- the unbound provider ref under review
    source_type VARCHAR NOT NULL,              -- issuing provider
    provider_ticker VARCHAR,                   -- Plaid ticker_symbol (reviewer display + match basis)
    provider_name VARCHAR,                     -- Plaid security name
    candidate_security_id VARCHAR NOT NULL,    -- existing app.securities entry proposed as merge survivor
    confidence_score DECIMAL(5, 4),            -- match confidence (0-1)
    match_signals JSON,                        -- which signal fired + value (match_decisions convention)
    status VARCHAR NOT NULL                    -- review lifecycle
        CHECK (status IN ('pending', 'accepted', 'rejected', 'reversed')),
    decided_by VARCHAR NOT NULL
        CHECK (decided_by IN ('auto', 'user')),
    match_reason VARCHAR,                      -- e.g. fuzzy_name
    decided_at TIMESTAMP NOT NULL,             -- when this decision row last changed state
    reversed_at TIMESTAMP,                     -- when reversed; NULL otherwise
    reversed_by VARCHAR                        -- who reversed; NULL otherwise
        CHECK (reversed_by IS NULL OR reversed_by IN ('auto', 'user')),
    PRIMARY KEY (decision_id)
);
