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
    ref_kind VARCHAR NOT NULL                  -- provider-ref kind under review; an ambiguous market-feed key derivation (tiingo_ticker, coingecko_slug) queues here rather than binding silently
        CHECK (ref_kind IN ('plaid_security_id', 'institution_security_id', 'tiingo_ticker', 'coingecko_slug')),
    ref_value VARCHAR NOT NULL,                -- the unbound provider ref under review
    source_type VARCHAR NOT NULL,              -- issuing provider
    provider_ticker VARCHAR,                   -- the PROVIDER's symbol, never the catalog's (reviewer display + match basis): Plaid ticker_symbol, or the symbol a market feed was queried by. NULL when the provider was never consulted (a ticker ambiguous inside our own catalog is refused before any round-trip)
    provider_name VARCHAR,                     -- the PROVIDER's name for that symbol. Writing the catalog's own name here shows the reviewer two identical names and hides the divergence under review
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
