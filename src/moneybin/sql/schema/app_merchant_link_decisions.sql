/* Merchant-link review queue (M1T, merchant-entity-resolution.md Decision 2). match_decisions-shaped:
   one row per (unbound provider id, candidate merchant) proposal — candidates are relational rows, not JSON.
   The review queue reads pending rows; this is the only place ambiguous merchant-binding state lives.
   Written only through MerchantLinkDecisionsRepo (Invariant 10). */
CREATE TABLE IF NOT EXISTS app.merchant_link_decisions (
    decision_id VARCHAR NOT NULL,              -- uuid4[:12] primary key
    ref_kind VARCHAR NOT NULL                  -- 'merchant_entity_id'
        CHECK (ref_kind IN ('merchant_entity_id')),
    ref_value VARCHAR NOT NULL,                -- the unbound provider id under review
    source_type VARCHAR NOT NULL,              -- issuing provider
    provider_merchant_name VARCHAR,            -- provider's merchant_name (reviewer display + match basis)
    candidate_merchant_id VARCHAR NOT NULL,    -- existing merchant proposed as the binding target
    confidence_score DECIMAL(5, 4),            -- informational; fuzzy matches always go to review
    match_signals VARCHAR,                     -- JSON: which signal fired + value (per match_decisions convention)
    status VARCHAR NOT NULL                    -- pending | accepted | rejected | reversed
        CHECK (status IN ('pending', 'accepted', 'rejected', 'reversed')),
    decided_by VARCHAR NOT NULL                -- auto | user (decisions are initiated by auto-categorization or user review; system-harvested backfills write bindings to app.merchant_links directly, never a decision row)
        CHECK (decided_by IN ('auto', 'user')),
    match_reason VARCHAR,                      -- short human reason (e.g. signal name)
    decided_at TIMESTAMP NOT NULL,
    reversed_at TIMESTAMP,
    reversed_by VARCHAR
        CHECK (reversed_by IS NULL OR reversed_by IN ('auto', 'user')),
    PRIMARY KEY (decision_id)
);
