/* Durable provider-id -> canonical merchant binding (M1T, merchant-entity-resolution.md Decision 2).
   One row per (provider id, canonical merchant). Status is binary: accepted (live) or reversed (undone).
   Provider-neutral: (source_type, ref_kind, ref_value) is the strong-ref key; a second aggregator is a
   new source_type, zero schema change. N:1 — one merchant_id may own many provider ids. No source_native
   translation role (unlike account_links): merchant identity is assigned at categorization time, not in
   staging. Written only through MerchantLinksRepo, which pairs every mutation with app.audit_log (Invariant 10). */
/* Invariant: at most one accepted binding per (source_type, ref_kind, ref_value).
   Enforced at the repository layer (MerchantLinksRepo._guard_uniqueness) — DuckDB cannot
   express a partial unique index (WHERE status='accepted') or a unique constraint
   on a generated column, so this cannot be a storage-level constraint. */
CREATE TABLE IF NOT EXISTS app.merchant_links (
    link_id VARCHAR NOT NULL,                  -- uuid4[:12] primary key for this binding
    merchant_id VARCHAR NOT NULL,              -- canonical merchant this provider id maps to
    ref_kind VARCHAR NOT NULL                  -- which kind of provider reference this row carries
        CHECK (ref_kind IN ('merchant_entity_id')),
    ref_value VARCHAR NOT NULL,                -- the provider's stable merchant id (opaque, non-PII)
    source_type VARCHAR NOT NULL,              -- issuing provider: plaid (future: mx, simplefin, ...)
    status VARCHAR NOT NULL                    -- accepted (live) or reversed (undone)
        CHECK (status IN ('accepted', 'reversed')),
    decided_by VARCHAR NOT NULL                -- domain actor: auto, user (human OR agent ratification), or system
        CHECK (decided_by IN ('auto', 'user', 'system')),
    decided_at TIMESTAMP NOT NULL,             -- when this binding was decided
    reversed_at TIMESTAMP,                     -- when reversed; NULL while accepted
    reversed_by VARCHAR                        -- domain actor who reversed; NULL while accepted
        CHECK (reversed_by IS NULL OR reversed_by IN ('auto', 'user', 'system')),
    PRIMARY KEY (link_id)
);
