/* Durable provider-ref -> canonical security binding (sync-plaid-investments.md,
   mirroring merchant-entity-resolution.md Decision 2). One row per (provider ref,
   canonical security). Status is binary: accepted (live) or reversed (undone).
   Provider-neutral: (source_type, ref_kind, ref_value) is the strong-ref key; N:1 —
   one security_id may own many provider refs (Plaid security_id churn on corporate
   actions re-binds to the same canonical security). Written only through
   SecurityLinksRepo, which pairs every mutation with app.audit_log (Invariant 10). */
/* Invariant: at most one accepted binding per (source_type, ref_kind, ref_value).
   Enforced at the repository layer (SecurityLinksRepo._guard_uniqueness) — DuckDB
   cannot express a partial unique index (WHERE status='accepted'), so this cannot
   be a storage-level constraint. */
CREATE TABLE IF NOT EXISTS app.security_links (
    link_id VARCHAR NOT NULL,                  -- uuid4[:12] primary key for this binding
    security_id VARCHAR NOT NULL,              -- canonical app.securities entry this provider ref maps to
    ref_kind VARCHAR NOT NULL                  -- which kind of provider reference this row carries
        CHECK (ref_kind IN ('plaid_security_id', 'institution_security_id')),
    ref_value VARCHAR NOT NULL,                -- the provider ref; institution rows store '{institution_id}:{institution_security_id}'
    source_type VARCHAR NOT NULL,              -- issuing provider: plaid (future: ofx institutions, ...)
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
