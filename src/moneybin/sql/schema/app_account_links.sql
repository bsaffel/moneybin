/* Durable native-ref -> canonical account mapping + idempotency substrate (M1S, account-identity-resolution.md Decision 2).
   One row per (canonical account, native ref). Status is binary: accepted (live) or reversed (undone) -- no
   pending/provisional state lives here (that is app.account_link_decisions). Every source account always has an
   accepted source_native mapping, so it is always translatable to a canonical id by the staging JOIN.
   Written only through AccountLinksRepo, which pairs every mutation with an app.audit_log row (Invariant 10). */
CREATE TABLE IF NOT EXISTS app.account_links (
    link_id VARCHAR NOT NULL,                  -- uuid4[:12] primary key for this mapping
    account_id VARCHAR NOT NULL,               -- canonical (opaque, minted) account this ref maps to
    ref_kind VARCHAR NOT NULL                  -- which kind of native reference this row carries
        CHECK (ref_kind IN ('source_native', 'persistent_token', 'full_number')),
    ref_value VARCHAR NOT NULL,                -- the native identifier; read-surface masking is per-ref_kind (taxonomy)
    source_type VARCHAR NOT NULL,              -- provenance: ofx, csv, pdf, plaid, ...
    source_origin VARCHAR NOT NULL,            -- institution/connection/format; scopes source_native against slug collisions
    status VARCHAR NOT NULL                    -- accepted (live) or reversed (undone)
        CHECK (status IN ('accepted', 'reversed')),
    decided_by VARCHAR NOT NULL                -- domain actor: auto, user (human OR agent ratification), or system
        CHECK (decided_by IN ('auto', 'user', 'system')),
    decided_at TIMESTAMP NOT NULL,             -- when this mapping was decided
    reversed_at TIMESTAMP,                     -- when reversed; NULL while accepted
    reversed_by VARCHAR                        -- domain actor who reversed; NULL while accepted
        CHECK (reversed_by IS NULL OR reversed_by IN ('auto', 'user', 'system')),
    PRIMARY KEY (link_id)
);
