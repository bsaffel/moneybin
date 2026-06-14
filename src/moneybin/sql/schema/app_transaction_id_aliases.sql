/* Transaction-id forwarding map (M1S, account-identity-resolution.md Decision 4 / ADR-015).
   Append-only old_id -> new_id pointers, written only when a dedup merge changes a transaction_id (the Plaid
   pending_transaction_id model). SQL, agent, external, and curation-FK references resolve old->new through it,
   so a held id stays resolvable -- never an orphan -- even though it is not byte-stable across merges.
   old_transaction_id is the PK: each old id forwards to exactly one new id. */
CREATE TABLE IF NOT EXISTS app.transaction_id_aliases (
    old_transaction_id VARCHAR NOT NULL,       -- the superseded transaction_id (forwards from)
    new_transaction_id VARCHAR NOT NULL,       -- the current canonical transaction_id (forwards to)
    created_at TIMESTAMP NOT NULL,             -- when this alias was recorded
    PRIMARY KEY (old_transaction_id)
);
