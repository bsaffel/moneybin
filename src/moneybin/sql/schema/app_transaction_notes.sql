/* Free-form user notes on individual transactions. Multi-note shape: each
   transaction may carry zero or more notes; each note has its own primary
   key. Migrated from the prior single-note shape by V007. */
CREATE TABLE IF NOT EXISTS app.transaction_notes (
    note_id        VARCHAR PRIMARY KEY,                          -- Truncated UUID4 (12 hex); unique per note
    transaction_id VARCHAR NOT NULL,                             -- Foreign key to core.fct_transactions
    text           VARCHAR NOT NULL,                             -- Note body; max 2000 chars (service-layer enforced)
    author         VARCHAR NOT NULL,                             -- 'cli', 'mcp', 'legacy' (migrated rows), or future user identity
    created_at     TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP  -- When the note was added
);
CREATE INDEX IF NOT EXISTS idx_transaction_notes_txn ON app.transaction_notes(transaction_id);
