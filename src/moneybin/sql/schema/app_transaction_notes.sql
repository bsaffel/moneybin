/* Free-form user notes on individual transactions; one note per transaction */
-- Query examples for the LLM: see src/moneybin/services/schema_catalog.py (EXAMPLES dict)
CREATE TABLE IF NOT EXISTS app.transaction_notes (
    transaction_id VARCHAR PRIMARY KEY, -- Foreign key to core.fct_transactions; one note per transaction
    note VARCHAR NOT NULL, -- Free-form text note added by the user about this transaction
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP -- Timestamp when this note was created
);
