/* Slug-flavored tags applied to individual transactions. Pattern
   ^[a-z0-9_-]+(:[a-z0-9_-]+)?$ is enforced at the service layer. Composite
   primary key prevents duplicate (transaction, tag) pairs. */
CREATE TABLE IF NOT EXISTS app.transaction_tags (
    transaction_id VARCHAR NOT NULL,                             -- Foreign key to core.fct_transactions
    tag            VARCHAR NOT NULL,                             -- 'namespace:value' or bare 'value'; pattern enforced at service layer
    applied_at     TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, -- When this tag was applied
    applied_by     VARCHAR NOT NULL,                             -- 'cli', 'mcp', or future user identity
    PRIMARY KEY (transaction_id, tag)
);
CREATE INDEX IF NOT EXISTS idx_transaction_tags_tag ON app.transaction_tags(tag);
