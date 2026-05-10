/* Split children of a parent gold transaction. The sum of child amounts
   should equal parent.amount but is not strictly enforced — curator workflow
   is iterative. The CLI warns when sums are unbalanced after each add/remove. */
CREATE TABLE IF NOT EXISTS app.transaction_splits (
    split_id       VARCHAR PRIMARY KEY,                          -- Truncated UUID4 (12 hex)
    transaction_id VARCHAR NOT NULL,                             -- Foreign key to core.fct_transactions; the parent
    amount         DECIMAL(18, 2) NOT NULL,                      -- Signed; sum across children should equal parent.amount but is not strictly enforced
    category       VARCHAR,                                      -- References category taxonomy (string for now; migrates with category_id introduction)
    subcategory    VARCHAR,                                      -- References subcategory taxonomy
    note           VARCHAR,                                      -- Optional per-split note
    ord            INTEGER NOT NULL DEFAULT 0,                   -- Display order; ties broken by split_id
    created_at     TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, -- When the split was added
    created_by     VARCHAR NOT NULL                              -- 'cli' or 'mcp'
);
CREATE INDEX IF NOT EXISTS idx_transaction_splits_txn ON app.transaction_splits(transaction_id);
