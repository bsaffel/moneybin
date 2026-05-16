/* Mutable merchant entries: user-created, LLM-created, Plaid-created, migration-created.
   Replaces the legacy app.merchants table. Exposed via the
   core.dim_merchants resolved-dim view. */
CREATE TABLE IF NOT EXISTS app.user_merchants (
    merchant_id VARCHAR PRIMARY KEY, -- 12-char UUID hex from uuid.uuid4().hex[:12]
    raw_pattern VARCHAR, -- match pattern; NULL when merchant is exemplar-only (match_type='oneOf')
    match_type VARCHAR NOT NULL DEFAULT 'oneOf', -- 'exact' | 'contains' | 'regex' | 'oneOf'
    canonical_name VARCHAR NOT NULL, -- display name; LLM-proposed for created_by='ai'
    category VARCHAR, -- default category (joined to core.dim_categories)
    subcategory VARCHAR, -- default subcategory
    created_by VARCHAR NOT NULL, -- 'user' | 'ai' | 'rule' | 'plaid' | 'migration'
    exemplars VARCHAR[] DEFAULT [], -- exact match_text values for oneOf set-membership lookup
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, -- when this entry was added
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP -- Latest of all per-row input timestamps contributing to this row's current values. Set on UPDATE by service writes.
);
