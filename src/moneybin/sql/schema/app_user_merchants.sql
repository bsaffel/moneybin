/* Mutable merchant entries: user-created, LLM-created, Plaid-created, migration-created.
   Replaces today's app.merchants table. Exposed alongside seeds via the app.merchants view. */
CREATE TABLE IF NOT EXISTS app.user_merchants (
    merchant_id VARCHAR PRIMARY KEY, -- 12-char UUID hex from uuid.uuid4().hex[:12]
    raw_pattern VARCHAR NOT NULL, -- match pattern against normalized description
    match_type VARCHAR NOT NULL, -- 'exact' | 'contains' | 'regex'
    canonical_name VARCHAR NOT NULL, -- display name; LLM-proposed for created_by='ai'
    category VARCHAR, -- default category (joined to app.categories)
    subcategory VARCHAR, -- default subcategory
    created_by VARCHAR NOT NULL, -- 'user' | 'ai' | 'plaid' | 'migration'
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP -- when this entry was added
);
