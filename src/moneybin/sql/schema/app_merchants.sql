-- Merchant name normalization and category cache
-- Dual purpose: cleans messy descriptions AND caches merchant-to-category mappings
CREATE TABLE IF NOT EXISTS app.merchants (
    merchant_id VARCHAR PRIMARY KEY,
    raw_pattern VARCHAR NOT NULL,
    match_type VARCHAR DEFAULT 'contains',
    canonical_name VARCHAR NOT NULL,
    category VARCHAR,
    subcategory VARCHAR,
    created_by VARCHAR DEFAULT 'ai',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
