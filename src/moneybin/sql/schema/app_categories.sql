-- Category definitions with hierarchy
-- Seeded with Plaid PFCv2 taxonomy; users can add custom categories
CREATE TABLE IF NOT EXISTS app.categories (
    category_id VARCHAR PRIMARY KEY,
    category VARCHAR NOT NULL,
    subcategory VARCHAR,
    description VARCHAR,
    is_default BOOLEAN DEFAULT false,
    is_active BOOLEAN DEFAULT true,
    plaid_detailed VARCHAR,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (category, subcategory)
);
