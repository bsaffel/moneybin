/* Spending category definitions; seeded from Plaid PFCv2 taxonomy and extended with user-defined categories */
CREATE TABLE IF NOT EXISTS app.categories (
    category_id VARCHAR PRIMARY KEY, -- Slug-style identifier derived from category and subcategory, e.g. food_and_drink.coffee
    category VARCHAR NOT NULL, -- Top-level spending category name, e.g. Food and Drink
    subcategory VARCHAR, -- Subcategory within the parent category, e.g. Coffee; NULL for top-level-only entries
    description VARCHAR, -- Human-readable description of what transactions belong in this category
    is_default BOOLEAN DEFAULT false, -- True for categories seeded from Plaid PFCv2 taxonomy; false for user-defined categories
    is_active BOOLEAN DEFAULT true, -- False for categories that have been soft-deleted or disabled
    plaid_detailed VARCHAR, -- Corresponding Plaid PFCv2 detailed category name; NULL for user-defined categories
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, -- Timestamp when this category was added to the table
    UNIQUE (category, subcategory)
);
