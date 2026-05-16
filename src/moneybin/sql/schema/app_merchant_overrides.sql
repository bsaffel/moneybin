/* User overrides on seed merchant entries. Mirrors app.category_overrides. */
CREATE TABLE IF NOT EXISTS app.merchant_overrides (
    merchant_id VARCHAR PRIMARY KEY, -- references seeds.merchants_*.merchant_id
    is_active BOOLEAN NOT NULL, -- false to hide a seed merchant
    category VARCHAR, -- override default category (NULL = inherit seed)
    subcategory VARCHAR, -- override default subcategory
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP -- when this override was last changed
);
