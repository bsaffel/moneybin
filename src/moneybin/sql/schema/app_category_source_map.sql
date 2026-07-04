/* Provider category-code → canonical MoneyBin category. User extensions/overrides; seed defaults live in seeds.category_source_map, unioned via core.bridge_category_source_map. */
CREATE TABLE IF NOT EXISTS app.category_source_map (
    source_type VARCHAR NOT NULL, -- Provider provenance (plaid, mx, simplefin)
    source_category_code VARCHAR NOT NULL, -- Provider category code, stored verbatim
    code_level VARCHAR NOT NULL DEFAULT 'detailed' CHECK (code_level IN ('detailed', 'primary')), -- 'detailed' or 'primary'; detailed wins in reverse lookup
    category_id VARCHAR NOT NULL, -- FK to core.dim_categories.category_id (may be a user category)
    source_taxonomy_version VARCHAR, -- Provider taxonomy revision this row was curated against
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, -- When the user added this mapping
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, -- Last change to this mapping
    PRIMARY KEY (source_type, source_category_code)
);
