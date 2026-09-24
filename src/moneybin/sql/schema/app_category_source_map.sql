/* Provider category-code → canonical MoneyBin category. User extensions/overrides; seed defaults live in seeds.category_source_map, unioned via core.bridge_category_source_map. */
CREATE TABLE IF NOT EXISTS app.category_source_map (
    source_type VARCHAR NOT NULL, -- Taxonomy namespace: a provider tag (plaid, mx, simplefin) or a source_origin slug for imported mappings
    source_category_code VARCHAR NOT NULL, -- Provider category code, stored verbatim
    source_subcategory_code VARCHAR NOT NULL DEFAULT '', -- Second half of the source's (category, subcategory) key; '' is the sentinel for "no subcategory" (DuckDB PKs reject NULL) — never a distinct real value
    code_level VARCHAR NOT NULL DEFAULT 'detailed' CHECK (code_level IN ('detailed', 'primary')), -- 'detailed' or 'primary'; detailed wins in reverse lookup
    category_id VARCHAR, -- FK to core.dim_categories.category_id (may be a user category); NULL means the user marked this source label as ignored -- it categorizes nothing
    source_taxonomy_version VARCHAR, -- Provider taxonomy revision this row was curated against
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, -- When the user added this mapping
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, -- Last change to this mapping
    PRIMARY KEY (source_type, source_category_code, source_subcategory_code)
);
