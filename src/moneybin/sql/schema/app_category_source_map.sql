/* Category-source mapping → canonical MoneyBin category. User curations and provider overrides; seed defaults live in seeds.category_source_map, unioned via core.bridge_category_source_map. */
CREATE TABLE IF NOT EXISTS app.category_source_map (
    source_type VARCHAR NOT NULL, -- The transaction row's own source_type: plaid for provider rows; csv, tsv, excel, parquet, feather, pdf, or manual for imported rows. Never a provider alias or an origin slug
    source_origin VARCHAR NOT NULL, -- The row's source_origin for an imported mapping (e.g. chase_credit; '' when the import carries an empty slug). '' on a provider row means provider-wide
    source_category_code VARCHAR NOT NULL, -- Source category text, stored verbatim
    source_subcategory_code VARCHAR NOT NULL DEFAULT '', -- Second half of the source's (category, subcategory) key; '' is the sentinel for "no subcategory" (DuckDB PKs reject NULL) — never a distinct real value
    code_level VARCHAR NOT NULL DEFAULT 'detailed' CHECK (code_level IN ('detailed', 'primary')), -- 'detailed' or 'primary'; detailed wins in reverse lookup
    category_id VARCHAR NOT NULL, -- FK to core.dim_categories.category_id (may be a user category)
    source_taxonomy_version VARCHAR, -- Provider taxonomy revision this row was curated against
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, -- When the user added this mapping
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, -- Last change to this mapping
    PRIMARY KEY (source_type, source_origin, source_category_code, source_subcategory_code)
);
