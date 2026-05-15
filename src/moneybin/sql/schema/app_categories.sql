/* User-created spending categories. Seed defaults live in seeds.categories (managed by SQLMesh) and are exposed alongside these via the app.categories view */
CREATE TABLE IF NOT EXISTS app.user_categories (
    category_id VARCHAR PRIMARY KEY, -- 12-char UUID hex assigned at creation
    category VARCHAR NOT NULL, -- Top-level spending category name
    subcategory VARCHAR, -- Subcategory within the parent category; NULL for top-level-only entries
    description VARCHAR, -- Human-readable description of what transactions belong in this category
    is_active BOOLEAN DEFAULT true, -- False to soft-delete a user category without losing existing categorizations
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, -- Timestamp when this category was added
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, -- Latest of all per-row input timestamps contributing to this row's current values. Set on UPDATE by service writes.
    UNIQUE (category, subcategory)
);

/* User overrides on default (seeded) categories — the only mutation users can make to defaults is deactivation */
CREATE TABLE IF NOT EXISTS app.category_overrides (
    category_id VARCHAR PRIMARY KEY, -- Matches seeds.categories.category_id
    is_active BOOLEAN NOT NULL, -- False to hide a default category from the taxonomy
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP -- Timestamp when this override was last changed
);
