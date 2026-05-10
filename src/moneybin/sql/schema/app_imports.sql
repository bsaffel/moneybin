/* User-applied labels on import batches. One row per labeled import; absence
   means no user-state on that batch. Labels follow the same slug pattern as
   transaction tags but live in their own column because import labels and
   transaction tags are queried independently. */
CREATE TABLE IF NOT EXISTS app.imports (
    import_id  VARCHAR PRIMARY KEY,                          -- Foreign key to raw.import_log.import_id; one row per labeled import
    labels     VARCHAR[],                                    -- LIST(VARCHAR); NULL when no labels; same slug pattern as tags
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, -- When the labels were last updated
    updated_by VARCHAR NOT NULL                              -- 'cli' or 'mcp'
);
