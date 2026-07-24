/* Exact staged-import bytes. The database is encrypted at rest; bytes never enter app audit images and are deleted in the same transaction as successful preview consumption. */
CREATE TABLE IF NOT EXISTS raw.import_preview_snapshots (
    preview_id VARCHAR PRIMARY KEY, -- Opaque app.import_previews handle
    source_bytes BLOB NOT NULL, -- Exact immutable source bytes parsed at preview and loaded at confirm
    created_at TIMESTAMP NOT NULL -- UTC insertion time for orphan cleanup
);
