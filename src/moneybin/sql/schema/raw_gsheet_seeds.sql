/* Row-level storage for the seed (catch-all) gsheet adapter. JSON column holds the original sheet row data; per-connection auto-generated views in raw.gsheet_<alias> project the JSON paths into typed columns for ergonomic SQL access. Diff semantics: each pull recomputes row_hash for every current row — matches are no-ops (or undelete), missing hashes get deleted_from_source_at set, new hashes get inserted. */
CREATE TABLE IF NOT EXISTS raw.gsheet_seeds (
    connection_id VARCHAR NOT NULL, -- FK reference to app.gsheet_connections.connection_id (logical FK; not enforced cross-schema)
    spreadsheet_id VARCHAR NOT NULL, -- Denormalized from app.gsheet_connections for query convenience
    sheet_gid INTEGER NOT NULL, -- Denormalized tab gid
    row_number INTEGER NOT NULL, -- 1-based row index in the source sheet (informational; may shift on edits)
    row_hash VARCHAR NOT NULL, -- Content hash of the row's cell values (stable identity for the seed adapter)
    data JSON NOT NULL, -- JSON object: column-name → cell-value, captured verbatim from the sheet
    deleted_from_source_at TIMESTAMP NULL, -- Set when the row is observed absent on a subsequent pull (soft-delete; mirrors the raw.tabular_transactions pattern)
    import_id VARCHAR NOT NULL, -- Import run that wrote this row (FK to import_runs)
    loaded_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, -- Timestamp this row was first observed (does NOT update on subsequent pulls)
    PRIMARY KEY (connection_id, row_hash)
);
