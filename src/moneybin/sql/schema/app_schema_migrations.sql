/* Schema migration history — one row per applied migration file */
CREATE TABLE IF NOT EXISTS app.schema_migrations (
    version INTEGER PRIMARY KEY,           -- monotonic integer from V### filename prefix
    filename VARCHAR NOT NULL,             -- full migration filename including extension
    checksum VARCHAR NOT NULL,             -- lowercase hex SHA-256 of file contents at apply time
    success BOOLEAN NOT NULL DEFAULT TRUE, -- FALSE if migration failed mid-execution
    execution_ms INTEGER,                  -- migration duration in milliseconds
    applied_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP -- when applied
);
