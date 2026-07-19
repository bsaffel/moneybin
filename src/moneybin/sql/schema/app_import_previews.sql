/* Persisted trust state for staged imports. A preview binds confirmation to one immutable file snapshot and one canonical detection result; every lifecycle mutation is audited through ImportPreviewsRepo. */
CREATE TABLE IF NOT EXISTS app.import_previews (
    preview_id VARCHAR PRIMARY KEY, -- Opaque truncated UUID (uuid4().hex[:12]); never derived from file content
    file_path VARCHAR NOT NULL, -- Canonical absolute path used only to re-open the exact previewed file
    file_sha256 VARCHAR NOT NULL CHECK (length(file_sha256) = 64), -- Full SHA-256 of previewed file bytes for exact unchanged-file rebinding
    file_size_bytes BIGINT NOT NULL CHECK (file_size_bytes >= 0), -- Previewed byte length; defense-in-depth alongside SHA-256
    channel VARCHAR NOT NULL CHECK (channel IN ('tabular', 'pdf', 'ofx')), -- Import channel whose canonical detection snapshot is stored
    snapshot_json JSON NOT NULL, -- Complete canonical preview/detection payload required to explain and rebind confirmation
    issued_at TIMESTAMP NOT NULL, -- UTC time at which this preview became confirmable
    expires_at TIMESTAMP NOT NULL, -- UTC deadline; confirmation refuses at or after this timestamp
    consumed_at TIMESTAMP, -- UTC time the preview entered one successful caller-owned import transaction; NULL while reusable after rollback
    import_id VARCHAR, -- Resulting raw.import_log id, set in the same transaction as consumption and import completion
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, -- Latest lifecycle mutation watermark for audit-coverage checks
    CHECK (expires_at > issued_at),
    CHECK (import_id IS NULL OR consumed_at IS NOT NULL)
);
