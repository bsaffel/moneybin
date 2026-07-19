"""V037: create persisted import-preview trust state."""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)

_CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS app.import_previews (
    preview_id VARCHAR PRIMARY KEY,
    file_path VARCHAR NOT NULL,
    file_sha256 VARCHAR NOT NULL CHECK (length(file_sha256) = 64),
    file_size_bytes BIGINT NOT NULL CHECK (file_size_bytes >= 0),
    channel VARCHAR NOT NULL CHECK (channel IN ('tabular', 'pdf', 'ofx')),
    snapshot_json JSON NOT NULL,
    issued_at TIMESTAMP NOT NULL,
    expires_at TIMESTAMP NOT NULL,
    consumed_at TIMESTAMP,
    import_id VARCHAR,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CHECK (expires_at > issued_at),
    CHECK (import_id IS NULL OR consumed_at IS NOT NULL)
)
"""

_CREATE_SNAPSHOTS_SQL = """
CREATE TABLE IF NOT EXISTS raw.import_preview_snapshots (
    preview_id VARCHAR PRIMARY KEY,
    source_bytes BLOB NOT NULL,
    created_at TIMESTAMP NOT NULL
)
"""


def migrate(conn: object) -> None:
    """Create additive import-preview metadata and encrypted byte storage."""
    logger.debug("V037: CREATE TABLE IF NOT EXISTS app.import_previews")
    conn.execute(_CREATE_TABLE_SQL)  # type: ignore[union-attr]
    conn.execute(_CREATE_SNAPSHOTS_SQL)  # type: ignore[union-attr]
    logger.debug("V037: import preview metadata and snapshots ready")
