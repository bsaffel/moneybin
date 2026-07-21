"""V041: create app.export_destinations.

Saved export destinations are application configuration. Local destinations
point to a visible directory; Google Sheets destinations point to a workbook
and own a managed-tab namespace. The database enforces those mutually
exclusive shapes so every consumer sees a complete destination.
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)

_CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS app.export_destinations (
    destination_id VARCHAR PRIMARY KEY,
    name VARCHAR NOT NULL UNIQUE,
    kind VARCHAR NOT NULL CHECK (kind IN ('local', 'sheets')),
    local_path VARCHAR,
    spreadsheet_id VARCHAR,
    managed_tab_prefix VARCHAR,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CHECK (
        (kind = 'local' AND local_path IS NOT NULL
         AND spreadsheet_id IS NULL AND managed_tab_prefix IS NULL)
        OR
        (kind = 'sheets' AND local_path IS NULL
         AND spreadsheet_id IS NOT NULL AND managed_tab_prefix IS NOT NULL)
    )
)
"""

_COLUMN_COMMENTS: list[tuple[str, str]] = [
    (
        "destination_id",
        "Opaque truncated UUID (uuid4().hex[:12]) identifying this saved destination",
    ),
    (
        "name",
        "User-facing destination name; unique across local and Google Sheets kinds",
    ),
    (
        "kind",
        "Destination type: 'local' for a visible directory or 'sheets' for a workbook",
    ),
    (
        "local_path",
        "Visible local export directory; required only when kind='local'",
    ),
    (
        "spreadsheet_id",
        "Google Sheets workbook id; required only when kind='sheets'",
    ),
    (
        "managed_tab_prefix",
        "Prefix for tabs managed by MoneyBin in this workbook; required only for Sheets",
    ),
    ("created_at", "Destination configuration creation timestamp"),
    (
        "updated_at",
        "Last destination-configuration mutation timestamp",
    ),
]


def migrate(conn: object) -> None:
    """Create app.export_destinations and apply catalog comments."""
    logger.debug("V041: CREATE TABLE IF NOT EXISTS app.export_destinations")
    conn.execute(_CREATE_TABLE_SQL)  # type: ignore[union-attr]

    for column, comment in _COLUMN_COMMENTS:
        escaped = comment.replace("'", "''")
        conn.execute(  # type: ignore[union-attr]
            f"COMMENT ON COLUMN app.export_destinations.{column} "  # noqa: S608  # static identifier + escaped literal
            f"IS '{escaped}'"
        )

    logger.debug("V041: app.export_destinations ready")
