"""V046: add file_sha256 to raw.import_log.

Re-import detection keyed on the source path alone, so a second download saved
as ``statement (1).pdf`` — or the same file moved out of Downloads — imported
again as a fresh batch. Additive, nullable: batches predating this column keep
NULL and stay matchable by path.
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


def migrate(conn: object) -> None:
    """Add raw.import_log.file_sha256. Idempotent."""
    logger.debug("V046: ADD COLUMN IF NOT EXISTS raw.import_log.file_sha256")
    conn.execute(  # type: ignore[union-attr]
        "ALTER TABLE raw.import_log ADD COLUMN IF NOT EXISTS file_sha256 VARCHAR"
    )
    conn.execute(  # type: ignore[union-attr]
        "COMMENT ON COLUMN raw.import_log.file_sha256 IS "
        "'SHA-256 over the source file bytes; identifies the same document across "
        "renames and moves. NULL for batches imported before this column existed'"
    )
