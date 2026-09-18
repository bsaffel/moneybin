"""V046: add file_sha256 to raw.import_log.

Re-import detection keyed on the source path alone, so a second download saved
as ``statement (1).pdf`` — or the same file moved out of Downloads — imported
again as a fresh batch. Additive, nullable: batches predating this column keep
NULL and stay matchable by path.

MB-255: a later migration (V066) moves the table to ``app.import_log``, and
the schema file a fresh install runs already declares ``file_sha256`` on the
new table. So on a fresh install ``raw.import_log`` never exists — the table
guard below skips rather than ALTERing a table that isn't there, the same
precedent V003 set for ``raw.ofx_institutions`` (MB-256). Already-upgraded
databases still have ``raw.import_log`` at this point in the ladder (V066
hasn't run yet) and take the ALTER exactly as before.
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


def migrate(conn: object) -> None:
    """Add raw.import_log.file_sha256, if raw.import_log still exists. Idempotent."""
    exists = conn.execute(  # type: ignore[union-attr]
        "SELECT 1 FROM information_schema.tables "
        "WHERE table_schema = 'raw' AND table_name = 'import_log'"
    ).fetchone()
    if exists is None:
        logger.debug(
            "V046: raw.import_log does not exist (fresh install already has "
            "app.import_log.file_sha256), skipping"
        )
        return

    logger.debug("V046: ADD COLUMN IF NOT EXISTS raw.import_log.file_sha256")
    conn.execute(  # type: ignore[union-attr]
        "ALTER TABLE raw.import_log ADD COLUMN IF NOT EXISTS file_sha256 VARCHAR"
    )
    conn.execute(  # type: ignore[union-attr]
        "COMMENT ON COLUMN raw.import_log.file_sha256 IS "
        "'SHA-256 over the source file bytes; identifies the same document across "
        "renames and moves. NULL for batches imported before this column existed'"
    )
