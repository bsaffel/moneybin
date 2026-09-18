"""V066: move import_log from raw to app (MB-255).

``import_log`` is MoneyBin-written bookkeeping with status transitions
(``importing`` -> ``complete`` / ``partial`` / ``failed`` / ``reverted``) whose
history (e.g. ``reverted_at``) cannot be rebuilt by re-importing files -- by
``docs/specs/architecture-shared-primitives.md``'s own definitions of ``raw``
("untouched data … re-importable from the original file") and ``app``
("user-state … not derivable from raw"), it belongs in ``app``. Its siblings
for the same entity, ``app.imports`` and ``app.import_previews``, already live
there.

``init_schemas`` (``sql/schema/app_import_log.sql``) always runs before
migrations (``Database.__init__``), so by the time this runs, an
``app.import_log`` table already exists — freshly created and empty on a
database that never had ``raw.import_log``, or created empty alongside a
still-populated ``raw.import_log`` on a database upgrading across this
change. This migration only has work to do in the second case.

Also reconciles V046's stored checksum. V046's body gained a table-existence
guard in this same change (a fresh install no longer creates
``raw.import_log`` at all, so the old unconditional ``ALTER TABLE`` would
fail it) — same precedent as V064 reconciling V003's checksum for the
``raw.ofx_institutions`` drop. Recomputed from V046's current on-disk
content and overwritten; a no-op on a fresh install, where V046 just ran
with that same content.
"""

from __future__ import annotations

import hashlib
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

# Deliberately hardcoded — migrations are frozen historical artifacts and must
# not import live module constants (e.g. moneybin.migrations.short_hash)
# whose meaning could shift. Matches V064's precedent for the same reason.
_MIGRATIONS_DIR = Path(__file__).resolve().parent
_V046_FILENAME = "V046__add_file_sha256_to_import_log.py"

# Explicit column list (not `SELECT *`) so a future column added to one side
# without the other fails loudly here rather than silently misaligning by
# position. Matches raw.import_log's shape as of V046 (file_sha256 last).
_COLUMNS = (
    "import_id",
    "source_file",
    "source_type",
    "source_origin",
    "format_name",
    "format_source",
    "account_names",
    "status",
    "rows_total",
    "rows_imported",
    "rows_rejected",
    "rows_skipped_trailing",
    "rejection_details",
    "detection_confidence",
    "number_format",
    "date_format",
    "sign_convention",
    "balance_validated",
    "started_at",
    "completed_at",
    "reverted_at",
    "file_sha256",
)


def migrate(conn: object) -> None:
    """Copy raw.import_log rows into app.import_log, then drop raw.import_log."""
    exists = conn.execute(  # type: ignore[union-attr]
        "SELECT 1 FROM information_schema.tables "
        "WHERE table_schema = 'raw' AND table_name = 'import_log'"
    ).fetchone()
    if exists is None:
        logger.debug("V066: raw.import_log does not exist, nothing to move")
        return

    column_list = ", ".join(_COLUMNS)
    conn.execute(  # type: ignore[union-attr]  # allowlisted literal, no user input
        f"INSERT INTO app.import_log ({column_list}) "  # noqa: S608  # column_list built from the hardcoded _COLUMNS tuple, not user input
        f"SELECT {column_list} FROM raw.import_log"
    )
    conn.execute("DROP TABLE raw.import_log")  # type: ignore[union-attr]
    logger.info("V066: moved raw.import_log to app.import_log")

    v046_path = _MIGRATIONS_DIR / _V046_FILENAME
    if v046_path.exists():
        content = v046_path.read_bytes()
        checksum = hashlib.sha256(content).hexdigest()
        content_hash = checksum[:16]
        conn.execute(  # type: ignore[union-attr]
            "UPDATE app.schema_migrations SET checksum = ?, content_hash = ? "
            "WHERE version = 46",
            [checksum, content_hash],
        )
        logger.info("V066: reconciled V046 checksum after its table-guard edit")
