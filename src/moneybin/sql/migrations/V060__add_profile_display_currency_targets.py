"""V060: persist the currencies a profile explicitly reads reports in.

DuckDB rejects ``ADD COLUMN ... NOT NULL DEFAULT`` together. Follow V033's
add/default → commit → tighten sequence so existing rows receive the empty
target collection before the constraint is created.
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)

#: Mirrors the trailing `--` comment on this column in
#: app_profile_settings.sql. `init_schemas()` applies that schema-file
#: comment (and the `[class: currency]` classification sigil) before
#: migrations run, so it silently skips a column this migration hasn't
#: added yet — set both directly here so an upgraded database's catalog
#: never goes a full startup cycle without either (V044 applies its own
#: column comments the same way).
_COLUMN_COMMENT = "Explicit read targets; empty leaves refresh cost unchanged"


def migrate(conn: object) -> None:
    """Add an empty-by-default target list without changing accounting state."""
    row = conn.execute(  # type: ignore[union-attr]
        """
        SELECT is_nullable
        FROM duckdb_columns()
        WHERE schema_name = 'app'
          AND table_name = 'profile_settings'
          AND column_name = 'display_currency_targets'
        """
    ).fetchone()
    needs_tighten = False
    if row is None:
        logger.debug("V060: ADD COLUMN app.profile_settings.display_currency_targets")
        conn.execute(  # type: ignore[union-attr]
            "ALTER TABLE app.profile_settings "
            "ADD COLUMN display_currency_targets VARCHAR[] DEFAULT []"
        )
        needs_tighten = True
        escaped = _COLUMN_COMMENT.replace("'", "''")
        conn.execute(  # type: ignore[union-attr]
            "COMMENT ON COLUMN app.profile_settings.display_currency_targets "
            f"IS '{escaped}'"
        )
        from moneybin.privacy.comment_sync import sync_classification_comments

        sync_classification_comments(conn)  # type: ignore[arg-type]
    elif row[0]:
        logger.debug(
            "V060: backfilling nullable app.profile_settings.display_currency_targets"
        )
        conn.execute(  # type: ignore[union-attr]
            "UPDATE app.profile_settings "
            "SET display_currency_targets = [] "
            "WHERE display_currency_targets IS NULL"
        )
        needs_tighten = True
    if needs_tighten:
        conn.execute("COMMIT")  # type: ignore[union-attr]
        conn.execute("BEGIN TRANSACTION")  # type: ignore[union-attr]
        logger.debug(
            "V060: ALTER COLUMN app.profile_settings.display_currency_targets SET NOT NULL"
        )
        conn.execute(  # type: ignore[union-attr]
            "ALTER TABLE app.profile_settings "
            "ALTER COLUMN display_currency_targets SET NOT NULL"
        )
