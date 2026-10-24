"""V077: add canonical type override and classification change times."""

from __future__ import annotations


def migrate(conn: object) -> None:
    """Append the override without deriving or replacing any source evidence."""
    conn.execute(  # type: ignore[attr-defined]
        "ALTER TABLE app.account_settings ADD COLUMN IF NOT EXISTS account_type VARCHAR"
    )
    conn.execute(  # type: ignore[attr-defined]
        "COMMENT ON COLUMN app.account_settings.account_type IS "
        "'Canonical user type override; NULL uses merged source evidence without changing subtype'"
    )

    for column in ("account_type_changed_at", "account_subtype_changed_at"):
        conn.execute(  # type: ignore[attr-defined]
            f"ALTER TABLE app.account_settings ADD COLUMN IF NOT EXISTS {column} TIMESTAMPTZ"
        )
        conn.execute(  # type: ignore[attr-defined]
            f"COMMENT ON COLUMN app.account_settings.{column} IS "
            "'Last classification edit, clear, merge or undo; accounting eligibility freshness'"
        )
    # As in V075, backfill only after committing the catalog appends.
    conn.execute("COMMIT")  # type: ignore[attr-defined]
    conn.execute("BEGIN TRANSACTION")  # type: ignore[attr-defined]
    for column in ("account_type_changed_at", "account_subtype_changed_at"):
        conn.execute(  # type: ignore[attr-defined]
            f"UPDATE app.account_settings SET {column} = updated_at "  # noqa: S608  # literal column names
            f"WHERE {column} IS NULL"
        )
