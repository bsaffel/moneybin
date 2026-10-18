"""V076: add the nullable user override for canonical account type."""

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
