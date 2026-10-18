"""V077: append a safe derived label without rewriting retained account evidence."""

from __future__ import annotations


def migrate(conn: object) -> None:
    """Older observations retain NULL; original provider names never become labels."""
    conn.execute(  # type: ignore[attr-defined]
        "ALTER TABLE raw.simplefin_accounts ADD COLUMN IF NOT EXISTS account_label VARCHAR"
    )
    conn.execute(  # type: ignore[attr-defined]
        "COMMENT ON COLUMN raw.simplefin_accounts.account_label IS "
        "'Display-ready name using the shared account-number masker; original name retained'"
    )
