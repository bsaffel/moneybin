"""V071: append a safe institution label without rewriting retained evidence."""

from __future__ import annotations


def migrate(conn: object) -> None:
    """Older observations remain nullable; the original response is unchanged."""
    conn.execute(  # type: ignore[attr-defined]
        "ALTER TABLE raw.simplefin_accounts ADD COLUMN IF NOT EXISTS institution_label VARCHAR"
    )
    conn.execute(  # type: ignore[attr-defined]
        "COMMENT ON COLUMN raw.simplefin_accounts.institution_label IS "
        "'Display-ready institution using the shared masker; response remains original'"
    )
