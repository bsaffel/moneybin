"""V043: create app.security_price_overrides.

Phase C.2 of ``investments-price-feeds.md``: a user can set a price by hand for
any security and date, including securities no feed covers, and a later provider
fetch never overwrites that mark. Marks are the user-authored third of
``core.fct_security_prices``' union, beside provider observations and
trade-implied prices.

Fresh installs get the table from ``app_security_price_overrides.sql``; this
migration is the existing-DB path (database-migration.md dual-path). Pure
additive DDL, so ``CREATE TABLE IF NOT EXISTS`` is the whole of it.
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)

_CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS app.security_price_overrides (
    security_id VARCHAR NOT NULL,
    price_date DATE NOT NULL,
    quote_currency VARCHAR NOT NULL,
    close DECIMAL(28, 10) NOT NULL CHECK (close > 0),
    note VARCHAR,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (security_id, price_date, quote_currency)
)
"""

_TABLE_COMMENT = (
    "COMMENT ON TABLE app.security_price_overrides IS "
    "'User price marks; rank above every provider for their own date and are "
    "never overwritten by a fetch'"
)

_COLUMN_COMMENTS: list[tuple[str, str]] = [
    (
        "security_id",
        "FK to app.securities; the canonical id, never a provider key",
    ),
    (
        "price_date",
        "The date this mark applies to; per-date scoping is what lets a mark "
        "survive re-fetch without suppressing newer closes",
    ),
    (
        "quote_currency",
        "ISO 4217; in the key so a dual-quoted security keeps both marks",
    ),
    (
        "close",
        "The user's price for one unit in quote_currency; must be positive",
    ),
    ("note", "Why the user set it; optional"),
    ("created_at", "When this mark was first entered"),
    ("updated_at", "When this mark last changed value"),
]


def migrate(conn: object) -> None:
    """Create app.security_price_overrides. Idempotent."""
    logger.debug("V043: creating app.security_price_overrides")
    conn.execute(_CREATE_TABLE_SQL)  # type: ignore[union-attr]
    conn.execute(_TABLE_COMMENT)  # type: ignore[union-attr]
    for column, comment in _COLUMN_COMMENTS:
        escaped = comment.replace("'", "''")
        conn.execute(  # type: ignore[union-attr]
            f"COMMENT ON COLUMN app.security_price_overrides.{column} IS '{escaped}'"  # noqa: S608  # code-supplied column/comment constants, not user input
        )
