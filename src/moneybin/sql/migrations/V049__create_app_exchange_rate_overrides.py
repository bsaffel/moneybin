"""V049: create app.exchange_rate_overrides.

``multi-currency.md`` Requirement 14: a user may correct an auto-fetched
reference rate when the bank's actual rate differs from the ECB mid. The
correction is mutable user-authored state, so it lives in ``app.*`` behind a
``*Repo`` with paired audit (Invariant 10) rather than in the append-only
``raw.exchange_rates`` cache — and the conversion layer prefers it for its own
pair and date.

Fresh installs get the table from ``app_exchange_rate_overrides.sql``; this
migration is the existing-DB path (database-migration.md dual-path). Pure
additive DDL, so ``CREATE TABLE IF NOT EXISTS`` is the whole of it.
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)

_CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS app.exchange_rate_overrides (
    from_currency VARCHAR NOT NULL,
    to_currency VARCHAR NOT NULL,
    rate_date DATE NOT NULL,
    rate DECIMAL(18, 8) NOT NULL CHECK (rate > 0),
    note VARCHAR,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (from_currency, to_currency, rate_date)
)
"""

_TABLE_COMMENT = (
    "COMMENT ON TABLE app.exchange_rate_overrides IS "
    "'User rate corrections; outrank every cached provider rate for their own "
    "pair and date and survive a refetch'"
)

_COLUMN_COMMENTS: list[tuple[str, str]] = [
    ("from_currency", "ISO 4217, upper; the currency being converted out of"),
    ("to_currency", "ISO 4217, upper; the currency being converted into"),
    (
        "rate_date",
        "The business day this correction applies to, not the day it was "
        "entered; per-date scoping is what lets it survive a refetch",
    ),
    (
        "rate",
        "The user's rate; multiply a from_currency amount by this to get "
        "to_currency. Must be positive",
    ),
    ("note", "Why the user overrode the provider rate; optional"),
    ("created_at", "When this override was first entered"),
    ("updated_at", "When this override last changed value"),
]


def migrate(conn: object) -> None:
    """Create app.exchange_rate_overrides. Idempotent."""
    logger.debug("V049: creating app.exchange_rate_overrides")
    conn.execute(_CREATE_TABLE_SQL)  # type: ignore[union-attr]
    conn.execute(_TABLE_COMMENT)  # type: ignore[union-attr]
    for column, comment in _COLUMN_COMMENTS:
        escaped = comment.replace("'", "''")
        conn.execute(  # type: ignore[union-attr]
            f"COMMENT ON COLUMN app.exchange_rate_overrides.{column} IS '{escaped}'"  # noqa: S608  # code-supplied column/comment constants, not user input
        )
