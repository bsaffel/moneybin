"""V048: create raw.exchange_rates.

The provider cache behind ``multi-currency.md``'s display conversion: every
reference rate a feed publishes, stored once so a conversion never depends on
the network being reachable twice. Append-only — a published rate for a date is
a historical fact. A user who disagrees with one writes to
``app.exchange_rate_overrides``, which outranks every row here.

Fresh installs get the table from ``raw_exchange_rates.sql``; this migration is
the existing-DB path (database-migration.md dual-path). Pure additive DDL, so
``CREATE TABLE IF NOT EXISTS`` is the whole of it.
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)

_CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS raw.exchange_rates (
    from_currency VARCHAR NOT NULL,
    to_currency VARCHAR NOT NULL,
    rate_date DATE NOT NULL,
    rate DECIMAL(18, 8) NOT NULL CHECK (rate > 0),
    source_type VARCHAR NOT NULL,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (from_currency, to_currency, rate_date, source_type)
)
"""

_TABLE_COMMENT = (
    "COMMENT ON TABLE raw.exchange_rates IS "
    "'Append-only cache of provider reference rates; a user override outranks "
    "every row and no row here is ever edited'"
)

_COLUMN_COMMENTS: list[tuple[str, str]] = [
    ("from_currency", "ISO 4217, upper; the currency being converted out of"),
    ("to_currency", "ISO 4217, upper; the currency being converted into"),
    (
        "rate_date",
        "The business day the provider published this rate for, which may "
        "precede the date requested",
    ),
    (
        "rate",
        "Multiply a from_currency amount by this to get to_currency; must be positive",
    ),
    (
        "source_type",
        "The provider that published it; in the key so two feeds can both "
        "answer one pair and date",
    ),
    ("loaded_at", "When this record was inserted locally"),
]


def migrate(conn: object) -> None:
    """Create raw.exchange_rates. Idempotent."""
    logger.debug("V048: creating raw.exchange_rates")
    conn.execute(_CREATE_TABLE_SQL)  # type: ignore[union-attr]
    conn.execute(_TABLE_COMMENT)  # type: ignore[union-attr]
    for column, comment in _COLUMN_COMMENTS:
        escaped = comment.replace("'", "''")
        conn.execute(  # type: ignore[union-attr]
            f"COMMENT ON COLUMN raw.exchange_rates.{column} IS '{escaped}'"  # noqa: S608  # code-supplied column/comment constants, not user input
        )
