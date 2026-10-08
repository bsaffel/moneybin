"""V071: create raw.exchange_rate_coverage.

The spans a rate provider answered for completely, so the offline market-closure
rule prices a date only when it is proven closed rather than merely unfetched
(``multi-currency.md`` Requirement 13).

Fresh installs get the table from ``raw_exchange_rate_coverage.sql``; this
migration is the existing-DB path (database-migration.md dual-path). Pure
additive DDL, so ``CREATE TABLE IF NOT EXISTS`` is the whole of it. Existing
caches gain spans on their next refresh; nothing here can vouch for rows already
stored.
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)

_CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS raw.exchange_rate_coverage (
    from_currency VARCHAR NOT NULL,
    to_currency VARCHAR NOT NULL,
    start_date DATE NOT NULL,
    end_date DATE NOT NULL,
    source_type VARCHAR NOT NULL,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (from_currency, to_currency, start_date, end_date, source_type),
    CHECK (start_date <= end_date)
)
"""

_TABLE_COMMENT = (
    "COMMENT ON TABLE raw.exchange_rate_coverage IS "
    "'Date spans an exchange-rate feed answered for completely; append-only'"
)

_COLUMN_COMMENTS: list[tuple[str, str]] = [
    ("from_currency", "ISO 4217, upper; matches raw.exchange_rates.from_currency"),
    ("to_currency", "ISO 4217, upper; matches raw.exchange_rates.to_currency"),
    ("start_date", "First date of the span the provider answered for completely"),
    ("end_date", "Last date of that span, inclusive"),
    (
        "source_type",
        "Provider that answered; matches raw.exchange_rates.source_type",
    ),
    ("loaded_at", "When this answer was recorded locally"),
]


def migrate(conn: object) -> None:
    """Create raw.exchange_rate_coverage. Idempotent."""
    logger.debug("V071: creating raw.exchange_rate_coverage")
    conn.execute(_CREATE_TABLE_SQL)  # type: ignore[union-attr]
    conn.execute(_TABLE_COMMENT)  # type: ignore[union-attr]
    for column, comment in _COLUMN_COMMENTS:
        escaped = comment.replace("'", "''")
        conn.execute(  # type: ignore[union-attr]
            f"COMMENT ON COLUMN raw.exchange_rate_coverage.{column} IS '{escaped}'"  # code-supplied column/comment constants, not user input
        )
