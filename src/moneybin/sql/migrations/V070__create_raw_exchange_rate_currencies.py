"""V070: create raw.exchange_rate_currencies.

The provider's published-currency list, recorded when refresh or ``fx rate``
reads it, so a report read can tell an unsupported pair from an unfetched one
without the network (``multi-currency.md`` Requirement 18).

Fresh installs get the table from ``raw_exchange_rate_currencies.sql``; this
migration is the existing-DB path (database-migration.md dual-path). Pure
additive DDL, so ``CREATE TABLE IF NOT EXISTS`` is the whole of it.
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)

_CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS raw.exchange_rate_currencies (
    source_type VARCHAR NOT NULL,
    currency_code VARCHAR NOT NULL,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (source_type, currency_code)
)
"""

_TABLE_COMMENT = (
    "COMMENT ON TABLE raw.exchange_rate_currencies IS "
    "'The currencies each exchange-rate feed publishes, replaced whole per "
    "provider each time its list is read'"
)

_COLUMN_COMMENTS: list[tuple[str, str]] = [
    (
        "source_type",
        "Provider whose list this is; matches raw.exchange_rates.source_type",
    ),
    ("currency_code", "ISO 4217, upper; a currency that provider publishes rates for"),
    ("loaded_at", "When this list was read from the provider"),
]


def migrate(conn: object) -> None:
    """Create raw.exchange_rate_currencies. Idempotent."""
    logger.debug("V070: creating raw.exchange_rate_currencies")
    conn.execute(_CREATE_TABLE_SQL)  # type: ignore[union-attr]
    conn.execute(_TABLE_COMMENT)  # type: ignore[union-attr]
    for column, comment in _COLUMN_COMMENTS:
        escaped = comment.replace("'", "''")
        conn.execute(  # type: ignore[union-attr]
            f"COMMENT ON COLUMN raw.exchange_rate_currencies.{column} IS '{escaped}'"  # code-supplied column/comment constants, not user input
        )
