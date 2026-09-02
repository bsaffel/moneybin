"""V053: drop the ``'USD'`` default on raw.manual_investment_transactions.currency_code.

The column shipped in V034 with ``DEFAULT 'USD'``, the one blind currency guess
multi-currency.md Requirement 3 forbids ("never a blind 'USD'"): it denominates a
EUR account's lot in dollars, and the fabricated value then reaches cost basis
and realized gains. With the default gone, an omitted currency stays NULL and
``core.fct_investment_transactions`` inherits the account's own — the same
resolution ``core.fct_transactions`` already applies to the cash grain.

Deliberately NOT backfilled. Every write path passed an explicit ``'USD'`` when
the caller omitted a currency, so a stored ``'USD'`` is indistinguishable from a
currency the user actually typed. Rewriting them to NULL would erase real
answers to un-guess the fabricated ones; leaving them keeps existing ledgers
reading exactly as they do today, and only the *next* omitted currency is left
unfabricated. ``accounts set --currency`` repairs every event that carries no currency at
all; one already carrying a wrong value cannot be relabelled yet — a manual
investment event has no delete or revert, so re-recording appends a second row
rather than replacing the first.
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


def migrate(conn: object) -> None:
    """Drop the column default; leave every stored currency untouched."""
    logger.debug(
        "V053: DROP DEFAULT on raw.manual_investment_transactions.currency_code"
    )
    conn.execute(  # type: ignore[union-attr]
        """
        ALTER TABLE raw.manual_investment_transactions
        ALTER COLUMN currency_code DROP DEFAULT
        """
    )
