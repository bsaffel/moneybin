"""V038: add iso_currency_code and unofficial_currency_code to raw.plaid_balances.

Plaid's balance objects return iso_currency_code and unofficial_currency_code
(mutually exclusive) in the live API; both were captured for
securities/investment-transactions/holdings but never wired into the
balances path (multi-currency.md M1K.1 Requirement 1). Additive, nullable --
idempotent.
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


def migrate(conn: object) -> None:
    """Add raw.plaid_balances.{iso_currency_code,unofficial_currency_code}. Idempotent."""
    logger.debug("V038: ADD COLUMN IF NOT EXISTS raw.plaid_balances.iso_currency_code")
    conn.execute(  # type: ignore[union-attr]
        "ALTER TABLE raw.plaid_balances ADD COLUMN IF NOT EXISTS iso_currency_code VARCHAR"
    )
    conn.execute(  # type: ignore[union-attr]
        "COMMENT ON COLUMN raw.plaid_balances.iso_currency_code IS "
        "'Plaid iso_currency_code, verbatim; NULL when Plaid reports "
        "unofficial_currency_code instead or omits both'"
    )
    logger.debug(
        "V038: ADD COLUMN IF NOT EXISTS raw.plaid_balances.unofficial_currency_code"
    )
    conn.execute(  # type: ignore[union-attr]
        "ALTER TABLE raw.plaid_balances ADD COLUMN IF NOT EXISTS "
        "unofficial_currency_code VARCHAR"
    )
    conn.execute(  # type: ignore[union-attr]
        "COMMENT ON COLUMN raw.plaid_balances.unofficial_currency_code IS "
        "'Plaid unofficial_currency_code, verbatim; set only when Plaid cannot "
        "map the currency to ISO 4217 (crypto-adjacent or specialty accounts)'"
    )
