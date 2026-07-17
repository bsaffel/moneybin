"""V037: add currency_code to raw.ofx_transactions and raw.ofx_balances.

Captures OFX's statement-level CURDEF, currently discarded (multi-currency.md
M1K.1 Requirement 1). Additive, nullable -- idempotent.
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)

_TABLES = ("ofx_transactions", "ofx_balances")


def migrate(conn: object) -> None:
    """Add raw.ofx_transactions/raw.ofx_balances.currency_code. Idempotent."""
    for table in _TABLES:
        logger.debug(f"V037: ADD COLUMN IF NOT EXISTS raw.{table}.currency_code")
        conn.execute(  # type: ignore[union-attr]
            f"ALTER TABLE raw.{table} ADD COLUMN IF NOT EXISTS currency_code VARCHAR"
        )
        conn.execute(  # type: ignore[union-attr]
            f"COMMENT ON COLUMN raw.{table}.currency_code IS "
            "'OFX CURDEF, verbatim (e.g. USD); NULL for files parsed before this "
            "column existed or lacking a CURDEF element'"
        )
