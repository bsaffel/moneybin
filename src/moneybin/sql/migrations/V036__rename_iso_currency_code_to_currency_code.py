"""V036: rename app.account_settings.iso_currency_code -> currency_code.

Aligns the accounts-side currency column with the name every other core.*
currency column already uses (fct_transactions, fct_investment_transactions,
dim_securities, dim_holdings). Direct rename, no deprecation shim -- confirmed
pre-launch per docs/specs/multi-currency.md Key Decision 5. Idempotent.
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


def migrate(conn: object) -> None:
    """Rename app.account_settings.iso_currency_code -> currency_code. Idempotent."""
    cols: list[tuple[str]] = conn.execute(  # type: ignore[union-attr]
        """
        SELECT column_name FROM information_schema.columns
        WHERE table_schema = 'app' AND table_name = 'account_settings'
        """
    ).fetchall()
    existing = {c[0] for c in cols}
    if "currency_code" in existing:
        logger.debug("V036: currency_code already present; skipping")
        return
    logger.debug(
        "V036: renaming app.account_settings.iso_currency_code -> currency_code"
    )
    conn.execute(  # type: ignore[union-attr]
        "ALTER TABLE app.account_settings RENAME COLUMN iso_currency_code TO currency_code"
    )
    conn.execute(  # type: ignore[union-attr]
        "COMMENT ON COLUMN app.account_settings.currency_code IS "
        "'ISO-4217 (USD, EUR, ...); NULL inherits the account''s core.dim_accounts.currency_code fallback'"
    )
