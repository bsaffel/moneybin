"""V057: capture the Plaid balance fields the client was discarding.

``SyncBalance`` declared six of the nine keys moneybin-sync sends in
``balances[]``. Pydantic's default ``extra='ignore'`` destroyed the other three
at validation with no error and no log line, so they reached nothing downstream
— the same mechanism that stranded ``persistent_account_id`` (V050).

``margin_loan_amount`` is the one that carried money. Plaid reports an
investment account's ``current`` as the total value of *assets* and the funds
borrowed against them separately, so summing ``current`` alone books the
broker's money as the holder's and overstates a margin account's net worth by
the whole loan. ``core.fct_balances`` now subtracts it.

``balance_limit`` renames the wire's ``limit`` — a SQL reserved word, and a
different fact from the user-asserted ``app.account_settings.credit_limit``. It
and ``last_updated_datetime`` are captured here but not yet wired to core.

Pure additive DDL — ``ADD COLUMN IF NOT EXISTS ... NULL`` with no DEFAULT, so no
backfill and the migration is a no-op on replay. Existing rows land NULL until
re-pulled (``moneybin sync pull --force`` re-fetches and upserts the values).
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)

# (column, sql_type, comment) — applied in order; types match raw_plaid_balances.sql.
#
# Each comment is byte-identical to the one in raw_plaid_balances.sql.
# `_apply_comments` re-runs that DDL's comments on every startup while this
# migration runs once, so a divergent string here would be overwritten on the
# next open and the catalog description would differ by which ran last. V050 and
# V052 carry the same note; test_migration_v057 derives the DDL side and asserts
# the pair rather than restating either literal.
_COLUMNS: list[tuple[str, str, str]] = [
    (
        "balance_limit",
        "DECIMAL(18, 2)",
        "Plaid limit: credit limit, or overdraft limit on depository; distinct "
        "from the user-asserted app.account_settings.credit_limit",
    ),
    (
        "margin_loan_amount",
        "DECIMAL(18, 2)",
        "Borrowed funds on a margin account; investment accounts only, NULL "
        "elsewhere. current_balance is the gross value of assets, so "
        "core.fct_balances subtracts this",
    ),
    (
        "last_updated_datetime",
        "TIMESTAMP",
        'Provider "as-of" time for the balance; populated only by some institutions',
    ),
]


def migrate(conn: object) -> None:
    """Add the dropped Plaid balance columns to raw.plaid_balances. Idempotent."""
    for name, sql_type, comment in _COLUMNS:
        logger.debug(f"V057: ADD COLUMN IF NOT EXISTS raw.plaid_balances.{name}")
        conn.execute(  # type: ignore[attr-defined]
            f"ALTER TABLE raw.plaid_balances ADD COLUMN IF NOT EXISTS {name} {sql_type}"
        )
        # DuckDB's COMMENT ON does not accept `?` parameters; use a literal like V030.
        # name + comment come from the hardcoded _COLUMNS list (no user input); comments
        # contain no apostrophes.
        conn.execute(  # type: ignore[attr-defined]  # noqa: S608  # DDL from hardcoded constants, not user input
            f"COMMENT ON COLUMN raw.plaid_balances.{name} IS '{comment}'"
        )
