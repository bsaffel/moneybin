"""V075: add currency and cost-basis change times to app.account_settings.

docs/specs/materialization-freshness-contract.md. Materialized models fold these
instead of the row's own updated_at, which a rename also advances. Appended
last because DuckDB's ALTER TABLE ADD COLUMN always appends, and init_schemas()
must produce the same column order as an upgraded database.

Existing rows take their updated_at: the latest moment either setting could
have changed, and the value those models folded before, so no derived
updated_at rewinds.
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)

_STAMPS = (
    (
        "currency_code_changed_at",
        "When currency_code last changed, including a clear and an undo; NULL if "
        "it never changed. Folded into prep.int_accounts__merged.updated_at, which "
        "materialized models fold where they inherit the account''s currency",
    ),
    (
        "default_cost_basis_method_changed_at",
        "When default_cost_basis_method last changed, including a clear and an "
        "undo; NULL if it never changed. Folded into the investment and currency "
        "lots'' updated_at, whose disposals it reorders",
    ),
)


def migrate(conn: object) -> None:
    """Add and backfill the two change-time columns. Idempotent."""
    for column, comment in _STAMPS:
        conn.execute(  # type: ignore[union-attr]
            "ALTER TABLE app.account_settings "
            f"ADD COLUMN IF NOT EXISTS {column} TIMESTAMP"
        )
        conn.execute(  # type: ignore[union-attr]
            f"COMMENT ON COLUMN app.account_settings.{column} IS '{comment}'"
        )
    # DuckDB refuses an UPDATE of a table altered earlier in the same
    # transaction. A crash after this COMMIT leaves the columns NULL; a rerun
    # skips the ADDs and backfills only the NULLs, so it finishes the job.
    conn.execute("COMMIT")  # type: ignore[union-attr]
    conn.execute("BEGIN TRANSACTION")  # type: ignore[union-attr]
    for column, _comment in _STAMPS:
        conn.execute(  # type: ignore[union-attr]
            f"UPDATE app.account_settings SET {column} = updated_at "  # noqa: S608  # literal column names
            f"WHERE {column} IS NULL"
        )
    logger.debug("V075: added ledger setting change times to app.account_settings")
