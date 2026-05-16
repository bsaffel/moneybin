"""Add updated_at to app.balance_assertions so edits advance freshness.

Per docs/specs/core-updated-at-convention.md, `core.fct_balances.updated_at`
sources its user-assertion timestamps from `app.balance_assertions`. The
existing `created_at` is preserved on re-assertion (BalanceService.assert_balance
ON CONFLICT semantics), so edits to an existing assertion are invisible to
"changed since T" consumers without a separate mutable column. The write path
refreshes `updated_at` on the ON CONFLICT DO UPDATE branch.

Two-step ADD then SET NOT NULL with an interim COMMIT — see V010 for the
DuckDB "outstanding updates" rationale and the idempotent recovery branch.
"""

import logging

logger = logging.getLogger(__name__)


def migrate(conn: object) -> None:
    """Add updated_at to app.balance_assertions. Idempotent."""
    cols: list[tuple[str, bool]] = conn.execute(  # type: ignore[union-attr]
        """
        SELECT column_name, is_nullable FROM duckdb_columns()
        WHERE schema_name = 'app' AND table_name = 'balance_assertions'
        """,
    ).fetchall()
    col_map: dict[str, bool] = {c[0]: c[1] for c in cols}

    if not col_map:
        # Fresh install: app_balance_assertions.sql carries the end-state.
        return

    if "updated_at" not in col_map:
        logger.info("Adding updated_at to app.balance_assertions")
        conn.execute(  # type: ignore[union-attr]
            "ALTER TABLE app.balance_assertions "
            "ADD COLUMN updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP"
        )
        # Commit the backfill before SET NOT NULL — see module docstring.
        conn.execute("COMMIT")  # type: ignore[union-attr]
        conn.execute("BEGIN TRANSACTION")  # type: ignore[union-attr]
        conn.execute(  # type: ignore[union-attr]
            "ALTER TABLE app.balance_assertions ALTER COLUMN updated_at SET NOT NULL"
        )
    elif col_map["updated_at"] is True:
        logger.info("Tightening app.balance_assertions.updated_at to NOT NULL")
        conn.execute(  # type: ignore[union-attr]
            "ALTER TABLE app.balance_assertions ALTER COLUMN updated_at SET NOT NULL"
        )

    logger.info("V011 migration complete")
