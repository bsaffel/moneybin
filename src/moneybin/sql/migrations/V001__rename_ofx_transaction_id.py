"""Rename transaction_id → source_transaction_id in raw.ofx_transactions.

Frees up transaction_id for the gold key in core.fct_transactions.
Skips if the column is already renamed (idempotent for fresh installs).
DuckDB ALTER TABLE RENAME COLUMN preserves PK constraints.
"""


def migrate(conn: object) -> None:
    """Rename transaction_id to source_transaction_id if it still exists."""
    has_old_column = conn.execute(  # type: ignore[union-attr]
        """
        SELECT COUNT(*) FROM information_schema.columns
        WHERE table_schema = 'raw'
          AND table_name = 'ofx_transactions'
          AND column_name = 'transaction_id'
        """
    ).fetchone()[0]

    if has_old_column:
        conn.execute(  # type: ignore[union-attr]
            "ALTER TABLE raw.ofx_transactions RENAME COLUMN transaction_id TO source_transaction_id"
        )
