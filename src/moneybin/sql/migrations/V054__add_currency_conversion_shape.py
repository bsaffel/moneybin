"""V054: reserve received-leg fields for single-row currency conversions."""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)

_STEPS = (
    ("raw.ofx_transactions", "to_amount", "DECIMAL(18, 2)"),
    ("raw.ofx_transactions", "to_currency", "VARCHAR"),
    ("raw.tabular_transactions", "to_amount", "DECIMAL(18, 2)"),
    ("raw.tabular_transactions", "to_currency", "VARCHAR"),
    ("raw.plaid_transactions", "to_amount", "DECIMAL(18, 2)"),
    ("raw.plaid_transactions", "to_currency", "VARCHAR"),
    ("raw.manual_transactions", "to_amount", "DECIMAL(18, 2)"),
    ("raw.manual_transactions", "to_currency", "VARCHAR"),
)


def migrate(conn: object) -> None:
    """Add nullable received-leg fields without rewriting existing rows."""
    for table, column, column_type in _STEPS:
        logger.debug(f"V054: ADD COLUMN {table}.{column}")
        conn.execute(  # type: ignore[union-attr]
            f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {column} {column_type}"  # noqa: S608  # closed internal migration plan
        )
