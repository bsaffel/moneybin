"""Shared synthetic-data reset: the scoped-deletion allowlist + executor.

Extracted from ``cli/commands/synthetic.py`` so both ``synthetic reset`` and the
demo preset (``DemoService``) share one security-sensitive allowlist rather than
duplicating the DELETE statements.
"""

import logging

from moneybin.database import Database
from moneybin.tables import (
    GROUND_TRUTH,
    OFX_ACCOUNTS,
    OFX_BALANCES,
    OFX_TRANSACTIONS,
    TABULAR_ACCOUNTS,
    TABULAR_TRANSACTIONS,
)

logger = logging.getLogger(__name__)

# Tables to scope-delete during reset (allowlist from TableRef constants).
RESET_DELETIONS: dict[str, str] = {
    GROUND_TRUTH.full_name: "WHERE TRUE",
    OFX_TRANSACTIONS.full_name: "WHERE source_file LIKE 'synthetic://%'",
    OFX_ACCOUNTS.full_name: "WHERE source_file LIKE 'synthetic://%'",
    OFX_BALANCES.full_name: "WHERE source_file LIKE 'synthetic://%'",
    TABULAR_TRANSACTIONS.full_name: "WHERE source_file LIKE 'synthetic://%'",
    TABULAR_ACCOUNTS.full_name: "WHERE source_file LIKE 'synthetic://%'",
}


def has_synthetic_ground_truth(db: Database) -> bool:
    """True if this DB holds the generator's `synthetic.ground_truth` table.

    The presence of that table is what marks a profile as generator-created —
    the safety signal both `synthetic reset` and the demo preset gate on before
    wiping rows.
    """
    try:
        row = db.execute(
            "SELECT COUNT(*) FROM information_schema.tables "
            "WHERE table_schema = 'synthetic' AND table_name = 'ground_truth'"
        ).fetchone()
        return bool(row and row[0])
    except Exception:  # noqa: BLE001 — fresh DB with no synthetic schema
        return False


def reset_synthetic_rows(db: Database) -> None:
    """Delete generator-created rows from raw.* (allowlisted tables only)."""
    for table, where in RESET_DELETIONS.items():
        try:
            db.execute(f"DELETE FROM {table} {where}")  # noqa: S608  # allowlisted table names + literal WHERE clauses
        except Exception:  # noqa: BLE001,S110 — table may not exist in a fresh DB
            pass
