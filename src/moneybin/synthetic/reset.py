"""Shared synthetic-data reset: the scoped-deletion allowlist + executor.

Extracted from ``cli/commands/synthetic.py`` so both ``synthetic reset`` and the
demo preset (``DemoService``) share one security-sensitive allowlist rather than
duplicating the DELETE statements.
"""

import logging

from moneybin.database import Database
from moneybin.tables import (
    BALANCE_ASSERTIONS,
    GROUND_TRUTH,
    GSHEET_SEEDS,
    MANUAL_TRANSACTIONS,
    OFX_ACCOUNTS,
    OFX_BALANCES,
    OFX_TRANSACTIONS,
    PLAID_ACCOUNTS,
    PLAID_BALANCES,
    PLAID_TRANSACTIONS,
    TABULAR_ACCOUNTS,
    TABULAR_TRANSACTIONS,
)

logger = logging.getLogger(__name__)

# Tables to scope-delete during reset (allowlist from TableRef constants).
#
# Deliberately raw/synthetic ONLY. Derived `app.*` state (match_decisions,
# transaction_categories) is NOT cleared here: those tables are audited and may
# only be mutated through their `*Repo` (Invariant 10), never by raw DELETE.
# The demo preset therefore does not use this surgical path at all — it rebuilds
# the profile's database from scratch, which leaves no orphaned derived rows.
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


def has_non_synthetic_data(db: Database) -> bool:
    """True if the profile holds any financial state the generator did NOT create.

    The generator only ever writes OFX/tabular rows (transactions, accounts,
    balances) tagged ``source_file LIKE 'synthetic://%'``, plus
    ``synthetic.ground_truth``. Real state therefore appears as non-``synthetic://``
    rows in those tables, or as ANY row in a table the generator never touches
    (Plaid, manual entry, user balance assertions). Any such row means this is a
    real financial profile — the demo preset must refuse to seed it, regardless
    of whether the ``synthetic.ground_truth`` marker table exists, and regardless
    of whether the real data is transactions or balances/accounts alone.
    """
    # (table, extra WHERE). Generator-written tables → real := non-`synthetic://`
    # rows; tables the generator never writes → any row is real.
    real_row_checks = (
        (OFX_TRANSACTIONS.full_name, "WHERE source_file NOT LIKE 'synthetic://%'"),
        (TABULAR_TRANSACTIONS.full_name, "WHERE source_file NOT LIKE 'synthetic://%'"),
        (OFX_BALANCES.full_name, "WHERE source_file NOT LIKE 'synthetic://%'"),
        (OFX_ACCOUNTS.full_name, "WHERE source_file NOT LIKE 'synthetic://%'"),
        (TABULAR_ACCOUNTS.full_name, "WHERE source_file NOT LIKE 'synthetic://%'"),
        (PLAID_TRANSACTIONS.full_name, ""),
        (PLAID_BALANCES.full_name, ""),
        (PLAID_ACCOUNTS.full_name, ""),
        (MANUAL_TRANSACTIONS.full_name, ""),
        (GSHEET_SEEDS.full_name, ""),
        (BALANCE_ASSERTIONS.full_name, ""),
    )
    for table, where in real_row_checks:
        try:
            row = db.execute(f"SELECT 1 FROM {table} {where} LIMIT 1").fetchone()  # noqa: S608  # allowlisted TableRef names + literal WHERE clauses
        except Exception:  # noqa: BLE001,S112 — table may not exist in a fresh/partial DB
            continue
        if row:
            return True
    return False


def reset_synthetic_rows(db: Database) -> None:
    """Delete generator-created rows from raw.* (allowlisted tables only)."""
    for table, where in RESET_DELETIONS.items():
        try:
            db.execute(f"DELETE FROM {table} {where}")  # noqa: S608  # allowlisted table names + literal WHERE clauses
        except Exception:  # noqa: BLE001,S110 — table may not exist in a fresh DB
            pass
