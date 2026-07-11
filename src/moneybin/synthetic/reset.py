"""Shared synthetic-data reset: the scoped-deletion allowlist + executor.

Extracted from ``cli/commands/synthetic.py`` so both ``synthetic reset`` and the
demo preset (``DemoService``) share one security-sensitive allowlist rather than
duplicating the DELETE statements.
"""

import logging

from sqlglot import exp

from moneybin.database import Database
from moneybin.tables import (
    BALANCE_ASSERTIONS,
    GROUND_TRUTH,
    OFX_ACCOUNTS,
    OFX_BALANCES,
    OFX_TRANSACTIONS,
    TABULAR_ACCOUNTS,
    TABULAR_TRANSACTIONS,
)

logger = logging.getLogger(__name__)

# The complete, closed set of `raw.*` tables the generator writes, and the
# predicate identifying the rows it wrote. `SyntheticWriter` is the only producer,
# so this set is ours to keep exact — unlike the open-ended set of tables that
# might hold *real* data, which grows with every new import source.
GENERATOR_WRITTEN_TABLES: tuple[str, ...] = (
    OFX_TRANSACTIONS.full_name,
    OFX_ACCOUNTS.full_name,
    OFX_BALANCES.full_name,
    TABULAR_TRANSACTIONS.full_name,
    TABULAR_ACCOUNTS.full_name,
)
_SYNTHETIC_ROWS = "source_file LIKE 'synthetic://%'"

# Relies on `source_file` being NOT NULL in every table above (it is, in all five).
# If that constraint is ever relaxed, this negation starts evaluating to NULL —
# not TRUE — for those rows, and they go invisible to the real-data guard below.
_NON_SYNTHETIC_ROWS = f"NOT ({_SYNTHETIC_ROWS})"

# Tables to scope-delete during reset (allowlist from TableRef constants).
#
# Deliberately raw/synthetic ONLY. Derived `app.*` state (match_decisions,
# transaction_categories) is NOT cleared here: those tables are audited and may
# only be mutated through their `*Repo` (Invariant 10), never by raw DELETE.
# The demo preset therefore does not use this surgical path at all — it rebuilds
# the profile's database from scratch, which leaves no orphaned derived rows.
RESET_DELETIONS: dict[str, str] = {
    GROUND_TRUTH.full_name: "WHERE TRUE",
    **dict.fromkeys(GENERATOR_WRITTEN_TABLES, f"WHERE {_SYNTHETIC_ROWS}"),
}


def _quote(schema: str, name: str) -> str:
    """Double-quote a catalog-sourced identifier pair for interpolation into SQL."""
    return (
        f"{exp.to_identifier(schema, quoted=True).sql('duckdb')}."
        f"{exp.to_identifier(name, quoted=True).sql('duckdb')}"
    )


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


def _real_row_checks(db: Database) -> list[tuple[str, str]]:
    """Build the (quoted table, WHERE clause) checks that detect real user data.

    Inverted deliberately. Enumerating the tables that *might* hold real data goes
    stale the moment a new import source lands — and it fails OPEN, silently
    leaving the new table's rows invisible to the guard (exactly how `pdf_seeds`
    and `manual_investment_transactions` slipped past it). So instead we enumerate
    the closed set the generator writes and read the live catalog for everything
    else: an unrecognized `raw.*` table is real data by default, and a new import
    source is guarded from the day it is added.
    """
    rows = db.execute(
        "SELECT schema_name, table_name FROM duckdb_tables() WHERE schema_name = ?",
        [OFX_TRANSACTIONS.schema],
    ).fetchall()

    checks = [
        (
            _quote(schema, name),
            f"WHERE {_NON_SYNTHETIC_ROWS}"
            if f"{schema}.{name}" in GENERATOR_WRITTEN_TABLES
            else "",
        )
        for schema, name in rows
    ]
    # User-authored balances live outside `raw` and the generator never writes them.
    checks.append((_quote(BALANCE_ASSERTIONS.schema, BALANCE_ASSERTIONS.name), ""))
    return checks


# The only tables a freshly-created profile has rows in — migration bookkeeping,
# not user data. Locked by `test_a_fresh_profile_holds_no_user_content`, which
# fails loudly if `init_db` ever starts populating something else.
_INIT_POPULATED_TABLES = frozenset({"app.schema_migrations", "app.versions"})


def has_any_user_content(db: Database) -> bool:
    """True if this database holds ANY row a freshly-created profile would not.

    For a database we CANNOT attribute to the generator, there is no such thing as
    a safe table: `app.securities`, `app.budgets`, and `app.user_categories` are
    all real, user-authored state reachable with no transaction behind them, and
    the list grows. So rather than enumerate them, refuse on any row at all
    outside migration bookkeeping.

    Being maximally conservative is free here precisely because this is the
    not-ours path. Demo's own re-run path is generator-made and goes through
    `has_non_synthetic_data` instead, so nothing this function does can
    false-positive the happy path or `synthetic reset`.
    """
    rows = db.execute(
        "SELECT schema_name, table_name FROM duckdb_tables() "
        "WHERE schema_name IN ('app', 'raw')"
    ).fetchall()
    for schema, name in rows:
        if f"{schema}.{name}" in _INIT_POPULATED_TABLES:
            continue
        try:
            found = db.execute(
                f"SELECT 1 FROM {_quote(schema, name)} LIMIT 1"  # noqa: S608  # catalog-sourced, double-quoted identifier
            ).fetchone()
        except Exception:  # noqa: BLE001,S112 — table may not exist in a partial DB
            continue
        if found:
            return True
    return False


def has_non_synthetic_data(db: Database) -> bool:
    """True if the profile holds any financial state the generator did NOT create.

    Any such row means this is a real financial profile — the demo preset must
    refuse to seed it, regardless of whether the ``synthetic.ground_truth`` marker
    table exists, and regardless of whether the real data is transactions or
    balances/accounts alone.
    """
    for table, where in _real_row_checks(db):
        try:
            row = db.execute(f"SELECT 1 FROM {table} {where} LIMIT 1").fetchone()  # noqa: S608  # catalog-sourced, double-quoted identifiers + literal WHERE clauses
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
