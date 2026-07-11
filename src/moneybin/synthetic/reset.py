"""Shared synthetic-data reset: the scoped-deletion allowlist + executor.

Extracted from ``cli/commands/synthetic.py`` so both ``synthetic reset`` and the
demo preset (``DemoService``) share one security-sensitive allowlist rather than
duplicating the DELETE statements.
"""

import logging

from sqlglot import exp

from moneybin.database import Database
from moneybin.tables import (
    GROUND_TRUTH,
    OFX_ACCOUNTS,
    OFX_BALANCES,
    OFX_TRANSACTIONS,
    TABULAR_ACCOUNTS,
    TABULAR_TRANSACTIONS,
    USER_MERCHANTS,
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


# Platform state, never the user's financial data. A rebuild recreates every one of
# these, so none is the user's to lose:
#   - schema_migrations / versions — migration bookkeeping, written by `init_db`.
#   - seed_source_priority — reference data the transform seeds.
#   - metrics — operational telemetry. EVERY CLI process that opened a write
#     connection flushes it at exit (`observability.py`: atexit → flush_metrics), so
#     a demo profile has rows here the moment the first `moneybin demo` returns.
#
# Getting this set wrong in the *other* direction is the live hazard: the guard reads
# any `app.*` table outside it as the user's, so a missing entry here makes demo
# refuse to rebuild its OWN profile. `app.metrics` did exactly that — and no
# in-process test could see it, because `atexit` never fires there. The regression
# guard is `test_demo_rerun_after_a_real_cli_run`, which runs `moneybin demo` twice
# as a real subprocess.
_NON_USER_TABLES = frozenset({
    "app.schema_migrations",
    "app.versions",
    "app.seed_source_priority",
    "app.metrics",
    # Evidence, not financial state — and redundant here: every mutation it records
    # also landed in a table this guard already checks.
    "app.audit_log",
    # Strictly derived from transactions, and written by demo's own match/categorize
    # steps. Safe to exclude because they cannot exist without a transaction behind
    # them: if that transaction is real, the raw table it came from already flags the
    # profile; if it is synthetic, the rebuild regenerates these rows anyway. Contrast
    # `_OURS_IN_APP` below — a user can author a merchant with no transactions at all,
    # so that table needs a provenance filter rather than a blanket exclusion.
    "app.transaction_categories",
    "app.match_decisions",
})

# App tables BOTH we and the user write. Blanket-excluding one would blind the guard
# to real user state; blanket-including it would make demo refuse to rebuild its own
# profile (the merchant seeder writes here). So guard the rows we did NOT write —
# the same shape as GENERATOR_WRITTEN_TABLES on the raw side.
#
# `synthetic` is the provenance the seeder stamps (`merchant_seed.py`), so a merchant
# the user authored inside the demo profile still reads as theirs and is protected.
_OURS_IN_APP: dict[str, str] = {
    USER_MERCHANTS.full_name: "created_by <> 'synthetic'",
}


def _real_row_checks(db: Database) -> list[tuple[str, str]]:
    """Build the (quoted table, WHERE clause) checks that detect real user data.

    Inverted deliberately, on both schemas. Enumerating the tables that *might* hold
    real data goes stale the moment a new source or feature lands — and it fails
    OPEN, silently leaving the new table's rows invisible to the guard. That is
    exactly how `raw.pdf_seeds`, `raw.manual_investment_transactions`, and then
    `app.securities` each slipped past it in turn.

    So enumerate the closed sets *we* write — the generator's raw tables, and the
    platform tables in `_NON_USER_TABLES` — and read the live catalog for everything
    else. Any other `raw.*` or `app.*` table is real user data by default:
    `app.securities`, `app.budgets`, and `app.user_categories` are all user-authored
    with no transaction behind them, and the list only grows.
    """
    rows = db.execute(
        "SELECT schema_name, table_name FROM duckdb_tables() "
        "WHERE schema_name IN ('app', 'raw')"
    ).fetchall()

    checks: list[tuple[str, str]] = []
    for schema, name in rows:
        qualified = f"{schema}.{name}"
        if qualified in _NON_USER_TABLES:
            continue  # platform state; the rebuild recreates it

        if qualified in GENERATOR_WRITTEN_TABLES:
            where = f"WHERE {_NON_SYNTHETIC_ROWS}"
        elif qualified in _OURS_IN_APP:
            where = f"WHERE {_OURS_IN_APP[qualified]}"
        else:
            where = ""
        checks.append((_quote(schema, name), where))
    return checks


def has_any_user_content(db: Database) -> bool:
    """True if this database holds ANY row that isn't ours or the platform's.

    For a database we CANNOT attribute to the generator, there is no such thing as
    a safe table: `app.securities`, `app.budgets`, and `app.user_categories` are
    all real, user-authored state reachable with no transaction behind them, and
    the list grows. So rather than enumerate them, refuse on any row at all outside
    `_NON_USER_TABLES`.

    Being maximally conservative is nearly free here: this is the not-ours path, so
    over-refusing only ever declines to destroy someone else's profile. It must
    still ignore platform tables, though — a `db init` profile that has run any CLI
    command already carries an `app.metrics` snapshot.
    """
    rows = db.execute(
        "SELECT schema_name, table_name FROM duckdb_tables() "
        "WHERE schema_name IN ('app', 'raw')"
    ).fetchall()
    for schema, name in rows:
        if f"{schema}.{name}" in _NON_USER_TABLES:
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
