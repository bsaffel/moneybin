"""Domain (business-rule) assertions for the canonical core schema."""

from __future__ import annotations

from datetime import date

from moneybin.database import Database
from moneybin.tables import FCT_TRANSACTIONS
from moneybin.validation.assertions._helpers import quote_ident, split_table_ident
from moneybin.validation.result import AssertionResult

# Each predicate matches *violations*, not valid rows. Transfers are
# identified by ``is_transfer = TRUE`` in the data model, not by a literal
# category string — excluding them via that flag (rather than
# ``category != 'Transfer'``) avoids a NULL-NOT-IN dead path.
_EXPENSE_SIGN_VIOLATIONS = "category != 'Income' AND amount > 0 AND is_transfer = FALSE"
_INCOME_SIGN_VIOLATIONS = "category = 'Income' AND amount < 0"


def assert_sign_convention(db: Database) -> AssertionResult:
    """Expenses negative, income positive. Transfers exempted via ``is_transfer``."""
    violations = int(
        db.execute(
            f"SELECT COUNT(*) FROM {FCT_TRANSACTIONS.full_name} "  # noqa: S608  # TableRef constant + module-level predicate strings
            f"WHERE ({_EXPENSE_SIGN_VIOLATIONS}) OR ({_INCOME_SIGN_VIOLATIONS})"
        ).fetchone()[0]  # type: ignore[index]
    )
    return AssertionResult(
        name="sign_convention",
        passed=violations == 0,
        details={"violations": violations},
    )


def assert_balanced_transfers(db: Database) -> AssertionResult:
    """Confirmed transfer pairs (transfer_pair_id NOT NULL) must net to zero."""
    rows = db.execute(
        f"SELECT transfer_pair_id, SUM(amount) FROM {FCT_TRANSACTIONS.full_name} "  # noqa: S608  # TableRef constant
        "WHERE transfer_pair_id IS NOT NULL "
        "GROUP BY transfer_pair_id HAVING SUM(amount) IS DISTINCT FROM 0"
    ).fetchall()
    unbalanced = [
        (pair, float(total) if total is not None else None) for pair, total in rows
    ]
    return AssertionResult(
        name="balanced_transfers",
        passed=not unbalanced,
        details={
            "unbalanced_pairs": unbalanced[:20],
            "unbalanced_count": len(unbalanced),
        },
    )


def assert_date_continuity(
    db: Database, *, table: str, date_col: str, account_col: str
) -> AssertionResult:
    """No month-gaps per account in the given table."""
    t, dc, ac = quote_ident(table), quote_ident(date_col), quote_ident(account_col)
    rows = db.execute(
        f"WITH per AS ("  # noqa: S608  # identifiers validated by quote_ident
        f"  SELECT {ac} AS account, DATE_TRUNC('month', {dc}) AS m FROM {t} GROUP BY 1, 2"
        f"), bounds AS ("
        f"  SELECT account, MIN(m) AS lo, MAX(m) AS hi, COUNT(*) AS observed FROM per GROUP BY account"
        f") SELECT account, observed, DATE_DIFF('month', lo, hi) + 1 AS expected"
        f" FROM bounds WHERE observed IS DISTINCT FROM DATE_DIFF('month', lo, hi) + 1"
    ).fetchall()
    gaps = [(acc, obs, exp) for acc, obs, exp in rows]
    return AssertionResult(
        name="date_continuity",
        passed=not gaps,
        details={"gap_accounts": gaps[:20], "gap_count": len(gaps)},
    )


def assert_amount_precision(
    db: Database,
    *,
    table: str,
    column: str,
    precision: int,
    scale: int,
) -> AssertionResult:
    """Assert ``column`` in ``table`` is ``DECIMAL(precision, scale)``.

    Catches the silent regression where an upstream cast drops a money column
    to ``DOUBLE``, losing exact representation. Compares against
    ``information_schema.columns.data_type`` — DuckDB renders DECIMAL types
    as ``DECIMAL(p,s)`` literally, so a string-equality check is sufficient.
    """
    expected_type = f"DECIMAL({precision},{scale})"
    schema, name = split_table_ident(table)
    if schema is None:
        rows = db.execute(
            "SELECT data_type FROM information_schema.columns "
            "WHERE table_name = ? AND column_name = ?",
            [name, column],
        ).fetchall()
    else:
        rows = db.execute(
            "SELECT data_type FROM information_schema.columns "
            "WHERE table_schema = ? AND table_name = ? AND column_name = ?",
            [schema, name, column],
        ).fetchall()
    actual_type = str(rows[0][0]) if rows else "<missing>"
    return AssertionResult(
        name="amount_precision",
        passed=actual_type == expected_type,
        details={
            "expected_type": expected_type,
            "actual_type": actual_type,
        },
    )


def assert_date_bounds(
    db: Database,
    *,
    table: str,
    column: str,
    min_date: date | str,
    max_date: date | str,
) -> AssertionResult:
    """Assert every ``column`` value falls within ``[min_date, max_date]`` inclusive.

    NULL values count as out-of-bounds — a missing date cannot be verified to
    fall within the window, and silently accepting NULLs would mask broken
    date extraction. Empty tables still pass (no rows to violate).

    Accepts ISO-format strings for ``min_date`` / ``max_date`` so YAML
    scenarios (which serialize dates as quoted strings) can call this
    primitive without manual coercion in the registry shim.
    """
    if isinstance(min_date, str):
        min_date = date.fromisoformat(min_date)
    if isinstance(max_date, str):
        max_date = date.fromisoformat(max_date)
    if min_date > max_date:
        raise ValueError(f"min_date {min_date} must be <= max_date {max_date}")
    t = quote_ident(table)
    c = quote_ident(column)
    row = db.execute(
        f"SELECT "  # noqa: S608  # identifiers validated by quote_ident
        f"  SUM(CASE WHEN {c} < ? THEN 1 ELSE 0 END), "
        f"  SUM(CASE WHEN {c} > ? THEN 1 ELSE 0 END), "
        f"  SUM(CASE WHEN {c} IS NULL THEN 1 ELSE 0 END), "
        f"  MIN({c}), MAX({c}) "
        f"FROM {t}",
        [min_date, max_date],
    ).fetchone()
    below = int(row[0]) if row and row[0] is not None else 0
    above = int(row[1]) if row and row[1] is not None else 0
    null_count = int(row[2]) if row and row[2] is not None else 0
    observed_min = row[3] if row else None
    observed_max = row[4] if row else None
    return AssertionResult(
        name="date_bounds",
        passed=below == 0 and above == 0 and null_count == 0,
        details={
            "min_date": min_date.isoformat(),
            "max_date": max_date.isoformat(),
            "observed_min": observed_min.isoformat() if observed_min else None,
            "observed_max": observed_max.isoformat() if observed_max else None,
            "below_min_count": below,
            "above_max_count": above,
            "null_count": null_count,
        },
    )
