"""Business-rule assertions for the canonical core schema."""

from __future__ import annotations

from duckdb import DuckDBPyConnection

from moneybin.tables import FCT_TRANSACTIONS
from moneybin.validation.assertions.relational import quote_ident
from moneybin.validation.result import AssertionResult

# Each predicate matches *violations*, not valid rows. Transfers are
# identified by ``is_transfer = TRUE`` in the data model, not by a literal
# category string — excluding them via that flag (rather than
# ``category != 'Transfer'``) avoids a NULL-NOT-IN dead path.
_EXPENSE_SIGN_VIOLATIONS = "category != 'Income' AND amount > 0 AND is_transfer = FALSE"
_INCOME_SIGN_VIOLATIONS = "category = 'Income' AND amount < 0"


def assert_sign_convention(conn: DuckDBPyConnection) -> AssertionResult:
    """Expenses negative, income positive. Transfers exempted via ``is_transfer``."""
    violations = int(
        conn.execute(
            f"SELECT COUNT(*) FROM {FCT_TRANSACTIONS.full_name} "  # noqa: S608  # TableRef constant + module-level predicate strings
            f"WHERE ({_EXPENSE_SIGN_VIOLATIONS}) OR ({_INCOME_SIGN_VIOLATIONS})"
        ).fetchone()[0]  # type: ignore[index]
    )
    return AssertionResult(
        name="sign_convention",
        passed=violations == 0,
        details={"violations": violations},
    )


def assert_balanced_transfers(conn: DuckDBPyConnection) -> AssertionResult:
    """Confirmed transfer pairs (transfer_pair_id NOT NULL) must net to zero."""
    rows = conn.execute(
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
    conn: DuckDBPyConnection, *, table: str, date_col: str, account_col: str
) -> AssertionResult:
    """No month-gaps per account in the given table."""
    t, dc, ac = quote_ident(table), quote_ident(date_col), quote_ident(account_col)
    rows = conn.execute(
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
