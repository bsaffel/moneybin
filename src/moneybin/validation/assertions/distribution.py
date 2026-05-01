"""Distributional / cardinality smoke checks. Bounds are scenario-author chosen — soft signal."""

from __future__ import annotations

from moneybin.database import Database
from moneybin.tables import FCT_TRANSACTIONS, GROUND_TRUTH, INT_TRANSACTIONS_MATCHED
from moneybin.validation.assertions._helpers import quote_ident
from moneybin.validation.result import AssertionResult


def assert_distribution_within_bounds(
    db: Database,
    *,
    table: str,
    col: str,
    min_value: float,
    max_value: float,
    mean_range: tuple[float, float],
) -> AssertionResult:
    """Assert column statistics fall within author-specified bounds."""
    t, c = quote_ident(table), quote_ident(col)
    row = db.execute(
        f"SELECT MIN({c}), MAX({c}), AVG({c}) FROM {t}"  # noqa: S608  # identifiers validated by quote_ident
    ).fetchone()
    if row is None or row[0] is None:
        return AssertionResult(
            name="distribution_within_bounds",
            passed=False,
            details={"reason": "table is empty"},
        )
    mn, mx, avg = float(row[0]), float(row[1]), float(row[2])
    failures: list[str] = []
    if mn < min_value:
        failures.append(f"min {mn} < {min_value}")
    if mx > max_value:
        failures.append(f"max {mx} > {max_value}")
    if not (mean_range[0] <= avg <= mean_range[1]):
        failures.append(f"mean {avg} outside {mean_range}")
    return AssertionResult(
        name="distribution_within_bounds",
        passed=not failures,
        details={
            "min_observed": mn,
            "max_observed": mx,
            "mean_observed": round(avg, 4),
            "failures": failures,
        },
    )


def assert_unique_value_count(
    db: Database,
    *,
    table: str,
    col: str,
    expected: int,
    tolerance_pct: float,
) -> AssertionResult:
    """Assert the number of distinct values in a column is within tolerance of expected."""
    t, c = quote_ident(table), quote_ident(col)
    actual = int(db.execute(f"SELECT COUNT(DISTINCT {c}) FROM {t}").fetchone()[0])  # noqa: S608  # type: ignore[index]  # identifiers validated by quote_ident
    if expected == 0:
        # Cannot compute a percentage delta against 0; treat any actual rows as a fail.
        passed = actual == 0
        delta_pct = 0.0 if passed else float("inf")
    else:
        delta_pct = abs(actual - expected) / expected * 100
        passed = delta_pct <= tolerance_pct
    return AssertionResult(
        name="unique_value_count",
        passed=passed,
        details={
            "expected": expected,
            "actual": actual,
            "delta_pct": round(delta_pct, 2) if delta_pct != float("inf") else None,
        },
    )


def assert_ground_truth_coverage(
    db: Database, *, min_coverage: float
) -> AssertionResult:
    """Assert labeled-fraction of ``core.fct_transactions`` meets ``min_coverage``.

    Coverage = (rows in fct that join through ``prep.int_transactions__matched``
    to ``synthetic.ground_truth`` with non-null expected_category) / (total rows
    in fct). Catches the failure mode where evaluations achieve high accuracy by
    scoring only a tiny labeled subset of the fact table.
    """
    if not 0.0 <= min_coverage <= 1.0:
        raise ValueError(f"min_coverage must be in [0, 1], got {min_coverage}")
    total_row = db.execute(
        f"SELECT COUNT(*) FROM {FCT_TRANSACTIONS.full_name}"  # noqa: S608  # TableRef constant
    ).fetchone()
    total = int(total_row[0]) if total_row else 0
    labeled_row = db.execute(
        f"""
        SELECT COUNT(*) FROM {FCT_TRANSACTIONS.full_name} t
        JOIN {INT_TRANSACTIONS_MATCHED.full_name} m
          ON m.transaction_id = t.transaction_id
        JOIN {GROUND_TRUTH.full_name} gt
          ON gt.source_transaction_id = m.source_transaction_id
        WHERE gt.expected_category IS NOT NULL
        """  # noqa: S608  # TableRef constants
    ).fetchone()
    labeled = int(labeled_row[0]) if labeled_row else 0
    coverage = (labeled / total) if total else 0.0
    return AssertionResult(
        name="ground_truth_coverage",
        passed=coverage >= min_coverage,
        details={
            "labeled": labeled,
            "total": total,
            "coverage": round(coverage, 4),
            "min_coverage": min_coverage,
        },
    )
