"""Distributional / cardinality smoke checks. Bounds are scenario-author chosen — soft signal."""

from __future__ import annotations

from duckdb import DuckDBPyConnection

from moneybin.validation.assertions._helpers import quote_ident as _quote_ident
from moneybin.validation.result import AssertionResult


def assert_distribution_within_bounds(
    conn: DuckDBPyConnection,
    *,
    table: str,
    col: str,
    min_value: float,
    max_value: float,
    mean_range: tuple[float, float],
) -> AssertionResult:
    """Assert column statistics fall within author-specified bounds."""
    t, c = _quote_ident(table), _quote_ident(col)
    row = conn.execute(
        f"SELECT MIN({c}), MAX({c}), AVG({c}) FROM {t}"  # noqa: S608  # identifiers validated by _quote_ident
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
    conn: DuckDBPyConnection,
    *,
    table: str,
    col: str,
    expected: int,
    tolerance_pct: float,
) -> AssertionResult:
    """Assert the number of distinct values in a column is within tolerance of expected."""
    t, c = _quote_ident(table), _quote_ident(col)
    actual = int(conn.execute(f"SELECT COUNT(DISTINCT {c}) FROM {t}").fetchone()[0])  # noqa: S608  # type: ignore[index]  # identifiers validated by _quote_ident
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
