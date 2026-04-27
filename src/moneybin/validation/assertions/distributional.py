"""Distributional / cardinality smoke checks. Bounds are scenario-author chosen — soft signal."""

from __future__ import annotations

from duckdb import DuckDBPyConnection

from moneybin.validation.assertions.relational import (
    _quote_ident,  # pyright: ignore[reportPrivateUsage]
)
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
    mn, mx, avg = float(row[0]), float(row[1]), float(row[2])  # type: ignore[index]
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
    delta_pct = abs(actual - expected) / expected * 100 if expected else 0.0
    return AssertionResult(
        name="unique_value_count",
        passed=delta_pct <= tolerance_pct,
        details={
            "expected": expected,
            "actual": actual,
            "delta_pct": round(delta_pct, 2),
        },
    )
