# tests/scenarios/test_sql_query_reports.py
"""sql_query against real reports.* views masks CRITICAL columns (ADR-013 + M2O).

Builds the real SQLMesh report views (a trivial fixture view would not exercise
the pointer-view + declared-class path) and asserts sql_query masks any
CRITICAL-declared column exactly as the typed report tools do.
"""

from __future__ import annotations

import pytest

from moneybin.database import Database
from moneybin.privacy.sql_query import execute_sql_query
from moneybin.privacy.taxonomy import Tier
from moneybin.reports._framework.registry import spec_of
from moneybin.reports.definitions import ALL_REPORTS
from moneybin.validation.result import AssertionResult
from tests.scenarios._runner import load_shipped_scenario, run_scenario


def _masking_assertions(db: Database) -> list[AssertionResult]:
    results: list[AssertionResult] = []
    for runner in ALL_REPORTS:
        spec = spec_of(runner)
        critical = [c for c, dc in spec.classes.items() if dc.tier is Tier.CRITICAL]
        if not critical:
            continue
        col = critical[0]
        res = execute_sql_query(
            db,
            f"SELECT {col} FROM {spec.view.full_name} LIMIT 5",  # noqa: S608  # column/view from declared spec, not user input
            max_rows=5,
        )
        vals = [r[col] for r in res.records if r.get(col) is not None]
        masked = all(str(v).startswith("*") for v in vals)
        results.append(
            AssertionResult(
                name=f"{spec.name}_{col}_masked_via_sql_query",
                passed=masked and res.tier is Tier.CRITICAL,
                details={
                    "view": spec.view.full_name,
                    "column": col,
                    "sample": vals[:1],
                },
                error=(
                    None
                    if masked
                    else f"{spec.name}.{col} returned unmasked via sql_query: {vals[:1]}"
                ),
            )
        )
    return results


@pytest.mark.scenarios
def test_sql_query_masks_reports_critical_columns() -> None:
    scenario = load_shipped_scenario("reports-recipe-library")
    assert scenario is not None
    result = run_scenario(scenario, extra_assertions=_masking_assertions)
    assert result.passed, result.failure_summary()
