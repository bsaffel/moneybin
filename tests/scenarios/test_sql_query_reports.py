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
    verified_nonempty = 0
    for runner in ALL_REPORTS:
        spec = spec_of(runner)
        critical = [c for c, dc in spec.classes.items() if dc.tier is Tier.CRITICAL]
        for col in critical:
            res = execute_sql_query(
                db,
                f"SELECT {col} FROM {spec.view.full_name} LIMIT 5",  # noqa: S608  # column/view from declared spec, not user input
                max_rows=5,
            )
            vals = [r[col] for r in res.records if r.get(col) is not None]
            if vals:
                verified_nonempty += 1
            masked = all(str(v).startswith("****") for v in vals)
            tier_ok = res.tier is Tier.CRITICAL
            results.append(
                AssertionResult(
                    name=f"{spec.name}_{col}_masked_via_sql_query",
                    passed=masked and tier_ok,
                    details={
                        "view": spec.view.full_name,
                        "column": col,
                        "sample": vals[:1],
                    },
                    error=(
                        None
                        if masked and tier_ok
                        else (
                            f"{spec.name}.{col} via sql_query: masked={masked}, "
                            f"tier={res.tier} (expected CRITICAL), sample={vals[:1]}"
                        )
                    ),
                )
            )
    # Structural guard: the point of this test is to observe REAL masked CRITICAL
    # values. Every per-report assertion above passes vacuously on an empty result
    # (all([]) is True), so require at least one report to have returned a
    # non-empty CRITICAL sample — otherwise masking was never actually exercised.
    results.append(
        AssertionResult(
            name="at_least_one_critical_report_column_exercised",
            passed=verified_nonempty >= 1,
            details={"reports_with_nonempty_critical_samples": verified_nonempty},
            error=(
                None
                if verified_nonempty >= 1
                else "no CRITICAL report column returned any rows — masking never exercised"
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
