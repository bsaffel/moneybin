"""Report classification contract, against the REAL SQLMesh-built views.

ADR-013: a report declares its output-column→DataClass map on ``@report`` and
redaction masks by that map. SQLMesh deploys each ``reports.*`` view as a
``SELECT * FROM <internal physical table>`` pointer, so lineage on the view body
can't classify it — declared classes are the contract. ``sql_query`` allows the
whole ``reports`` schema, so this scenario enumerates every DEPLOYED
``reports.*`` view (not just the ``@report`` runners — some views predate a
runner or have none yet) and asserts each is fully covered by
``reports_class_map()`` (a runner's declared classes OR the transitional
bridge in ``reports/definitions/_bridged_classes.py``):

  1. **Completeness** — every column the deployed view exposes is declared, so
     no column hits the fail-closed fallback at runtime.
  2. **Identifier safety** — ``account_id``, where present, is declared
     ``ACCOUNT_IDENTIFIER`` (CRITICAL), which ``redact_records`` masks.

A trivial hand-written fixture view (as the unit tests use) would not catch a
gap between a report's declared map and its real multi-CTE view — that gap is
exactly how the lineage approach leaked. This test closes that hole.
"""

from __future__ import annotations

import pytest

from moneybin.database import Database
from moneybin.privacy.taxonomy import DataClass
from moneybin.validation.result import AssertionResult
from tests.scenarios._runner import load_shipped_scenario, run_scenario


def _classification_assertions(db: Database) -> list[AssertionResult]:
    from moneybin.privacy.sql_lineage import reports_class_map

    class_map = reports_class_map()
    results: list[AssertionResult] = []
    # Every DEPLOYED reports.* view must be covered by reports_class_map (a
    # runner's declared classes OR the transitional bridge). sql_query allows
    # the whole reports schema, so an uncovered view leaks via the fallback.
    deployed = [
        r[0]
        for r in db.execute(
            "SELECT DISTINCT table_name FROM duckdb_columns() "
            "WHERE schema_name = 'reports' ORDER BY table_name"
        ).fetchall()
    ]
    assert deployed, "expected deployed reports.* views in this scenario"
    for view in deployed:
        cursor = db.execute(f"SELECT * FROM reports.{view} LIMIT 0")  # noqa: S608  # catalog view name
        real_cols = [d[0] for d in cursor.description] if cursor.description else []
        declared = class_map.get(("reports", view), {})
        undeclared = [c for c in real_cols if c not in declared]
        results.append(
            AssertionResult(
                name=f"reports_{view}_fully_classified",
                passed=not undeclared,
                details={"view": f"reports.{view}", "undeclared": undeclared},
                error=(
                    f"reports.{view}: columns not in reports_class_map: {undeclared}"
                    if undeclared
                    else None
                ),
            )
        )
        if "account_id" in real_cols:
            declared_ac = declared.get("account_id")
            ok = declared_ac is DataClass.ACCOUNT_IDENTIFIER
            results.append(
                AssertionResult(
                    name=f"reports_{view}_account_id_is_critical",
                    passed=ok,
                    details={"account_id": str(declared_ac)},
                    error=(
                        None
                        if ok
                        else f"reports.{view}: account_id is {declared_ac}, not ACCOUNT_IDENTIFIER"
                    ),
                )
            )
    return results


@pytest.mark.scenarios
def test_reports_declared_classes_cover_real_views() -> None:
    scenario = load_shipped_scenario("reports-recipe-library")
    assert scenario is not None
    result = run_scenario(scenario, extra_assertions=_classification_assertions)
    assert result.passed, result.failure_summary()
