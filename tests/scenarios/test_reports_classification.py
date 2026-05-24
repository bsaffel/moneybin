"""Report classification contract, against the REAL SQLMesh-built views.

ADR-011: a report declares its output-column→DataClass map on ``@report`` and
redaction masks by that map. SQLMesh deploys each ``reports.*`` view as a
``SELECT * FROM <internal physical table>`` pointer, so lineage on the view body
can't classify it — declared classes are the contract. This scenario builds the
real views and asserts:

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
from moneybin.reports._framework.registry import spec_of
from moneybin.reports.definitions import ALL_REPORTS
from moneybin.validation.result import AssertionResult
from tests.scenarios._runner import load_shipped_scenario, run_scenario


def _classification_assertions(db: Database) -> list[AssertionResult]:
    results: list[AssertionResult] = []
    for runner in ALL_REPORTS:
        spec = spec_of(runner)
        cursor = db.execute(f"SELECT * FROM {spec.view.full_name} LIMIT 0")  # noqa: S608  # TableRef
        real_cols = [d[0] for d in cursor.description] if cursor.description else []

        undeclared = [c for c in real_cols if c not in spec.classes]
        results.append(
            AssertionResult(
                name=f"{spec.name}_classes_cover_view",
                passed=not undeclared,
                details={"view": spec.view.full_name, "undeclared": undeclared},
                error=(
                    f"{spec.name}: view columns not in declared classes: {undeclared}"
                    if undeclared
                    else None
                ),
            )
        )

        if "account_id" in real_cols:
            declared = spec.classes.get("account_id")
            ok = declared is DataClass.ACCOUNT_IDENTIFIER
            results.append(
                AssertionResult(
                    name=f"{spec.name}_account_id_is_critical",
                    passed=ok,
                    details={"account_id": str(declared)},
                    error=(
                        None
                        if ok
                        else f"{spec.name}: account_id is {declared}, not ACCOUNT_IDENTIFIER"
                    ),
                )
            )
    return results


@pytest.mark.scenarios
def test_reports_declared_classes_cover_real_views() -> None:
    scenario = load_shipped_scenario("reports-recipe-library")
    assert scenario is not None
    run_scenario(scenario, extra_assertions=_classification_assertions)
