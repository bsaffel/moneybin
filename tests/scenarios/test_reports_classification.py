"""Report classification completeness, against the REAL SQLMesh-built views.

ADR-013: a report declares its output-column→DataClass map on ``@report`` and
redaction masks by that map. SQLMesh deploys each ``reports.*`` view as a
``SELECT * FROM <internal physical table>`` pointer, so lineage on the view body
can't classify it — declared classes are the contract. ``sql_query`` allows the
whole ``reports`` schema, so this scenario enumerates every DEPLOYED
``reports.*`` view (not just the ``@report`` runners — some views predate a
runner or have none yet) and asserts **completeness**: every column the
deployed view exposes is declared in ``reports_class_map()`` (a runner's
declared classes OR the generated ``reports/definitions/_derived_classes.py``
module), so no column hits the fail-closed fallback at runtime.

This does not additionally require ``account_id`` to be declared
``ACCOUNT_IDENTIFIER``: it is a deliberately opaque minted surrogate
classified ``RECORD_ID`` (LOW) everywhere in ``CLASSIFICATION`` (spec D6,
commit c465f181), and derivation reproduces that answer for reports.* views
that select it unchanged. A handful of runners over-declare it
``ACCOUNT_IDENTIFIER`` anyway — safe because ``RECORD_ID`` is LOW, so that
over-declares ACROSS tiers, which
``test_declared_classes_match_derivation``'s ``(tier, mask strength)``
comparison in ``tests/privacy/test_report_class_derivation.py`` allows. It is
NOT an instance of "over-declaring never leaks": at equal CRITICAL tier a
partial-masking class standing in for a whole-masking one does leak. Requiring
``ACCOUNT_IDENTIFIER`` uniformly here would be wrong for a view that correctly
declares ``RECORD_ID``.

A trivial hand-written fixture view (as the unit tests use) would not catch a
gap between a report's declared map and its real multi-CTE view — that gap is
exactly how the lineage approach leaked. This test closes that hole, and is
the one piece of this contract that genuinely needs a real, deployed
database: enumerating "every column a real view exposes" means querying
``duckdb_columns()`` against a built catalog, which no connectionless deriver
can do. The declared-vs-derived tier comparison (``test_declared_classes_match_derivation``
and its ``core.*`` generalization) needs no database at all — both live in
``tests/privacy/test_report_class_derivation.py`` and run in the default unit
gate instead of here.
"""

from __future__ import annotations

import pytest

from moneybin.database import Database
from moneybin.validation.result import AssertionResult
from tests.scenarios._runner import load_shipped_scenario, run_scenario


def _classification_assertions(db: Database) -> list[AssertionResult]:
    from moneybin.privacy.sql_lineage import reports_class_map

    class_map = reports_class_map()
    results: list[AssertionResult] = []
    # Every DEPLOYED reports.* view must be covered by reports_class_map (a
    # runner's declared classes OR the generated _derived_classes.py module).
    # sql_query allows the whole reports schema, so an uncovered view leaks
    # via the fallback.
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
    return results


@pytest.mark.scenarios
def test_reports_declared_classes_cover_real_views() -> None:
    scenario = load_shipped_scenario("reports-recipe-library")
    assert scenario is not None
    result = run_scenario(scenario, extra_assertions=_classification_assertions)
    assert result.passed, result.failure_summary()
