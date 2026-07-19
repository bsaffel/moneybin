"""Report classification contract, against the REAL SQLMesh-built views.

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
``ACCOUNT_IDENTIFIER`` anyway (safe — over-declaring never leaks, per
``test_declared_classes_match_derivation``'s tier comparison below), but
requiring it uniformly here would be wrong for a view that correctly
declares ``RECORD_ID``.

A trivial hand-written fixture view (as the unit tests use) would not catch a
gap between a report's declared map and its real multi-CTE view — that gap is
exactly how the lineage approach leaked. This test closes that hole.
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


def _all_class_downgrades() -> dict[tuple[str, str], dict[str, str]]:
    """(schema, table) -> {column: reason}, from every ``@report`` runner.

    Runner-less views (``reports/definitions/_derived_classes.py``, generated)
    carry no ``class_downgrades`` — a generated entry is derivation's own
    answer, not a decorator-attached spec with an author-supplied override, so
    there is nothing to downgrade *from* here.
    """
    from moneybin.reports._framework.registry import spec_of
    from moneybin.reports.definitions import ALL_REPORTS

    out: dict[tuple[str, str], dict[str, str]] = {}
    for runner in ALL_REPORTS:
        spec = spec_of(runner)
        out[(spec.view.schema, spec.view.name)] = dict(spec.class_downgrades)
    return out


@pytest.mark.scenarios
def test_declared_classes_match_derivation() -> None:
    """Every declared class is derivation-matched or explicitly downgraded.

    ``derive_report_classes`` (build-time, no DB — see ADR-013 follow-up in
    ``report_class_derivation.py``) recomputes each column's class from the
    SQLMesh model source; this compares it against the declared contract.
    Compares by **tier**, not class identity: redaction is tier-driven, so
    ``declared.tier >= derived.tier`` is always safe (over-declaring never
    leaks). Only a genuine downgrade (``declared.tier < derived.tier``)
    requires an explicit, reasoned ``class_downgrades`` entry.
    """
    from moneybin.privacy.report_class_derivation import derive_report_classes
    from moneybin.privacy.sql_lineage import reports_class_map

    derived = derive_report_classes()
    declared = reports_class_map()
    downgrades = _all_class_downgrades()

    problems: list[str] = []
    for key, derived_cols in derived.items():
        for column, derived_class in derived_cols.items():
            declared_class = declared.get(key, {}).get(column)
            if declared_class is None:
                problems.append(f"{key[0]}.{key[1]}.{column}: undeclared")
                continue
            if declared_class.tier >= derived_class.tier:
                continue
            reason = downgrades.get(key, {}).get(column)
            if not reason:
                problems.append(
                    f"{key[0]}.{key[1]}.{column}: declared {declared_class.name} "
                    f"(tier {declared_class.tier.name}) below derived "
                    f"{derived_class.name} (tier {derived_class.tier.name}) "
                    "with no class_downgrades reason"
                )
    assert not problems, "Class declarations disagree with derivation:\n" + "\n".join(
        problems
    )
