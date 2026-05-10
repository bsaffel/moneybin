"""Scenario: reports-recipe-library — exercise reports.* views + migrations.

Verifies that the eight ``reports.*`` SQLMesh views (added in Wave 2C) all
materialize against the basic synthetic persona, and that the four atomic
migrations they entail landed cleanly:

  - ``core.agg_net_worth`` retired in favor of ``reports.net_worth``.
  - ``app.categories`` retired in favor of ``core.dim_categories``.
  - ``app.merchants`` retired in favor of ``core.dim_merchants``.

The per-view row-count floors and the migration drops are encoded here as
extra assertions (rather than YAML primitives) because the generic scenario
runner doesn't ship table-existence / does-not-exist / inline-SQL
primitives — and adding such primitives just for this scenario would
expose them to every future YAML author. The Python entry point is the
right scope.

Derivation of expected values (per testing.md "Scenario Expectations Must
Be Independently Derived"):

  * Row-count floors come from the basic persona's deterministic recurring
    block: 2 years x 12 months x 5 monthly recurring outflows = 120 txns
    minimum. Reports views aggregating that input must produce >=1 row.
  * ``reports.net_worth`` ≥ 1: ``fct_balances_daily`` is a daily carry-forward
    table populated for every day each account is open; with 2 years of
    history and 2 accounts this is >=730 distinct dates.
  * ``reports.cash_flow`` ≥ 1: monthly grain over 2 years across 2 accounts
    gives >=24 rows even before category split.
  * ``reports.spending_trend`` ≥ 1: monthly grain across 6 spending
    categories over 2 years gives >=24 rows.
  * ``reports.merchant_activity`` ≥ 1: persona's merchant catalogs supply
    multiple distinct merchants per spending category.
  * ``reports.large_transactions`` ≥ 1: view returns one row per non-transfer
    transaction; ≥ 100 rows by the same persona derivation as fct above.
  * ``reports.uncategorized_queue`` ≥ 0: may legitimately be empty if
    categorize covered every txn — kept as a smoke check (view materializes).
  * ``reports.recurring_subscriptions`` ≥ 0: persona has 5 monthly recurring
    descriptions but they're generated under varied amounts (electric is
    stochastic). >=1 is plausible but not guaranteed by the persona config
    alone, so the floor is 0 (view materializes without SQL error).
  * ``reports.balance_drift`` ≥ 0: the basic persona generator does not
    write to ``app.balance_assertions``, so this view is expected empty.
    The assertion proves it materializes and SELECT-s cleanly.

Sentinel SQL invariants:

  * ``confidence`` in ``reports.recurring_subscriptions`` is computed via
    ``LEAST(1.0, ...) * GREATEST(0.0, 1.0 - LEAST(1.0, ...))``, which the
    SQL pins to [0, 1]. The invariant check would catch a future refactor
    that drops a clamp.
  * ``reports.net_worth`` groups by ``balance_date`` — at least one distinct
    date must appear if the view is wired to a non-empty
    ``core.fct_balances_daily``.
"""

from __future__ import annotations

import pytest

from moneybin.database import Database
from moneybin.validation.result import AssertionResult
from tests.scenarios._runner import load_shipped_scenario, run_scenario

# Tables/views expected to be ABSENT from the catalog after the migrations.
# Tracked here (not in YAML) so adding a new retired-entity check is a
# single-line code edit rather than a YAML primitive addition.
_RETIRED_ENTITIES: tuple[str, ...] = (
    "core.agg_net_worth",
    "app.categories",
    "app.merchants",
)

# Tables/views expected to be PRESENT after the migrations. Includes both
# the eight reports.* views and the two new core dimensions.
_PRESENT_ENTITIES: tuple[str, ...] = (
    "core.dim_categories",
    "core.dim_merchants",
    "reports.net_worth",
    "reports.cash_flow",
    "reports.spending_trend",
    "reports.uncategorized_queue",
    "reports.merchant_activity",
    "reports.large_transactions",
    "reports.balance_drift",
    "reports.recurring_subscriptions",
)

# Per-view minimum row counts. See module docstring for derivation.
_VIEW_MIN_ROWS: dict[str, int] = {
    "reports.net_worth": 1,
    "reports.cash_flow": 1,
    "reports.spending_trend": 1,
    "reports.merchant_activity": 1,
    "reports.large_transactions": 1,
    # 0-floor views materialize but may legitimately be empty on the basic
    # persona; the row-count check still catches a SQL error during SELECT.
    "reports.uncategorized_queue": 0,
    "reports.recurring_subscriptions": 0,
    "reports.balance_drift": 0,
}


def _catalog_entities(db: Database) -> set[str]:
    """Return the set of ``schema.name`` strings for every table and view."""
    rows = db.execute(
        """
        SELECT schema_name || '.' || table_name FROM duckdb_tables()
        UNION ALL
        SELECT schema_name || '.' || view_name FROM duckdb_views()
        """
    ).fetchall()
    return {row[0] for row in rows}


def _reports_assertions(db: Database) -> list[AssertionResult]:
    """Build the migration/existence/row-count/invariant assertion set."""
    results: list[AssertionResult] = []
    catalog = _catalog_entities(db)

    # Retired entities must be absent.
    for name in _RETIRED_ENTITIES:
        present = name in catalog
        results.append(
            AssertionResult(
                name=f"retired_{name}_absent",
                passed=not present,
                details={"entity": name, "found_in_catalog": present},
                error=(
                    f"{name} still exists in catalog after migration"
                    if present
                    else None
                ),
            )
        )

    # New + reports entities must be present.
    for name in _PRESENT_ENTITIES:
        present = name in catalog
        results.append(
            AssertionResult(
                name=f"present_{name}",
                passed=present,
                details={"entity": name, "found_in_catalog": present},
                error=(
                    None if present else f"{name} missing from catalog after pipeline"
                ),
            )
        )

    # Per-view row-count floors. Each SELECT also doubles as a "view
    # materializes without SQL error" smoke check — a parser bug in
    # reports.large_transactions (MEDIAN+MAD nesting) would surface here.
    for view, min_rows in _VIEW_MIN_ROWS.items():
        if view not in catalog:
            # The present-check above already failed; skip the row probe so
            # we don't double-report (and so a missing view doesn't crash
            # the SELECT below).
            continue
        try:
            row = db.execute(f"SELECT COUNT(*) FROM {view}").fetchone()  # noqa: S608  # view name from compile-time allowlist
            count = int(row[0]) if row else 0
            results.append(
                AssertionResult(
                    name=f"rows_{view}_at_least_{min_rows}",
                    passed=count >= min_rows,
                    details={"view": view, "count": count, "min_rows": min_rows},
                    error=(
                        None
                        if count >= min_rows
                        else f"{view} has {count} rows; expected >= {min_rows}"
                    ),
                )
            )
        except Exception as exc:  # noqa: BLE001 — surface as structured failure
            results.append(
                AssertionResult(
                    name=f"rows_{view}_at_least_{min_rows}",
                    passed=False,
                    details={"view": view},
                    error=f"SELECT from {view} failed: {type(exc).__name__}: {exc}",
                )
            )

    # Sentinel SQL invariants.
    # 1. confidence in [0, 1]. SQL clamps via LEAST/GREATEST; an out-of-band
    #    row would indicate a future refactor dropped a clamp.
    if "reports.recurring_subscriptions" in catalog:
        try:
            row = db.execute(
                "SELECT COUNT(*) FROM reports.recurring_subscriptions "
                "WHERE confidence < 0 OR confidence > 1"
            ).fetchone()
            bad = int(row[0]) if row else 0
            results.append(
                AssertionResult(
                    name="recurring_subscriptions_confidence_in_unit_interval",
                    passed=bad == 0,
                    details={"out_of_range_rows": bad},
                    error=(
                        None
                        if bad == 0
                        else f"{bad} rows have confidence outside [0, 1]"
                    ),
                )
            )
        except Exception as exc:  # noqa: BLE001 — surface as structured failure
            results.append(
                AssertionResult(
                    name="recurring_subscriptions_confidence_in_unit_interval",
                    passed=False,
                    details={},
                    error=f"invariant query failed: {type(exc).__name__}: {exc}",
                )
            )

    # 2. reports.net_worth must group over >=1 distinct balance_date when
    #    fct_balances_daily is populated. 0 would mean the GROUP BY is
    #    silently dropping every row.
    if "reports.net_worth" in catalog:
        try:
            row = db.execute(
                "SELECT COUNT(DISTINCT balance_date) FROM reports.net_worth"
            ).fetchone()
            distinct_dates = int(row[0]) if row else 0
            results.append(
                AssertionResult(
                    name="net_worth_has_distinct_dates",
                    passed=distinct_dates > 0,
                    details={"distinct_balance_dates": distinct_dates},
                    error=(
                        None
                        if distinct_dates > 0
                        else "reports.net_worth has 0 distinct balance_date rows"
                    ),
                )
            )
        except Exception as exc:  # noqa: BLE001 — surface as structured failure
            results.append(
                AssertionResult(
                    name="net_worth_has_distinct_dates",
                    passed=False,
                    details={},
                    error=f"invariant query failed: {type(exc).__name__}: {exc}",
                )
            )

    return results


@pytest.mark.scenarios
@pytest.mark.slow
def test_reports_recipe_library() -> None:
    """Eight reports.* views materialize; four migrations are clean on basic persona."""
    scenario = load_shipped_scenario("reports-recipe-library")
    assert scenario is not None
    result = run_scenario(scenario, extra_assertions=_reports_assertions)
    assert result.passed, result.failure_summary()
