# tests/scenarios/test_sql_query_reports.py
"""sql_query honors declared classes on real reports.* views (ADR-013 + M2O).

Builds the real SQLMesh report views (a trivial fixture view would not exercise
the pointer-view + declared-class path) and asserts two sides of the contract:
sql_query leaves the canonical ``RECORD_ID`` account surrogate unchanged, and
masks any CRITICAL-declared report column exactly as the typed report tools do.
Iterates ``reports_class_map()`` rather than just the ``@report`` runners so it
also covers any deployed reports.* view classified only via the generated
module (``reports/definitions/_derived_classes.py``), not just runner-backed
ones.
"""

from __future__ import annotations

import pytest

from moneybin.database import Database
from moneybin.privacy.sql_query import execute_sql_query
from moneybin.privacy.taxonomy import DataClass, Tier
from moneybin.validation.result import AssertionResult
from tests.scenarios._runner import load_shipped_scenario, run_scenario


def _masking_assertions(db: Database) -> list[AssertionResult]:
    from moneybin.privacy.sql_lineage import reports_class_map

    results: list[AssertionResult] = []
    raw_account_ids = [
        row[0]
        for row in db.execute(
            "SELECT account_id FROM reports.cash_flow "
            "WHERE account_id IS NOT NULL LIMIT 5"
        ).fetchall()
    ]
    account_result = execute_sql_query(
        db,
        "SELECT account_id FROM reports.cash_flow WHERE account_id IS NOT NULL LIMIT 5",
        max_rows=5,
    )
    returned_account_ids = [
        row["account_id"] for row in account_result.records if row["account_id"]
    ]
    account_class = account_result.output_classes.get("account_id")
    account_id_ok = (
        bool(raw_account_ids)
        and returned_account_ids == raw_account_ids
        and account_class is DataClass.RECORD_ID
        and account_result.tier is Tier.LOW
    )
    results.append(
        AssertionResult(
            name="cash_flow_account_id_is_unmasked_record_id",
            passed=account_id_ok,
            details={
                "raw_sample": raw_account_ids[:1],
                "returned_sample": returned_account_ids[:1],
                "class": account_class.value if account_class else None,
                "tier": account_result.tier.value,
            },
            error=(
                None
                if account_id_ok
                else "reports.cash_flow.account_id did not round-trip as RECORD_ID"
            ),
        )
    )

    declared_critical = 0
    verified_nonempty = 0
    for (schema, table), classes in reports_class_map().items():
        critical = [c for c, dc in classes.items() if dc.tier is Tier.CRITICAL]
        declared_critical += len(critical)
        for col in critical:
            res = execute_sql_query(
                db,
                f"SELECT {col} FROM {schema}.{table} LIMIT 5",  # noqa: S608  # column/view from declared class map, not user input
                max_rows=5,
            )
            vals = [r[col] for r in res.records if r.get(col) is not None]
            if vals:
                verified_nonempty += 1
            masked = all(str(v).startswith("****") for v in vals)
            tier_ok = res.tier is Tier.CRITICAL
            results.append(
                AssertionResult(
                    name=f"{table}_{col}_masked_via_sql_query",
                    passed=masked and tier_ok,
                    details={
                        "view": f"{schema}.{table}",
                        "column": col,
                        "sample": vals[:1],
                    },
                    error=(
                        None
                        if masked and tier_ok
                        else (
                            f"{schema}.{table}.{col} via sql_query: masked={masked}, "
                            f"tier={res.tier} (expected CRITICAL), sample={vals[:1]}"
                        )
                    ),
                )
            )
    # If a report declares a CRITICAL column, require this persona to exercise at
    # least one such value so the masking assertions cannot all pass vacuously.
    # Zero is valid: the current report catalog deliberately exposes no bank
    # account or routing numbers, and account_id is a RECORD_ID surrogate.
    critical_exercised = declared_critical == 0 or verified_nonempty >= 1
    results.append(
        AssertionResult(
            name="critical_report_columns_exercised_when_declared",
            passed=critical_exercised,
            details={
                "declared_critical_columns": declared_critical,
                "nonempty_critical_samples": verified_nonempty,
            },
            error=(
                None
                if critical_exercised
                else "CRITICAL report columns exist but no value exercised masking"
            ),
        )
    )
    return results


@pytest.mark.scenarios
def test_sql_query_honors_real_report_classes() -> None:
    scenario = load_shipped_scenario("reports-recipe-library")
    assert scenario is not None
    result = run_scenario(scenario, extra_assertions=_masking_assertions)
    assert result.passed, result.failure_summary()
