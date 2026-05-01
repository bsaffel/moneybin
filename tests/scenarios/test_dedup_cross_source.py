"""Scenario: cross-source dedup collapses 6 fixture rows into 3 gold records."""

from __future__ import annotations

from datetime import date

import pytest

from moneybin.database import Database
from moneybin.validation.assertions import (
    assert_amount_precision,
    assert_date_bounds,
    assert_row_count_exact,
    assert_schema_snapshot,
    assert_source_system_populated,
)
from moneybin.validation.result import AssertionResult
from tests.scenarios._runner import load_shipped_scenario, run_scenario
from tests.scenarios._tier1_backfill import FCT_TRANSACTIONS_SCHEMA


@pytest.mark.scenarios
@pytest.mark.slow
def test_dedup_cross_source() -> None:
    """tiers: T1, T2-dedup-collapse.

    Row count and date window are fixture-derived (six labeled rows across
    two CSVs collapse to three gold records; all dates fall in 2024-03).
    """
    scenario = load_shipped_scenario("dedup-cross-source")
    assert scenario is not None

    def _backfill(db: Database) -> list[AssertionResult]:
        return [
            assert_source_system_populated(
                db,
                table="core.fct_transactions",
                expected_sources={"csv", "ofx"},
                column="source_type",
            ),
            assert_amount_precision(
                db,
                table="core.fct_transactions",
                column="amount",
                precision=18,
                scale=2,
            ),
            assert_date_bounds(
                db,
                table="core.fct_transactions",
                column="transaction_date",
                # Fixture window — three rows on 2024-03-{15,22,30}.
                min_date=date(2024, 3, 1),
                max_date=date(2024, 3, 31),
            ),
            assert_row_count_exact(
                db,
                table="core.fct_transactions",
                # Hand-counted from the fixture: six labeled rows
                # collapse to three gold records.
                expected=3,
            ),
            assert_schema_snapshot(
                db,
                table="core.fct_transactions",
                expected=FCT_TRANSACTIONS_SCHEMA,
            ),
        ]

    result = run_scenario(scenario, extra_assertions=_backfill)
    assert result.passed, result.failure_summary()
