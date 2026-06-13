"""Scenario: exact-key auto-merge must pair N true duplicates 1:1, not all-to-all.

Two genuinely-distinct $5 charges on the same day, each exported by both
formats (2 csv + 2 ofx). The cardinality guard must keep them as TWO gold
records (source_count=2 each) instead of collapsing all four into one
(source_count=4), which would silently delete a real charge.
"""

from __future__ import annotations

import pytest

from moneybin.database import Database
from moneybin.validation.result import AssertionResult
from tests.scenarios._runner import load_shipped_scenario, run_scenario


@pytest.mark.scenarios
@pytest.mark.slow
def test_dedup_overmerge_guard() -> None:
    """tiers: T1, T2-negative-expectations (over-merge precision guard)."""
    scenario = load_shipped_scenario("dedup-overmerge-guard")
    assert scenario is not None

    def _backfill(db: Database) -> list[AssertionResult]:
        # Both gold records must carry exactly two sources — proof that neither
        # over-collapsed (which would yield one record with source_count=4) nor
        # failed to merge (two records with source_count=1).
        rows = db.execute(
            "SELECT source_count FROM core.fct_transactions"  # noqa: S608 — no input
        ).fetchall()
        counts = sorted(int(r[0]) for r in rows)
        return [
            AssertionResult(
                name="both_gold_records_source_count_is_2",
                passed=counts == [2, 2],
                details={"source_counts": counts, "expected": [2, 2]},
            ),
        ]

    result = run_scenario(scenario, extra_assertions=_backfill)
    assert result.passed, result.failure_summary()
