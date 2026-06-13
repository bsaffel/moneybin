"""Scenario: OFX-truncated descriptions auto-merge with CSV twins on exact key.

Reproduces the WF `.qfx` + `.csv` doubling (558 rows instead of 279): same
account + exact amount + same day, but cross-format description similarity is
well below the auto-merge threshold. The exact-key rule must collapse each twin
pair to one gold record with source_count=2.
"""

from __future__ import annotations

import pytest

from moneybin.database import Database
from moneybin.validation.assertions import assert_row_count_exact
from moneybin.validation.result import AssertionResult
from tests.scenarios._runner import load_shipped_scenario, run_scenario


@pytest.mark.scenarios
@pytest.mark.slow
def test_dedup_cross_format_truncation() -> None:
    """tiers: T1, T2-dedup-collapse (low-similarity cross-format)."""
    scenario = load_shipped_scenario("dedup-cross-format-truncation")
    assert scenario is not None

    def _backfill(db: Database) -> list[AssertionResult]:
        # Hand-derived: 4 CSV + 4 OFX twin rows collapse to 4 gold records,
        # each with exactly two contributing sources.
        rows = db.execute(
            "SELECT source_count FROM core.fct_transactions"  # noqa: S608 — no input
        ).fetchall()
        counts = sorted(int(r[0]) for r in rows)
        return [
            assert_row_count_exact(db, table="core.fct_transactions", expected=4),
            AssertionResult(
                name="every_gold_record_source_count_is_2",
                passed=counts == [2, 2, 2, 2],
                details={"source_counts": counts, "expected": [2, 2, 2, 2]},
            ),
        ]

    result = run_scenario(scenario, extra_assertions=_backfill)
    assert result.passed, result.failure_summary()
