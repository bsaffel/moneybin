"""Scenario: cross-account transfer pairs detected; F1 vs ground truth."""

from __future__ import annotations

import pytest

from moneybin.database import Database
from moneybin.validation.assertions import assert_date_continuity
from moneybin.validation.result import AssertionResult
from tests.scenarios._runner import load_shipped_scenario, run_scenario
from tests.scenarios._tier1_backfill import tier1_backfill


@pytest.mark.scenarios
@pytest.mark.slow
def test_transfer_detection() -> None:
    """tiers: T1, T2-balanced-transfers, T2-transfer-f1, T4."""
    scenario = load_shipped_scenario("transfer-detection-cross-account")
    assert scenario is not None
    base = tier1_backfill(scenario.setup)

    def extra(db: Database) -> list[AssertionResult]:
        return [
            *base(db),
            # Tier 4 date continuity: every account gap must be < 31 days.
            assert_date_continuity(
                db,
                table="core.fct_transactions",
                date_col="transaction_date",
                account_col="account_id",
            ),
        ]

    result = run_scenario(scenario, extra_assertions=extra)
    assert result.passed, result.failure_summary()

    # Assert transfer-detection precision and recall separately, not just F1,
    # to catch one-sided bias (high-precision low-recall or the inverse).
    # Baseline measured 2026-05-01: P=1.0, R=1.0 over 72 true pairs. 0.95
    # floor leaves headroom for occasional generator/rule drift.
    tx_eval = next(e for e in result.evaluations if e.name == "transfer_f1")
    assert tx_eval.breakdown["precision"] >= 0.95, tx_eval.breakdown
    assert tx_eval.breakdown["recall"] >= 0.95, tx_eval.breakdown
