"""Scenario: end-to-end pipeline correctness for the family persona (3 years)."""

from __future__ import annotations

import pytest

from moneybin.database import Database
from moneybin.validation.assertions import (
    assert_date_continuity,
    assert_ground_truth_coverage,
)
from moneybin.validation.result import AssertionResult
from tests.scenarios._runner import load_shipped_scenario, run_scenario
from tests.scenarios._tier1_backfill import tier1_backfill

# Per-category recall is too noisy to gate on at small support; skip
# the assertion for categories with fewer than this many labeled rows.
_MIN_SUPPORT_FOR_RECALL_CHECK = 5


@pytest.mark.scenarios
@pytest.mark.slow
def test_family_full_pipeline() -> None:
    """tiers: T1, T2-balanced-transfers, T2-categorization-pr, T2-transfer-f1, T4.

    The Tier 1 row-count check uses the deterministic GeneratorEngine output
    (replacing the previous +/-15% tolerance band that was an observe-and-paste
    expectation -- see .claude/rules/testing.md).
    """
    scenario = load_shipped_scenario("family-full-pipeline")
    assert scenario is not None
    base = tier1_backfill(scenario.setup)

    def extra(db: Database) -> list[AssertionResult]:
        return [
            *base(db),
            # Tier 4 ground-truth coverage. The synthetic family persona
            # labels every generated transaction, so coverage is expected
            # to be ~1.0; 0.9 leaves headroom for downstream filtering.
            assert_ground_truth_coverage(db, min_coverage=0.9),
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

    # Assert per-category recall floors. These complement the overall
    # categorization_accuracy threshold by catching one-sided failures
    # (e.g. a single dominant category masking poor recall on the rest).
    # Floor calibrated to (current min recall - 0.05) rounded down. Baseline
    # measured 2026-05-01: Shopping=0.776 (lowest of 10 categories with
    # support >= 67); all others 1.0. 0.7 catches regressions without flapping
    # on rule-set tweaks. Categories with <5 labeled rows are skipped because
    # per-category P/R is too noisy at small support to gate on.
    cat_eval = next(
        e for e in result.evaluations if e.name == "categorization_accuracy"
    )
    per_cat = cat_eval.breakdown["per_category"]
    for category, stats in per_cat.items():
        if stats["support"] >= _MIN_SUPPORT_FOR_RECALL_CHECK:
            assert stats["recall"] >= 0.7, f"recall too low for {category}: {stats}"

    # Assert transfer-detection precision and recall separately, not just F1,
    # to catch one-sided bias (high-precision low-recall or the inverse).
    # Baseline measured 2026-05-01: P=1.0, R=1.0 over 108 true pairs. 0.95
    # floor leaves headroom for occasional generator/rule drift.
    tx_eval = next(e for e in result.evaluations if e.name == "transfer_f1")
    assert tx_eval.breakdown["precision"] >= 0.95, tx_eval.breakdown
    assert tx_eval.breakdown["recall"] >= 0.95, tx_eval.breakdown
