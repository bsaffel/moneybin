"""Scenario: end-to-end pipeline correctness for the family persona (3 years)."""

from __future__ import annotations

import pytest

from tests.scenarios._runner import load_shipped_scenario, run_scenario
from tests.scenarios._tier1_backfill import tier1_backfill


@pytest.mark.scenarios
@pytest.mark.slow
def test_family_full_pipeline() -> None:
    """tiers: T1, T2-balanced-transfers, T2-categorization-pr, T2-transfer-f1.

    The Tier 1 row-count check uses the deterministic GeneratorEngine output
    (replacing the previous ±15% tolerance band that was an observe-and-paste
    expectation — see .claude/rules/testing.md).
    """
    scenario = load_shipped_scenario("family-full-pipeline")
    assert scenario is not None
    result = run_scenario(
        scenario,
        extra_assertions=tier1_backfill(scenario.setup),
    )
    assert result.passed, result.failure_summary()
