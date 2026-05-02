"""Scenario: end-to-end pipeline correctness for the basic persona."""

from __future__ import annotations

import pytest

from tests.scenarios._runner import load_shipped_scenario, run_scenario
from tests.scenarios._tier1_backfill import tier1_backfill


@pytest.mark.scenarios
@pytest.mark.slow
def test_basic_full_pipeline() -> None:
    """tiers: T1 (source/schema/amount/date/row-count), T2-categorization-pr."""
    scenario = load_shipped_scenario("basic-full-pipeline")
    assert scenario is not None
    result = run_scenario(
        scenario,
        extra_assertions=tier1_backfill(scenario.setup),
    )
    assert result.passed, result.failure_summary()
