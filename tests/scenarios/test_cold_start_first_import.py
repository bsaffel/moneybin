"""Scenario: cold-start first import validates merchant matcher coverage.

A populated user_merchants catalog (as the snowball would produce) feeds the
matcher; this scenario confirms that the resolved core.dim_merchants view
plus categorize_pending() yields meaningful coverage. The literal first-run
state (empty user_merchants + LLM-assist) is a separate scenario not yet
implemented in the runner.
"""

from __future__ import annotations

import pytest

from tests.scenarios._runner import load_shipped_scenario, run_scenario


@pytest.mark.scenarios
@pytest.mark.slow
def test_cold_start_first_import() -> None:
    """Merchant rules cover a meaningful fraction of first-import transactions."""
    scenario = load_shipped_scenario("cold-start-first-import")
    assert scenario is not None
    result = run_scenario(scenario)
    assert result.passed, result.failure_summary()
