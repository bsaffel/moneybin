"""Scenario: cold-start first import validates seed merchant coverage.

A fresh install has only global seed merchants. This scenario confirms that
after a single import + categorize pass, seed rules provide meaningful
first-pass coverage — the starting point of the snowball effect where each
assisted categorization improves future auto-labeling.
"""

from __future__ import annotations

import pytest

from tests.scenarios._runner import load_shipped_scenario, run_scenario


@pytest.mark.scenarios
@pytest.mark.slow
@pytest.mark.skip(
    reason=(
        "Forward-looking scenario. Placeholder seed CSVs (one row each) do not overlap "
        "the basic persona's merchant catalog, so deterministic accuracy is 0.0. Enable "
        "after Phase 12 seed curation lands (~2100 entries) and the runner supports "
        "simulate_llm_assist for the snowball half of the scenario."
    )
)
def test_cold_start_first_import() -> None:
    """Seeds-only merchant rules cover a meaningful fraction of first-import transactions."""
    scenario = load_shipped_scenario("cold-start-first-import")
    assert scenario is not None
    result = run_scenario(scenario)
    assert result.passed, result.failure_summary()
