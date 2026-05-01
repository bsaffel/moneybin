"""Scenario: migrations apply to the right schema; populated columns survive."""

from __future__ import annotations

import pytest

from tests.scenarios._runner import load_shipped_scenario, run_scenario


@pytest.mark.scenarios
@pytest.mark.slow
def test_migration_roundtrip() -> None:
    scenario = load_shipped_scenario("migration-roundtrip")
    assert scenario is not None
    result = run_scenario(scenario)
    assert result.passed, result.failure_summary()
