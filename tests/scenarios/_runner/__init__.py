"""Scenario runner harness — moved from ``src/moneybin/testing/scenarios``.

Underscore-prefixed so pytest doesn't try to collect tests from it.
"""

from tests.scenarios._runner.loader import (
    Scenario,
    ScenarioValidationError,
    list_shipped_scenarios,
    load_scenario,
    load_scenario_from_string,
    load_shipped_scenario,
)
from tests.scenarios._runner.result import ScenarioResult
from tests.scenarios._runner.runner import run_scenario

__all__ = [
    "Scenario",
    "ScenarioResult",
    "ScenarioValidationError",
    "list_shipped_scenarios",
    "load_scenario",
    "load_scenario_from_string",
    "load_shipped_scenario",
    "run_scenario",
]
