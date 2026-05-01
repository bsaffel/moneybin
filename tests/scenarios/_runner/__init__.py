"""Scenario runner harness — moved from ``src/moneybin/testing/scenarios``.

Underscore-prefixed so pytest doesn't try to collect tests from it.
"""

from tests.scenarios._runner.result import ScenarioResult

__all__ = ["ScenarioResult"]
