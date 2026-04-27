"""Tests for the scenario YAML loader."""

from pathlib import Path

import pytest

from moneybin.testing.scenarios.loader import (
    Scenario,
    ScenarioValidationError,
    load_scenario_from_string,
)

VALID = """
scenario: test
description: minimal valid scenario
setup:
  persona: family
  seed: 42
  years: 1
  fixtures: []
pipeline:
  - generate
  - transform
assertions:
  - name: rc
    fn: assert_row_count_exact
    args:
      table: core.fct_transactions
      expected: 100
gates:
  required_assertions: all
"""


def test_minimal_valid_scenario_loads() -> None:
    """A minimal well-formed scenario parses into a Scenario model."""
    s = load_scenario_from_string(VALID)
    assert isinstance(s, Scenario)
    assert s.name == "test"
    assert len(s.assertions) == 1


def test_unknown_step_rejected() -> None:
    """Pipeline steps not in the registry raise a validation error."""
    bad = VALID.replace("- transform", "- nonexistent_step")
    with pytest.raises(ScenarioValidationError) as exc:
        load_scenario_from_string(bad)
    assert "nonexistent_step" in str(exc.value)


def test_path_traversal_rejected() -> None:
    """Fixture paths escaping tests/fixtures/ are rejected."""
    bad = VALID.replace(
        "fixtures: []",
        "fixtures:\n    - path: ../../../etc/passwd\n      account: x\n      source_type: csv",
    )
    with pytest.raises(ScenarioValidationError) as exc:
        load_scenario_from_string(bad)
    assert "tests/fixtures" in str(exc.value).lower()


def test_threshold_min_required_for_evaluations() -> None:
    """Evaluation thresholds must include a numeric `min` field."""
    bad = VALID + (
        "evaluations:\n"
        "  - name: cat\n"
        "    fn: score_categorization\n"
        "    threshold:\n"
        "      metric: accuracy\n"
    )
    with pytest.raises(ScenarioValidationError):
        load_scenario_from_string(bad)


def test_loads_shipped_scenarios(tmp_path: Path) -> None:
    """Placeholder for Task 11; the import-or-skip guards future scenarios."""
    # Will be populated in Task 11; skip until scenarios exist.
    pytest.importorskip("moneybin.testing.scenarios")
