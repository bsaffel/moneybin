"""Tests for the scenario YAML loader."""

import pytest

from tests.scenarios._runner.loader import (
    Scenario,
    ScenarioValidationError,
    list_shipped_scenarios,
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
    """Fixture paths escaping the bundled fixtures root are rejected."""
    bad = VALID.replace(
        "fixtures: []",
        "fixtures:\n    - path: ../../../etc/passwd\n      account: x\n      source_type: csv",
    )
    with pytest.raises(ScenarioValidationError) as exc:
        load_scenario_from_string(bad)
    assert "data/fixtures" in str(exc.value).lower()


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


def test_import_path_traversal_rejected() -> None:
    """Import paths escaping the tests/fixtures root are rejected."""
    bad = VALID.replace(
        "fixtures: []",
        "fixtures: []\n  imports:\n    - path: ../../../etc/passwd",
    )
    with pytest.raises(ScenarioValidationError) as exc:
        load_scenario_from_string(bad)
    assert "tests/fixtures" in str(exc.value).lower()


def test_expect_error_substring_requires_expect_failure() -> None:
    """expect_error_substring without expect_failure must fail validation."""
    bad = VALID.replace(
        "fixtures: []",
        "fixtures: []\n"
        "  imports:\n"
        "    - path: ofx/sample_minimal.ofx\n"
        "      expect_error_substring: boom",
    )
    with pytest.raises(ScenarioValidationError) as exc:
        load_scenario_from_string(bad)
    assert "expect_failure" in str(exc.value)


def test_loads_shipped_scenarios() -> None:
    """All bundled scenario YAMLs parse and the canonical names are present."""
    scenarios = list_shipped_scenarios()
    names = {s.name for s in scenarios}
    assert names >= {"basic-full-pipeline", "family-full-pipeline"}
