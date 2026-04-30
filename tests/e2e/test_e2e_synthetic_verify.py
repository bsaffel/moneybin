"""E2E coverage for `moneybin synthetic verify`."""

from __future__ import annotations

import json

import pytest

from tests.e2e.conftest import run_cli

pytestmark = pytest.mark.e2e


def test_list_prints_scenarios() -> None:
    """`--list` prints shipped scenario names in text mode."""
    result = run_cli("synthetic", "verify", "--list")
    result.assert_success()
    assert "basic-full-pipeline" in result.stdout


def test_list_json_output() -> None:
    """`--list --output=json` emits a parseable array of scenario records."""
    result = run_cli("synthetic", "verify", "--list", "--output=json")
    result.assert_success()
    data = json.loads(result.stdout)
    assert "basic-full-pipeline" in [entry["name"] for entry in data]


def test_unknown_scenario_returns_nonzero() -> None:
    """An unknown scenario name exits non-zero with an error message."""
    result = run_cli("synthetic", "verify", "--scenario=does-not-exist")
    assert result.exit_code != 0


def test_no_target_returns_nonzero() -> None:
    """Invoking verify without --list/--scenario/--all exits non-zero."""
    result = run_cli("synthetic", "verify")
    assert result.exit_code != 0
