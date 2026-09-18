"""Subprocess CLI coverage for review-only investment matching."""

import json
from pathlib import Path

import pytest

from tests.e2e.conftest import make_workflow_env, run_cli

pytestmark = pytest.mark.e2e


def test_run_and_inspect_review_only_planner_from_cli(tmp_path: Path) -> None:
    env = make_workflow_env(tmp_path, "investment-matches")
    planned = run_cli("investments", "matches", "run", "--output", "json", env=env)
    planned.assert_success()
    assert json.loads(planned.stdout)["data"]["stages"][0]["step"] == "investment_match"
    for command in ("pending", "history"):
        result = run_cli("investments", "matches", command, "--output", "json", env=env)
        result.assert_success()
        assert json.loads(result.stdout)["data"]["rows"] == []
