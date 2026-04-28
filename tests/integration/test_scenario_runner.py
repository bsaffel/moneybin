"""End-to-end scenario runner integration tests.

These exercise the real scenario runner: a fresh encrypted Database in a
tempdir, real SQLMesh transform, real assertions. They are slow.
"""

from __future__ import annotations

from pathlib import Path
from textwrap import dedent

import pytest

from moneybin.testing.scenarios.loader import load_scenario_from_string
from moneybin.testing.scenarios.runner import run_scenario

TINY = dedent("""
    scenario: tiny
    description: smallest possible scenario
    setup:
      persona: basic
      seed: 42
      years: 1
      fixtures: []
    pipeline:
      - generate
      - transform
    assertions:
      - name: catalog
        fn: assert_sqlmesh_catalog_matches
      - name: rc
        fn: assert_row_count_delta
        args:
          table: core.fct_transactions
          expected: 300
          tolerance_pct: 90
    gates:
      required_assertions: all
""")


@pytest.mark.integration
@pytest.mark.slow
def test_runner_returns_envelope_for_passing_scenario() -> None:
    """Runner builds a passing envelope when assertions hold."""
    s = load_scenario_from_string(TINY)
    env = run_scenario(s)
    assert env.data["scenario"] == "tiny"
    assert env.data["passed"] is True
    assert any(a["name"] == "sqlmesh_catalog_matches" for a in env.data["assertions"])


@pytest.mark.integration
@pytest.mark.slow
def test_runner_reports_failure_without_crashing() -> None:
    """Failing assertions surface as ``passed=False`` without raising."""
    bad = TINY.replace("expected: 300", "expected: 9999999").replace(
        "tolerance_pct: 90", "tolerance_pct: 1"
    )
    s = load_scenario_from_string(bad)
    env = run_scenario(s)
    assert env.data["passed"] is False
    assert any(not a["passed"] for a in env.data["assertions"])


@pytest.mark.integration
@pytest.mark.slow
def test_keep_tmpdir_preserves_directory() -> None:
    """``keep_tmpdir=True`` leaves the scenario tempdir on disk."""
    s = load_scenario_from_string(TINY)
    env = run_scenario(s, keep_tmpdir=True)
    tmpdir = Path(env.data["tmpdir"])
    try:
        assert tmpdir.exists()
    finally:
        import shutil

        shutil.rmtree(tmpdir, ignore_errors=True)
