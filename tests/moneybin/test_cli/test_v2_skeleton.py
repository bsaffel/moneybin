"""Smoke tests that the v2 top-level command groups are registered.

These tests verify that --help works for each new group; they don't
test behavior. Behavior tests land in later tasks.
"""

from typer.testing import CliRunner

from moneybin.cli.main import app

runner = CliRunner()

V2_GROUPS = [
    "accounts",
    "transactions",
    "assets",
    "categories",
    "merchants",
    "reports",
    "tax",
    "system",
    "budget",
]


def test_v2_groups_registered() -> None:
    for group in V2_GROUPS:
        result = runner.invoke(app, [group, "--help"])
        assert result.exit_code == 0, f"{group} --help failed: {result.output}"
