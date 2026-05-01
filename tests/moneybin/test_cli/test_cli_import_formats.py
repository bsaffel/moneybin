"""Tests for the import formats sub-group shape."""

from __future__ import annotations

import pytest
from typer.testing import CliRunner

from moneybin.cli.commands.import_cmd import app as import_app


@pytest.fixture()
def runner() -> CliRunner:
    """Provide a Typer CliRunner for invoking the import app."""
    return CliRunner()


class TestImportFormatsSubgroup:
    """Verify the import formats sub-group structure."""

    @pytest.mark.unit
    def test_formats_help_lists_actions(self, runner: CliRunner) -> None:
        """`import formats --help` should list list/show/delete actions."""
        result = runner.invoke(import_app, ["formats", "--help"])
        assert result.exit_code == 0
        for action in ("list", "show", "delete"):
            assert action in result.stdout

    @pytest.mark.unit
    def test_old_compound_names_no_longer_exist(self, runner: CliRunner) -> None:
        """Old compound command names should no longer be invokable."""
        for old in ("list-formats", "show-format", "delete-format"):
            result = runner.invoke(import_app, [old, "--help"])
            assert result.exit_code != 0, f"{old} should be gone"
