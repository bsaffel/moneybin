"""Tests for the sync key sub-group shape."""

from __future__ import annotations

import pytest
from typer.testing import CliRunner

from moneybin.cli.commands.sync import app as sync_app


@pytest.fixture()
def runner() -> CliRunner:
    """Provide a Typer CliRunner for invoking the sync app."""
    return CliRunner()


class TestSyncKeySubgroup:
    """Verify the sync key sub-group structure."""

    @pytest.mark.unit
    def test_sync_key_help_lists_rotate(self, runner: CliRunner) -> None:
        """`sync key --help` should list the `rotate` action."""
        result = runner.invoke(sync_app, ["key", "--help"])
        assert result.exit_code == 0
        assert "rotate" in result.stdout

    @pytest.mark.unit
    def test_sync_rotate_key_no_longer_exists(self, runner: CliRunner) -> None:
        """The old flat `rotate-key` command should no longer be registered."""
        result = runner.invoke(sync_app, ["rotate-key", "--help"])
        assert result.exit_code != 0
