"""Tests for the sync key sub-group shape."""

from __future__ import annotations

import pytest
from typer.testing import CliRunner

from moneybin.cli.commands.sync import app as sync_app


class TestSyncKeySubgroup:
    """Verify the sync key sub-group structure."""

    @pytest.mark.unit
    def test_sync_key_rotate_is_hidden_but_still_invocable(
        self, runner: CliRunner
    ) -> None:
        """`rotate` is an unimplemented stub: reserved, invocable, unadvertised."""
        listing = runner.invoke(sync_app, ["key", "--help"])
        assert listing.exit_code == 0
        assert "rotate" not in listing.stdout

        invoked = runner.invoke(sync_app, ["key", "rotate"])
        assert invoked.exit_code == 0

    @pytest.mark.unit
    def test_sync_rotate_key_no_longer_exists(self, runner: CliRunner) -> None:
        """The old flat `rotate-key` command should no longer be registered."""
        result = runner.invoke(sync_app, ["rotate-key", "--help"])
        assert result.exit_code != 0
