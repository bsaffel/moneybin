"""Tests for the db key sub-group shape and stubs."""

from __future__ import annotations

from pathlib import Path

import pytest
from typer.testing import CliRunner

from moneybin.cli.commands.db import app as db_app


@pytest.fixture()
def runner() -> CliRunner:
    """Provide a Typer CliRunner for invoking the db app."""
    return CliRunner()


class TestDbKeySubgroup:
    """Verify the db key sub-group structure and stub behavior."""

    @pytest.mark.unit
    def test_key_help_lists_all_actions(self, runner: CliRunner) -> None:
        """`db key --help` should list show/rotate/export/import/verify."""
        result = runner.invoke(db_app, ["key", "--help"])
        assert result.exit_code == 0
        for action in ("show", "rotate", "export", "import", "verify"):
            assert action in result.stdout

    @pytest.mark.unit
    @pytest.mark.parametrize("action", ["export", "import", "verify"])
    def test_stub_actions_exit_with_not_implemented(
        self, runner: CliRunner, action: str, tmp_path: Path
    ) -> None:
        """Stub sub-commands exit 1 with a "not yet implemented" message."""
        argv = ["key", action]
        if action == "import":
            argv.append(str(tmp_path / "envelope.bin"))
        result = runner.invoke(db_app, argv)
        assert result.exit_code == 1
        combined = (result.output or "").lower()
        assert "not yet implemented" in combined

    @pytest.mark.unit
    def test_old_rotate_key_no_longer_exists(self, runner: CliRunner) -> None:
        """The old flat `rotate-key` command should no longer be registered."""
        result = runner.invoke(db_app, ["rotate-key", "--help"])
        assert result.exit_code != 0
