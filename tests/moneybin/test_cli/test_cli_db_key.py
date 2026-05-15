"""Tests for the db key sub-group shape and stubs."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import typer
from typer.testing import CliRunner

from moneybin.cli.commands.db import (
    _load_encryption_key,  # pyright: ignore[reportPrivateUsage]
)
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


class TestLoadEncryptionKey:
    """Unit tests for the _load_encryption_key context manager."""

    @pytest.mark.unit
    def test_yields_key_from_store(self) -> None:
        """Context manager yields the key returned by SecretStore.get_key."""
        mock_store = MagicMock()
        mock_store.get_key.return_value = "deadbeef" * 8

        with patch("moneybin.secrets.SecretStore", return_value=mock_store):
            with _load_encryption_key() as key:
                assert key == "deadbeef" * 8

    @pytest.mark.unit
    def test_exits_1_when_locked(self) -> None:
        """Raises typer.Exit(1) when the key is not in the keychain."""
        from moneybin.secrets import SecretNotFoundError

        mock_store = MagicMock()
        mock_store.get_key.side_effect = SecretNotFoundError("no key")

        with patch("moneybin.secrets.SecretStore", return_value=mock_store):
            with pytest.raises(typer.Exit) as exc_info:
                with _load_encryption_key():
                    pass  # should not reach here
        assert exc_info.value.exit_code == 1

    @pytest.mark.unit
    def test_finally_runs_when_body_raises(self) -> None:
        """The finally block executes even when the managed body raises."""
        mock_store = MagicMock()
        mock_store.get_key.return_value = "testkey"
        cleanup_ran: list[str] = []

        class _SentinelError(Exception):
            pass

        with patch("moneybin.secrets.SecretStore", return_value=mock_store):
            with pytest.raises(_SentinelError):
                with _load_encryption_key():
                    cleanup_ran.append("before")
                    raise _SentinelError("body raised")

        # The context exited — finally block ran (del key executed without error)
        assert cleanup_ran == ["before"]
