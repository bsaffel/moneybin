"""Tests for the db key sub-group shape and stubs."""

from __future__ import annotations

import logging
from collections.abc import Generator
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import typer
from typer.testing import CliRunner

from moneybin.cli.commands.db import (
    _load_encryption_key,  # pyright: ignore[reportPrivateUsage]
)
from moneybin.cli.commands.db import app as db_app


class TestDbKeySubgroup:
    """Verify the db key sub-group structure and stub behavior."""

    @pytest.mark.unit
    def test_key_help_lists_only_working_actions(self, runner: CliRunner) -> None:
        """`db key --help` lists what works and hides the stubs (req 31).

        The group itself stays visible: `show` and `rotate` are real, so
        hiding it would take working commands out of --help with it.
        """
        result = runner.invoke(db_app, ["key", "--help"])
        assert result.exit_code == 0
        for action in ("show", "rotate"):
            assert action in result.stdout
        for stub in ("export", "import", "verify"):
            assert stub not in result.stdout

    @pytest.mark.unit
    @pytest.mark.parametrize("action", ["export", "import", "verify"])
    def test_stub_actions_exit_with_not_implemented(
        self,
        runner: CliRunner,
        action: str,
        tmp_path: Path,
    ) -> None:
        """Stub sub-commands stay invocable and exit 1 with a message.

        Read from ``result.output``: the shared ``_not_implemented`` helper
        writes with ``typer.echo(err=True)``, straight to stderr, so the message
        does not depend on ``setup_logging(cli_mode=True)`` — installed by the
        root app, not by this sub-app — to reach a real user.
        """
        argv = ["key", action]
        if action == "import":
            argv.append(str(tmp_path / "envelope.bin"))

        result = runner.invoke(db_app, argv)

        assert result.exit_code == 1
        assert "not yet implemented" in result.output.lower()

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
    def test_body_exception_propagates_from_cm(self) -> None:
        """Exceptions raised inside the with block propagate out of the context manager."""
        mock_store = MagicMock()
        mock_store.get_key.return_value = "testkey"

        class _SentinelError(Exception):
            pass

        with patch("moneybin.secrets.SecretStore", return_value=mock_store):
            with pytest.raises(_SentinelError):
                with _load_encryption_key():
                    raise _SentinelError("body raised")


class TestDbKeyRotateSecrets:
    """`db key rotate` must not write key material to the durable CLI log.

    Rotation attaches both databases directly rather than through
    ``_attach_encrypted``, then reports failure with
    ``logger.error(f"... {e}")``. DuckDB echoes the failing statement back in
    ``ParserException`` messages, and the file handler has no level filter, so
    an unscrubbed message persists to ``cli_YYYY-MM-DD.log``.
    ``SanitizedLogFormatter`` cannot help: it masks SSNs, dollar amounts and
    8+ *digit* runs, and a 64-hex key is none of those.
    """

    _OLD_KEY = "8c4b1f70d29e35a6be08d417c5392fda60b7e148a2c93f5d071e6ba4c82d9e17"
    _NEW_KEY = "1a7e93c05b2d48f6ae310c97d5e824bf0639a1c7e5840b2c96f1a3d70e85b4cf"

    @staticmethod
    def _leaks(text: str, key: str, run: int = 8) -> bool:
        return any(key[i : i + run] in text for i in range(len(key) - run + 1))

    @pytest.mark.unit
    def test_rotation_failure_does_not_log_key_material(
        self,
        runner: CliRunner,
        tmp_path: Path,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        import contextlib

        import duckdb

        db_file = tmp_path / "moneybin.duckdb"
        db_file.write_bytes(b"encrypted-bytes")

        settings = MagicMock()
        settings.database.path = db_file

        @contextlib.contextmanager
        def _fake_key() -> Generator[str, None, None]:
            yield self._OLD_KEY

        def _execute(sql: str, *_a: object, **_k: object) -> object:
            if "ENCRYPTION_KEY" in sql:
                # Mirrors DuckDB's LINE 1 excerpt, windowed past the key.
                raise duckdb.ParserException(
                    'Parser Error: syntax error at or near "GARBAGE"\n\n'
                    f"LINE 1: ...{sql[30:]} GARBAGE ;;\n"
                )
            return MagicMock()

        conn = MagicMock()
        conn.execute.side_effect = _execute

        with (
            patch("moneybin.config.get_settings", return_value=settings),
            patch("moneybin.secrets.SecretStore", return_value=MagicMock()),
            patch("moneybin.cli.commands.db._load_encryption_key", _fake_key),
            patch("secrets.token_hex", return_value=self._NEW_KEY),
            patch("duckdb.connect", return_value=conn),
            caplog.at_level(logging.ERROR),
        ):
            result = runner.invoke(db_app, ["key", "rotate", "--yes"])

        assert result.exit_code == 1
        assert "Key rotation failed" in caplog.text, "expected the failure path to run"
        assert not self._leaks(caplog.text, self._OLD_KEY), "old key in the CLI log"
        assert not self._leaks(caplog.text, self._NEW_KEY), "new key in the CLI log"
