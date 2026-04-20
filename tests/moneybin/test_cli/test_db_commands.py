# ruff: noqa: S101,S106
"""Tests for database management CLI commands.

Tests CLI-specific functionality: argument parsing, exit codes, error handling,
and subprocess command building for DuckDB CLI wrapper commands.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest
from typer.testing import CliRunner

from moneybin.cli.commands.db import app


def _make_settings_mock(db_path: Path, mocker: Any) -> MagicMock:
    """Create a mock settings object with a database.path set to db_path."""
    mock_settings = MagicMock()
    mock_settings.database.path = db_path
    mock_settings.database.encryption_key_mode = "auto"
    mock_settings.database.backup_path = None
    # get_settings is imported lazily inside each command function, so we patch
    # the canonical source rather than a module-level reference in db.py.
    return mocker.patch(
        "moneybin.config.get_settings",
        return_value=mock_settings,
    )


class TestShellCommand:
    """Tests for 'moneybin db shell'."""

    @pytest.fixture
    def runner(self) -> CliRunner:
        return CliRunner()

    @pytest.fixture
    def mock_subprocess_run(self, mocker: Any) -> MagicMock:
        return mocker.patch("moneybin.cli.commands.db.subprocess.run")

    @pytest.fixture
    def mock_duckdb_cli(self, mocker: Any) -> MagicMock:
        return mocker.patch(
            "moneybin.cli.commands.db.shutil.which",
            return_value="/usr/local/bin/duckdb",
        )

    @pytest.fixture
    def mock_no_duckdb_cli(self, mocker: Any) -> MagicMock:
        return mocker.patch("moneybin.cli.commands.db.shutil.which", return_value=None)

    @pytest.fixture
    def mock_create_init_script(self, mocker: Any, tmp_path: Path) -> MagicMock:
        """Mock _create_init_script to avoid hitting SecretStore."""
        script = tmp_path / "init.sql"
        script.touch()
        return mocker.patch(
            "moneybin.cli.commands.db._create_init_script",
            return_value=script,
        )

    def test_shell_opens_with_init_flag(
        self,
        runner: CliRunner,
        mocker: Any,
        mock_subprocess_run: MagicMock,
        mock_duckdb_cli: MagicMock,
        mock_create_init_script: MagicMock,
        tmp_path: Path,
    ) -> None:
        """Shell command passes -init flag with temp script."""
        test_db = tmp_path / "test.duckdb"
        test_db.touch()
        _make_settings_mock(test_db, mocker)

        result = runner.invoke(app, ["shell"])
        assert result.exit_code == 0

        call_args = mock_subprocess_run.call_args[0][0]
        assert call_args[0] == "duckdb"
        assert "-init" in call_args
        assert "-c" not in call_args
        assert "-ui" not in call_args

    def test_shell_with_custom_database(
        self,
        runner: CliRunner,
        mock_subprocess_run: MagicMock,
        mock_duckdb_cli: MagicMock,
        mock_create_init_script: MagicMock,
        tmp_path: Path,
    ) -> None:
        """Shell command accepts --database option."""
        custom_db = tmp_path / "custom.duckdb"
        custom_db.touch()

        result = runner.invoke(app, ["shell", "--database", str(custom_db)])
        assert result.exit_code == 0

        mock_create_init_script.assert_called_once_with(custom_db)

    def test_shell_database_not_found(
        self,
        runner: CliRunner,
        mocker: Any,
        mock_duckdb_cli: MagicMock,
        tmp_path: Path,
    ) -> None:
        """Shell command fails when database doesn't exist."""
        _make_settings_mock(tmp_path / "missing.duckdb", mocker)
        result = runner.invoke(app, ["shell"])
        assert result.exit_code == 1

    def test_shell_duckdb_cli_not_installed(
        self,
        runner: CliRunner,
        mocker: Any,
        mock_no_duckdb_cli: MagicMock,
        tmp_path: Path,
    ) -> None:
        """Shell command fails when DuckDB CLI is not installed."""
        test_db = tmp_path / "test.duckdb"
        test_db.touch()
        _make_settings_mock(test_db, mocker)

        result = runner.invoke(app, ["shell"])
        assert result.exit_code == 1

    def test_shell_handles_keyboard_interrupt(
        self,
        runner: CliRunner,
        mocker: Any,
        mock_subprocess_run: MagicMock,
        mock_duckdb_cli: MagicMock,
        mock_create_init_script: MagicMock,
        tmp_path: Path,
    ) -> None:
        """Shell command handles Ctrl+C gracefully."""
        test_db = tmp_path / "test.duckdb"
        test_db.touch()
        _make_settings_mock(test_db, mocker)
        mock_subprocess_run.side_effect = KeyboardInterrupt()

        result = runner.invoke(app, ["shell"])
        assert result.exit_code == 0


class TestUiCommand:
    """Tests for 'moneybin db ui'."""

    @pytest.fixture
    def runner(self) -> CliRunner:
        return CliRunner()

    @pytest.fixture
    def mock_subprocess_run(self, mocker: Any) -> MagicMock:
        return mocker.patch("moneybin.cli.commands.db.subprocess.run")

    @pytest.fixture
    def mock_duckdb_cli(self, mocker: Any) -> MagicMock:
        return mocker.patch(
            "moneybin.cli.commands.db.shutil.which",
            return_value="/usr/local/bin/duckdb",
        )

    @pytest.fixture
    def mock_no_duckdb_cli(self, mocker: Any) -> MagicMock:
        return mocker.patch("moneybin.cli.commands.db.shutil.which", return_value=None)

    @pytest.fixture
    def mock_create_init_script(self, mocker: Any, tmp_path: Path) -> MagicMock:
        script = tmp_path / "init.sql"
        script.touch()
        return mocker.patch(
            "moneybin.cli.commands.db._create_init_script",
            return_value=script,
        )

    def test_ui_uses_config_database(
        self,
        runner: CliRunner,
        mocker: Any,
        mock_subprocess_run: MagicMock,
        mock_duckdb_cli: MagicMock,
        mock_create_init_script: MagicMock,
        tmp_path: Path,
    ) -> None:
        """UI command uses database from config by default."""
        test_db = tmp_path / "test.duckdb"
        test_db.touch()
        mock_get = _make_settings_mock(test_db, mocker)

        result = runner.invoke(app, ["ui"])
        assert result.exit_code == 0
        mock_get.assert_called_once()

        call_args = mock_subprocess_run.call_args[0][0]
        assert call_args[0] == "duckdb"
        assert "-ui" in call_args

    def test_ui_with_custom_database(
        self,
        runner: CliRunner,
        mock_subprocess_run: MagicMock,
        mock_duckdb_cli: MagicMock,
        mock_create_init_script: MagicMock,
        tmp_path: Path,
    ) -> None:
        """UI command accepts --database option."""
        custom_db = tmp_path / "custom.duckdb"
        custom_db.touch()

        result = runner.invoke(app, ["ui", "--database", str(custom_db)])
        assert result.exit_code == 0

        call_args = mock_subprocess_run.call_args[0][0]
        assert "-ui" in call_args

    def test_ui_database_not_found(
        self,
        runner: CliRunner,
        mocker: Any,
        mock_duckdb_cli: MagicMock,
        tmp_path: Path,
    ) -> None:
        """UI command fails when database doesn't exist."""
        _make_settings_mock(tmp_path / "missing.duckdb", mocker)
        result = runner.invoke(app, ["ui"])
        assert result.exit_code == 1

    def test_ui_duckdb_cli_not_installed(
        self,
        runner: CliRunner,
        mocker: Any,
        mock_no_duckdb_cli: MagicMock,
        tmp_path: Path,
    ) -> None:
        """UI command fails when DuckDB CLI is not installed."""
        test_db = tmp_path / "test.duckdb"
        test_db.touch()
        _make_settings_mock(test_db, mocker)

        result = runner.invoke(app, ["ui"])
        assert result.exit_code == 1

    def test_ui_handles_keyboard_interrupt(
        self,
        runner: CliRunner,
        mocker: Any,
        mock_subprocess_run: MagicMock,
        mock_duckdb_cli: MagicMock,
        mock_create_init_script: MagicMock,
        tmp_path: Path,
    ) -> None:
        """UI command handles Ctrl+C gracefully."""
        test_db = tmp_path / "test.duckdb"
        test_db.touch()
        _make_settings_mock(test_db, mocker)
        mock_subprocess_run.side_effect = KeyboardInterrupt()

        result = runner.invoke(app, ["ui"])
        assert result.exit_code == 0


class TestQueryCommand:
    """Tests for 'moneybin db query'."""

    @pytest.fixture
    def runner(self) -> CliRunner:
        return CliRunner()

    @pytest.fixture
    def mock_subprocess_run(self, mocker: Any) -> MagicMock:
        return mocker.patch("moneybin.cli.commands.db.subprocess.run")

    @pytest.fixture
    def mock_duckdb_cli(self, mocker: Any) -> MagicMock:
        return mocker.patch(
            "moneybin.cli.commands.db.shutil.which",
            return_value="/usr/local/bin/duckdb",
        )

    @pytest.fixture
    def mock_no_duckdb_cli(self, mocker: Any) -> MagicMock:
        return mocker.patch("moneybin.cli.commands.db.shutil.which", return_value=None)

    @pytest.fixture
    def mock_create_init_script(self, mocker: Any, tmp_path: Path) -> MagicMock:
        script = tmp_path / "init.sql"
        script.touch()
        return mocker.patch(
            "moneybin.cli.commands.db._create_init_script",
            return_value=script,
        )

    def test_query_builds_correct_command(
        self,
        runner: CliRunner,
        mocker: Any,
        mock_subprocess_run: MagicMock,
        mock_duckdb_cli: MagicMock,
        mock_create_init_script: MagicMock,
        tmp_path: Path,
    ) -> None:
        """Query command includes -c and the SQL in the command."""
        test_db = tmp_path / "test.duckdb"
        test_db.touch()
        _make_settings_mock(test_db, mocker)

        result = runner.invoke(app, ["query", "SELECT 1"])
        assert result.exit_code == 0

        call_args = mock_subprocess_run.call_args[0][0]
        assert call_args[0] == "duckdb"
        assert "-c" in call_args
        assert "SELECT 1" in call_args

    def test_query_with_format_options(
        self,
        runner: CliRunner,
        mocker: Any,
        mock_subprocess_run: MagicMock,
        mock_duckdb_cli: MagicMock,
        mock_create_init_script: MagicMock,
        tmp_path: Path,
    ) -> None:
        """Query command passes correct format flags."""
        test_db = tmp_path / "test.duckdb"
        test_db.touch()
        _make_settings_mock(test_db, mocker)

        formats = {
            "csv": "-csv",
            "json": "-json",
            "markdown": "-markdown",
            "box": "-box",
        }

        for format_name, format_flag in formats.items():
            mock_subprocess_run.reset_mock()
            result = runner.invoke(app, ["query", "SELECT 1", "--format", format_name])
            assert result.exit_code == 0
            call_args = mock_subprocess_run.call_args[0][0]
            assert format_flag in call_args

    def test_query_with_custom_database(
        self,
        runner: CliRunner,
        mock_subprocess_run: MagicMock,
        mock_duckdb_cli: MagicMock,
        mock_create_init_script: MagicMock,
        tmp_path: Path,
    ) -> None:
        """Query command accepts --database option."""
        custom_db = tmp_path / "custom.duckdb"
        custom_db.touch()

        result = runner.invoke(
            app,
            ["query", "SELECT 1", "--database", str(custom_db)],
        )
        assert result.exit_code == 0
        mock_create_init_script.assert_called_once_with(custom_db)

    def test_query_database_not_found(
        self,
        runner: CliRunner,
        mocker: Any,
        mock_duckdb_cli: MagicMock,
        tmp_path: Path,
    ) -> None:
        """Query command fails when database doesn't exist."""
        _make_settings_mock(tmp_path / "missing.duckdb", mocker)
        result = runner.invoke(app, ["query", "SELECT 1"])
        assert result.exit_code == 1

    def test_query_duckdb_cli_not_installed(
        self,
        runner: CliRunner,
        mocker: Any,
        mock_no_duckdb_cli: MagicMock,
        tmp_path: Path,
    ) -> None:
        """Query command fails when DuckDB CLI is not installed."""
        test_db = tmp_path / "test.duckdb"
        test_db.touch()
        _make_settings_mock(test_db, mocker)

        result = runner.invoke(app, ["query", "SELECT 1"])
        assert result.exit_code == 1


class TestDbLockCommand:
    """Tests for 'moneybin db lock'."""

    @pytest.fixture
    def runner(self) -> CliRunner:
        return CliRunner()

    def test_lock_deletes_key(self, runner: CliRunner, mocker: Any) -> None:
        """Lock command removes encryption key from keychain."""
        mock_store = MagicMock()
        mocker.patch("moneybin.secrets.SecretStore", return_value=mock_store)

        result = runner.invoke(app, ["lock"])
        assert result.exit_code == 0
        mock_store.delete_key.assert_called_once_with("DATABASE__ENCRYPTION_KEY")

    def test_lock_already_locked(self, runner: CliRunner, mocker: Any) -> None:
        """Lock command succeeds gracefully when already locked."""
        from moneybin.secrets import SecretNotFoundError

        mock_store = MagicMock()
        mock_store.delete_key.side_effect = SecretNotFoundError("not found")
        mocker.patch("moneybin.secrets.SecretStore", return_value=mock_store)

        result = runner.invoke(app, ["lock"])
        assert result.exit_code == 0


class TestDbKeyCommand:
    """Tests for 'moneybin db key'."""

    @pytest.fixture
    def runner(self) -> CliRunner:
        return CliRunner()

    def test_key_prints_key(self, runner: CliRunner, mocker: Any) -> None:
        """Key command prints the encryption key."""
        mock_store = MagicMock()
        mock_store.get_key.return_value = "abc123deadbeef"
        mocker.patch("moneybin.secrets.SecretStore", return_value=mock_store)

        result = runner.invoke(app, ["key"])
        assert result.exit_code == 0
        assert "abc123deadbeef" in result.output

    def test_key_fails_when_locked(self, runner: CliRunner, mocker: Any) -> None:
        """Key command fails when no key is available."""
        from moneybin.secrets import SecretNotFoundError

        mock_store = MagicMock()
        mock_store.get_key.side_effect = SecretNotFoundError("not found")
        mocker.patch("moneybin.secrets.SecretStore", return_value=mock_store)

        result = runner.invoke(app, ["key"])
        assert result.exit_code == 1


class TestDbBackupCommand:
    """Tests for 'moneybin db backup'."""

    @pytest.fixture
    def runner(self) -> CliRunner:
        return CliRunner()

    def test_backup_creates_file(
        self, runner: CliRunner, mocker: Any, tmp_path: Path
    ) -> None:
        """Backup command creates a timestamped backup file."""
        test_db = tmp_path / "test.duckdb"
        test_db.write_bytes(b"data")
        _make_settings_mock(test_db, mocker)

        result = runner.invoke(app, ["backup"])
        assert result.exit_code == 0

        backups = list((tmp_path / "backups").glob("*.duckdb"))
        assert len(backups) == 1

    def test_backup_to_custom_output(
        self, runner: CliRunner, mocker: Any, tmp_path: Path
    ) -> None:
        """Backup command writes to specified --output path."""
        test_db = tmp_path / "test.duckdb"
        test_db.write_bytes(b"data")
        _make_settings_mock(test_db, mocker)

        output_path = tmp_path / "my_backup.duckdb"
        result = runner.invoke(app, ["backup", "--output", str(output_path)])
        assert result.exit_code == 0
        assert output_path.exists()

    def test_backup_fails_when_db_missing(
        self, runner: CliRunner, mocker: Any, tmp_path: Path
    ) -> None:
        """Backup command fails when database doesn't exist."""
        _make_settings_mock(tmp_path / "missing.duckdb", mocker)
        result = runner.invoke(app, ["backup"])
        assert result.exit_code == 1


class TestDatabaseCommandsIntegration:
    """Integration tests for database CLI commands."""

    @pytest.fixture
    def runner(self) -> CliRunner:
        return CliRunner()

    @pytest.mark.integration
    def test_commands_handle_subprocess_errors(
        self, runner: CliRunner, tmp_path: Path
    ) -> None:
        """Test that commands handle subprocess errors gracefully."""
        test_db = tmp_path / "test.duckdb"
        test_db.touch()

        # Test with a database that exists but may cause DuckDB errors
        # Should fail gracefully with proper exit code, not crash
        result = runner.invoke(
            app, ["query", "INVALID SQL", "--database", str(test_db)]
        )

        # Should fail with exit code 1, not crash with unhandled exception
        # The exact behavior depends on whether DuckDB CLI is installed
        assert result.exit_code in [0, 1]
        assert "Traceback" not in result.output
