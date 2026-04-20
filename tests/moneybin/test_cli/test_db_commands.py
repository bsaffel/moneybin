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
        assert call_args[0] == "/usr/local/bin/duckdb"
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

    def test_shell_locked_database_exits_1(
        self,
        runner: CliRunner,
        mocker: Any,
        mock_duckdb_cli: MagicMock,
        tmp_path: Path,
    ) -> None:
        """Shell command exits 1 with a helpful message when database is locked."""
        from moneybin.secrets import SecretNotFoundError

        test_db = tmp_path / "test.duckdb"
        test_db.touch()
        _make_settings_mock(test_db, mocker)
        mocker.patch(
            "moneybin.cli.commands.db._create_init_script",
            side_effect=SecretNotFoundError("locked"),
        )

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
        assert call_args[0] == "/usr/local/bin/duckdb"
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

    def test_ui_locked_database_exits_1(
        self,
        runner: CliRunner,
        mocker: Any,
        mock_duckdb_cli: MagicMock,
        tmp_path: Path,
    ) -> None:
        """UI command exits 1 with a helpful message when database is locked."""
        from moneybin.secrets import SecretNotFoundError

        test_db = tmp_path / "test.duckdb"
        test_db.touch()
        _make_settings_mock(test_db, mocker)
        mocker.patch(
            "moneybin.cli.commands.db._create_init_script",
            side_effect=SecretNotFoundError("locked"),
        )

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
        assert call_args[0] == "/usr/local/bin/duckdb"
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

    def test_query_locked_database_exits_1(
        self,
        runner: CliRunner,
        mocker: Any,
        mock_duckdb_cli: MagicMock,
        tmp_path: Path,
    ) -> None:
        """Query command exits 1 with a helpful message when database is locked."""
        from moneybin.secrets import SecretNotFoundError

        test_db = tmp_path / "test.duckdb"
        test_db.touch()
        _make_settings_mock(test_db, mocker)
        mocker.patch(
            "moneybin.cli.commands.db._create_init_script",
            side_effect=SecretNotFoundError("locked"),
        )

        result = runner.invoke(app, ["query", "SELECT 1"])
        assert result.exit_code == 1


class TestDbInitCommand:
    """Tests for 'moneybin db init'."""

    @pytest.fixture
    def runner(self) -> CliRunner:
        return CliRunner()

    def _mock_deps(self, mocker: Any, tmp_path: Path) -> tuple[MagicMock, MagicMock]:
        """Return (mock_store, mock_db_class) with settings patched."""
        _make_settings_mock(tmp_path / "moneybin.duckdb", mocker)
        mock_store = MagicMock()
        mocker.patch("moneybin.secrets.SecretStore", return_value=mock_store)
        mock_db = MagicMock()
        mocker.patch("moneybin.database.Database", return_value=mock_db)
        return mock_store, mock_db

    def test_init_auto_key_stores_key_and_creates_db(
        self, runner: CliRunner, mocker: Any, tmp_path: Path
    ) -> None:
        """Auto-key mode generates a key, stores it, and creates the database."""
        mock_store, mock_db = self._mock_deps(mocker, tmp_path)

        result = runner.invoke(app, ["init", "--yes"])

        assert result.exit_code == 0
        mock_store.set_key.assert_called_once()
        key_name, key_value = mock_store.set_key.call_args[0]
        assert key_name == "DATABASE__ENCRYPTION_KEY"
        assert len(key_value) == 64  # 32 bytes → 64 hex chars
        mock_db.close.assert_called_once()

    def test_init_passphrase_mismatch_exits_1(
        self, runner: CliRunner, mocker: Any, tmp_path: Path
    ) -> None:
        """Passphrase mode exits 1 when passphrases don't match."""
        self._mock_deps(mocker, tmp_path)

        result = runner.invoke(
            app, ["init", "--passphrase", "--yes"], input="password1\npassword2\n"
        )

        assert result.exit_code == 1

    def test_init_passphrase_match_stores_key_and_salt(
        self, runner: CliRunner, mocker: Any, tmp_path: Path
    ) -> None:
        """Passphrase mode derives a key via Argon2id and stores key + salt."""
        mock_store, mock_db = self._mock_deps(mocker, tmp_path)
        fake_raw_key = b"\xde\xad\xbe\xef" * 8  # 32 bytes
        mocker.patch("argon2.low_level.hash_secret_raw", return_value=fake_raw_key)

        result = runner.invoke(
            app, ["init", "--passphrase", "--yes"], input="mypassphrase\nmypassphrase\n"
        )

        assert result.exit_code == 0
        assert mock_store.set_key.call_count == 2
        key_calls = {c[0][0]: c[0][1] for c in mock_store.set_key.call_args_list}
        assert key_calls["DATABASE__ENCRYPTION_KEY"] == fake_raw_key.hex()
        assert "DATABASE__PASSPHRASE_SALT" in key_calls
        mock_db.close.assert_called_once()

    def test_init_existing_db_prompt_declined_exits_0(
        self, runner: CliRunner, mocker: Any, tmp_path: Path
    ) -> None:
        """Declining the overwrite prompt exits 0 without creating the database."""
        db_path = tmp_path / "moneybin.duckdb"
        db_path.touch()
        mock_store, mock_db = self._mock_deps(mocker, tmp_path)

        result = runner.invoke(app, ["init"], input="n\n")

        assert result.exit_code == 0
        mock_store.set_key.assert_not_called()
        mock_db.close.assert_not_called()

    def test_init_yes_flag_skips_overwrite_prompt(
        self, runner: CliRunner, mocker: Any, tmp_path: Path
    ) -> None:
        """--yes flag skips the overwrite confirmation prompt."""
        db_path = tmp_path / "moneybin.duckdb"
        db_path.touch()
        mock_store, _ = self._mock_deps(mocker, tmp_path)

        result = runner.invoke(app, ["init", "--yes"])

        assert result.exit_code == 0
        mock_store.set_key.assert_called_once()


class TestDbUnlockCommand:
    """Tests for 'moneybin db unlock'."""

    @pytest.fixture
    def runner(self) -> CliRunner:
        return CliRunner()

    def test_unlock_no_salt_exits_1(self, runner: CliRunner, mocker: Any) -> None:
        """Unlock fails when no passphrase salt is found in keychain."""
        from moneybin.secrets import SecretNotFoundError

        mock_store = MagicMock()
        mock_store.get_key.side_effect = SecretNotFoundError("no salt")
        mocker.patch("moneybin.secrets.SecretStore", return_value=mock_store)

        result = runner.invoke(app, ["unlock"], input="anypassphrase\n")

        assert result.exit_code == 1

    def test_unlock_wrong_passphrase_deletes_key_and_exits_1(
        self, runner: CliRunner, mocker: Any, tmp_path: Path
    ) -> None:
        """Wrong passphrase: database open fails, key is deleted, exits 1."""
        import base64

        fake_salt = base64.b64encode(b"\x00" * 16).decode()
        mock_store = MagicMock()
        mock_store.get_key.return_value = fake_salt
        mocker.patch("moneybin.secrets.SecretStore", return_value=mock_store)
        mocker.patch("argon2.low_level.hash_secret_raw", return_value=b"\x00" * 32)

        _make_settings_mock(tmp_path / "moneybin.duckdb", mocker)
        mocker.patch("moneybin.database.Database", side_effect=Exception("bad key"))

        result = runner.invoke(app, ["unlock"], input="wrongpass\n")

        assert result.exit_code == 1
        mock_store.delete_key.assert_called_once_with("DATABASE__ENCRYPTION_KEY")

    def test_unlock_correct_passphrase_exits_0(
        self, runner: CliRunner, mocker: Any, tmp_path: Path
    ) -> None:
        """Correct passphrase: key stored, database opens, exits 0."""
        import base64

        fake_salt = base64.b64encode(b"\x00" * 16).decode()
        mock_store = MagicMock()
        mock_store.get_key.return_value = fake_salt
        mocker.patch("moneybin.secrets.SecretStore", return_value=mock_store)
        mocker.patch("argon2.low_level.hash_secret_raw", return_value=b"\xaa" * 32)

        _make_settings_mock(tmp_path / "moneybin.duckdb", mocker)
        mock_db = MagicMock()
        mocker.patch("moneybin.database.Database", return_value=mock_db)

        result = runner.invoke(app, ["unlock"], input="correctpass\n")

        assert result.exit_code == 0
        mock_store.set_key.assert_called_once_with(
            "DATABASE__ENCRYPTION_KEY", (b"\xaa" * 32).hex()
        )
        mock_db.close.assert_called_once()


class TestDbRotateKeyCommand:
    """Tests for 'moneybin db rotate-key'."""

    @pytest.fixture
    def runner(self) -> CliRunner:
        return CliRunner()

    def _mock_rotate_deps(
        self, mocker: Any, tmp_path: Path
    ) -> tuple[MagicMock, MagicMock]:
        """Patch settings, SecretStore, duckdb.connect, and shutil.move."""
        db_path = tmp_path / "moneybin.duckdb"
        db_path.touch()
        _make_settings_mock(db_path, mocker)

        mock_store = MagicMock()
        mock_store.get_key.return_value = "oldkey" * 10  # 60 hex chars
        mocker.patch("moneybin.secrets.SecretStore", return_value=mock_store)

        mock_conn = MagicMock()
        mocker.patch("duckdb.connect", return_value=mock_conn)
        mocker.patch("moneybin.cli.commands.db.shutil.move")

        return mock_store, mock_conn

    def test_rotate_key_db_not_found_exits_1(
        self, runner: CliRunner, mocker: Any, tmp_path: Path
    ) -> None:
        """Rotate-key fails when database file doesn't exist."""
        _make_settings_mock(tmp_path / "missing.duckdb", mocker)
        mock_store = MagicMock()
        mocker.patch("moneybin.secrets.SecretStore", return_value=mock_store)

        result = runner.invoke(app, ["rotate-key", "--yes"])

        assert result.exit_code == 1
        mock_store.set_key.assert_not_called()

    def test_rotate_key_success_stores_new_key(
        self, runner: CliRunner, mocker: Any, tmp_path: Path
    ) -> None:
        """Happy path: new key generated and stored, old backup removed."""
        mock_store, _ = self._mock_rotate_deps(mocker, tmp_path)

        result = runner.invoke(app, ["rotate-key", "--yes"])

        assert result.exit_code == 0
        mock_store.set_key.assert_called_once()
        key_name, new_key = mock_store.set_key.call_args[0]
        assert key_name == "DATABASE__ENCRYPTION_KEY"
        assert len(new_key) == 64  # 32 bytes → 64 hex chars
        assert new_key != mock_store.get_key.return_value

    def test_rotate_key_duckdb_copy_fails_exits_1(
        self, runner: CliRunner, mocker: Any, tmp_path: Path
    ) -> None:
        """DuckDB copy failure exits 1 without updating the keychain."""
        mock_store, mock_conn = self._mock_rotate_deps(mocker, tmp_path)
        mock_conn.execute.side_effect = Exception("copy failed")

        result = runner.invoke(app, ["rotate-key", "--yes"])

        assert result.exit_code == 1
        mock_store.set_key.assert_not_called()

    def test_rotate_key_keychain_update_fails_exits_1(
        self,
        runner: CliRunner,
        mocker: Any,
        tmp_path: Path,
    ) -> None:
        """set_key failure after file moves exits 1 and prints recovery key to stderr."""
        mock_store, _ = self._mock_rotate_deps(mocker, tmp_path)
        mock_store.set_key.side_effect = Exception("keychain locked")

        result = runner.invoke(app, ["rotate-key", "--yes"])

        assert result.exit_code == 1
        # Recovery key is printed via typer.echo(err=True), which CliRunner
        # captures in result.output (stderr is mixed in by default)
        assert "MONEYBIN_DATABASE__ENCRYPTION_KEY" in result.output

    def test_rotate_key_confirmation_prompt_declined_exits_0(
        self, runner: CliRunner, mocker: Any, tmp_path: Path
    ) -> None:
        """Declining the confirmation prompt exits 0 without rotating."""
        mock_store, _ = self._mock_rotate_deps(mocker, tmp_path)

        result = runner.invoke(app, ["rotate-key"], input="n\n")

        assert result.exit_code == 0
        mock_store.set_key.assert_not_called()


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


class TestDbInfoCommand:
    """Tests for 'moneybin db info'."""

    @pytest.fixture
    def runner(self) -> CliRunner:
        return CliRunner()

    def test_info_shows_file_and_tables(
        self, runner: CliRunner, mocker: Any, tmp_path: Path
    ) -> None:
        """Info command opens the database and queries tables when unlocked."""
        test_db = tmp_path / "test.duckdb"
        test_db.write_bytes(b"x" * 2048)
        _make_settings_mock(test_db, mocker)

        mock_store = MagicMock()
        mock_store.get_key.return_value = "abc123"
        mocker.patch("moneybin.secrets.SecretStore", return_value=mock_store)

        mock_db = MagicMock()
        mock_db.execute.return_value.fetchall.return_value = [
            ("core", "fct_transactions")
        ]
        mock_db.execute.return_value.fetchone.return_value = (42,)
        mock_db.sql.return_value.fetchone.return_value = ("v1.2.3",)
        mocker.patch("moneybin.database.Database", return_value=mock_db)

        result = runner.invoke(app, ["info"])
        assert result.exit_code == 0
        # Verify Database was opened and table query was executed
        mock_db.execute.assert_called()

    def test_info_shows_locked_state_when_key_missing(
        self, runner: CliRunner, mocker: Any, tmp_path: Path
    ) -> None:
        """Info command exits 0 without opening the database when key is missing."""
        from moneybin.secrets import SecretNotFoundError

        test_db = tmp_path / "test.duckdb"
        test_db.write_bytes(b"data")
        _make_settings_mock(test_db, mocker)

        mock_store = MagicMock()
        mock_store.get_key.side_effect = SecretNotFoundError("not found")
        mocker.patch("moneybin.secrets.SecretStore", return_value=mock_store)
        mock_database_cls = mocker.patch("moneybin.database.Database")

        result = runner.invoke(app, ["info"])
        assert result.exit_code == 0
        # Database should not be opened when locked
        mock_database_cls.assert_not_called()

    def test_info_fails_when_database_not_found(
        self, runner: CliRunner, mocker: Any, tmp_path: Path
    ) -> None:
        """Info command exits 1 when database file does not exist."""
        _make_settings_mock(tmp_path / "missing.duckdb", mocker)
        result = runner.invoke(app, ["info"])
        assert result.exit_code == 1


class TestDbRestoreCommand:
    """Tests for 'moneybin db restore'."""

    @pytest.fixture
    def runner(self) -> CliRunner:
        return CliRunner()

    def test_restore_from_specified_path(
        self, runner: CliRunner, mocker: Any, tmp_path: Path
    ) -> None:
        """Restore copies the backup file and opens it with the current key."""
        test_db = tmp_path / "test.duckdb"
        test_db.write_bytes(b"current")
        backup = tmp_path / "backup.duckdb"
        backup.write_bytes(b"backup")
        _make_settings_mock(test_db, mocker)

        mock_db = MagicMock()
        mocker.patch("moneybin.database.Database", return_value=mock_db)
        mocker.patch("moneybin.secrets.SecretStore")

        result = runner.invoke(app, ["restore", "--from", str(backup), "--yes"])
        assert result.exit_code == 0
        # Verify the backup content was copied to the database path
        assert test_db.read_bytes() == b"backup"

    def test_restore_fails_when_backup_file_not_found(
        self, runner: CliRunner, mocker: Any, tmp_path: Path
    ) -> None:
        """Restore exits 1 when the specified backup file does not exist."""
        test_db = tmp_path / "test.duckdb"
        test_db.write_bytes(b"data")
        _make_settings_mock(test_db, mocker)

        result = runner.invoke(
            app, ["restore", "--from", str(tmp_path / "nonexistent.duckdb"), "--yes"]
        )
        assert result.exit_code == 1

    def test_restore_fails_when_no_backups_found(
        self, runner: CliRunner, mocker: Any, tmp_path: Path
    ) -> None:
        """Restore exits 1 when no backups exist in the backup directory."""
        test_db = tmp_path / "test.duckdb"
        _make_settings_mock(test_db, mocker)
        # backup_path is None → falls back to db_path.parent / "backups", which doesn't exist

        result = runner.invoke(app, ["restore"])
        assert result.exit_code == 1

    def test_restore_auto_backs_up_current_database(
        self, runner: CliRunner, mocker: Any, tmp_path: Path
    ) -> None:
        """Restore creates an auto-backup of the current database before overwriting."""
        test_db = tmp_path / "test.duckdb"
        test_db.write_bytes(b"current")
        backup = tmp_path / "backup.duckdb"
        backup.write_bytes(b"backup")
        _make_settings_mock(test_db, mocker)

        mock_db = MagicMock()
        mocker.patch("moneybin.database.Database", return_value=mock_db)
        mocker.patch("moneybin.secrets.SecretStore")

        result = runner.invoke(app, ["restore", "--from", str(backup), "--yes"])
        assert result.exit_code == 0

        backup_dir = tmp_path / "backups"
        pre_restore = list(backup_dir.glob("*_pre_restore.duckdb"))
        assert len(pre_restore) == 1

    def test_restore_fails_with_wrong_key(
        self, runner: CliRunner, mocker: Any, tmp_path: Path
    ) -> None:
        """Restore exits 1 when restored database can't be opened with current key."""
        test_db = tmp_path / "test.duckdb"
        test_db.write_bytes(b"current")
        backup = tmp_path / "backup.duckdb"
        backup.write_bytes(b"backup")
        _make_settings_mock(test_db, mocker)

        mocker.patch(
            "moneybin.database.Database",
            side_effect=Exception("wrong encryption key"),
        )
        mocker.patch("moneybin.secrets.SecretStore")

        result = runner.invoke(app, ["restore", "--from", str(backup), "--yes"])
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

        # Should fail with exit code 1 (either DuckDB CLI not found, or query
        # rejected) — never a crash with an unhandled exception
        assert result.exit_code == 1
        assert "Traceback" not in result.output
