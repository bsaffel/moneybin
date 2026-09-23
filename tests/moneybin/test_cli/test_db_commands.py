"""Tests for database management CLI commands.

Tests CLI-specific functionality: argument parsing, exit codes, error handling,
and subprocess command building for DuckDB CLI wrapper commands.
"""

from __future__ import annotations

import os
import shutil
import subprocess  # noqa: S404  # test executes the installed DuckDB CLI with static arguments
from pathlib import Path
from typing import Any, cast
from unittest.mock import MagicMock

import pytest
import typer
from typer.testing import CliRunner

from moneybin.cli.commands import db as db_commands
from moneybin.cli.commands.db import app
from moneybin.secrets import SecretNotFoundError, SecretUnavailableError


def test_unlock_refuses_redirected_passphrase_before_derivation(
    mocker: Any,
    tmp_path: Path,
) -> None:
    _make_settings_mock(tmp_path / "test.duckdb", mocker)
    store = mocker.patch("moneybin.secrets.SecretStore").return_value
    store.get_key.return_value = "c3ludGhldGljLXNhbHQ="
    mocker.patch("typer.prompt", side_effect=AssertionError("must not prompt"))
    mocker.patch(
        "moneybin.database.derive_key_from_passphrase",
        side_effect=AssertionError("must not derive a key"),
    )

    result = CliRunner().invoke(app, ["unlock"], input="synthetic passphrase\n")

    assert result.exit_code == 1, result.output
    assert "requires an interactive terminal" in result.stderr
    assert "never reads passphrases from redirected input" in result.stderr
    store.set_key.assert_not_called()


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


def _make_settings_mock_for_hint(db_path: Path, mocker: Any) -> MagicMock:
    """Like `_make_settings_mock`, but also patches database.py's own binding.

    `database_key_error_hint()` resolves `get_settings` via the module-level
    `from moneybin.config import get_settings` in database.py — a separate
    bound name that patching `moneybin.config.get_settings` alone does not
    reach, since that import already ran at collection time.
    """
    mock_settings = _make_settings_mock(db_path, mocker).return_value
    mocker.patch("moneybin.database.get_settings", return_value=mock_settings)
    return mock_settings


class TestDuckDBCLIInitialization:
    """Tests for DuckDB CLI encryption setup."""

    def test_init_script_uses_duckdb_environment_lookup(self, tmp_path: Path) -> None:
        """The init script must not persist the encryption key it uses."""
        script_path = db_commands._create_init_script(  # pyright: ignore[reportPrivateUsage]  # unit test covers init script content
            tmp_path / "test.duckdb"
        )
        try:
            script = script_path.read_text()
        finally:
            script_path.unlink(missing_ok=True)

        assert "getenv('MONEYBIN_DATABASE__ENCRYPTION_KEY')" in script
        assert "ENCRYPTION_KEY '" not in script

    @pytest.mark.integration
    def test_duckdb_cli_supports_environment_lookup(self) -> None:
        """The bundled CLI, rather than the Python engine, evaluates ``getenv``."""
        duckdb_path = shutil.which("duckdb")
        if duckdb_path is None:
            pytest.skip("DuckDB CLI is not installed")

        environment = os.environ.copy()
        environment["MONEYBIN_TEST_DUCKDB_GETENV"] = "synthetic-cli-value"
        result = subprocess.run(  # noqa: S603  # installed CLI and static test query, never user input
            [
                duckdb_path,
                "-c",
                "SELECT getenv('MONEYBIN_TEST_DUCKDB_GETENV') AS observed;",
            ],
            check=True,
            capture_output=True,
            text=True,
            env=environment,
        )

        assert "synthetic-cli-value" in result.stdout

    def test_cli_uses_child_key_environment_without_redirecting_streams(
        self, mocker: Any, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """CLI setup scopes the key to its child and preserves interactive I/O."""
        test_db = tmp_path / "test.duckdb"
        test_db.touch()
        synthetic_key = "synthetic-key-only-for-this-test"
        monkeypatch.setenv("MONEYBIN_DATABASE__ENCRYPTION_KEY", "parent-key")
        mock_store = MagicMock()
        mock_store.get_key.return_value = synthetic_key
        mocker.patch("moneybin.secrets.SecretStore", return_value=mock_store)
        mocker.patch("moneybin.cli.commands.db.shutil.which", return_value="duckdb")
        observed: dict[str, object] = {}

        def record_cli_call(command: list[str], **kwargs: object) -> None:
            observed["command"] = command
            observed["kwargs"] = kwargs

        mocker.patch(
            "moneybin.cli.commands.db.subprocess.run", side_effect=record_cli_call
        )

        db_commands._run_duckdb_cli(  # pyright: ignore[reportPrivateUsage]  # unit test covers subprocess environment
            test_db, start_msg="", hint_msg=""
        )

        child_environment = observed["kwargs"].get("env")  # type: ignore[union-attr]
        assert isinstance(child_environment, dict)
        assert child_environment["MONEYBIN_DATABASE__ENCRYPTION_KEY"] == synthetic_key
        assert child_environment["PATH"] == os.environ["PATH"]
        assert os.environ["MONEYBIN_DATABASE__ENCRYPTION_KEY"] == "parent-key"
        assert observed["kwargs"] == {"check": True, "env": child_environment}
        command = observed["command"]
        assert isinstance(command, list)
        assert not Path(cast(str, command[2])).exists()

    @pytest.mark.parametrize(
        "error_type",
        [
            pytest.param(SecretNotFoundError, id="missing-key"),
            pytest.param(SecretUnavailableError, id="denied-keychain"),
        ],
    )
    def test_cli_removes_init_script_when_child_environment_cannot_load_key(
        self,
        mocker: Any,
        tmp_path: Path,
        error_type: type[SecretNotFoundError],
    ) -> None:
        """A key read failure cannot leave the generated init script behind."""
        test_db = tmp_path / "test.duckdb"
        test_db.touch()
        init_script = tmp_path / "init.sql"
        init_script.touch()
        mocker.patch("moneybin.cli.commands.db.shutil.which", return_value="duckdb")
        mocker.patch(
            "moneybin.cli.commands.db._create_init_script", return_value=init_script
        )
        mocker.patch(
            "moneybin.cli.commands.db._duckdb_cli_environment",
            side_effect=error_type("key lookup failed"),
        )

        with pytest.raises(typer.Exit):
            db_commands._run_duckdb_cli(  # pyright: ignore[reportPrivateUsage]  # unit test covers child-environment failure cleanup
                test_db, start_msg="", hint_msg=""
            )

        assert not init_script.exists()

    def test_launcher_presents_start_and_cancelled_outcome_on_stderr(
        self, mocker: Any, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Native child commentary must not depend on filtered INFO logging."""
        database = tmp_path / "test.duckdb"
        database.touch()
        mocker.patch("moneybin.cli.commands.db.shutil.which", return_value="duckdb")
        mocker.patch(
            "moneybin.cli.commands.db._duckdb_cli_environment", return_value={}
        )
        mocker.patch(
            "moneybin.cli.commands.db.subprocess.run", side_effect=KeyboardInterrupt
        )

        with pytest.raises(typer.Exit) as exit_info:
            db_commands._run_duckdb_cli(  # pyright: ignore[reportPrivateUsage]
                database, start_msg="Opening DuckDB shell", hint_msg="Type .help"
            )

        assert exit_info.value.exit_code == 130
        stderr = capsys.readouterr().err
        assert "Opening DuckDB shell" in stderr
        assert "Type .help" in stderr
        assert "cancelled" in stderr.lower()

    def test_launcher_keeps_cancelled_outcome_when_quiet(
        self, mocker: Any, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Quiet hides launch chatter but not the child's interrupted outcome."""
        from moneybin.cli.output import OutputFormat
        from moneybin.cli.utils import set_output_flag, set_quiet_flag

        database = tmp_path / "test.duckdb"
        database.touch()
        mocker.patch("moneybin.cli.commands.db.shutil.which", return_value="duckdb")
        mocker.patch(
            "moneybin.cli.commands.db._duckdb_cli_environment", return_value={}
        )
        mocker.patch(
            "moneybin.cli.commands.db.subprocess.run", side_effect=KeyboardInterrupt
        )
        set_output_flag(OutputFormat.TEXT)
        set_quiet_flag(True)
        try:
            with pytest.raises(typer.Exit) as exit_info:
                db_commands._run_duckdb_cli(  # pyright: ignore[reportPrivateUsage]
                    database, start_msg="Opening DuckDB shell", hint_msg="Type .help"
                )
        finally:
            set_quiet_flag(False)

        assert exit_info.value.exit_code == 130
        stderr = capsys.readouterr().err
        assert "Opening DuckDB shell" not in stderr
        assert "Type .help" not in stderr
        assert "DuckDB shell cancelled" in stderr

    def test_direct_db_banner_uses_the_active_attention_marker(
        self, mocker: Any
    ) -> None:
        """The direct-DB safety disclosure follows ASCII terminal policy."""
        from moneybin.cli.terminal import TerminalPolicy, TerminalSymbols

        policy = TerminalPolicy(
            output="text",
            interactive=False,
            page=False,
            color=False,
            style=False,
            animate_progress=False,
            stage_chatter=False,
            ascii=True,
            width=80,
            height=24,
            symbols=TerminalSymbols(
                success="OK", attention="!", failure="X", action=">"
            ),
            minus="-",
        )
        mocker.patch("moneybin.cli.utils.get_terminal_policy", return_value=policy)

        banner = db_commands._direct_db_banner()  # pyright: ignore[reportPrivateUsage]

        assert banner.startswith("! Direct DB access")
        assert "⚠️" not in banner

    def test_launcher_missing_database_keeps_recovery_on_stderr(
        self,
        tmp_path: Path,
        caplog: pytest.LogCaptureFixture,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        """A launcher refusal keeps its recovery command outside filtered INFO."""
        database = tmp_path / "missing.duckdb"

        with caplog.at_level("ERROR"), pytest.raises(typer.Exit) as exit_info:
            db_commands._run_duckdb_cli(  # pyright: ignore[reportPrivateUsage]
                database, start_msg="", hint_msg=""
            )

        assert exit_info.value.exit_code == 1
        assert "Database file not found" in caplog.text
        assert "moneybin db init" in capsys.readouterr().err


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
        mocker.patch(
            "moneybin.cli.commands.db._duckdb_cli_environment",
            return_value=os.environ.copy(),
        )
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
            "moneybin.cli.commands.db._duckdb_cli_environment",
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
        assert result.exit_code == 130


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
        mocker.patch(
            "moneybin.cli.commands.db._duckdb_cli_environment",
            return_value=os.environ.copy(),
        )
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
            "moneybin.cli.commands.db._duckdb_cli_environment",
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
        assert result.exit_code == 130


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
        mocker.patch(
            "moneybin.cli.commands.db._duckdb_cli_environment",
            return_value=os.environ.copy(),
        )
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
        """Query command passes correct format flags for -o text|json|csv|markdown|box."""
        test_db = tmp_path / "test.duckdb"
        test_db.touch()
        _make_settings_mock(test_db, mocker)

        formats = {
            "text": "-table",
            "json": "-json",
            "csv": "-csv",
            "markdown": "-markdown",
            "box": "-box",
        }

        for format_name, format_flag in formats.items():
            mock_subprocess_run.reset_mock()
            result = runner.invoke(app, ["query", "SELECT 1", "--output", format_name])
            assert result.exit_code == 0
            call_args = mock_subprocess_run.call_args[0][0]
            assert format_flag in call_args
            # Format flag must appear before -c so DuckDB applies it before running the query
            assert call_args.index(format_flag) < call_args.index("-c")

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
            "moneybin.cli.commands.db._duckdb_cli_environment",
            side_effect=SecretNotFoundError("locked"),
        )

        result = runner.invoke(app, ["query", "SELECT 1"])
        assert result.exit_code == 1

    def test_query_locked_database_offers_unlock_and_env_var_never_db_init(
        self,
        runner: CliRunner,
        mocker: Any,
        mock_duckdb_cli: MagicMock,
        tmp_path: Path,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """The message names both recovery paths, never the dangerous db init.

        A denied keychain read and a missing key raise the same exception
        (see #419) — against an existing database file, suggesting `db init`
        first is the dangerous case the issue was filed about.
        """
        from moneybin.secrets import SecretNotFoundError

        test_db = tmp_path / "test.duckdb"
        test_db.touch()
        _make_settings_mock_for_hint(test_db, mocker)
        mocker.patch(
            "moneybin.cli.commands.db._duckdb_cli_environment",
            side_effect=SecretNotFoundError("locked"),
        )

        with caplog.at_level("INFO"):
            result = runner.invoke(app, ["query", "SELECT 1"])

        assert result.exit_code == 1
        assert "db unlock" in caplog.text
        assert "MONEYBIN_DATABASE__ENCRYPTION_KEY" in caplog.text
        assert "db init" not in caplog.text

    def test_query_denied_keychain_message_is_confident_not_a_hedge(
        self,
        runner: CliRunner,
        mocker: Any,
        mock_duckdb_cli: MagicMock,
        tmp_path: Path,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """SecretUnavailableError names the OS denial, not a generic miss."""
        from moneybin.secrets import SecretUnavailableError

        test_db = tmp_path / "test.duckdb"
        test_db.touch()
        _make_settings_mock_for_hint(test_db, mocker)
        mocker.patch(
            "moneybin.cli.commands.db._duckdb_cli_environment",
            side_effect=SecretUnavailableError("denied"),
        )

        with caplog.at_level("INFO"):
            result = runner.invoke(app, ["query", "SELECT 1"])

        assert result.exit_code == 1
        assert "denied" in caplog.text


class TestDbInitCommand:
    """Tests for 'moneybin db init'."""

    @pytest.fixture
    def runner(self) -> CliRunner:
        return CliRunner()

    @pytest.fixture(autouse=True)
    def interactive_prompt(self, mocker: Any) -> None:
        mocker.patch("moneybin.cli.commands.db._require_interactive_prompt")

    def _mock_deps(self, mocker: Any, tmp_path: Path) -> tuple[MagicMock, MagicMock]:
        """Return (mock_store, mock_db_class) with settings patched."""
        from moneybin.secrets import SecretNotFoundError

        _make_settings_mock(tmp_path / "moneybin.duckdb", mocker)
        mock_store = MagicMock()
        # Default: no existing key, so auto-key mode generates one
        mock_store.get_key.side_effect = SecretNotFoundError("no key")
        mock_store.has_keychain_entry.return_value = False
        mocker.patch("moneybin.secrets.SecretStore", return_value=mock_store)
        mock_db = MagicMock()
        mocker.patch("moneybin.database.Database", return_value=mock_db)
        # init_db calls materialize_seeds inside the with block; bypass real
        # SQLMesh + view creation since Database itself is mocked here.
        mocker.patch("moneybin.seeds.materialize_seeds")
        return mock_store, mock_db

    def test_init_auto_key_stores_key_and_creates_db(
        self, runner: CliRunner, mocker: Any, tmp_path: Path
    ) -> None:
        """Auto-key mode generates a key, stores it, and creates the database."""
        mock_store, _mock_db = self._mock_deps(mocker, tmp_path)

        result = runner.invoke(app, ["init", "--yes"])

        assert result.exit_code == 0
        mock_store.set_key.assert_called_once()
        key_name, key_value = mock_store.set_key.call_args[0]
        assert key_name == "DATABASE__ENCRYPTION_KEY"
        assert len(key_value) == 64  # 32 bytes → 64 hex chars

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
        mock_store, _mock_db = self._mock_deps(mocker, tmp_path)
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

    def test_init_existing_db_prompt_declined_exits_0(
        self, runner: CliRunner, mocker: Any, tmp_path: Path
    ) -> None:
        """Declining the overwrite prompt exits 0 without creating the database."""
        db_path = tmp_path / "moneybin.duckdb"
        db_path.touch()
        mock_store, _mock_db = self._mock_deps(mocker, tmp_path)

        result = runner.invoke(app, ["init"], input="n\n")

        assert result.exit_code == 0
        mock_store.set_key.assert_not_called()

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

    @pytest.fixture(autouse=True)
    def interactive_prompt(self, mocker: Any) -> None:
        mocker.patch("moneybin.cli.commands.db._require_interactive_prompt")

    def test_unlock_no_salt_exits_1(self, runner: CliRunner, mocker: Any) -> None:
        """Unlock fails when no passphrase salt is found in keychain."""
        from moneybin.secrets import SecretNotFoundError

        mock_store = MagicMock()
        mock_store.get_key.side_effect = SecretNotFoundError("no salt")
        mocker.patch("moneybin.secrets.SecretStore", return_value=mock_store)

        result = runner.invoke(app, ["unlock"], input="anypassphrase\n")

        assert result.exit_code == 1

    def test_unlock_no_salt_on_fresh_install_points_at_db_init(
        self,
        runner: CliRunner,
        mocker: Any,
        tmp_path: Path,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """No database at all — the salt miss is unambiguous: run db init."""
        from moneybin.secrets import SecretNotFoundError

        mock_store = MagicMock()
        mock_store.get_key.side_effect = SecretNotFoundError("no salt")
        mocker.patch("moneybin.secrets.SecretStore", return_value=mock_store)
        _make_settings_mock(tmp_path / "never-created.duckdb", mocker)

        with caplog.at_level("INFO"):
            result = runner.invoke(app, ["unlock"], input="anypassphrase\n")

        assert result.exit_code == 1
        assert "db init --passphrase" in caplog.text
        assert "MONEYBIN_DATABASE__ENCRYPTION_KEY" not in caplog.text

    def test_unlock_no_salt_on_existing_database_names_env_denial(
        self,
        runner: CliRunner,
        mocker: Any,
        tmp_path: Path,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Database already exists — a denied read looks identical to a missing salt.

        Offers the env-var route (which needs no further keychain access)
        instead of only "was this created with --passphrase mode?" (see #419).
        """
        from moneybin.secrets import SecretNotFoundError

        mock_store = MagicMock()
        mock_store.get_key.side_effect = SecretNotFoundError("no salt")
        mocker.patch("moneybin.secrets.SecretStore", return_value=mock_store)
        existing = tmp_path / "moneybin.duckdb"
        existing.write_bytes(b"")
        _make_settings_mock(existing, mocker)

        with caplog.at_level("INFO"):
            result = runner.invoke(app, ["unlock"], input="anypassphrase\n")

        assert result.exit_code == 1
        assert "already exists" in caplog.text
        assert "MONEYBIN_DATABASE__ENCRYPTION_KEY" in caplog.text

    def test_unlock_database_not_found_deletes_key_and_exits_1(
        self, runner: CliRunner, mocker: Any, tmp_path: Path
    ) -> None:
        """Database file missing: key is deleted from keychain and exits 1."""
        import base64

        fake_salt = base64.b64encode(b"\x00" * 16).decode()
        mock_store = MagicMock()
        mock_store.get_key.return_value = fake_salt
        mocker.patch("moneybin.secrets.SecretStore", return_value=mock_store)
        mocker.patch("argon2.low_level.hash_secret_raw", return_value=b"\x00" * 32)

        _make_settings_mock(tmp_path / "missing.duckdb", mocker)

        result = runner.invoke(app, ["unlock"], input="anypass\n")

        assert result.exit_code == 1
        mock_store.delete_key.assert_called_once_with("DATABASE__ENCRYPTION_KEY")

    def test_unlock_wrong_passphrase_deletes_key_and_exits_1(
        self, runner: CliRunner, mocker: Any, tmp_path: Path
    ) -> None:
        """Wrong passphrase: database open fails, key is deleted, exits 1."""
        import base64

        db_path = tmp_path / "moneybin.duckdb"
        db_path.touch()
        fake_salt = base64.b64encode(b"\x00" * 16).decode()
        mock_store = MagicMock()
        mock_store.get_key.return_value = fake_salt
        mocker.patch("moneybin.secrets.SecretStore", return_value=mock_store)
        mocker.patch("argon2.low_level.hash_secret_raw", return_value=b"\x00" * 32)

        _make_settings_mock(db_path, mocker)
        mocker.patch("moneybin.database.Database", side_effect=Exception("bad key"))

        result = runner.invoke(app, ["unlock"], input="wrongpass\n")

        assert result.exit_code == 1
        mock_store.delete_key.assert_called_once_with("DATABASE__ENCRYPTION_KEY")
        assert "derived key was removed" in result.output

    def test_unlock_reports_uncertain_cleanup_without_claiming_locked(
        self, runner: CliRunner, mocker: Any, tmp_path: Path
    ) -> None:
        """A failed keychain cleanup leaves the lock state unknown."""
        import base64

        db_path = tmp_path / "moneybin.duckdb"
        db_path.touch()
        mock_store = MagicMock()
        mock_store.get_key.return_value = base64.b64encode(b"\x00" * 16).decode()
        mock_store.delete_key.side_effect = Exception("keychain unavailable")
        mocker.patch("moneybin.secrets.SecretStore", return_value=mock_store)
        mocker.patch("argon2.low_level.hash_secret_raw", return_value=b"\x00" * 32)
        _make_settings_mock(db_path, mocker)
        mocker.patch("moneybin.database.Database", side_effect=Exception("bad key"))

        result = runner.invoke(app, ["unlock"], input="wrongpass\n")

        assert result.exit_code == 1
        assert "could not confirm removal" in result.output
        assert "remains locked" not in result.output

    def test_unlock_correct_passphrase_exits_0(
        self, runner: CliRunner, mocker: Any, tmp_path: Path
    ) -> None:
        """Correct passphrase: key stored, database opens, exits 0."""
        import base64

        db_path = tmp_path / "moneybin.duckdb"
        db_path.touch()
        fake_salt = base64.b64encode(b"\x00" * 16).decode()
        mock_store = MagicMock()
        mock_store.get_key.return_value = fake_salt
        mocker.patch("moneybin.secrets.SecretStore", return_value=mock_store)
        mocker.patch("argon2.low_level.hash_secret_raw", return_value=b"\xaa" * 32)

        _make_settings_mock(db_path, mocker)
        mock_db = MagicMock()
        mocker.patch("moneybin.database.Database", return_value=mock_db)

        result = runner.invoke(app, ["unlock"], input="correctpass\n")

        assert result.exit_code == 0
        mock_store.set_key.assert_called_once_with(
            "DATABASE__ENCRYPTION_KEY", (b"\xaa" * 32).hex()
        )


class TestDbRotateKeyCommand:
    """Tests for 'moneybin db rotate-key'."""

    @pytest.fixture
    def runner(self) -> CliRunner:
        return CliRunner()

    @pytest.fixture(autouse=True)
    def interactive_prompt(self, mocker: Any) -> None:
        mocker.patch("moneybin.cli.commands.db._require_interactive_prompt")

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

        result = runner.invoke(app, ["key", "rotate", "--yes"])

        assert result.exit_code == 1
        mock_store.set_key.assert_not_called()

    def test_rotate_key_success_stores_new_key(
        self, runner: CliRunner, mocker: Any, tmp_path: Path
    ) -> None:
        """Happy path: new key generated and stored, old backup removed."""
        mock_store, _ = self._mock_rotate_deps(mocker, tmp_path)

        result = runner.invoke(app, ["key", "rotate", "--yes"])

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

        result = runner.invoke(app, ["key", "rotate", "--yes"])

        assert result.exit_code == 1
        mock_store.set_key.assert_not_called()

    def test_rotate_key_copy_interrupt_removes_partial_candidate(
        self, runner: CliRunner, mocker: Any, tmp_path: Path
    ) -> None:
        """An interrupted COPY leaves no temporary rotated database behind."""
        mock_store, mock_conn = self._mock_rotate_deps(mocker, tmp_path)
        rotated_path = tmp_path / "moneybin.rotated.duckdb"

        def interrupt_copy(sql: str, *_args: object, **_kwargs: object) -> MagicMock:
            if "COPY FROM DATABASE" in sql:
                rotated_path.write_bytes(b"partial")
                raise KeyboardInterrupt
            return MagicMock()

        mock_conn.execute.side_effect = interrupt_copy

        result = runner.invoke(app, ["key", "rotate", "--yes"])

        assert result.exit_code == 130
        assert not rotated_path.exists()
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

        result = runner.invoke(app, ["key", "rotate", "--yes"])

        assert result.exit_code == 1
        # Recovery key is printed via typer.echo(err=True), which CliRunner
        # captures in result.output (stderr is mixed in by default)
        assert "MONEYBIN_DATABASE__ENCRYPTION_KEY" in result.output

    def test_rotate_key_interrupt_after_swap_keeps_recovery_channel(
        self, runner: CliRunner, mocker: Any, tmp_path: Path
    ) -> None:
        """An interrupted keychain update has the same recovery boundary."""
        mock_store, _ = self._mock_rotate_deps(mocker, tmp_path)
        mock_store.set_key.side_effect = KeyboardInterrupt

        result = runner.invoke(app, ["key", "rotate", "--yes"])

        assert result.exit_code == 130
        assert "MONEYBIN_DATABASE__ENCRYPTION_KEY" in result.output
        assert "keychain update is unconfirmed" in result.output

    def test_rotate_key_interrupt_after_keychain_update_reports_completed_write(
        self, runner: CliRunner, mocker: Any, tmp_path: Path
    ) -> None:
        """An interrupt after set_key returns must not describe it as unconfirmed."""
        import contextlib

        mock_store, _ = self._mock_rotate_deps(mocker, tmp_path)

        def report_event(_event: object) -> None:
            return None

        @contextlib.contextmanager
        def interrupt_after_rotation_progress(*_args: object, **_kwargs: object) -> Any:
            yield report_event
            raise KeyboardInterrupt

        mocker.patch(
            "moneybin.cli.commands.db.operation_progress",
            interrupt_after_rotation_progress,
        )

        result = runner.invoke(app, ["key", "rotate", "--yes"])

        assert result.exit_code == 130
        mock_store.set_key.assert_called_once()
        assert "keychain update completed" in result.output.lower()
        assert "keychain update is unconfirmed" not in result.output.lower()
        assert "MONEYBIN_DATABASE__ENCRYPTION_KEY" not in result.output

    def test_rotate_key_interrupt_before_replacement_retains_recovery_artifacts(
        self,
        runner: CliRunner,
        mocker: Any,
        caplog: pytest.LogCaptureFixture,
        tmp_path: Path,
    ) -> None:
        """An interrupted replacement reports each retained recovery artifact."""
        mock_store, _ = self._mock_rotate_deps(mocker, tmp_path)
        db_path = tmp_path / "moneybin.duckdb"
        old_backup = tmp_path / "moneybin.old.duckdb"
        rotated_path = tmp_path / "moneybin.rotated.duckdb"
        rotated_path.write_bytes(b"rotated candidate")
        moves = 0

        def interrupt_before_replacement(source: str, destination: str) -> str:
            nonlocal moves
            moves += 1
            if moves == 2:
                raise KeyboardInterrupt
            return str(Path(source).replace(destination))

        mocker.patch(
            "moneybin.cli.commands.db.shutil.move",
            side_effect=interrupt_before_replacement,
        )
        synthetic_new_key = "synthetic-new-key"
        mocker.patch("secrets.token_hex", return_value=synthetic_new_key)

        result = runner.invoke(app, ["key", "rotate", "--yes"])

        assert result.exit_code == 130
        assert old_backup.read_bytes() == b""
        assert rotated_path.read_bytes() == b"rotated candidate"
        assert not db_path.exists()
        assert "original backup is retained" in result.output.lower()
        assert "replacement is unconfirmed" in result.output.lower()
        assert str(old_backup) in result.output
        assert str(rotated_path) in result.output
        assert str(db_path) in result.output
        assert "original backup requires the old key" in result.output.lower()
        assert "only access a confirmed rotated candidate" in result.output.lower()
        assert "MONEYBIN_DATABASE__ENCRYPTION_KEY" in result.output
        assert synthetic_new_key not in result.stdout
        assert synthetic_new_key in result.stderr
        assert synthetic_new_key not in caplog.text
        mock_store.set_key.assert_not_called()

    def test_rotate_key_interrupt_after_replacement_keeps_backup_and_candidate_guidance(
        self,
        runner: CliRunner,
        mocker: Any,
        caplog: pytest.LogCaptureFixture,
        tmp_path: Path,
    ) -> None:
        """A post-move interrupt keeps the original backup and bounds new-key recovery."""
        mock_store, _ = self._mock_rotate_deps(mocker, tmp_path)
        db_path = tmp_path / "moneybin.duckdb"
        old_backup = tmp_path / "moneybin.old.duckdb"
        rotated_path = tmp_path / "moneybin.rotated.duckdb"
        rotated_path.write_bytes(b"rotated candidate")
        moves = 0

        def interrupt_after_replacement(source: str, destination: str) -> str:
            nonlocal moves
            moves += 1
            result = str(Path(source).replace(destination))
            if moves == 2:
                raise KeyboardInterrupt
            return result

        mocker.patch(
            "moneybin.cli.commands.db.shutil.move",
            side_effect=interrupt_after_replacement,
        )
        synthetic_new_key = "synthetic-new-key"
        mocker.patch("secrets.token_hex", return_value=synthetic_new_key)

        result = runner.invoke(app, ["key", "rotate", "--yes"])

        assert result.exit_code == 130
        assert old_backup.read_bytes() == b""
        assert db_path.read_bytes() == b"rotated candidate"
        assert not rotated_path.exists()
        assert "original backup is retained" in result.output.lower()
        assert "replacement is unconfirmed" in result.output.lower()
        assert "original backup requires the old key" in result.output.lower()
        assert "only access a confirmed rotated candidate" in result.output.lower()
        assert "MONEYBIN_DATABASE__ENCRYPTION_KEY" in result.output
        assert synthetic_new_key not in result.stdout
        assert synthetic_new_key in result.stderr
        assert synthetic_new_key not in caplog.text
        mock_store.set_key.assert_not_called()

    def test_rotate_key_second_move_failure_reports_archived_original(
        self, runner: CliRunner, mocker: Any, tmp_path: Path
    ) -> None:
        """The swap's first completed move is a partial outcome, not a silent error."""
        mock_store, _ = self._mock_rotate_deps(mocker, tmp_path)
        move = mocker.patch("moneybin.cli.commands.db.shutil.move")
        move.side_effect = [None, OSError("synthetic replacement move failure")]

        result = runner.invoke(app, ["key", "rotate", "--yes"])

        assert result.exit_code == 1
        assert "original database was archived" in result.output.lower()
        assert "replacement state is unknown" in result.output.lower()
        assert "MONEYBIN_DATABASE__ENCRYPTION_KEY" not in result.output
        mock_store.set_key.assert_not_called()

    def test_rotate_key_first_move_failure_has_recovery_guidance(
        self, runner: CliRunner, mocker: Any, tmp_path: Path
    ) -> None:
        """A failed first swap move must not surface as an uncontextualized error."""
        mock_store, _ = self._mock_rotate_deps(mocker, tmp_path)
        mocker.patch(
            "moneybin.cli.commands.db.shutil.move",
            side_effect=OSError("synthetic archive move failure"),
        )

        result = runner.invoke(app, ["key", "rotate", "--yes"])

        assert result.exit_code == 1
        assert "file state is unknown" in result.output.lower()
        assert "inspect" in result.output.lower()
        mock_store.set_key.assert_not_called()

    def test_rotate_key_confirmation_prompt_declined_exits_0(
        self, runner: CliRunner, mocker: Any, tmp_path: Path
    ) -> None:
        """Declining the confirmation prompt exits 0 without rotating."""
        mock_store, _ = self._mock_rotate_deps(mocker, tmp_path)

        result = runner.invoke(app, ["key", "rotate"], input="n\n")

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

        result = runner.invoke(app, ["key", "show"])
        assert result.exit_code == 0
        assert "abc123deadbeef" in result.output

    def test_key_fails_when_locked(self, runner: CliRunner, mocker: Any) -> None:
        """Key command fails when no key is available."""
        from moneybin.secrets import SecretNotFoundError

        mock_store = MagicMock()
        mock_store.get_key.side_effect = SecretNotFoundError("not found")
        mocker.patch("moneybin.secrets.SecretStore", return_value=mock_store)

        result = runner.invoke(app, ["key", "show"])
        assert result.exit_code == 1

    def test_key_fails_with_confident_message_when_keychain_denied(
        self, runner: CliRunner, mocker: Any, caplog: pytest.LogCaptureFixture
    ) -> None:
        """SecretUnavailableError names the OS denial, not a generic miss (#419)."""
        from moneybin.secrets import SecretUnavailableError

        mock_store = MagicMock()
        mock_store.get_key.side_effect = SecretUnavailableError("denied")
        mocker.patch("moneybin.secrets.SecretStore", return_value=mock_store)

        with caplog.at_level("INFO"):
            result = runner.invoke(app, ["key", "show"])

        assert result.exit_code == 1
        assert "denied" in caplog.text


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
        mock_db.__enter__ = lambda self: self  # type: ignore[assignment]

        def execute_side_effect(sql: str, *_args: Any, **_kw: Any) -> MagicMock:
            result = MagicMock()
            if "information_schema" in sql:
                result.fetchall.return_value = [("core", "fct_transactions")]
            else:
                result.fetchall.return_value = [("core", "fct_transactions", 42)]
            return result

        mock_db.execute.side_effect = execute_side_effect
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

    def test_info_pages_long_unlocked_metadata(
        self, runner: CliRunner, mocker: Any, tmp_path: Path
    ) -> None:
        """The composed metadata/table answer is eligible for the shared pager."""
        from moneybin.cli.terminal import TerminalPolicy, TerminalSymbols

        db_path = tmp_path / "moneybin.duckdb"
        db_path.write_bytes(b"data")
        _make_settings_mock(db_path, mocker)
        store = MagicMock()
        store.get_key.return_value = "synthetic"
        mocker.patch("moneybin.secrets.SecretStore", return_value=store)
        database = MagicMock()
        database.__enter__ = lambda self: self  # type: ignore[assignment]
        database.execute.side_effect = [
            MagicMock(fetchall=MagicMock(return_value=[("core", "fct_transactions")])),
            MagicMock(
                fetchall=MagicMock(return_value=[("core", "fct_transactions", 1)])
            ),
        ]
        database.sql.return_value.fetchone.return_value = ("v1",)
        mocker.patch("moneybin.database.Database", return_value=database)
        policy = TerminalPolicy(
            output="text",
            interactive=True,
            page=True,
            color=False,
            style=False,
            animate_progress=False,
            stage_chatter=False,
            ascii=True,
            width=80,
            height=1,
            symbols=TerminalSymbols(
                success="OK", attention="!", failure="X", action=">"
            ),
            minus="-",
        )
        mocker.patch(
            "moneybin.cli.commands.db.get_terminal_policy", return_value=policy
        )
        page = mocker.patch("moneybin.cli.pager.page_text", return_value=True)

        result = runner.invoke(app, ["info"])

        assert result.exit_code == 0
        page.assert_called_once()

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
        assert test_db.read_bytes() == b"backup"
        assert "Database was replaced" in result.output
        assert "Pre-restore backup saved" in result.output

    def test_restore_copy_failure_does_not_claim_replacement(
        self, runner: CliRunner, mocker: Any, tmp_path: Path
    ) -> None:
        """A normal replacement-copy error leaves the target state unconfirmed."""
        db_path = tmp_path / "moneybin.duckdb"
        db_path.write_bytes(b"current")
        source = tmp_path / "backup.duckdb"
        source.write_bytes(b"restore")
        _make_settings_mock(db_path, mocker)
        original_copy = shutil.copy2
        calls = 0

        def fail_replacement(source_path: str, target_path: str) -> str:
            nonlocal calls
            calls += 1
            if calls == 2:
                raise OSError("synthetic replacement failure")
            return str(original_copy(source_path, target_path))

        mocker.patch(
            "moneybin.cli.commands.db.shutil.copy2", side_effect=fail_replacement
        )

        result = runner.invoke(app, ["restore", "--from", str(source), "--yes"])

        assert result.exit_code == 1
        assert "Pre-restore backup saved" in result.output
        assert "replacement may be incomplete" in result.output.lower()
        assert "Database was replaced" not in result.output


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


class TestDbPsCommand:
    """'moneybin db ps' — the process roll is a table, so it renders like one."""

    @pytest.fixture
    def runner(self) -> CliRunner:
        return CliRunner()

    @pytest.fixture(autouse=True)
    def interactive_prompt(self, mocker: Any) -> None:
        mocker.patch("moneybin.cli.commands.db._require_interactive_prompt")

    @pytest.fixture
    def two_processes(self, mocker: Any) -> MagicMock:
        processes = [
            {"pid": 101, "command": "duckdb", "cmdline": "duckdb moneybin.duckdb"},
            {"pid": 202, "command": "python", "cmdline": "python -m etl"},
        ]
        mocker.patch(
            "moneybin.cli.commands.db._capture_process_snapshot",
            return_value={101: MagicMock(), 202: MagicMock()},
        )
        return mocker.patch(
            "moneybin.cli.commands.db._find_db_processes",
            return_value=processes,
        )

    def test_ps_renders_the_process_roll_through_the_shared_renderer(
        self,
        runner: CliRunner,
        tmp_path: Path,
        two_processes: MagicMock,
        wide_terminal: None,
    ) -> None:
        """Requirement 1: the framed table is the observable difference.

        The hand-padded columns this replaces emitted the same values in the
        same order; the border is what says they now come from `render_rows`
        rather than from a format spec at the call site.
        """
        db_file = tmp_path / "moneybin.duckdb"
        db_file.touch()

        result = runner.invoke(app, ["ps", "--database", str(db_file)])

        assert result.exit_code == 0
        assert "┃" in result.output
        assert "pid" in result.output

    def test_ps_renders_one_row_per_process(
        self,
        runner: CliRunner,
        tmp_path: Path,
        two_processes: MagicMock,
        wide_terminal: None,
    ) -> None:
        """Requirement 35: every process the scan found reaches the table."""
        db_file = tmp_path / "moneybin.duckdb"
        db_file.touch()

        result = runner.invoke(app, ["ps", "--database", str(db_file)])

        for cell in ("101", "duckdb moneybin.duckdb", "202", "python -m etl"):
            assert cell in result.output

    def test_ps_pages_a_long_finite_roll(
        self,
        runner: CliRunner,
        mocker: Any,
        tmp_path: Path,
        two_processes: MagicMock,
    ) -> None:
        """Finite process results use the shared pager only when they exceed height."""
        from moneybin.cli.terminal import TerminalPolicy, TerminalSymbols

        db_file = tmp_path / "moneybin.duckdb"
        db_file.touch()
        policy = TerminalPolicy(
            output="text",
            interactive=True,
            page=True,
            color=False,
            style=False,
            animate_progress=False,
            stage_chatter=False,
            ascii=True,
            width=80,
            height=1,
            symbols=TerminalSymbols(
                success="OK", attention="!", failure="X", action=">"
            ),
            minus="-",
        )
        mocker.patch(
            "moneybin.cli.commands.db.get_terminal_policy", return_value=policy
        )
        page = mocker.patch("moneybin.cli.pager.page_text", return_value=True)

        result = runner.invoke(app, ["ps", "--database", str(db_file)])

        assert result.exit_code == 0
        page.assert_called_once()

    def test_ps_no_pager_prints_the_same_finite_roll(
        self,
        runner: CliRunner,
        mocker: Any,
        tmp_path: Path,
        two_processes: MagicMock,
    ) -> None:
        """The explicit flag overrides automatic paging without changing rows."""
        from moneybin.cli.terminal import TerminalPolicy, TerminalSymbols

        db_file = tmp_path / "moneybin.duckdb"
        db_file.touch()
        policy = TerminalPolicy(
            output="text",
            interactive=True,
            page=True,
            color=False,
            style=False,
            animate_progress=False,
            stage_chatter=False,
            ascii=True,
            width=80,
            height=1,
            symbols=TerminalSymbols(
                success="OK", attention="!", failure="X", action=">"
            ),
            minus="-",
        )
        mocker.patch(
            "moneybin.cli.commands.db.get_terminal_policy", return_value=policy
        )
        page = mocker.patch("moneybin.cli.pager.page_text", return_value=True)

        result = runner.invoke(app, ["ps", "--database", str(db_file), "--no-pager"])

        assert result.exit_code == 0
        page.assert_not_called()
        assert "101" in result.output

    def test_kill_lists_the_same_table_before_asking(
        self,
        runner: CliRunner,
        tmp_path: Path,
        two_processes: MagicMock,
        wide_terminal: None,
    ) -> None:
        """`ps` and `kill`'s preamble printed the same table two ways.

        Both now go through one renderer, so a column added to the roll cannot
        reach one command and not the other.
        """
        db_file = tmp_path / "moneybin.duckdb"
        db_file.touch()

        result = runner.invoke(app, ["kill", "--database", str(db_file)], input="n\n")

        assert "┃" in result.output
        assert "101" in result.output


class TestDbKillProcessIdentity:
    """`db kill` binds the confirmed roll to psutil process identities."""

    @pytest.fixture
    def runner(self) -> CliRunner:
        return CliRunner()

    @staticmethod
    def _processes() -> list[dict[str, str | int]]:
        return [
            {"pid": 101, "command": "duckdb", "cmdline": "duckdb moneybin.duckdb"},
            {"pid": 202, "command": "python", "cmdline": "python -m etl"},
        ]

    def test_kill_noop_receipts_name_absent_database_and_no_processes(
        self, runner: CliRunner, mocker: Any, tmp_path: Path
    ) -> None:
        """Successful no-op kills are visible answers, not filtered INFO."""
        missing = tmp_path / "missing.duckdb"
        missing_result = runner.invoke(app, ["kill", "--database", str(missing)])

        present = tmp_path / "present.duckdb"
        present.touch()
        mocker.patch("moneybin.cli.commands.db._find_db_processes", return_value=[])
        no_processes_result = runner.invoke(app, ["kill", "--database", str(present)])

        assert missing_result.exit_code == 0, missing_result.output
        assert "Database file does not exist yet" in missing_result.stdout
        assert no_processes_result.exit_code == 0, no_processes_result.output
        assert (
            "No other processes have present.duckdb open" in no_processes_result.stdout
        )

    def test_kill_rescans_and_uses_original_identity_bound_processes(
        self, runner: CliRunner, mocker: Any, tmp_path: Path
    ) -> None:
        """The post-confirmation signal goes through the original snapshots."""
        db_file = tmp_path / "moneybin.duckdb"
        db_file.touch()
        processes = self._processes()
        mocker.patch(
            "moneybin.cli.commands.db._find_db_processes",
            side_effect=[processes, processes],
        )
        first = {101: MagicMock(), 202: MagicMock()}
        second = {101: MagicMock(), 202: MagicMock()}
        for process in first.values():
            process.is_running.return_value = True
        mocker.patch(
            "moneybin.cli.commands.db._capture_process_snapshot",
            side_effect=[first, second],
        )
        mocker.patch(
            "moneybin.cli.commands.db._same_process_selection", return_value=True
        )

        result = runner.invoke(app, ["kill", "--database", str(db_file), "--yes"])

        assert result.exit_code == 0
        first[101].terminate.assert_called_once_with()
        first[202].terminate.assert_called_once_with()
        second[101].terminate.assert_not_called()
        assert "Sent SIGTERM to 2 processes" in result.output

    def test_kill_refuses_changed_selection_without_signaling(
        self, runner: CliRunner, mocker: Any, tmp_path: Path
    ) -> None:
        """A changed second scan is stale even when `--yes` waived the prompt."""
        db_file = tmp_path / "moneybin.duckdb"
        db_file.touch()
        preview = self._processes()
        changed = [preview[0]]
        mocker.patch(
            "moneybin.cli.commands.db._find_db_processes",
            side_effect=[preview, changed],
        )
        first = {101: MagicMock(), 202: MagicMock()}
        second = {101: MagicMock()}
        mocker.patch(
            "moneybin.cli.commands.db._capture_process_snapshot",
            side_effect=[first, second],
        )
        mocker.patch(
            "moneybin.cli.commands.db._same_process_selection", return_value=False
        )

        result = runner.invoke(app, ["kill", "--database", str(db_file), "--yes"])

        assert result.exit_code == 1
        first[101].terminate.assert_not_called()
        assert "scope changed" in result.output.lower()
        assert "rerun" in result.output.lower()

    def test_kill_reports_sent_exited_and_denied_without_claiming_termination(
        self, runner: CliRunner, mocker: Any, tmp_path: Path
    ) -> None:
        """A SIGTERM dispatch is not presented as a completed process kill."""
        import psutil

        db_file = tmp_path / "moneybin.duckdb"
        db_file.touch()
        processes = self._processes() + [
            {"pid": 303, "command": "other", "cmdline": "other"}
        ]
        mocker.patch(
            "moneybin.cli.commands.db._find_db_processes",
            side_effect=[processes, processes],
        )
        sent = MagicMock()
        exited = MagicMock()
        denied = MagicMock()
        sent.is_running.return_value = True
        exited.is_running.return_value = False
        denied.is_running.return_value = True
        denied.terminate.side_effect = psutil.AccessDenied(pid=303)
        first = {101: sent, 202: exited, 303: denied}
        second = {101: MagicMock(), 202: MagicMock(), 303: MagicMock()}
        mocker.patch(
            "moneybin.cli.commands.db._capture_process_snapshot",
            side_effect=[first, second],
        )
        mocker.patch(
            "moneybin.cli.commands.db._same_process_selection", return_value=True
        )

        result = runner.invoke(app, ["kill", "--database", str(db_file), "--yes"])

        assert result.exit_code == 1
        assert "Sent SIGTERM to 1 process" in result.output
        assert "1 already exited or was reused" in result.output
        assert "1 permission denied" in result.output
        assert "killed" not in result.output.lower()

    def test_kill_guarded_terminate_no_such_process_is_not_counted_as_sent(
        self, runner: CliRunner, mocker: Any, tmp_path: Path
    ) -> None:
        """Psutil can detect PID reuse at the final guarded terminate call."""
        import psutil

        db_file = tmp_path / "moneybin.duckdb"
        db_file.touch()
        processes = [{"pid": 101, "command": "duckdb", "cmdline": "duckdb db"}]
        mocker.patch(
            "moneybin.cli.commands.db._find_db_processes",
            side_effect=[processes, processes],
        )
        captured = MagicMock()
        captured.is_running.return_value = True
        captured.terminate.side_effect = psutil.NoSuchProcess(pid=101)
        mocker.patch(
            "moneybin.cli.commands.db._capture_process_snapshot",
            side_effect=[{101: captured}, {101: MagicMock()}],
        )
        mocker.patch(
            "moneybin.cli.commands.db._same_process_selection", return_value=True
        )

        result = runner.invoke(app, ["kill", "--database", str(db_file), "--yes"])

        assert result.exit_code == 1
        assert "already exited or was reused" in result.output
        assert "Sent SIGTERM" not in result.output


class TestDbPromptPolicy:
    """Maintenance prompts do not consume redirected stdin."""

    def test_init_passphrase_refuses_redirected_input_even_with_yes(
        self, mocker: Any, tmp_path: Path
    ) -> None:
        _make_settings_mock(tmp_path / "moneybin.duckdb", mocker)
        mocker.patch("moneybin.cli.commands.db.sys.stdin.isatty", return_value=False)
        mocker.patch("moneybin.cli.commands.db.sys.stdout.isatty", return_value=False)

        result = CliRunner().invoke(
            app,
            ["init", "--passphrase", "--yes"],
            input="not-a-passphrase-prompt\n",
        )

        assert result.exit_code == 1
        assert "never reads passphrases from redirected input" in result.output
        assert "never reads passphrases from redirected input" in result.stderr
        assert "never reads passphrases from redirected input" not in result.stdout


class TestDbMaintenanceInterrupts:
    """Long maintenance operations state only their known interrupted facts."""

    def test_backup_interrupt_does_not_claim_a_created_backup(
        self, mocker: Any, tmp_path: Path
    ) -> None:
        db_path = tmp_path / "moneybin.duckdb"
        db_path.write_bytes(b"source")
        _make_settings_mock(db_path, mocker)
        mocker.patch(
            "moneybin.cli.commands.db.shutil.copy2", side_effect=KeyboardInterrupt
        )

        result = CliRunner().invoke(
            app, ["backup", "--output", str(tmp_path / "copy.duckdb")]
        )

        assert result.exit_code == 130
        assert "interrupted" in result.output.lower()
        assert "Backup created" not in result.output

    def test_restore_interrupt_after_auto_backup_names_saved_backup(
        self, mocker: Any, tmp_path: Path
    ) -> None:
        db_path = tmp_path / "moneybin.duckdb"
        db_path.write_bytes(b"current")
        source = tmp_path / "backup.duckdb"
        source.write_bytes(b"restore")
        _make_settings_mock(db_path, mocker)
        original_copy = shutil.copy2
        calls = 0

        def interrupt_replacement(source_path: str, target_path: str) -> str:
            nonlocal calls
            calls += 1
            if calls == 2:
                raise KeyboardInterrupt
            return str(original_copy(source_path, target_path))

        mocker.patch(
            "moneybin.cli.commands.db.shutil.copy2", side_effect=interrupt_replacement
        )

        result = CliRunner().invoke(app, ["restore", "--from", str(source), "--yes"])

        assert result.exit_code == 130
        assert "Pre-restore backup saved" in result.output
        assert "replacement may be incomplete" in result.output.lower()
