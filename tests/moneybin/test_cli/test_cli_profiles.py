"""Tests for CLI profile handling.

This module tests the CLI's profile flag parsing, validation,
and integration with the configuration system.

These tests focus on profile mechanics (parsing, validation, propagation)
without requiring specific data source configurations like Plaid.
"""

from __future__ import annotations

import logging
import os
from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path

import pytest
from pytest_mock import MockerFixture
from typer.testing import CliRunner

from moneybin.cli.main import app
from moneybin.config import get_base_dir, get_current_profile
from moneybin.utils.user_config import (
    generate_profile_config,
    normalize_profile_name,
)
from tests.moneybin.conftest import temp_profile

runner = CliRunner()


@pytest.fixture(autouse=True)
def _isolated_moneybin_home(  # pyright: ignore[reportUnusedFunction]
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Give each test its own MONEYBIN_HOME to prevent xdist worker races.

    Tests in this file create profile directories with fixed names
    ("alice", "bob"). Without isolation, parallel xdist workers race on
    the shared ``~/.moneybin/profiles/`` tree — one worker's cleanup
    deletes another's profile mid-test, surfacing as a CLI
    "Profile 'alice' does not exist" error.
    """
    monkeypatch.setenv("MONEYBIN_HOME", str(tmp_path))


@pytest.fixture(autouse=True)
def _restore_logging_handlers() -> Generator[None, None, None]:  # pyright: ignore[reportUnusedFunction]
    """Undo the file handlers profile resolution installs.

    Resolving a profile calls ``setup_observability``, which attaches root
    and ``sqlmesh`` FileHandlers under *this* test's temporary
    MONEYBIN_HOME. pytest deletes that tree afterwards, so a handler left
    behind kills the next test in the same xdist worker with
    ``FileNotFoundError`` the moment anything logs — far from the test that
    actually leaked it.
    """
    root = logging.getLogger()
    sqlmesh_logger = logging.getLogger("sqlmesh")
    root_handlers = list(root.handlers)
    root_level = root.level
    sqlmesh_handlers = list(sqlmesh_logger.handlers)
    sqlmesh_propagates = sqlmesh_logger.propagate

    yield

    for handler in (*root.handlers, *sqlmesh_logger.handlers):
        if handler not in root_handlers and handler not in sqlmesh_handlers:
            handler.close()
    root.handlers, root.level = root_handlers, root_level
    sqlmesh_logger.handlers = sqlmesh_handlers
    sqlmesh_logger.propagate = sqlmesh_propagates


@contextmanager
def _create_profile(name: str) -> Generator[str, None, None]:
    """Create a profile directory and clean up after."""
    normalized = normalize_profile_name(name)
    profile_dir = get_base_dir() / "profiles" / normalized
    profile_dir.mkdir(parents=True, exist_ok=True)
    (profile_dir / "config.yaml").touch()
    with temp_profile(name):
        yield normalized


class TestCLIProfileHandling:
    """Test suite for CLI profile handling."""

    def test_default_profile_from_config(self, mocker: MockerFixture) -> None:
        """Test that CLI uses saved default profile when no flag is provided."""
        mocker.patch.dict(os.environ, {})
        os.environ.pop("MONEYBIN_PROFILE", None)

        mocker.patch("moneybin.cli.utils.ensure_default_profile", return_value="test")

        with _create_profile("test"):
            mocker.patch("moneybin.cli.commands.logs.get_settings")
            result = runner.invoke(app, ["logs", "--print-path"])

            assert result.exit_code == 0
            assert get_current_profile() == "test"

    def test_profile_source_names_the_source_that_resolved(
        self, mocker: MockerFixture
    ) -> None:
        """Req 19: the banner names one source, never a list of candidates.

        `config.yaml or first-run wizard` told the reader that one of two
        things happened without saying which — the exact ambiguity the
        requirement exists to forbid.

        Captured with a handler bound to the source logger rather than
        ``caplog``: ``resolve_profile`` calls ``setup_observability`` before it
        reports, which rebuilds root handlers and drops caplog's.
        """
        mocker.patch.dict(os.environ, {})
        os.environ.pop("MONEYBIN_PROFILE", None)

        from moneybin.cli.utils import resolve_profile
        from moneybin.utils.user_config import set_default_profile

        records: list[logging.LogRecord] = []

        class _Capture(logging.Handler):
            def emit(self, record: logging.LogRecord) -> None:
                records.append(record)

        source_logger = logging.getLogger("moneybin.cli.utils.profile_source")
        handler = _Capture(level=logging.INFO)
        source_logger.addHandler(handler)
        source_logger.setLevel(logging.INFO)
        try:
            with _create_profile("test"):
                # Make config.yaml genuinely name the active profile, so the
                # real resolver takes the config path rather than the wizard.
                set_default_profile("test")
                resolve_profile()
        finally:
            source_logger.removeHandler(handler)

        reported = "\n".join(record.getMessage() for record in records)

        assert reported, "the profile source was never reported"
        assert " or " not in reported, (
            f"banner names more than one candidate source: {reported!r}"
        )
        assert reported == "Profile resolved from config.yaml", reported

    def test_explicit_profile_bob(self, mocker: MockerFixture) -> None:
        """Test setting a different user profile."""
        with _create_profile("bob"):
            mocker.patch("moneybin.cli.commands.logs.get_settings")
            result = runner.invoke(app, ["--profile=bob", "logs", "--print-path"])

            assert result.exit_code == 0
            assert get_current_profile() == "bob"

    def test_short_profile_flag(self, mocker: MockerFixture) -> None:
        """Test using short -p flag for profile."""
        with _create_profile("alice"):
            mocker.patch("moneybin.cli.commands.logs.get_settings")
            result = runner.invoke(app, ["-p", "alice", "logs", "--print-path"])

            assert result.exit_code == 0
            assert get_current_profile() == "alice"

    def test_profile_name_with_slash_gets_normalized(
        self, mocker: MockerFixture
    ) -> None:
        """Test that profile name with slash gets normalized (slash removed)."""
        with _create_profile("invalid/profile"):
            mocker.patch("moneybin.cli.commands.logs.get_settings")
            result = runner.invoke(
                app,
                ["--profile=invalid/profile", "logs", "--print-path"],
            )

            assert result.exit_code == 0
            assert get_current_profile() == "invalidprofile"

    def test_profile_name_with_space_gets_normalized(
        self, mocker: MockerFixture
    ) -> None:
        """Test that profile name with space gets normalized (space -> hyphen)."""
        with _create_profile("bad profile"):
            mocker.patch("moneybin.cli.commands.logs.get_settings")
            result = runner.invoke(
                app, ["--profile=bad profile", "logs", "--print-path"]
            )

            assert result.exit_code == 0
            assert get_current_profile() == "bad-profile"

    def test_profile_environment_variable(self, mocker: MockerFixture) -> None:
        """Test setting profile via MONEYBIN_PROFILE environment variable."""
        with _create_profile("alice"):
            mocker.patch.dict(os.environ, {"MONEYBIN_PROFILE": "alice"})
            mocker.patch("moneybin.cli.commands.logs.get_settings")

            result = runner.invoke(app, ["logs", "--print-path"])

            assert result.exit_code == 0
            assert get_current_profile() == "alice"

    def test_cli_flag_overrides_environment_variable(
        self, mocker: MockerFixture
    ) -> None:
        """Test that CLI flag takes precedence over environment variable."""
        with _create_profile("alice"), _create_profile("bob"):
            mocker.patch.dict(os.environ, {"MONEYBIN_PROFILE": "alice"})
            mocker.patch("moneybin.cli.commands.logs.get_settings")

            result = runner.invoke(app, ["--profile=bob", "logs", "--print-path"])

            assert result.exit_code == 0
            assert get_current_profile() == "bob"

    def test_profile_propagates_correctly(self, mocker: MockerFixture) -> None:
        """Test that profile is set correctly in the config system."""
        with _create_profile("alice"):
            mocker.patch("moneybin.cli.commands.logs.get_settings")
            result = runner.invoke(app, ["--profile=alice", "logs", "--print-path"])

            assert result.exit_code == 0
            assert get_current_profile() == "alice"

    def test_help_shows_profile_option(self) -> None:
        """Test that help text shows the profile option."""
        result = runner.invoke(app, ["--help"])

        assert result.exit_code == 0
        assert "--profile" in result.stdout or "-p" in result.stdout

    def test_valid_profile_with_dash(self, mocker: MockerFixture) -> None:
        """Test that profile names with dashes are valid."""
        with _create_profile("alice-personal"):
            mocker.patch("moneybin.cli.commands.logs.get_settings")
            result = runner.invoke(
                app,
                ["--profile=alice-personal", "logs", "--print-path"],
            )

            assert result.exit_code == 0
            assert get_current_profile() == "alice-personal"

    def test_profile_with_underscore_gets_normalized(
        self, mocker: MockerFixture
    ) -> None:
        """Test that profile names with underscores get normalized to hyphens."""
        with _create_profile("alice_work"):
            mocker.patch("moneybin.cli.commands.logs.get_settings")
            result = runner.invoke(
                app, ["--profile=alice_work", "logs", "--print-path"]
            )

            assert result.exit_code == 0
            assert get_current_profile() == "alice-work"


class TestProfileCommandsResolveProfileButSkipWizard:
    """Profile subcommands honor --profile/env but never trigger first-run wizard."""

    def test_profile_list_skips_ensure_default(self, mocker: MockerFixture) -> None:
        """Profile commands must never trigger the first-run wizard."""
        mocker.patch.dict(os.environ, {})
        os.environ.pop("MONEYBIN_PROFILE", None)

        mock_ensure = mocker.patch("moneybin.cli.utils.ensure_default_profile")

        result = runner.invoke(app, ["profile", "list"])

        assert result.exit_code == 0
        mock_ensure.assert_not_called()

    def test_profile_list_honors_profile_flag(self) -> None:
        """--profile flag should resolve current profile for profile commands too."""
        with _create_profile("alice"):
            result = runner.invoke(app, ["--profile=alice", "profile", "list"])

            assert result.exit_code == 0
            assert get_current_profile() == "alice"

    def test_profile_list_honors_env_var(self, mocker: MockerFixture) -> None:
        """MONEYBIN_PROFILE env var should resolve for profile commands too."""
        mocker.patch.dict(os.environ, {"MONEYBIN_PROFILE": "alice"})

        with _create_profile("alice"):
            result = runner.invoke(app, ["profile", "list"])

            assert result.exit_code == 0
            assert get_current_profile() == "alice"


class TestExplicitProfileConfiguresLogging:
    """``-p X`` must configure logging exactly as switching to X would.

    Profile-aware logging setup — the dedicated sqlmesh log file,
    ``propagate=False`` on the ``sqlmesh`` logger, and an armed console
    denylist — runs only from ``resolve_profile``, the lazy resolver
    registered in ``main_callback``. That callback also sets the profile
    *eagerly* when ``--profile`` or ``MONEYBIN_PROFILE`` is explicit, so
    while the resolver fired only on an unset profile the eager set
    disarmed it: an explicit profile left the process on the callback's
    console-only setup, writing no log files at all and putting every
    ``sqlmesh.*`` INFO record — one per executed statement, including full
    ``CREATE OR REPLACE VIEW`` bodies — on the user's terminal.
    """

    @pytest.fixture()
    def profile(self) -> Generator[str, None, None]:
        """A real profile plus the process state a fresh CLI start has.

        Clears the ``sqlmesh`` logger so each assertion measures what *this*
        invocation configured. Restoring it afterwards belongs to the
        module-level ``_restore_logging_handlers`` fixture.
        """
        name = normalize_profile_name("logging-probe")
        profile_dir = get_base_dir() / "profiles" / name
        profile_dir.mkdir(parents=True, exist_ok=True)
        generate_profile_config(profile_dir, name)

        sqlmesh_logger = logging.getLogger("sqlmesh")
        for handler in sqlmesh_logger.handlers[:]:
            sqlmesh_logger.removeHandler(handler)
        sqlmesh_logger.propagate = True

        with temp_profile(name):
            yield name

    @staticmethod
    def _run(profile: str) -> None:
        """Drive a real command that resolves settings under ``-p``."""
        result = runner.invoke(app, ["-p", profile, "logs", "--print-path"])
        assert result.exit_code == 0, result.output

    @staticmethod
    def _console_handler() -> logging.Handler:
        console = [
            h
            for h in logging.getLogger().handlers
            if isinstance(h, logging.StreamHandler)
            and not isinstance(h, logging.FileHandler)
        ]
        assert console, "Expected a console StreamHandler"
        return console[0]

    @staticmethod
    def _record(name: str, level: int) -> logging.LogRecord:
        return logging.LogRecord(
            name=name,
            level=level,
            pathname="",
            lineno=0,
            msg="test",
            args=(),
            exc_info=None,
        )

    @pytest.mark.unit
    def test_sqlmesh_output_lands_in_the_sqlmesh_log_file(self, profile: str) -> None:
        """SQLMesh gets its own file under the named profile's log directory.

        Suppressing these records from the console is only defensible
        because a file keeps the copy. Under an explicit ``-p`` no file
        handler was installed at all, so the copy did not exist.
        """
        self._run(profile)

        sqlmesh_files = [
            Path(h.baseFilename)
            for h in logging.getLogger("sqlmesh").handlers
            if isinstance(h, logging.FileHandler)
        ]

        assert sqlmesh_files, "Expected a sqlmesh FileHandler"
        assert all(
            f.parent == get_base_dir() / "profiles" / profile / "logs"
            for f in sqlmesh_files
        )
        assert all(f.name.startswith("sqlmesh_") for f in sqlmesh_files)

    @pytest.mark.unit
    def test_sqlmesh_info_stays_off_the_console(self, profile: str) -> None:
        """The reported defect: ~1000 SQL lines before four lines of output."""
        self._run(profile)

        assert not self._console_handler().filter(
            self._record("sqlmesh.core.engine_adapter.base", logging.INFO)
        )

    @pytest.mark.unit
    def test_sqlmesh_warning_still_reaches_the_console(self, profile: str) -> None:
        """Quieting noise must never quiet a problem.

        ``sync pull`` is exactly where staging rejects and categorization
        anomalies surface, so the fix must not buy silence at their cost.
        """
        self._run(profile)

        assert self._console_handler().filter(
            self._record("sqlmesh.core.engine_adapter.base", logging.WARNING)
        )
