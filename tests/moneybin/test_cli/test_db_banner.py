"""Tests for the operator-bypass banner on direct-DB CLI commands.

Verifies that db shell, db ui, and db query surface the privacy-middleware
bypass warning in --help output and emit it to stderr on invocation.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest
from typer.testing import CliRunner

from moneybin.cli.commands.db import app

# Stable substring from the banner — loosely matched so text tweaks don't
# require test updates, but the core meaning is pinned.
_BANNER_PHRASE = "no privacy middleware"
_BANNER_REDIRECT = "moneybin sql query"

_runner = CliRunner()


class TestDbQueryHelp:
    """--help for db query shows the operator-bypass banner."""

    def test_db_query_help_shows_no_middleware_phrase(self) -> None:
        result = _runner.invoke(app, ["query", "--help"])
        assert result.exit_code == 0
        assert _BANNER_PHRASE in result.output.lower()

    def test_db_query_help_shows_redirect(self) -> None:
        result = _runner.invoke(app, ["query", "--help"])
        assert result.exit_code == 0
        assert _BANNER_REDIRECT in result.output


class TestDbShellHelp:
    """--help for db shell shows the operator-bypass banner."""

    def test_db_shell_help_shows_no_middleware_phrase(self) -> None:
        result = _runner.invoke(app, ["shell", "--help"])
        assert result.exit_code == 0
        assert _BANNER_PHRASE in result.output.lower()

    def test_db_shell_help_shows_redirect(self) -> None:
        result = _runner.invoke(app, ["shell", "--help"])
        assert result.exit_code == 0
        assert _BANNER_REDIRECT in result.output


class TestDbUiHelp:
    """--help for db ui shows the operator-bypass banner."""

    def test_db_ui_help_shows_no_middleware_phrase(self) -> None:
        result = _runner.invoke(app, ["ui", "--help"])
        assert result.exit_code == 0
        assert _BANNER_PHRASE in result.output.lower()

    def test_db_ui_help_shows_redirect(self) -> None:
        result = _runner.invoke(app, ["ui", "--help"])
        assert result.exit_code == 0
        assert _BANNER_REDIRECT in result.output


class TestDbQueryInvocationBanner:
    """db query emits the banner to stderr on invocation (even on error)."""

    @pytest.fixture
    def mock_duckdb_cli(self, mocker: Any) -> MagicMock:
        return mocker.patch(
            "moneybin.cli.commands.db.shutil.which",
            return_value="/usr/local/bin/duckdb",
        )

    @pytest.fixture
    def mock_subprocess_run(self, mocker: Any) -> MagicMock:
        return mocker.patch("moneybin.cli.commands.db.subprocess.run")

    @pytest.fixture
    def mock_create_init_script(self, mocker: Any, tmp_path: Path) -> MagicMock:
        script = tmp_path / "init.sql"
        script.touch()
        return mocker.patch(
            "moneybin.cli.commands.db._create_init_script",
            return_value=script,
        )

    def test_db_query_invocation_emits_banner(
        self,
        mocker: Any,
        mock_duckdb_cli: MagicMock,
        mock_subprocess_run: MagicMock,
        mock_create_init_script: MagicMock,
        tmp_path: Path,
    ) -> None:
        """Banner is present in output (stderr merged) on a successful db query invocation."""
        test_db = tmp_path / "test.duckdb"
        test_db.touch()
        mock_settings = MagicMock()
        mock_settings.database.path = test_db
        mocker.patch("moneybin.config.get_settings", return_value=mock_settings)

        result = _runner.invoke(app, ["query", "SELECT 1"])
        # CliRunner merges stderr into result.output by default.
        assert _BANNER_PHRASE in result.output.lower()
        assert _BANNER_REDIRECT in result.output
