# ruff: noqa: S101,S106
"""Tests for import CLI commands.

Tests CLI-specific functionality: argument parsing, exit codes, error handling.
Business logic is tested in the import_service tests.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest
from typer.testing import CliRunner

from moneybin.cli.commands.import_cmd import app
from moneybin.services.import_service import ImportResult


class TestImportFileCommand:
    """Test the 'import file' CLI command."""

    @pytest.fixture
    def runner(self) -> CliRunner:
        return CliRunner()

    @pytest.fixture
    def mock_import_file(self, mocker: Any) -> MagicMock:
        """Mock the import_file service function."""
        mock = mocker.patch(
            "moneybin.services.import_service.import_file",
            return_value=ImportResult(
                file_path="test.ofx",
                file_type="ofx",
                accounts=2,
                transactions=15,
            ),
        )
        return mock

    @pytest.fixture
    def mock_get_database(self, mocker: Any) -> MagicMock:
        """Mock get_database to avoid requiring a real encrypted database."""
        return mocker.patch(
            "moneybin.database.get_database",
            return_value=MagicMock(),
        )

    def test_import_file_success(
        self,
        runner: CliRunner,
        mock_import_file: MagicMock,
        mock_get_database: MagicMock,
        tmp_path: Path,
    ) -> None:
        """Test successful file import."""
        test_file = tmp_path / "test.ofx"
        test_file.touch()

        result = runner.invoke(app, ["file", str(test_file)])
        assert result.exit_code == 0
        mock_import_file.assert_called_once_with(
            db=mock_get_database.return_value,
            file_path=test_file,
            run_transforms=True,
            institution=None,
            account_id=None,
        )

    def test_import_file_skip_transform(
        self,
        runner: CliRunner,
        mock_import_file: MagicMock,
        mock_get_database: MagicMock,
        tmp_path: Path,
    ) -> None:
        """Test --skip-transform flag passes run_transforms=False."""
        test_file = tmp_path / "test.ofx"
        test_file.touch()

        result = runner.invoke(app, ["file", str(test_file), "--skip-transform"])
        assert result.exit_code == 0
        mock_import_file.assert_called_once_with(
            db=mock_get_database.return_value,
            file_path=test_file,
            run_transforms=False,
            institution=None,
            account_id=None,
        )

    def test_import_file_with_institution(
        self,
        runner: CliRunner,
        mock_import_file: MagicMock,
        mock_get_database: MagicMock,
        tmp_path: Path,
    ) -> None:
        """Test --institution flag is passed through."""
        test_file = tmp_path / "test.qfx"
        test_file.touch()

        result = runner.invoke(
            app, ["file", str(test_file), "--institution", "Wells Fargo"]
        )
        assert result.exit_code == 0
        mock_import_file.assert_called_once_with(
            db=mock_get_database.return_value,
            file_path=test_file,
            run_transforms=True,
            institution="Wells Fargo",
            account_id=None,
        )

    def test_import_file_not_found(
        self,
        runner: CliRunner,
    ) -> None:
        """Test exit code 1 when file does not exist."""
        result = runner.invoke(app, ["file", "/nonexistent/file.ofx"])
        assert result.exit_code == 1

    def test_import_file_unsupported_type(
        self,
        runner: CliRunner,
        mock_import_file: MagicMock,
        mock_get_database: MagicMock,
        tmp_path: Path,
    ) -> None:
        """Test exit code 1 for unsupported file type."""
        test_file = tmp_path / "test.xlsx"
        test_file.touch()
        mock_import_file.side_effect = ValueError("Unsupported file type: .xlsx")

        result = runner.invoke(app, ["file", str(test_file)])
        assert result.exit_code == 1


class TestImportStatusCommand:
    """Test the 'import status' CLI command."""

    @pytest.fixture
    def runner(self) -> CliRunner:
        return CliRunner()

    def test_status_no_database(
        self,
        runner: CliRunner,
        mocker: Any,
        tmp_path: Path,
    ) -> None:
        """Test exit code 1 when database does not exist."""
        mocker.patch(
            "moneybin.config.get_database_path",
            return_value=tmp_path / "nonexistent.duckdb",
        )
        result = runner.invoke(app, ["status"])
        assert result.exit_code == 1

    def test_status_empty_database(
        self,
        runner: CliRunner,
        mocker: Any,
        tmp_path: Path,
    ) -> None:
        """Test status with database that has no raw tables."""
        import duckdb

        db_path = tmp_path / "test.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.close()

        mocker.patch(
            "moneybin.config.get_database_path",
            return_value=db_path,
        )

        mock_db = MagicMock()
        mock_db.execute.return_value.fetchall.return_value = []
        mocker.patch("moneybin.database.get_database", return_value=mock_db)

        result = runner.invoke(app, ["status"])
        assert result.exit_code == 0
        assert "No imported data found" in result.output

    def test_status_with_data(
        self,
        runner: CliRunner,
        mocker: Any,
        tmp_path: Path,
    ) -> None:
        """Test status with populated raw tables."""
        import duckdb

        db_path = tmp_path / "test.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("CREATE SCHEMA IF NOT EXISTS raw")
        conn.execute("CREATE TABLE raw.ofx_transactions (id INT, date_posted DATE)")
        conn.execute(
            "INSERT INTO raw.ofx_transactions VALUES (1, '2025-01-01'), (2, '2025-06-15')"
        )
        conn.close()

        mocker.patch(
            "moneybin.config.get_database_path",
            return_value=db_path,
        )

        # Mock get_database to return a real duckdb connection via a Database-like mock
        real_conn = duckdb.connect(str(db_path), read_only=True)
        mock_db = MagicMock()
        mock_db.execute.side_effect = real_conn.execute
        mocker.patch("moneybin.database.get_database", return_value=mock_db)

        result = runner.invoke(app, ["status"])
        real_conn.close()
        assert result.exit_code == 0
        assert "ofx_transactions" in result.output
        assert "2 rows" in result.output
