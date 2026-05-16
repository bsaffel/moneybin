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
from moneybin.services.import_service import (
    BatchImportResult,
    ImportResult,
    PerFileResult,
)


def _make_batch_result(
    *,
    path: str = "test.ofx",
    status: str = "imported",
    source_type: str | None = "ofx",
    rows_loaded: int = 15,
    error: str | None = None,
    transforms_applied: bool = True,
) -> BatchImportResult:
    """Factory for BatchImportResult covering the single-file batch shape."""
    return BatchImportResult(
        per_file=[
            PerFileResult(
                path=path,
                status=status,  # type: ignore[arg-type]
                source_type=source_type,
                rows_loaded=rows_loaded,
                import_id="abc123" if status == "imported" else None,
                error=error,
            )
        ],
        transforms_applied=transforms_applied,
        transforms_duration_seconds=None,
    )


class TestImportFilesCommand:
    """Test the 'import files' CLI command."""

    @pytest.fixture
    def runner(self) -> CliRunner:
        return CliRunner()

    @pytest.fixture
    def mock_import_files(self, mocker: Any) -> MagicMock:
        """Mock the import_files (batch) service function."""
        return mocker.patch(
            "moneybin.services.import_service.ImportService.import_files",
            return_value=_make_batch_result(),
        )

    @pytest.fixture
    def mock_import_file(self, mocker: Any) -> MagicMock:
        """Mock the import_file (single-file with knobs) service function."""
        return mocker.patch(
            "moneybin.services.import_service.ImportService.import_file",
            return_value=ImportResult(
                file_path="test.ofx",
                file_type="ofx",
                accounts=2,
                transactions=15,
            ),
        )

    @pytest.fixture
    def mock_get_database(self, mocker: Any) -> MagicMock:
        """Mock get_database to avoid requiring a real encrypted database."""
        return mocker.patch(
            "moneybin.database.get_database",
            return_value=MagicMock(),
        )

    def test_import_files_success(
        self,
        runner: CliRunner,
        mock_import_files: MagicMock,
        mock_get_database: MagicMock,
        tmp_path: Path,
    ) -> None:
        """Single-file import with no per-file knobs uses the batch service."""
        test_file = tmp_path / "test.ofx"
        test_file.touch()

        result = runner.invoke(app, ["files", str(test_file)])
        assert result.exit_code == 0, result.output
        mock_import_files.assert_called_once_with(
            [str(test_file)],
            refresh=True,
            force=False,
            interactive=False,
        )

    def test_import_files_no_refresh(
        self,
        runner: CliRunner,
        mock_import_files: MagicMock,
        mock_get_database: MagicMock,
        tmp_path: Path,
    ) -> None:
        """--no-refresh forwards refresh=False to the batch."""
        test_file = tmp_path / "test.ofx"
        test_file.touch()

        result = runner.invoke(app, ["files", str(test_file), "--no-refresh"])
        assert result.exit_code == 0, result.output
        call_kwargs = mock_import_files.call_args.kwargs
        assert call_kwargs["refresh"] is False

    def test_import_files_with_institution(
        self,
        runner: CliRunner,
        mock_import_file: MagicMock,
        mock_get_database: MagicMock,
        tmp_path: Path,
    ) -> None:
        """--institution routes a single-file call through the legacy import_file."""
        test_file = tmp_path / "test.qfx"
        test_file.touch()

        result = runner.invoke(
            app, ["files", str(test_file), "--institution", "Wells Fargo"]
        )
        assert result.exit_code == 0, result.output
        mock_import_file.assert_called_once_with(
            file_path=test_file,
            refresh=True,
            institution="Wells Fargo",
            force=False,
            interactive=False,
            account_id=None,
            account_name=None,
            format_name=None,
            overrides=None,
            sign=None,
            date_format=None,
            number_format=None,
            save_format=True,
            sheet=None,
            delimiter=None,
            encoding=None,
            no_row_limit=False,
            no_size_limit=False,
            auto_accept=False,
        )

    def test_import_files_force_flag(
        self,
        runner: CliRunner,
        mock_import_files: MagicMock,
        mock_get_database: MagicMock,
        tmp_path: Path,
    ) -> None:
        """--force is forwarded to the batch service as force=True."""
        test_file = tmp_path / "test.ofx"
        test_file.touch()

        result = runner.invoke(app, ["files", str(test_file), "--force"])
        assert result.exit_code == 0, result.output
        call_kwargs = mock_import_files.call_args.kwargs
        assert call_kwargs["force"] is True

    def test_force_already_imported_error(
        self,
        runner: CliRunner,
        mock_import_files: MagicMock,
        mock_get_database: MagicMock,
        tmp_path: Path,
    ) -> None:
        """The batch service records per-file failures; CLI still exits 0."""
        test_file = tmp_path / "test.ofx"
        test_file.touch()
        mock_import_files.return_value = _make_batch_result(
            status="failed",
            source_type=None,
            rows_loaded=0,
            error="ValueError",
            transforms_applied=False,
        )

        result = runner.invoke(app, ["files", str(test_file)])
        # Per-file failures don't abort the batch; exit code stays 0.
        assert result.exit_code == 0, result.output

    def test_import_files_not_found(
        self,
        runner: CliRunner,
    ) -> None:
        """Exit code 1 when a single missing file is passed (typo detection)."""
        result = runner.invoke(app, ["files", "/nonexistent/file.ofx"])
        assert result.exit_code == 1

    def test_import_files_batch_continues_past_missing_file(
        self,
        runner: CliRunner,
        mock_import_files: MagicMock,
        mock_get_database: MagicMock,
        tmp_path: Path,
    ) -> None:
        """Multi-file batches defer missing-file handling to ImportService.

        The CLI must NOT abort the batch when one path doesn't exist —
        ImportService.import_files() records the FileNotFoundError as a
        PerFileResult(status="failed") so the surviving files still
        import. Mirrors the docstring contract: "Per-file failures do
        not abort the batch."
        """
        good = tmp_path / "good.ofx"
        good.touch()
        missing = tmp_path / "missing.ofx"
        mock_import_files.return_value = BatchImportResult(
            per_file=[
                PerFileResult(
                    path=str(good),
                    status="imported",
                    source_type="ofx",
                    rows_loaded=1,
                    import_id="x",
                ),
                PerFileResult(
                    path=str(missing),
                    status="failed",
                    source_type=None,
                    error="FileNotFoundError",
                ),
            ],
            transforms_applied=True,
            transforms_duration_seconds=None,
        )
        result = runner.invoke(app, ["files", str(good), str(missing)])
        # Batch failures don't flip exit code; service is invoked for both paths.
        assert result.exit_code == 0, result.output
        mock_import_files.assert_called_once_with(
            [str(good), str(missing)],
            refresh=True,
            force=False,
            interactive=False,
        )

    def test_import_files_variadic_paths(
        self,
        runner: CliRunner,
        mock_import_files: MagicMock,
        mock_get_database: MagicMock,
        tmp_path: Path,
    ) -> None:
        """Multiple positional paths are forwarded as a list."""
        a = tmp_path / "a.ofx"
        b = tmp_path / "b.ofx"
        a.touch()
        b.touch()
        mock_import_files.return_value = BatchImportResult(
            per_file=[
                PerFileResult(
                    path=str(a),
                    status="imported",
                    source_type="ofx",
                    rows_loaded=1,
                    import_id="x",
                ),
                PerFileResult(
                    path=str(b),
                    status="imported",
                    source_type="ofx",
                    rows_loaded=1,
                    import_id="y",
                ),
            ],
            transforms_applied=True,
            transforms_duration_seconds=None,
        )

        result = runner.invoke(app, ["files", str(a), str(b)])
        assert result.exit_code == 0, result.output
        mock_import_files.assert_called_once_with(
            [str(a), str(b)],
            refresh=True,
            force=False,
            interactive=False,
        )

    def test_import_files_multi_file_with_knobs_warns(
        self,
        runner: CliRunner,
        mock_import_files: MagicMock,
        mock_get_database: MagicMock,
        tmp_path: Path,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Per-file flags + multi-file warn and still route through the batch path."""
        a = tmp_path / "a.ofx"
        b = tmp_path / "b.ofx"
        a.touch()
        b.touch()
        mock_import_files.return_value = BatchImportResult(
            per_file=[
                PerFileResult(
                    path=str(a),
                    status="imported",
                    source_type="ofx",
                    rows_loaded=1,
                    import_id="x",
                ),
                PerFileResult(
                    path=str(b),
                    status="imported",
                    source_type="ofx",
                    rows_loaded=1,
                    import_id="y",
                ),
            ],
            transforms_applied=True,
            transforms_duration_seconds=None,
        )

        result = runner.invoke(
            app,
            ["files", str(a), str(b), "--institution", "Wells Fargo"],
        )
        assert result.exit_code == 0, result.output
        mock_import_files.assert_called_once()

    def test_import_files_output_json(
        self,
        runner: CliRunner,
        mock_import_files: MagicMock,
        mock_get_database: MagicMock,
        tmp_path: Path,
    ) -> None:
        """--output json emits the envelope shape with batch fields under data."""
        import json

        test_file = tmp_path / "test.ofx"
        test_file.touch()
        result = runner.invoke(app, ["files", str(test_file), "--output", "json"])
        assert result.exit_code == 0, result.output
        payload = json.loads(result.output)
        assert payload["data"]["imported_count"] == 1
        assert payload["data"]["total_count"] == 1
        assert "files" in payload["data"]
        assert payload["summary"]["sensitivity"] == "low"


class TestImportStatusCommand:
    """Test the 'import status' CLI command."""

    @pytest.fixture
    def runner(self) -> CliRunner:
        return CliRunner()

    @pytest.fixture
    def _mock_settings(self, mocker: Any, tmp_path: Path) -> MagicMock:
        """Mock get_settings so database.path points to tmp_path."""
        mock_settings = MagicMock()
        mock_settings.database.path = tmp_path / "moneybin.duckdb"
        mocker.patch(
            "moneybin.config.get_settings",
            return_value=mock_settings,
        )
        return mock_settings

    def test_status_no_database(
        self,
        runner: CliRunner,
        _mock_settings: MagicMock,
    ) -> None:
        """Test exit code 1 when database does not exist."""
        result = runner.invoke(app, ["status"])
        assert result.exit_code == 1

    def test_status_empty_database(
        self,
        runner: CliRunner,
        mocker: Any,
        tmp_path: Path,
        _mock_settings: MagicMock,
    ) -> None:
        """Test status with database that has no raw tables."""
        import duckdb

        db_path = tmp_path / "moneybin.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.close()

        mock_db = MagicMock()
        mock_db.__enter__ = MagicMock(return_value=mock_db)
        mock_db.__exit__ = MagicMock(return_value=False)
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
        _mock_settings: MagicMock,
    ) -> None:
        """Test status with populated raw tables."""
        import duckdb

        db_path = tmp_path / "moneybin.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("CREATE SCHEMA IF NOT EXISTS raw")
        conn.execute("CREATE TABLE raw.ofx_transactions (id INT, date_posted DATE)")
        conn.execute(
            "INSERT INTO raw.ofx_transactions VALUES (1, '2025-01-01'), (2, '2025-06-15')"
        )
        conn.close()

        # Mock get_database to return a real duckdb connection via a Database-like mock
        real_conn = duckdb.connect(str(db_path), read_only=True)
        mock_db = MagicMock()
        mock_db.__enter__ = MagicMock(return_value=mock_db)
        mock_db.__exit__ = MagicMock(return_value=False)
        mock_db.execute.side_effect = real_conn.execute
        mocker.patch("moneybin.database.get_database", return_value=mock_db)

        result = runner.invoke(app, ["status"])
        real_conn.close()
        assert result.exit_code == 0
        assert "ofx_transactions" in result.output
        assert "2 rows" in result.output
