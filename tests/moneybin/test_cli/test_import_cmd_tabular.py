# ruff: noqa: S101,S106
"""Tests for tabular import CLI commands.

Tests CLI-specific functionality: argument parsing, exit codes, error handling.
Business logic is tested in the service and extractor tests.
"""

from __future__ import annotations

from contextlib import nullcontext
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest
from _pytest.logging import LogCaptureFixture
from typer.testing import CliRunner

from moneybin.cli.commands.import_cmd import app
from moneybin.database import Database
from moneybin.services.import_service import ImportResult

runner = CliRunner()


def _make_import_result(**kwargs: Any) -> ImportResult:
    """Factory for ImportResult with sensible defaults."""
    defaults: dict[str, Any] = {
        "file_path": "test.csv",
        "file_type": "csv",
        "accounts": 1,
        "transactions": 5,
    }
    defaults.update(kwargs)
    return ImportResult(**defaults)


class TestImportFileAccountName:
    """Tests for account_name passthrough."""

    @pytest.fixture
    def mock_get_database(self, mocker: Any) -> MagicMock:
        """Mock get_database to avoid requiring a real encrypted database."""
        return mocker.patch(
            "moneybin.database.get_database",
            return_value=MagicMock(),
        )

    @pytest.fixture
    def mock_import_file(self, mocker: Any) -> MagicMock:
        """Mock the import_file service function."""
        return mocker.patch(
            "moneybin.services.import_service.ImportService.import_file",
            return_value=_make_import_result(),
        )

    def test_account_name_passed_through(
        self,
        mock_get_database: MagicMock,
        mock_import_file: MagicMock,
        tmp_path: Path,
    ) -> None:
        """--account-name is forwarded to the import service."""
        csv_file = tmp_path / "test.csv"
        csv_file.write_text("Date,Amount,Description\n2025-01-01,100,Test\n")

        result = runner.invoke(
            app,
            ["files", str(csv_file), "--account-name", "Chase Checking"],
        )

        assert result.exit_code == 0
        mock_import_file.assert_called_once_with(
            file_path=csv_file,
            refresh=True,
            institution=None,
            force=False,
            interactive=False,
            account_id=None,
            account_name="Chase Checking",
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
            confirm=False,
            actor_kind="human",
        )


class TestImportFileValidation:
    """Tests for early argument validation in import file."""

    def test_file_not_found_exits_with_error(self, tmp_path: Path) -> None:
        """Missing file exits with code 1 before reaching the service."""
        result = runner.invoke(app, ["files", str(tmp_path / "missing.csv")])
        assert result.exit_code == 1

    def test_invalid_sign_convention_exits_with_error(self, tmp_path: Path) -> None:
        """An unrecognised --sign value exits with code 2 (Typer usage error)."""
        csv_file = tmp_path / "test.csv"
        csv_file.write_text("a,b,c\n1,2,3\n")

        result = runner.invoke(app, ["files", str(csv_file), "--sign", "invalid_sign"])

        assert result.exit_code == 2

    def test_invalid_number_format_exits_with_error(self, tmp_path: Path) -> None:
        """An unrecognised --number-format value exits with code 2 (Typer usage error)."""
        csv_file = tmp_path / "test.csv"
        csv_file.write_text("a,b,c\n1,2,3\n")

        result = runner.invoke(
            app, ["files", str(csv_file), "--number-format", "badformat"]
        )

        assert result.exit_code == 2

    def test_invalid_override_format_exits_with_error(self, tmp_path: Path) -> None:
        """An --override value without '=' exits with code 1."""
        csv_file = tmp_path / "test.csv"
        csv_file.write_text("a,b,c\n1,2,3\n")

        result = runner.invoke(app, ["files", str(csv_file), "--override", "badformat"])

        assert result.exit_code == 1

    def test_valid_sign_convention_passes_validation(
        self, mocker: Any, tmp_path: Path
    ) -> None:
        """A recognised --sign value clears validation (service handles the rest)."""
        csv_file = tmp_path / "test.csv"
        csv_file.write_text("a,b,c\n1,2,3\n")

        # Mock service so we don't need a real DB.
        mocker.patch("moneybin.database.get_database", return_value=MagicMock())
        mocker.patch(
            "moneybin.services.import_service.ImportService.import_file",
            return_value=_make_import_result(),
        )

        result = runner.invoke(
            app, ["files", str(csv_file), "--sign", "negative_is_expense"]
        )

        # Exit code 0 means validation passed (service may still fail with
        # non-zero but we mocked it to succeed).
        assert result.exit_code == 0

    def test_valid_number_format_passes_validation(
        self, mocker: Any, tmp_path: Path
    ) -> None:
        """A recognised --number-format value clears validation."""
        csv_file = tmp_path / "test.csv"
        csv_file.write_text("a,b,c\n1,2,3\n")

        mocker.patch("moneybin.database.get_database", return_value=MagicMock())
        mocker.patch(
            "moneybin.services.import_service.ImportService.import_file",
            return_value=_make_import_result(),
        )

        result = runner.invoke(
            app, ["files", str(csv_file), "--number-format", "european"]
        )

        assert result.exit_code == 0


class TestImportSignShapeValidation:
    """Real CLI paths reject sign conventions that cannot read their mapping."""

    def test_import_files_rejects_split_sign_for_single_mapping(
        self,
        db: Database,
        mocker: Any,
        tmp_path: Path,
        caplog: LogCaptureFixture,
    ) -> None:
        """``import files --sign`` fails before creating an import batch."""
        csv_file = tmp_path / "single.csv"
        csv_file.write_text(
            "Date,Description,Amount\n2026-01-05,Coffee,-4.75\n",
            encoding="utf-8",
        )
        mocker.patch(
            "moneybin.database.get_database",
            return_value=nullcontext(db),
        )

        result = runner.invoke(
            app,
            [
                "files",
                str(csv_file),
                "--mapping",
                "transaction_date=Date",
                "--mapping",
                "description=Description",
                "--mapping",
                "amount=Amount",
                "--sign",
                "split_debit_credit",
                "--account-id",
                "acct-single",
                "--confirm",
                "--no-save-format",
                "--no-refresh",
            ],
        )

        assert result.exit_code == 1
        assert "single amount column" in caplog.text
        assert "--sign negative_is_expense" in caplog.text
        log_rows = db.execute("SELECT COUNT(*) FROM raw.import_log").fetchone()
        assert log_rows is not None and log_rows[0] == 0

    def test_import_confirm_rejects_single_sign_for_split_mapping(
        self,
        db: Database,
        mocker: Any,
        tmp_path: Path,
        caplog: LogCaptureFixture,
    ) -> None:
        """``import confirm --sign`` shares the pre-batch shape boundary."""
        csv_file = tmp_path / "split.csv"
        csv_file.write_text(
            "Date,Description,Debit,Credit\n2026-01-05,Coffee,4.75,\n",
            encoding="utf-8",
        )
        mocker.patch(
            "moneybin.database.get_database",
            return_value=nullcontext(db),
        )

        result = runner.invoke(
            app,
            [
                "confirm",
                str(csv_file),
                "--mapping",
                "transaction_date=Date",
                "--mapping",
                "description=Description",
                "--mapping",
                "debit_amount=Debit",
                "--mapping",
                "credit_amount=Credit",
                "--sign",
                "negative_is_expense",
                "--account-id",
                "acct-split",
                "--no-save-format",
            ],
        )

        assert result.exit_code == 1
        assert "debit/credit pair" in caplog.text
        assert "--sign split_debit_credit" in caplog.text
        log_rows = db.execute("SELECT COUNT(*) FROM raw.import_log").fetchone()
        assert log_rows is not None and log_rows[0] == 0


class TestListFormats:
    """Tests for the formats list command."""

    def test_lists_builtin_formats(self) -> None:
        """Formats list exits 0 and includes known built-in format names."""
        result = runner.invoke(app, ["formats", "list"])
        assert result.exit_code == 0
        assert "tiller" in result.output

    def test_output_includes_institution_name(self) -> None:
        """Formats list output includes institution names."""
        result = runner.invoke(app, ["formats", "list"])
        assert result.exit_code == 0
        assert "Tiller" in result.output

    def test_lists_all_builtin_formats(self) -> None:
        """Formats list lists all expected built-in formats."""
        result = runner.invoke(app, ["formats", "list"])
        assert result.exit_code == 0
        for name in ("mint", "tiller", "ynab"):
            assert name in result.output, f"Expected format {name!r} in output"


class TestShowFormat:
    """Tests for the formats show command."""

    def test_shows_known_format(self) -> None:
        """Formats show exits 0 and prints details for a valid format name."""
        result = runner.invoke(app, ["formats", "show", "tiller"])
        assert result.exit_code == 0
        assert "Tiller" in result.output

    def test_shows_field_mapping(self) -> None:
        """Formats show output includes field mapping section."""
        result = runner.invoke(app, ["formats", "show", "tiller"])
        assert result.exit_code == 0
        assert "Field mapping" in result.output

    def test_unknown_format_exits_with_error(self) -> None:
        """Formats show exits 1 for an unrecognised format name."""
        result = runner.invoke(app, ["formats", "show", "nonexistent_format_xyz"])
        assert result.exit_code == 1


class TestDeleteFormat:
    """Tests for the formats delete command."""

    def test_builtin_format_cannot_be_deleted(self) -> None:
        """Attempting to delete a built-in format exits 1."""
        result = runner.invoke(app, ["formats", "delete", "tiller", "--yes"])
        assert result.exit_code == 1

    def test_unknown_format_exits_with_error(self, mocker: Any) -> None:
        """Attempting to delete an unknown user format exits 1."""
        mocker.patch("moneybin.database.get_database", return_value=MagicMock())
        service = mocker.patch(
            "moneybin.services.import_service.ImportService"
        ).return_value
        service.delete_saved_format.return_value = "not_found"
        result = runner.invoke(app, ["formats", "delete", "my_custom_format", "--yes"])
        assert result.exit_code == 1


class TestPreview:
    """Tests for the preview command."""

    def test_file_not_found_exits_with_error(self, tmp_path: Path) -> None:
        """Preview exits 1 when the file does not exist."""
        result = runner.invoke(app, ["preview", str(tmp_path / "missing.csv")])
        assert result.exit_code == 1

    def test_invalid_override_format_exits_with_error(self, tmp_path: Path) -> None:
        """Preview exits 1 when --override is missing '='."""
        csv_file = tmp_path / "test.csv"
        csv_file.write_text("Date,Amount,Description\n2025-01-01,100,Test\n")

        result = runner.invoke(
            app, ["preview", str(csv_file), "--override", "badformat"]
        )

        assert result.exit_code == 1

    def test_preview_succeeds_with_valid_csv(self, tmp_path: Path) -> None:
        """Preview exits 0 and prints column info for a readable CSV."""
        csv_file = tmp_path / "test.csv"
        csv_file.write_text("Date,Amount,Description\n2025-01-01,100,Test\n")

        result = runner.invoke(app, ["preview", str(csv_file)])

        assert result.exit_code == 0
        assert "Columns" in result.output

    def test_preview_warns_on_header_that_looks_like_data(
        self, tmp_path: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Preview surfaces the misdetection warning on a red-flag layout.

        A headerless Excel sheet (row 0 is a real transaction) trips
        header_row_looks_like_data on the auto-detect path the CLI uses. The
        warning routes through logger.warning (stderr) per cli.md, so assert it
        via caplog rather than CliRunner's stdout capture.
        """
        import logging

        import openpyxl

        wb = openpyxl.Workbook()
        ws = wb.active
        assert ws is not None
        ws.append(["2026-01-01", 42.50, "Coffee"])
        ws.append(["2026-01-02", 10.00, "Tea"])
        path = tmp_path / "headerless.xlsx"
        wb.save(path)

        with caplog.at_level(logging.WARNING):
            result = runner.invoke(app, ["preview", str(path)])

        assert result.exit_code == 0
        assert any("parses as a transaction" in r.message for r in caplog.records)
