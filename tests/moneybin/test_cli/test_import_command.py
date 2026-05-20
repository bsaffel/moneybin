"""Tests for the `moneybin import files` CLI command."""

from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest
from typer.testing import CliRunner

from moneybin.cli.commands.import_cmd import app
from moneybin.services.import_service import (
    BatchImportResult,
    ImportResult,
    PerFileResult,
)

runner = CliRunner()


@contextmanager
def _fake_db_ctx() -> Generator[object, None, None]:
    yield object()


@pytest.fixture()
def csv_path(tmp_path: Path) -> Path:
    """Tiny single-row CSV fixture for CLI smoke tests."""
    p = tmp_path / "x.csv"
    p.write_text("Date,Amount,Description\n2025-01-01,1.00,X\n")
    return p


def test_import_file_passes_yes_flag_through(csv_path: Path) -> None:
    """--yes is parsed and forwarded as auto_accept=True to the import service."""
    captured: dict[str, Any] = {}

    def fake_run_import(**kwargs: Any) -> ImportResult:
        captured.update(kwargs)
        return ImportResult(file_path=str(kwargs["file_path"]), file_type="tabular")

    with (
        patch(
            "moneybin.cli.utils.handle_cli_errors",
            _fake_db_ctx,
        ),
        patch(
            "moneybin.database.get_database",
            _fake_db_ctx,
        ),
        patch(
            "moneybin.services.import_service.ImportService.import_file",
            side_effect=fake_run_import,
        ),
    ):
        result = runner.invoke(
            app,
            ["files", str(csv_path), "--account-name", "Test", "--yes"],
        )

    assert result.exit_code == 0, result.output
    assert captured.get("auto_accept") is True


def test_import_file_default_auto_accept_false(csv_path: Path) -> None:
    """Without --yes, auto_accept defaults to False."""
    captured: dict[str, Any] = {}

    def fake_run_import(**kwargs: Any) -> ImportResult:
        captured.update(kwargs)
        return ImportResult(file_path=str(kwargs["file_path"]), file_type="tabular")

    with (
        patch(
            "moneybin.cli.utils.handle_cli_errors",
            _fake_db_ctx,
        ),
        patch(
            "moneybin.database.get_database",
            _fake_db_ctx,
        ),
        patch(
            "moneybin.services.import_service.ImportService.import_file",
            side_effect=fake_run_import,
        ),
    ):
        result = runner.invoke(
            app,
            ["files", str(csv_path), "--account-name", "Test"],
        )

    assert result.exit_code == 0, result.output
    assert captured.get("auto_accept") is False


def test_import_file_surfaces_sign_correction_warning(csv_path: Path) -> None:
    """When ImportResult.sign_correction_suggested=True, a warning goes to stderr."""

    def fake_run_import(**kwargs: Any) -> ImportResult:
        return ImportResult(
            file_path=str(kwargs["file_path"]),
            file_type="tabular",
            sign_correction_suggested=True,
        )

    with (
        patch(
            "moneybin.cli.utils.handle_cli_errors",
            _fake_db_ctx,
        ),
        patch(
            "moneybin.database.get_database",
            _fake_db_ctx,
        ),
        patch(
            "moneybin.services.import_service.ImportService.import_file",
            side_effect=fake_run_import,
        ),
    ):
        result = runner.invoke(
            app,
            ["files", str(csv_path), "--account-name", "Test"],
            catch_exceptions=False,
        )

    assert result.exit_code == 0, result.output
    # typer.echo(..., err=True) goes to stderr; CliRunner mixes it into output
    assert "Sign convention may be inverted" in result.output


def test_import_files_batch_surfaces_sign_correction_warning(csv_path: Path) -> None:
    """Batch path (no per-file knobs) emits the sign-inversion warning too.

    `moneybin import files <path>` without --account-name / --sign / --override
    falls into the batch branch (svc.import_files), not the single-file branch
    (svc.import_file). Both paths must surface the warning.
    """

    def fake_run_batch(*args: Any, **kwargs: Any) -> BatchImportResult:
        return BatchImportResult(
            per_file=[
                PerFileResult(
                    path=str(csv_path),
                    status="imported",
                    source_type="tabular",
                    rows_loaded=1,
                    sign_correction_suggested=True,
                )
            ],
            transforms_applied=False,
            transforms_duration_seconds=None,
        )

    with (
        patch(
            "moneybin.cli.utils.handle_cli_errors",
            _fake_db_ctx,
        ),
        patch(
            "moneybin.database.get_database",
            _fake_db_ctx,
        ),
        patch(
            "moneybin.services.import_service.ImportService.import_files",
            side_effect=fake_run_batch,
        ),
    ):
        result = runner.invoke(
            app,
            ["files", str(csv_path)],
            catch_exceptions=False,
        )

    assert result.exit_code == 0, result.output
    assert "Sign convention may be inverted" in result.output


def test_import_file_no_sign_warning_when_not_suggested(csv_path: Path) -> None:
    """When sign_correction_suggested=False, no warning is printed."""

    def fake_run_import(**kwargs: Any) -> ImportResult:
        return ImportResult(
            file_path=str(kwargs["file_path"]),
            file_type="tabular",
            sign_correction_suggested=False,
        )

    with (
        patch(
            "moneybin.cli.utils.handle_cli_errors",
            _fake_db_ctx,
        ),
        patch(
            "moneybin.database.get_database",
            _fake_db_ctx,
        ),
        patch(
            "moneybin.services.import_service.ImportService.import_file",
            side_effect=fake_run_import,
        ),
    ):
        result = runner.invoke(
            app,
            ["files", str(csv_path), "--account-name", "Test"],
            catch_exceptions=False,
        )

    assert result.exit_code == 0, result.output
    assert "Sign convention" not in result.output
