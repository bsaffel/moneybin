"""Tests for the `moneybin import file` CLI command."""

from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest
from typer.testing import CliRunner

from moneybin.cli.commands.import_cmd import app
from moneybin.services.import_service import ImportResult

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
            "moneybin.cli.utils.handle_database_errors",
            _fake_db_ctx,
        ),
        patch(
            "moneybin.services.import_service.import_file",
            side_effect=fake_run_import,
        ),
    ):
        result = runner.invoke(
            app,
            ["file", str(csv_path), "--account-name", "Test", "--yes"],
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
            "moneybin.cli.utils.handle_database_errors",
            _fake_db_ctx,
        ),
        patch(
            "moneybin.services.import_service.import_file",
            side_effect=fake_run_import,
        ),
    ):
        result = runner.invoke(
            app,
            ["file", str(csv_path), "--account-name", "Test"],
        )

    assert result.exit_code == 0, result.output
    assert captured.get("auto_accept") is False
