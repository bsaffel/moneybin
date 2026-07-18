# ruff: noqa: S101
"""`moneybin import preview` on a PDF.

Preview routed every file through the *tabular* format detector, so a PDF was
rejected with "Unsupported file type: '.pdf'" even though
``ImportService.pdf_preview`` existed and the MCP ``import_preview`` tool used
it. That left the whole PDF-import debug loop (is this statement deterministic?
how many rows would it yield?) undriveable from the CLI.

CLI wiring only, per `.claude/rules/cli.md` — the service is mocked; routing
behaviour is covered in the extractor and service suites.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest
from pytest_mock import MockerFixture
from typer.testing import CliRunner

from moneybin.cli.commands.import_cmd import app
from moneybin.extractors.confidence import Confidence
from moneybin.services.import_confirmation import (
    ConfirmationRequired,
    ImportConfirmationRequiredError,
    SignConventionProposal,
)
from moneybin.services.import_service import PdfPreviewResult

runner = CliRunner()


@pytest.fixture
def statement(tmp_path: Path) -> Path:
    """A file with a .pdf suffix. Content is irrelevant — the service is mocked."""
    path = tmp_path / "statement.pdf"
    path.write_bytes(b"%PDF-1.4 not a real pdf")
    return path


def _preview_result(path: Path, **overrides: object) -> PdfPreviewResult:
    defaults: dict[str, object] = {
        "file_path": str(path),
        "deterministic": True,
        "decision_reason": "passed",
        "confidence": 0.95,
        "row_count": 24,
    }
    return PdfPreviewResult(**{**defaults, **overrides})  # type: ignore[arg-type]


def _patch_service(mocker: MockerFixture, **kwargs: object) -> MagicMock:
    """Stub ImportService so no database or real PDF parsing is involved."""
    svc = MagicMock()
    mocker.patch("moneybin.services.import_service.ImportService", return_value=svc)
    mocker.patch("moneybin.database.get_database")
    for name, value in kwargs.items():
        setattr(svc, name, value)
    return svc


def test_preview_routes_a_pdf_to_the_pdf_service(
    statement: Path, mocker: MockerFixture
) -> None:
    """A .pdf must reach pdf_preview, not the tabular format detector."""
    svc = _patch_service(
        mocker, pdf_preview=MagicMock(return_value=_preview_result(statement))
    )

    result = runner.invoke(app, ["preview", str(statement)])

    assert result.exit_code == 0, result.output
    assert "Unsupported file type" not in result.output
    svc.pdf_preview.assert_called_once()


def test_preview_reports_the_deterministic_verdict_and_row_count(
    statement: Path, mocker: MockerFixture, caplog: pytest.LogCaptureFixture
) -> None:
    """The two facts the debug loop exists to answer must both be visible."""
    _patch_service(
        mocker, pdf_preview=MagicMock(return_value=_preview_result(statement))
    )

    with caplog.at_level("INFO"):
        result = runner.invoke(app, ["preview", str(statement)])

    assert result.exit_code == 0, result.output
    assert "24" in caplog.text  # row count
    assert "deterministic" in caplog.text.lower()


def test_preview_reports_a_non_deterministic_pdf_without_failing(
    statement: Path, mocker: MockerFixture, caplog: pytest.LogCaptureFixture
) -> None:
    """A statement the rung can't crack is a finding to report, not an error.

    Exit stays 0: preview answered the question it was asked. The reason is
    what the user needs, so it has to appear.
    """
    _patch_service(
        mocker,
        pdf_preview=MagicMock(
            return_value=_preview_result(
                statement,
                deterministic=False,
                decision_reason="no_transaction_table",
                confidence=0.0,
                row_count=0,
            )
        ),
    )

    with caplog.at_level("INFO"):
        result = runner.invoke(app, ["preview", str(statement)])

    assert result.exit_code == 0, result.output
    assert "no_transaction_table" in caplog.text


def test_preview_surfaces_a_pending_sign_confirmation(
    statement: Path, mocker: MockerFixture, caplog: pytest.LogCaptureFixture
) -> None:
    """A card statement's sign proposal must be shown, not raised as a traceback.

    pdf_preview signals this by RAISING, so an unhandled path would crash the
    command on exactly the statements that most need inspecting.
    """
    outcome = ConfirmationRequired(
        channel="pdf",
        confidence=Confidence(score=1.0, tier="high", flagged=(), missing_required=()),
        proposed=SignConventionProposal(
            sign_convention="negative_is_income",
            evidence=("Minimum Payment Due", "Credit Limit"),
            sample_rows=[
                {
                    "description": "COFFEE SHOP",
                    "as_printed": "12.50",
                    "as_recorded": "-12.50",
                }
            ],
        ),
        reason="sign_convention",
    )
    _patch_service(
        mocker,
        pdf_preview=MagicMock(side_effect=ImportConfirmationRequiredError(outcome)),
    )

    with caplog.at_level("INFO"):
        result = runner.invoke(app, ["preview", str(statement)])

    assert result.exception is None, result.exception
    assert "negative_is_income" in caplog.text


def test_preview_still_handles_a_csv(tmp_path: Path) -> None:
    """Regression: the tabular path must survive the PDF branch."""
    csv = tmp_path / "txns.csv"
    csv.write_text("Date,Description,Amount\n2024-01-15,Coffee,-4.50\n")

    result = runner.invoke(app, ["preview", str(csv)])

    assert result.exit_code == 0, result.output
    assert "Unsupported file type" not in result.output


def test_preview_reports_an_unreadable_pdf_cleanly(
    statement: Path, mocker: MockerFixture, caplog: pytest.LogCaptureFixture
) -> None:
    """An unreadable file is an error message, never a traceback.

    Real trigger: macOS TCC denies reads under ~/Documents, so pdfplumber's
    `open()` raises PermissionError from deep inside the extractor and the CLI
    dumped a full rich traceback at the user. Statements routinely live in
    exactly those protected directories.
    """
    _patch_service(
        mocker,
        pdf_preview=MagicMock(
            side_effect=PermissionError(1, "Operation not permitted", str(statement))
        ),
    )

    with caplog.at_level("INFO"):
        result = runner.invoke(app, ["preview", str(statement)])

    assert result.exception is None or isinstance(result.exception, SystemExit)
    assert "Traceback" not in result.output
    assert result.exit_code == 1
    # Both halves matter: what failed, and the one-click OS fix for it.
    assert "cannot read" in caplog.text.lower()
    assert "privacy" in caplog.text.lower()
