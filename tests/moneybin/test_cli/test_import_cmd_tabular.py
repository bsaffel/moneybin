"""Tests for tabular import CLI commands.

Tests CLI-specific functionality: argument parsing, exit codes, error handling.
Business logic is tested in the service and extractor tests.
"""

from __future__ import annotations

import json
import re
import shlex
from contextlib import nullcontext
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest
from _pytest.logging import LogCaptureFixture
from typer.testing import CliRunner

from moneybin import error_codes
from moneybin.cli.commands.import_cmd import app
from moneybin.database import Database
from moneybin.errors import UserError
from moneybin.services.import_service import ImportResult, SavedFormatDeletePlan

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
            account_bindings=None,
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
        log_rows = db.execute("SELECT COUNT(*) FROM app.import_log").fetchone()
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
        log_rows = db.execute("SELECT COUNT(*) FROM app.import_log").fetchone()
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
        service.plan_saved_format_delete.side_effect = UserError(
            "Saved format not found.",
            code="import_saved_format_not_found",
        )
        result = runner.invoke(app, ["formats", "delete", "my_custom_format", "--yes"])
        assert result.exit_code == 1

    def test_delete_uses_the_reviewed_plan_for_live_verification(
        self,
        mocker: Any,
    ) -> None:
        mocker.patch("moneybin.database.get_database", return_value=MagicMock())
        service = mocker.patch(
            "moneybin.services.import_service.ImportService"
        ).return_value
        plan = SavedFormatDeletePlan(
            format_name="my_custom_format",
            state_sha256="reviewed-state",
        )
        service.plan_saved_format_delete.return_value = plan

        def verify_live(
            _name: str,
            *,
            actor: str,
            verify: Any,
        ) -> str:
            assert actor == "cli"
            verify(plan)
            return "op_delete"

        service.delete_saved_format_confirmed.side_effect = verify_live

        result = runner.invoke(
            app,
            ["formats", "delete", "my_custom_format", "--yes"],
        )

        assert result.exit_code == 0
        assert "Format deleted" in result.stdout
        assert "my_custom_format" in result.stdout
        service.plan_saved_format_delete.assert_called_once_with("my_custom_format")
        service.delete_saved_format_confirmed.assert_called_once()

    def test_delete_requires_yes_when_noninteractive(self, mocker: Any) -> None:
        """A saved format cannot be deleted by a piped, unanswered prompt."""
        mocker.patch("moneybin.database.get_database", return_value=MagicMock())
        service = mocker.patch(
            "moneybin.services.import_service.ImportService"
        ).return_value
        service.plan_saved_format_delete.return_value = SavedFormatDeletePlan(
            format_name="my_custom_format", state_sha256="reviewed-state"
        )

        result = runner.invoke(app, ["formats", "delete", "my_custom_format"])

        assert result.exit_code == 1, result.output
        service.delete_saved_format_confirmed.assert_not_called()

    def test_delete_rejects_a_changed_live_plan_with_canonical_error(
        self,
        mocker: Any,
    ) -> None:
        mocker.patch("moneybin.database.get_database", return_value=MagicMock())
        service = mocker.patch(
            "moneybin.services.import_service.ImportService"
        ).return_value
        reviewed = SavedFormatDeletePlan(
            format_name="my_custom_format",
            state_sha256="reviewed-state",
        )
        live = SavedFormatDeletePlan(
            format_name="my_custom_format",
            state_sha256="changed-state",
        )
        service.plan_saved_format_delete.return_value = reviewed
        seen_codes: list[str] = []

        def verify_changed(
            _name: str,
            *,
            actor: str,
            verify: Any,
        ) -> str:
            assert actor == "cli"
            try:
                verify(live)
            except UserError as error:
                seen_codes.append(error.code)
                raise
            raise AssertionError("changed live plan was unexpectedly accepted")

        service.delete_saved_format_confirmed.side_effect = verify_changed

        result = runner.invoke(
            app,
            ["formats", "delete", "my_custom_format", "--yes"],
        )

        assert result.exit_code == 1
        assert seen_codes == [error_codes.MUTATION_CONFIRMATION_MISMATCH]
        service.delete_saved_format_confirmed.assert_called_once()


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

    def test_preview_names_the_bounded_sample_scope(self, tmp_path: Path) -> None:
        """Parsed values render as rows with an explicit first-five sample bound."""
        csv_file = tmp_path / "sample.csv"
        csv_file.write_text(
            "Date,Amount,Description\n"
            "2025-01-01,-10.00,Coffee\n"
            "2025-01-02,20.00,Refund\n"
        )

        result = runner.invoke(app, ["preview", str(csv_file)])

        assert result.exit_code == 0, result.output
        assert "Sample scope" in result.stdout
        assert "first 2 of 2 parsed rows" in result.stdout
        assert "Coffee" in result.stdout
        assert "Columns" in result.output

    def test_undetected_date_hint_only_names_flags_preview_accepts(
        self, tmp_path: Path
    ) -> None:
        """A hint that names a flag this command lacks is worse than silence.

        The user pastes it and gets "No such option". This has now shipped
        twice in one PR — `import confirm --date-format` and `import preview
        --mapping` — so assert against the command's *registered* options
        rather than a literal, which only re-pins whatever was written.
        The other half of the recovery genuinely lives on `import files`, so
        flags inside an explicit `moneybin import files` clause are exempt.
        """
        import re

        csv_file = tmp_path / "unreadable.csv"
        csv_file.write_text(
            "Date,Amount,Description\nnot-a-date,100,Coffee\nalso-not-a-date,200,Rent\n"
        )

        result = runner.invoke(app, ["preview", str(csv_file)])

        assert result.exit_code == 0
        line = next(
            ln for ln in result.output.splitlines() if ln.startswith("Date format:")
        )
        assert "not detected" in line, line

        from typing import cast

        import typer.main
        from typer._click import Command
        from typer.core import TyperGroup

        group = cast(TyperGroup, typer.main.get_command(app))
        preview_cmd: Command = group.commands["preview"]
        registered: set[str] = {
            opt
            for param in preview_cmd.params
            for opt in param.opts
            if opt.startswith("--")
        }
        # Drop the clause that deliberately delegates to another command.
        own = re.sub(r"`moneybin import files[^`]*`", "", line)
        for flag in re.findall(r"--[a-z-]+", own):
            assert flag in registered, (
                f"{flag} is named by `import preview` but not registered on it; "
                f"registered: {sorted(registered)}"
            )

    def test_preview_detects_headerless_excel_without_warning(
        self, tmp_path: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        """A headerless Excel sheet must preview cleanly, like CSV/Parquet (MB-449).

        Excel used to always consume row 0 as the header with no headerless
        detection, so this exact fixture used to trip the
        header_row_looks_like_data misdetection warning on the auto-detect
        path. Excel now shares detection with CSV/Parquet: both rows are kept,
        has_header is reported False, and no misdetection warning fires.
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
        assert "Header row detected: False" in result.output
        assert "2 in file = 0 skipped + 0 header + 2 read" in result.output
        assert not any("parses as a transaction" in r.message for r in caplog.records)

    def test_preview_named_format_date_format_reaches_header_detection(
        self, tmp_path: Path, mocker: Any
    ) -> None:
        """``--format <name>`` must feed its own date_format into header detection."""
        from moneybin.extractors.tabular.formats import TabularFormat

        csv_file = tmp_path / "headerless_yyyymmdd.csv"
        csv_file.write_text(
            "20260105,42.50,Coffee\n20260106,10.00,Tea\n20260107,-20.00,Groceries\n",
            encoding="utf-8",
        )
        saved_format = TabularFormat(
            name="yyyymmdd_test",
            institution_name="Test",
            file_type="csv",
            header_signature=["Date", "Amount", "Description"],
            field_mapping={
                "transaction_date": "Date",
                "amount": "Amount",
                "description": "Description",
            },
            sign_convention="negative_is_expense",
            date_format="%Y%m%d",
        )
        mocker.patch(
            "moneybin.cli.commands.import_cmd._load_all_formats",
            return_value=({saved_format.name: saved_format}, {}),
        )

        result = runner.invoke(
            app, ["preview", str(csv_file), "--format", saved_format.name]
        )

        assert result.exit_code == 0
        assert "Header row detected: False" in result.output
        assert re.search(r"Parsed rows:\s+3", result.output)

    def test_preview_reads_the_sheet_the_named_format_selects(
        self, tmp_path: Path, mocker: Any
    ) -> None:
        """`--format` must pick the format's sheet, not the largest one.

        A saved format names its sheet because the workbook holds others.
        `ImportService` passes it to `read_file`; a preview that left it unset
        auto-selected whichever sheet had the most rows, so the preview of an
        import reported another sheet's columns and row count entirely.
        `file_type` stays "auto" so the sheet is the only setting under test.
        """
        import openpyxl

        from moneybin.extractors.tabular.formats import TabularFormat

        wb = openpyxl.Workbook()
        ledger = wb.active
        assert ledger is not None
        ledger.title = "Ledger"
        ledger.append(["Posted", "Value", "Memo"])
        for day in range(1, 6):
            ledger.append([f"2026-02-{day:02d}", 5.00, "Not the import's sheet"])
        wanted = wb.create_sheet("Transactions")
        wanted.append(["Date", "Amount", "Description"])
        wanted.append(["2026-01-01", 42.50, "Coffee"])
        wanted.append(["2026-01-02", 10.00, "Tea"])
        path = tmp_path / "two_sheets.xlsx"
        wb.save(path)

        saved_format = TabularFormat(
            name="sheet_test",
            institution_name="Test",
            sheet="Transactions",
            header_signature=["Date", "Amount", "Description"],
            field_mapping={
                "transaction_date": "Date",
                "amount": "Amount",
                "description": "Description",
            },
            sign_convention="negative_is_expense",
            date_format="%Y-%m-%d",
        )
        mocker.patch(
            "moneybin.cli.commands.import_cmd._load_all_formats",
            return_value=({saved_format.name: saved_format}, {}),
        )

        result = runner.invoke(
            app, ["preview", str(path), "--format", saved_format.name]
        )

        assert result.exit_code == 0, result.output
        # The larger "Ledger" sheet has 5 data rows; the named one has 2.
        assert re.search(r"Parsed rows:\s+2", result.output), result.output
        assert "Posted" not in result.output, result.output

    def test_preview_skips_the_preamble_the_named_format_declares(
        self, tmp_path: Path, mocker: Any
    ) -> None:
        """`--format` must apply the format's skip_rows, as the import does.

        A format with `skip_rows` exists because the export carries preamble
        lines above its header. The import skips them; a preview that did not
        found its header somewhere else and reported a different row count.

        The preamble here is itself header-shaped, so auto-detection claims
        row 0 and reads 4 "data" rows. A gentler preamble would let detection
        find the real header unaided, and the test would pass with or without
        the setting it exists to check.
        """
        from moneybin.extractors.tabular.formats import TabularFormat

        csv_file = tmp_path / "preamble.csv"
        csv_file.write_text(
            "Account,Period,Currency\n"
            "Household,2026-01,USD\n"
            "Date,Amount,Description\n"
            "2026-01-01,42.50,Coffee\n"
            "2026-01-02,10.00,Tea\n",
            encoding="utf-8",
        )
        saved_format = TabularFormat(
            name="preamble_test",
            institution_name="Test",
            skip_rows=2,
            header_signature=["Date", "Amount", "Description"],
            field_mapping={
                "transaction_date": "Date",
                "amount": "Amount",
                "description": "Description",
            },
            sign_convention="negative_is_expense",
            date_format="%Y-%m-%d",
        )
        mocker.patch(
            "moneybin.cli.commands.import_cmd._load_all_formats",
            return_value=({saved_format.name: saved_format}, {}),
        )

        result = runner.invoke(
            app, ["preview", str(csv_file), "--format", saved_format.name]
        )

        assert result.exit_code == 0, result.output
        assert re.search(r"Parsed rows:\s+2", result.output), result.output
        assert "Header row detected: True" in result.output, result.output
        assert "transaction_date ← Date" in result.output, result.output

    def test_preview_reports_a_declared_format_the_detector_rejects(
        self, tmp_path: Path
    ) -> None:
        """Preview must answer the import's question, not the detector's.

        The two bars differ on purpose: `detect_date_format` holds a
        declaration to a 90% parse rate because it decides what detection
        believes, while the loader accepts it at 50% and counts the rows it
        could not read as rejected. A file of 6 readable dates in 10 therefore
        imports under `--date-format`, while the preview of that same import
        used to report "not detected" and advise supplying the very flag
        already on the command line.
        """
        rows = "".join(
            f"2026010{n},{n}.50,Item {n}\n" if n <= 6 else f"n/a,{n}.50,Item {n}\n"
            for n in range(1, 11)
        )
        csv_file = tmp_path / "dirty_dates.csv"
        csv_file.write_text(f"Date,Amount,Description\n{rows}", encoding="utf-8")

        result = runner.invoke(
            app, ["preview", str(csv_file), "--date-format", "%Y%m%d"]
        )

        assert result.exit_code == 0, result.output
        line = next(
            ln for ln in result.output.splitlines() if ln.startswith("Date format:")
        )
        assert "%Y%m%d" in line, line
        assert "declared" in line, line
        assert "not detected" not in line, line

    def test_preview_header_position_ambiguous_uses_shared_recovery_text(
        self, tmp_path: Path, caplog: LogCaptureFixture
    ) -> None:
        """`import preview` has no --confirm flag.

        The hand-rolled warning this replaced told the user to "re-run with
        --confirm to proceed" — a flag `import preview` itself does not
        register (it lives on `import files` / `import confirm`). Reusing
        the shared ``header_position_ambiguous_recovery`` helper names the
        commands that actually clear the gate. The recovery command goes to
        stderr and never a log record: it repeats the caller's read options,
        and ``--sheet``/``--format`` are arbitrary user text the log
        allowlist does not admit. The disputed row(s) go the same way,
        through ``echo_disputed_rows`` (allowlisted via ``disputed_row_
        fields``). Only the static diagnostic is logged.
        """
        import logging

        from moneybin.services.import_confirmation import (
            header_position_ambiguous_recovery,
        )

        csv_file = tmp_path / "data_before_header.csv"
        csv_file.write_text(
            "2026-01-01,42.50,Coffee\n"
            "2026-01-02,10.00,Tea\n"
            "Date,Amount,Description\n"
            "2026-01-03,5.00,Snack\n",
            encoding="utf-8",
        )

        with caplog.at_level(logging.WARNING):
            result = runner.invoke(app, ["preview", str(csv_file)])

        assert result.exit_code == 0
        expected = header_position_ambiguous_recovery(str(csv_file))
        assert expected in result.output, result.output
        # ...and never a log record, for the reason in the docstring.
        assert not any(expected in r.message for r in caplog.records), caplog.text
        # The static diagnostic is what the log keeps.
        assert any("looks like a transaction" in r.message for r in caplog.records), (
            caplog.text
        )
        # The disputed rows appear in the CLI's stderr-mixed output, already
        # allowlisted to dest=value pairs (disputed_row_fields) rather than
        # raw positional cells...
        assert (
            "transaction_date=2026-01-01, amount=42.50, description=Coffee"
            in result.output
        )
        assert (
            "transaction_date=2026-01-02, amount=10.00, description=Tea"
            in result.output
        )
        # ...and in no log record — a row can carry an account number, and
        # log_to_file defaults to True.
        assert not any(
            "transaction_date=2026-01-01" in r.message for r in caplog.records
        )

    def test_preview_consumed_header_names_a_working_recovery(
        self, tmp_path: Path, mocker: Any, caplog: pytest.LogCaptureFixture
    ) -> None:
        """A stale --format's skip_rows must not tell the caller to "fix --format".

        The hand-rolled warning this replaced said "Re-run with a corrected
        --format" — but no command edits a saved format's skip_rows, so that
        advice named nothing that exists. Reusing the shared
        ``header_row_consumed_recovery`` helper (with ``retry_command="import
        preview"``, since preview has nothing to load) names the one real
        fix: re-run without --format. The runnable retry goes to stderr and
        never the logger, same split as ``header_position_ambiguous`` just
        below it: it repeats the caller's --format, arbitrary user text the
        log allowlist does not admit.
        """
        import logging

        from moneybin.extractors.tabular.formats import TabularFormat
        from moneybin.services.import_confirmation import (
            TabularReadOptions,
            header_row_consumed_recovery,
        )

        # Row 0 is a preamble the format skips; row 1 then becomes the header
        # and is itself a transaction (date + negative amount + description).
        csv_file = tmp_path / "preamble_then_data.csv"
        csv_file.write_text(
            "Statement export\n2026-01-05,-4.50,Coffee\n2026-01-06,100.00,Payroll\n",
            encoding="utf-8",
        )
        saved_format = TabularFormat(
            name="skiprows_fixture",
            institution_name="Test",
            file_type="csv",
            header_signature=["2026-01-05", "-4.50", "Coffee"],
            field_mapping={
                "transaction_date": "2026-01-05",
                "amount": "-4.50",
                "description": "Coffee",
            },
            sign_convention="negative_is_expense",
            date_format="%Y-%m-%d",
            skip_rows=1,
        )
        mocker.patch(
            "moneybin.cli.commands.import_cmd._load_all_formats",
            return_value=({saved_format.name: saved_format}, {}),
        )

        with caplog.at_level(logging.WARNING):
            result = runner.invoke(
                app, ["preview", str(csv_file), "--format", saved_format.name]
            )

        assert result.exit_code == 0, result.output
        # The diagnostic is logged...
        assert any("parses as a transaction" in r.message for r in caplog.records), (
            caplog.text
        )
        # ...but no longer directs the caller to fix something no command
        # can fix.
        assert not any("corrected --format" in r.message for r in caplog.records), (
            caplog.text
        )
        # The real recovery — the same text `import files`'s confirmation
        # path prints for this reason, just naming `import preview` as the
        # retry — is echoed to stderr and never reaches the log. The printed
        # retry carries the format's own date_format/number_format/encoding
        # (its resolved defaults), not just the caller's raw --format flag,
        # per the fix under test.
        expected = header_row_consumed_recovery(
            str(csv_file),
            format_name=saved_format.name,
            read_options=TabularReadOptions(
                date_format=saved_format.date_format,
                number_format=saved_format.number_format,
                encoding=saved_format.encoding,
            ),
            retry_command="import preview",
        )
        assert expected in result.output, result.output
        assert not any(expected in r.message for r in caplog.records), caplog.text
        assert "moneybin import preview" in result.output, result.output
        assert "moneybin import files" not in result.output, result.output

    def test_preview_consumed_header_keeps_the_worksheet_the_stale_format_selected(
        self, tmp_path: Path, mocker: Any
    ) -> None:
        """The printed retry must not silently read a different worksheet.

        Mirrors ``test_the_retry_keeps_the_worksheet_the_stale_format_
        selected`` (test_tabular_import_service.py) at the ``import preview``
        surface. Dropping ``--format`` is the whole recovery, but the format
        carries the ``sheet`` alongside the ``skip_rows`` being escaped — with
        the sheet gone the reader auto-selects the LARGEST worksheet, so a
        workbook whose largest sheet also has compatible headers imports that
        sheet instead, silently. The larger ``Archive`` sheet here is exactly
        that trap. Asserts on the extracted retry command text, not the whole
        output — the surrounding prose legitimately mentions --format.
        """
        import openpyxl

        from moneybin.extractors.tabular.formats import TabularFormat

        wb = openpyxl.Workbook()
        statement = wb.active
        assert statement is not None
        statement.title = "Statement"
        statement.append(["Date", "Amount", "Description"])
        statement.append(["2026-01-05", -4.50, "Coffee"])
        statement.append(["2026-01-06", 100.00, "Payroll"])
        # Same headers, more rows — what the reader would auto-select.
        archive = wb.create_sheet("Archive")
        archive.append(["Date", "Amount", "Description"])
        for day in range(10, 20):
            archive.append([f"2025-01-{day}", -1.00, "Old"])
        xlsx = tmp_path / "two_sheets.xlsx"
        wb.save(xlsx)

        saved_format = TabularFormat(
            name="stale_skip_sheet_preview",
            institution_name="Test",
            file_type="excel",
            header_signature=["date", "amount", "description"],
            field_mapping={
                "transaction_date": "Date",
                "amount": "Amount",
                "description": "Description",
            },
            sign_convention="negative_is_expense",
            date_format="%Y-%m-%d",
            number_format="us",
            sheet="Statement",
            skip_rows=1,
        )
        mocker.patch(
            "moneybin.cli.commands.import_cmd._load_all_formats",
            return_value=({saved_format.name: saved_format}, {}),
        )

        result = runner.invoke(
            app, ["preview", str(xlsx), "--format", saved_format.name]
        )

        assert result.exit_code == 0, result.output
        match = re.search(r"`(moneybin import preview [^`]+)`", result.output)
        assert match is not None, result.output
        retry_command = match.group(1)
        assert "--sheet Statement" in retry_command, retry_command
        assert "--format" not in retry_command, retry_command

    def test_preview_consumed_header_names_the_callers_number_format(
        self, tmp_path: Path, mocker: Any
    ) -> None:
        """An explicit --number-format must outrank the saved format's value.

        `resolve_read_settings`'s contract is "an explicit flag always
        outranks the format" — but `import preview`'s call omitted
        `number_format` entirely, so `read_settings.number_format` always
        came from the FORMAT and the printed retry silently named the
        format's decimal-separator locale instead of the caller's own. The
        saved format's ``number_format`` ("us") is deliberately different
        from the override ("european") so the two are distinguishable.
        """
        from moneybin.extractors.tabular.formats import TabularFormat

        csv_file = tmp_path / "preamble_then_data.csv"
        csv_file.write_text(
            "Statement export\n2026-01-05,-4.50,Coffee\n2026-01-06,100.00,Payroll\n",
            encoding="utf-8",
        )
        saved_format = TabularFormat(
            name="skiprows_fixture_number_format",
            institution_name="Test",
            file_type="csv",
            header_signature=["2026-01-05", "-4.50", "Coffee"],
            field_mapping={
                "transaction_date": "2026-01-05",
                "amount": "-4.50",
                "description": "Coffee",
            },
            sign_convention="negative_is_expense",
            date_format="%Y-%m-%d",
            number_format="us",
            skip_rows=1,
        )
        mocker.patch(
            "moneybin.cli.commands.import_cmd._load_all_formats",
            return_value=({saved_format.name: saved_format}, {}),
        )

        result = runner.invoke(
            app,
            [
                "preview",
                str(csv_file),
                "--format",
                saved_format.name,
                "--number-format",
                "european",
            ],
        )

        assert result.exit_code == 0, result.output
        match = re.search(r"`(moneybin import preview [^`]+)`", result.output)
        assert match is not None, result.output
        retry_command = match.group(1)
        assert "--number-format european" in retry_command, retry_command
        assert "--number-format us" not in retry_command, retry_command

    def test_preview_disputed_row_evidence_survives_quoted_header(
        self, tmp_path: Path
    ) -> None:
        """A quoted CSV header must not blank out the disputed-row evidence.

        Round 18: ``_detect_header`` tokenized sample lines with a bare
        ``line.split(delimiter)``, while ``pl.read_csv`` is quote-aware. A
        quoted header produced ``header_cells`` carrying literal quote
        characters, which never equal a value in ``df.columns`` -- so every
        cell's column identity looked unresolvable and the whole disputed
        row was omitted, even though nothing in it was unsafe to show. Only
        an unmapped account-shaped column (never a date/amount/description
        cell) should ever be absent.
        """
        csv_file = tmp_path / "quoted_header.csv"
        csv_file.write_text(
            '"2026-01-01","42.50","Coffee","ACCT-XY9Z"\n'
            '"2026-01-02","10.00","Tea","1234"\n'
            '"Date","Amount","Description","AccountNumber"\n'
            '"2026-01-03","5.00","Snack","AB1234C"\n',
            encoding="utf-8",
        )

        result = runner.invoke(app, ["preview", str(csv_file)])

        assert result.exit_code == 0
        # The exact-match itself proves omission: an included AccountNumber
        # cell would change the string (an extra ", AccountNumber=..."
        # suffix), so matching exactly this text is sufficient -- no need
        # to additionally scan the whole output, which also renders the
        # real (non-disputed) data row's own AccountNumber column value.
        assert (
            "transaction_date=2026-01-01, amount=42.50, description=Coffee"
            in result.output
        )
        assert (
            "transaction_date=2026-01-02, amount=10.00, description=Tea"
            in result.output
        )

    def test_preview_quoted_description_with_delimiter_not_omitted(
        self, tmp_path: Path
    ) -> None:
        """A quoted description containing the delimiter must not length-mismatch.

        Before the fix, a bare ``line.split(",")`` split
        ``"Coffee, large"`` into two cells, making the disputed row longer
        than ``header_cells`` -- a length mismatch that omits the whole row.
        The real read always counts it as one cell, so the row must be
        shown, not omitted.
        """
        csv_file = tmp_path / "quoted_description_delimiter.csv"
        csv_file.write_text(
            '2026-01-01,42.50,"Coffee, large",ACCT-XY9Z\n'
            "2026-01-02,10.00,Tea,1234\n"
            "Date,Amount,Description,AccountNumber\n"
            "2026-01-03,5.00,Snack,AB1234C\n",
            encoding="utf-8",
        )

        result = runner.invoke(app, ["preview", str(csv_file)])

        assert result.exit_code == 0
        assert (
            "transaction_date=2026-01-01, amount=42.50, description=Coffee, large"
            in result.output
        )
        assert (
            "transaction_date=2026-01-02, amount=10.00, description=Tea"
            in result.output
        )

    def test_preview_maps_native_date_excel_column_correctly(
        self, tmp_path: Path
    ) -> None:
        """A native-date Excel column with unaliased headers must still map right.

        Regression: this invocation declares no format at all, so nothing
        reaches this command's normalize-before-map step and content-based
        discovery is on its own. Headers are deliberately unaliased
        ("Col1"/"Col2"/"Col3") so map_columns's content-based discovery is
        what has to get this right — skipping normalization here doesn't
        just fail to detect the date column, it misidentifies it as
        `description` while the real description column drops out of the
        mapping entirely.
        """
        from datetime import date

        import openpyxl

        wb = openpyxl.Workbook()
        ws = wb.active
        assert ws is not None
        ws.append(["Col1", "Col2", "Col3"])
        ws.append([date(2026, 1, 1), 42.50, "Coffee"])
        ws.append([date(2026, 1, 2), 10.00, "Tea"])
        path = tmp_path / "native_dates_unaliased.xlsx"
        wb.save(path)

        result = runner.invoke(app, ["preview", str(path)])

        assert result.exit_code == 0
        assert "transaction_date ← Col1" in result.output
        assert "description ← Col1" not in result.output

    def test_preview_override_scopes_native_date_normalization(
        self, tmp_path: Path
    ) -> None:
        """A caller --override transaction_date=<col> must scope normalization.

        Same shape as the MCP `_import_preview_tabular` fix: first-contact
        `import preview` (no saved/matched format) never passed
        `date_column` to `normalize_excel_date_columns_for_detection`, so
        an unrelated
        second native-date column ("Memo", auto-typed by some spreadsheet
        tool) got normalized too whenever a broad scan found it — even
        though the caller named the actual date column via `--override`.
        The printed sample table must show Memo's raw "<date> 00:00:00"
        text untouched, since only the overridden transaction_date column
        may be rewritten.
        """
        from datetime import date

        import openpyxl

        wb = openpyxl.Workbook()
        ws = wb.active
        assert ws is not None
        ws.append(["Date", "Amount", "Description", "Memo"])
        ws.append([date(2026, 1, 1), -4.50, "Coffee", date(2026, 1, 15)])
        path = tmp_path / "second_native_date_column.xlsx"
        wb.save(path)

        result = runner.invoke(
            app,
            ["preview", str(path), "--override", "transaction_date=Date"],
        )

        assert result.exit_code == 0
        assert "transaction_date ← Date" in result.output
        # Memo's raw native-date text must survive untouched — proof that
        # normalization was scoped to the overridden transaction_date column
        # rather than sweeping in every native-date column it can find.
        assert "2026-01-15 00:00:00" in result.output

    @pytest.mark.parametrize("command_name", ["files", "confirm", "preview"])
    def test_date_format_help_example_is_a_usable_strptime_string(
        self, command_name: str
    ) -> None:
        """The example each command prints must parse a date, not a percent sign.

        `%%` is how strptime spells a *literal* percent, and Click performs no
        %-substitution on help text — it prints what it is given. So a help
        string carrying `%%Y-%%m-%%d` showed exactly that, and a caller who
        copied it got a format matching the eight characters `%Y-%m-%d` and no
        date at all. All three commands print this example, so all three are
        checked here.

        The example is read back out of the help and run, rather than compared
        against a spelling: the property is that what MoneyBin prints is
        usable, and a spelling assertion would pass on the next unusable one.
        """
        import datetime
        import re

        from typer.core import TyperGroup, TyperOption
        from typer.main import get_command

        group = get_command(app)
        assert isinstance(group, TyperGroup)
        command = group.commands[command_name]
        option = next(p for p in command.params if "--date-format" in p.opts)
        assert isinstance(option, TyperOption)
        assert option.help is not None
        match = re.search(r"e\.g\. (\S+?)\)", option.help)
        assert match is not None, option.help
        example = match.group(1)

        assert datetime.datetime.strptime("2026-01-01", example) == datetime.datetime(
            2026, 1, 1
        )

    def test_preview_validates_a_named_formats_date_format(
        self, tmp_path: Path, mocker: Any
    ) -> None:
        """A matched format must be held to the import's own date-format gate.

        `ImportService` validates `date_format_override or resolved.date_format`
        on every branch, so the value a `--format` preview prints is the value
        the import refuses when it cannot read the mapped column. This branch
        used to print it as fact and exit 0, so `import preview` advertised a
        format the very next `import files` rejected — the one shape a preview
        must never take, since it exists to predict that import.

        The remedy named here is `--date-format`, not `--override`: this branch
        takes its mapping from the saved format and ignores `--override`.
        """
        from moneybin.extractors.tabular.formats import TabularFormat

        csv_file = tmp_path / "iso_dates.csv"
        csv_file.write_text(
            "Date,Amount,Description\n"
            "2026-01-05,42.50,Coffee\n"
            "2026-01-06,10.00,Tea\n"
            "2026-01-07,-20.00,Groceries\n",
            encoding="utf-8",
        )
        saved_format = TabularFormat(
            name="iso_test",
            institution_name="Test",
            file_type="csv",
            header_signature=["Date", "Amount", "Description"],
            field_mapping={
                "transaction_date": "Date",
                "amount": "Amount",
                "description": "Description",
            },
            sign_convention="negative_is_expense",
            date_format="%Y-%m-%d",
        )
        mocker.patch(
            "moneybin.cli.commands.import_cmd._load_all_formats",
            return_value=({saved_format.name: saved_format}, {}),
        )

        # %d/%m/%Y cannot read an ISO column: no row parses, so the loader's
        # 50% override gate refuses it rather than dropping most rows.
        result = runner.invoke(
            app,
            [
                "preview",
                str(csv_file),
                "--format",
                saved_format.name,
                "--date-format",
                "%d/%m/%Y",
            ],
        )

        assert result.exit_code == 0, result.output
        line = next(
            ln for ln in result.output.splitlines() if ln.startswith("Date format:")
        )
        assert "%d/%m/%Y" in line, line
        assert "does not read the mapped column" in line, line
        # The remedy must not name a flag this branch ignores.
        assert "--override" not in line, line

    def test_preview_asks_the_date_question_of_the_rendered_frame(
        self, tmp_path: Path
    ) -> None:
        """Excel native dates must be rendered before the format is judged.

        `normalize_excel_date_columns_after_mapping` states the ordering: the
        override validation, the disputed-row echo and the samples all read the
        mapped date column's text, and until it runs that text is still
        `2026-01-05 00:00:00` for a native cell. `ImportService` renders first
        and validates after, so it accepts `%m/%d/%Y` here and loads the rows
        it reads. The preview asked the same question one step too early, of
        pre-render text no import ever sees, and answered the opposite.

        Six native date cells and four unparseable ones put the declared format
        at 60% — deliberately between the loader's 50% gate and the detector's
        90% bar, so detection confirms nothing and the declared format is the
        one under test rather than a detected one standing in for it.
        """
        from datetime import date

        import openpyxl

        wb = openpyxl.Workbook()
        ws = wb.active
        assert ws is not None
        ws.append(["Date", "Amount", "Description"])
        for day in range(1, 7):
            ws.append([date(2026, 1, day), 5.00 + day, f"Item {day}"])
        for n in range(7, 11):
            ws.append(["n/a", 5.00 + n, f"Item {n}"])
        path = tmp_path / "native_dates_with_dirt.xlsx"
        wb.save(path)

        result = runner.invoke(app, ["preview", str(path), "--date-format", "%m/%d/%Y"])

        assert result.exit_code == 0, result.output
        line = next(
            ln for ln in result.output.splitlines() if ln.startswith("Date format:")
        )
        assert "%m/%d/%Y" in line, line
        assert "declared" in line, line
        # The pre-render frame's `2026-01-05 00:00:00` parses under no
        # `%m/%d/%Y`, so judging it early reported the opposite verdict.
        assert "does not read the mapped column" not in line, line

    def test_permission_error_is_classified_not_raw(
        self, tmp_path: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Preview must classify a read denial — every sibling import command does.

        The ❌ message still routes through the logger to stderr (per cli.md),
        which CliRunner keeps out of ``result.output`` — so that half is
        asserted on caplog. The 💡 hint no longer does: ``handle_cli_errors``
        prints it straight to stderr via ``typer.echo`` instead of
        ``logger.info``, specifically so it never reaches the durable CLI log
        file (a DuckDB-error hint can carry caller-authored query text — see
        ``tests/moneybin/test_cli/test_handle_cli_errors.py``). Since click
        8.2, ``result.output`` is its own stream mixing stdout and stderr in
        write order (``mix_stderr`` is gone — ``CliRunner.__init__`` no
        longer takes it), so a direct ``typer.echo(err=True)`` lands there
        too, and the hint is asserted there instead.
        ``result.exception`` is the load-bearing check: an unwrapped
        PermissionError also yields exit code 1 under CliRunner, so the exit
        code alone cannot distinguish classified from unhandled.
        """
        import logging

        target = tmp_path / "locked.csv"
        target.write_text("Date,Amount,Description\n2026-01-01,1.00,Coffee\n")
        target.chmod(0o000)
        try:
            with caplog.at_level(logging.INFO):
                result = runner.invoke(app, ["preview", str(target)])
        finally:
            target.chmod(0o644)

        assert result.exit_code == 1
        assert not isinstance(result.exception, PermissionError)
        # The terminal-policy failure marker has to name THIS failure (not merely be
        # some classified error) — asserting only the marker would pass
        # for any classified error at all.
        assert any(
            r.message.startswith("× ") and "Permission denied" in r.message
            for r in caplog.records
        )
        # The 💡 hint has to be the mode-denial one, not just any hint.
        assert "chmod" in result.output


class TestDeclaredDateFormatConfirmConverges:
    """A declared, non-built-in date format must not lose row 0 (#604).

    End-to-end: run `import files --date-format %Y%m%d`, then repeatedly
    extract MoneyBin's own printed `import confirm` retry command from the
    JSON envelope's ``actions`` and run it verbatim — first ratifying the
    mapping, then (a real, brand-new file has no known account) binding the
    proposed account — proving all rows import with no row lost to header
    misdetection, for both a headerless and a headered source file.
    """

    def _extract_confirm_command(self, actions: list[str]) -> str:
        """Pull the `moneybin import confirm ...` command out of a printed action.

        Prefers the `--accept` hint; falls back to an `--account-binding`
        recovery once mapping is settled. `<account_id|new>` is the CLI's own
        documented placeholder for "mint a new account" — substituted here the
        same way a human following the hint would.
        """
        action = next(
            a
            for a in actions
            if "moneybin import confirm" in a
            and ("--accept" in a or "--account-binding" in a)
        )
        match = re.search(r"`(moneybin import confirm[^`]*)`", action)
        assert match, action
        return match.group(1).replace("<account_id|new>", "new")

    def _run_printed_preview_commands(
        self,
        actions: list[str],
        csv_file: Path,
        *,
        expect_rows: int,
        expect_header: bool,
    ) -> int:
        """Run every printed `moneybin import preview` command verbatim.

        Exiting 0 is not the bar. A preview that reports a different row count
        than the import it previews is the defect, so the reported
        reconciliation is checked against the read the confirmation itself
        made: strip `--date-format` from the printed hint and a headerless
        file reports one row fewer, under a header it does not have.
        """
        ran = 0
        for action in actions:
            match = re.search(r"`(moneybin import preview[^`]*)`", action)
            if match is None:
                continue
            tokens = shlex.split(match.group(1))
            assert str(csv_file) in tokens, tokens
            preview_result = runner.invoke(app, tokens[2:])
            assert preview_result.exit_code == 0, preview_result.output
            # Thousands-separated, as the command prints it — an oversized
            # file's count is the one this would otherwise silently miss.
            assert re.search(
                rf"Parsed rows:\s+{expect_rows:,}", preview_result.output
            ), preview_result.output
            assert f"Header row detected: {expect_header}" in preview_result.output, (
                preview_result.output
            )
            ran += 1
        return ran

    def _converge(
        self,
        csv_file: Path,
        db: Database,
        mocker: Any,
        caplog: pytest.LogCaptureFixture,
        *,
        has_header: bool,
        max_rounds: int = 4,
    ) -> None:
        """Drive `import files` → repeated `import confirm` to a terminal `ok`."""
        import logging

        mocker.patch(
            "moneybin.database.get_database",
            return_value=nullcontext(db),
        )

        result = runner.invoke(
            app,
            [
                "files",
                str(csv_file),
                "--date-format",
                "%Y%m%d",
                "--output",
                "json",
                "--no-refresh",
                "--no-save-format",
            ],
        )
        assert result.exit_code == 1, result.output
        payload = json.loads(result.output)

        for _ in range(max_rounds):
            if payload["data"]["status"] == "ok":
                return
            assert payload["data"]["status"] == "confirmation_required", payload
            self._run_printed_preview_commands(
                payload["actions"],
                csv_file,
                expect_rows=3,
                expect_header=has_header,
            )
            printed = self._extract_confirm_command(payload["actions"])
            assert "--date-format %Y%m%d" in printed, printed
            tokens = shlex.split(printed)
            assert tokens[:3] == ["moneybin", "import", "confirm"]
            # `import confirm` has no `--no-refresh` flag (it hardcodes
            # refresh=False internally), so the printed command runs as-is.
            caplog.clear()
            with caplog.at_level(logging.INFO):
                confirm_result = runner.invoke(app, tokens[2:])
            if confirm_result.exit_code == 1:
                assert "Account binding required" in confirm_result.output
                confirm_result = runner.invoke(app, [*tokens[2:], "--output", "json"])
                assert confirm_result.exit_code == 0, confirm_result.output
                payload = json.loads(confirm_result.output)
                assert payload["data"]["status"] == "confirmation_required", payload
                continue
            assert confirm_result.exit_code == 0, confirm_result.output
            # A settled `import confirm` (no more confirmation_required) takes
            # the normal `--output` (default text) success-render path, not
            # the always-JSON-under-non-tty confirmation_required branch — so
            # a printed command with no `--output json` converges to plain
            # text, not another envelope. Confirm it is genuinely the success
            # render (the "✅ Imported" line every successful confirm logs) —
            # via caplog, not `result.output`: the logging handler holds its
            # own stream reference that CliRunner's stdout/stderr capture
            # does not redirect, unlike typer.echo's click-managed output.
            try:
                payload = json.loads(confirm_result.output)
            except json.JSONDecodeError:
                assert "Import complete" in confirm_result.output
                return

        pytest.fail(f"did not converge to status=ok within {max_rounds} rounds")

    def test_headerless_csv_converges_via_printed_confirm_commands(
        self,
        db: Database,
        mocker: Any,
        tmp_path: Path,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        csv_file = tmp_path / "headerless_yyyymmdd.csv"
        csv_file.write_text(
            "20260105,42.50,Coffee\n20260106,10.00,Tea\n20260107,-20.00,Groceries\n",
            encoding="utf-8",
        )

        self._converge(csv_file, db, mocker, caplog, has_header=False)

        rows = db.execute("SELECT COUNT(*) FROM raw.tabular_transactions").fetchone()
        assert rows is not None and rows[0] == 3, (
            "row 0 (2026-01-05) must not be lost to header misdetection"
        )

    def test_headered_csv_converges_via_printed_confirm_commands(
        self,
        db: Database,
        mocker: Any,
        tmp_path: Path,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """The headered twin: a real header row must still be detected as one."""
        csv_file = tmp_path / "headered_yyyymmdd.csv"
        csv_file.write_text(
            "Date,Amount,Description\n"
            "20260105,42.50,Coffee\n"
            "20260106,10.00,Tea\n"
            "20260107,-20.00,Groceries\n",
            encoding="utf-8",
        )

        self._converge(csv_file, db, mocker, caplog, has_header=True)

        rows = db.execute("SELECT COUNT(*) FROM raw.tabular_transactions").fetchone()
        assert rows is not None and rows[0] == 3

    def test_headerless_csv_under_a_spaced_directory_converges(
        self,
        db: Database,
        mocker: Any,
        tmp_path: Path,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """A directory name with a space must survive every printed hint.

        Every hint MoneyBin prints has to be the command it would accept
        back verbatim — a bare, unquoted path breaks that the moment the
        source file lives under a directory like "Bank Exports/". Both the
        `import confirm` and the `import preview` hints are run as printed;
        `test_every_printed_hint_runs_from_a_spaced_directory` covers the
        hints `import confirm` itself prints.
        """
        spaced_dir = tmp_path / "bank exports"
        spaced_dir.mkdir()
        csv_file = spaced_dir / "headerless_yyyymmdd.csv"
        csv_file.write_text(
            "20260105,42.50,Coffee\n20260106,10.00,Tea\n20260107,-20.00,Groceries\n",
            encoding="utf-8",
        )

        self._converge(csv_file, db, mocker, caplog, has_header=False)

        rows = db.execute("SELECT COUNT(*) FROM raw.tabular_transactions").fetchone()
        assert rows is not None and rows[0] == 3

    def test_headered_csv_under_a_spaced_directory_converges(
        self,
        db: Database,
        mocker: Any,
        tmp_path: Path,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """The headered twin of the spaced-directory convergence above."""
        spaced_dir = tmp_path / "bank exports"
        spaced_dir.mkdir()
        csv_file = spaced_dir / "headered_yyyymmdd.csv"
        csv_file.write_text(
            "Date,Amount,Description\n"
            "20260105,42.50,Coffee\n"
            "20260106,10.00,Tea\n"
            "20260107,-20.00,Groceries\n",
            encoding="utf-8",
        )

        self._converge(csv_file, db, mocker, caplog, has_header=True)

        rows = db.execute("SELECT COUNT(*) FROM raw.tabular_transactions").fetchone()
        assert rows is not None and rows[0] == 3

    def _spaced_headerless_csv(self, tmp_path: Path) -> Path:
        spaced_dir = tmp_path / "bank exports"
        spaced_dir.mkdir()
        csv_file = spaced_dir / "headerless_yyyymmdd.csv"
        csv_file.write_text(
            "20260105,42.50,Coffee\n20260106,10.00,Tea\n20260107,-20.00,Groceries\n",
            encoding="utf-8",
        )
        return csv_file

    def test_confirm_prints_runnable_hints_from_a_spaced_directory(
        self,
        db: Database,
        mocker: Any,
        tmp_path: Path,
    ) -> None:
        """`import confirm` prints its own hints, from its own handler.

        The convergence above only reaches the hints `import files` prints
        and the account-binding recovery. A rejected override leaves the
        layout unsettled, which is the branch where `import_confirm_command`
        builds its own accept-retry and preview hints.
        """
        mocker.patch(
            "moneybin.database.get_database",
            return_value=nullcontext(db),
        )
        csv_file = self._spaced_headerless_csv(tmp_path)

        result = runner.invoke(
            app,
            [
                "confirm",
                str(csv_file),
                "--date-format",
                "%Y%m%d",
                "--mapping",
                "transaction_date=no_such_column",
                "--output",
                "json",
            ],
        )
        assert result.exit_code == 0, result.output
        payload = json.loads(result.output)
        assert payload["data"]["status"] == "confirmation_required", payload

        ran = self._run_printed_preview_commands(
            payload["actions"], csv_file, expect_rows=3, expect_header=False
        )
        assert ran == 1
        retry = self._extract_confirm_command(payload["actions"])
        tokens = shlex.split(retry)
        assert str(csv_file) in tokens, tokens
        assert tokens[tokens.index("--date-format") + 1] == "%Y%m%d", tokens

    def test_confirm_hints_carry_the_limit_override_that_allowed_the_read(
        self,
        db: Database,
        mocker: Any,
        tmp_path: Path,
    ) -> None:
        """An oversized file's printed hints must carry `--no-row-limit`.

        `read_file` refuses above the 50,000-row threshold, so such a file
        reaches a confirmation only because the caller lifted the limit. A
        printed retry that drops the flag is refused in the read itself,
        before the confirmation it exists to resolve — and the preview hints
        are run verbatim here, so a hint that merely names a flag the command
        does not accept fails too.
        """
        mocker.patch(
            "moneybin.database.get_database",
            return_value=nullcontext(db),
        )
        rows = "\n".join(
            f"2026-01-{i % 28 + 1:02d},{i}.00,Item{i}" for i in range(50_001)
        )
        csv_file = tmp_path / "oversized.csv"
        csv_file.write_text(f"Date,Amount,Description\n{rows}\n")

        result = runner.invoke(
            app,
            [
                "confirm",
                str(csv_file),
                "--mapping",
                "transaction_date=no_such_column",
                "--no-row-limit",
                "--output",
                "json",
            ],
        )
        assert result.exit_code == 0, result.output
        payload = json.loads(result.output)
        assert payload["data"]["status"] == "confirmation_required", payload

        ran = self._run_printed_preview_commands(
            payload["actions"], csv_file, expect_rows=50_001, expect_header=True
        )
        assert ran == 1
        retry = self._extract_confirm_command(payload["actions"])
        tokens = shlex.split(retry)
        assert "--no-row-limit" in tokens, tokens

    def test_preview_reports_the_override_not_the_format_detection_fell_back_to(
        self,
        db: Database,
        mocker: Any,
        tmp_path: Path,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """An explicit --date-format outranks a detected one, as the import ranks them.

        `detect_date_format` drops a declaration that misses its 90% bar and
        falls through to the built-in scan, so a wrong `--date-format` over
        cleanly ISO dates leaves `%Y-%m-%d` standing as the *detected* format.
        `ImportService` resolves `date_format_override or
        mapping_result.date_format` (`import_service.py:2941`) and refuses on
        the override, so a preview reporting the detected value announced a
        format the import never uses and said nothing about the one that stops
        it. The import is run here too: a prediction is only worth printing if
        it is the import's actual behaviour.
        """
        mocker.patch(
            "moneybin.database.get_database",
            return_value=nullcontext(db),
        )
        csv_file = tmp_path / "iso_with_wrong_override.csv"
        csv_file.write_text(
            "Date,Amount,Description\n"
            "2026-01-05,42.50,Coffee\n"
            "2026-01-06,10.00,Tea\n"
            "2026-01-07,-20.00,Groceries\n",
            encoding="utf-8",
        )

        preview = runner.invoke(
            app, ["preview", str(csv_file), "--date-format", "%d/%m/%Y"]
        )
        assert preview.exit_code == 0, preview.output
        line = next(
            ln for ln in preview.output.splitlines() if ln.startswith("Date format:")
        )
        assert "%d/%m/%Y" in line, line
        assert "does not read the mapped column" in line, line
        # Detection's own reading of the column is the value the caller needs.
        assert re.search(r"Detection:\s+%Y-%m-%d", preview.output), preview.output

        imported = runner.invoke(
            app,
            [
                "files",
                str(csv_file),
                "--mapping",
                "transaction_date=Date",
                "--mapping",
                "amount=Amount",
                "--mapping",
                "description=Description",
                "--date-format",
                "%d/%m/%Y",
                "--account-id",
                "acct-iso",
                "--confirm",
                "--no-save-format",
                "--no-refresh",
            ],
        )
        assert imported.exit_code == 1, imported.output
        assert "could not read" in caplog.text, caplog.text

    @pytest.mark.parametrize("width", [40, 80])
    def test_preview_keeps_date_refusal_facts_at_narrow_widths(
        self, tmp_path: Path, mocker: Any, width: int
    ) -> None:
        """A wrapped receipt still names declared, detected, and refused meanings."""
        from moneybin.cli.terminal import TerminalPolicy, TerminalSymbols

        csv_file = tmp_path / "iso_dates.csv"
        csv_file.write_text(
            "Date,Amount,Description\n2026-01-05,42.50,Coffee\n",
            encoding="utf-8",
        )
        mocker.patch(
            "moneybin.cli.commands.import_cmd.get_terminal_policy",
            return_value=TerminalPolicy(
                output="text",
                interactive=False,
                page=False,
                color=False,
                style=False,
                animate_progress=False,
                stage_chatter=True,
                ascii=False,
                width=width,
                height=24,
                symbols=TerminalSymbols("✓", "!", "×", "›"),
                minus="−",
            ),
        )

        result = runner.invoke(
            app, ["preview", str(csv_file), "--date-format", "%d/%m/%Y"]
        )

        assert result.exit_code == 0, result.output
        assert "%d/%m/%Y" in result.output
        assert re.search(r"Detection:\s+%Y-%m-%d", result.output), result.output
        normalized = " ".join(result.output.split())
        assert "does not read the mapped column" in normalized
        assert "would refuse this date format" in normalized

    def test_confirm_hints_carry_the_size_override_that_allowed_the_read(
        self,
        db: Database,
        mocker: Any,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The size override's twin of the row-limit case, on its own gate.

        `--no-row-limit` is answered in `read_file` and `--no-size-limit` in
        `detect_format`, one stage earlier, so the row-limit test cannot reach
        this one's threading — its file clears the size check outright. The
        threshold is configuration rather than a constant, so a 0 MB limit
        refuses any non-empty file and keeps the fixture small; the refusal
        asserted first is what proves the gate is live rather than absent.
        """
        from moneybin.config import get_settings

        settings = get_settings()
        limited = settings.model_copy(
            update={
                "providers": settings.providers.model_copy(
                    update={
                        "tabular": settings.providers.tabular.model_copy(
                            update={"text_size_limit_mb": 0}
                        )
                    }
                )
            }
        )
        monkeypatch.setattr("moneybin.config.get_settings", lambda: limited)
        mocker.patch(
            "moneybin.database.get_database",
            return_value=nullcontext(db),
        )
        csv_file = tmp_path / "oversized.csv"
        csv_file.write_text(
            "Date,Amount,Description\n"
            "2026-01-01,42.50,Coffee\n"
            "2026-01-02,10.00,Tea\n"
            "2026-01-03,-20.00,Groceries\n"
        )
        confirm_argv = [
            "confirm",
            str(csv_file),
            "--mapping",
            "transaction_date=no_such_column",
            "--output",
            "json",
        ]

        # The gate must actually refuse this file, or everything below passes
        # for the wrong reason.
        refused = runner.invoke(app, confirm_argv)
        assert refused.exit_code != 0, refused.output

        result = runner.invoke(app, [*confirm_argv, "--no-size-limit"])
        assert result.exit_code == 0, result.output
        payload = json.loads(result.output)
        assert payload["data"]["status"] == "confirmation_required", payload

        ran = self._run_printed_preview_commands(
            payload["actions"], csv_file, expect_rows=3, expect_header=True
        )
        assert ran == 1
        retry = self._extract_confirm_command(payload["actions"])
        tokens = shlex.split(retry)
        assert "--no-size-limit" in tokens, tokens

    def test_confirm_mapping_failure_hint_is_runnable_from_a_spaced_directory(
        self,
        db: Database,
        mocker: Any,
        tmp_path: Path,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """The TTY branch's `💡 Inspect the proposal` hint is a command too.

        It goes to stderr rather than the envelope, so it is only reachable
        with a terminal attached — and it interpolates the same path.

        stderr specifically, never the logger: the hint repeats the caller's
        read options, and `--sheet`/`--format` are arbitrary user text that
        the log allowlist does not admit. The caplog assertion below is the
        half that fails if it is ever logged again.
        """
        import logging

        mocker.patch(
            "moneybin.database.get_database",
            return_value=nullcontext(db),
        )
        mock_sys = mocker.patch("moneybin.cli.commands.import_cmd.sys")
        mock_sys.stdout.isatty.return_value = True
        csv_file = self._spaced_headerless_csv(tmp_path)

        with caplog.at_level(logging.INFO):
            result = runner.invoke(
                app,
                [
                    "confirm",
                    str(csv_file),
                    "--date-format",
                    "%Y%m%d",
                    "--mapping",
                    "transaction_date=no_such_column",
                ],
            )

        assert result.exit_code == 1, result.output
        match = re.search(r"`(moneybin import preview[^`]*)`", result.output)
        assert match, result.output
        # The command carries the caller's read options, so it must not have
        # reached the log pipeline on its way to the terminal.
        assert "moneybin import preview" not in caplog.text, caplog.text
        tokens = shlex.split(match.group(1))
        assert str(csv_file) in tokens, tokens
        assert tokens[tokens.index("--date-format") + 1] == "%Y%m%d", tokens
        preview_result = runner.invoke(app, tokens[2:])
        assert preview_result.exit_code == 0, preview_result.output
        assert re.search(r"Parsed rows:\s+3", preview_result.output), (
            preview_result.output
        )
        assert "Header row detected: False" in preview_result.output, (
            preview_result.output
        )
