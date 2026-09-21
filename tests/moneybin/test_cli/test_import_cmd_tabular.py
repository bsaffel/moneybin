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

        import click
        import typer.main

        group = cast(click.Group, typer.main.get_command(app))
        preview_cmd: click.Command = group.commands["preview"]
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

    def test_preview_header_position_ambiguous_uses_shared_recovery_text(
        self, tmp_path: Path, caplog: LogCaptureFixture
    ) -> None:
        """`import preview` has no --confirm flag.

        The hand-rolled warning this replaced told the user to "re-run with
        --confirm to proceed" — a flag `import preview` itself does not
        register (it lives on `import files` / `import confirm`). Reusing
        the shared ``header_position_ambiguous_recovery`` helper names the
        commands that actually clear the gate. The helper's own text stays
        row-free (it can reach the log pipeline); the disputed row(s) go
        through ``echo_disputed_rows`` (allowlisted via ``disputed_row_
        fields``) on stderr only, never a log record.
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
        assert any(expected in r.message for r in caplog.records), caplog.text
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

        Regression: `import preview` has no --date-format flag, so no
        declared time-bearing format can ever reach this command's
        normalize-before-map step. Headers are deliberately unaliased
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
