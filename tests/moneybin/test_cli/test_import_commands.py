# ruff: noqa: S101,S106
# TestConfirmationEnvelopeData tests the module-private _confirmation_envelope_data builder:
# pyright: reportPrivateUsage=false
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
        mock_import_file: MagicMock,
        mock_get_database: MagicMock,
        tmp_path: Path,
    ) -> None:
        """Single-file import always uses import_file directly (not the batch service).

        Changed from the original batch-for-no-knobs behavior: single-path
        invocations now always call import_file so ImportConfirmationRequiredError
        can bubble to the CLI handler.
        """
        test_file = tmp_path / "test.ofx"
        test_file.touch()

        result = runner.invoke(app, ["files", str(test_file)])
        assert result.exit_code == 0, result.output
        mock_import_file.assert_called_once()
        call_kwargs = mock_import_file.call_args.kwargs
        assert call_kwargs["refresh"] is True
        assert call_kwargs["force"] is False
        assert call_kwargs["confirm"] is False
        assert call_kwargs["actor_kind"] == "human"

    def test_import_files_no_refresh(
        self,
        runner: CliRunner,
        mock_import_file: MagicMock,
        mock_get_database: MagicMock,
        tmp_path: Path,
    ) -> None:
        """--no-refresh forwards refresh=False to import_file."""
        test_file = tmp_path / "test.ofx"
        test_file.touch()

        result = runner.invoke(app, ["files", str(test_file), "--no-refresh"])
        assert result.exit_code == 0, result.output
        call_kwargs = mock_import_file.call_args.kwargs
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
            confirm=False,
            actor_kind="human",
        )

    def test_import_files_force_flag(
        self,
        runner: CliRunner,
        mock_import_file: MagicMock,
        mock_get_database: MagicMock,
        tmp_path: Path,
    ) -> None:
        """--force is forwarded to import_file as force=True."""
        test_file = tmp_path / "test.ofx"
        test_file.touch()

        result = runner.invoke(app, ["files", str(test_file), "--force"])
        assert result.exit_code == 0, result.output
        call_kwargs = mock_import_file.call_args.kwargs
        assert call_kwargs["force"] is True

    def test_force_already_imported_error(
        self,
        runner: CliRunner,
        mock_import_file: MagicMock,
        mock_get_database: MagicMock,
        tmp_path: Path,
    ) -> None:
        """import_file raising ValueError surfaces as a classified error envelope.

        The classification happens in handle_cli_errors, which owns every
        exception it recognizes on this path — hence a real `error.code` rather
        than the bare exception class name.
        """
        import json

        test_file = tmp_path / "test.ofx"
        test_file.touch()
        mock_import_file.side_effect = ValueError("already imported")

        result = runner.invoke(app, ["files", str(test_file)])
        assert result.exit_code == 1

        result = runner.invoke(app, ["files", str(test_file), "--output", "json"])
        assert result.exit_code == 1
        payload = json.loads(result.stdout)
        assert payload["status"] == "error"
        assert payload["error"]["code"] == "infra_invalid_input"
        assert payload["error"]["message"] == "already imported"

    def test_import_files_not_found(
        self,
        runner: CliRunner,
    ) -> None:
        """Exit code 1 when a single missing file is passed (typo detection)."""
        result = runner.invoke(app, ["files", "/nonexistent/file.ofx"])
        assert result.exit_code == 1

    def test_single_file_tcc_denial_is_classified_not_a_traceback(
        self,
        runner: CliRunner,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The headline scenario: a TCC-blocked single-file import.

        `Path.exists()` is itself a raising call — pathlib only swallows
        ENOENT/ENOTDIR/EBADF/ELOOP, so a macOS TCC denial (EPERM) propagates
        out of the preflight check. When that check sat outside
        `handle_cli_errors`, this exact invocation produced an unhandled
        traceback instead of the Full Disk Access guidance the affordance
        exists to give.

        The path sits under `~/Documents` and the platform is pinned to Darwin
        because together those select the Full-Disk-Access branch of
        `permission_advice` — an EPERM on another platform, or elsewhere on
        macOS, correctly gets generic advice instead. Pinning rather than
        skipping keeps this running on Linux CI: like
        `test_permission_advice.py`, it asserts OUR branching for a given
        platform, not the host's actual behavior.
        """
        monkeypatch.setattr("moneybin.errors.platform.system", lambda: "Darwin")
        target = Path.home() / "Documents" / "statement.qfx"
        real_exists = Path.exists

        def _deny(self: Path, **kwargs: Any) -> bool:
            # Scoped to the target so the privacy-log writer's own path checks
            # keep working — a blanket denial makes this test log write failures.
            if self == target:
                raise PermissionError(1, "Operation not permitted", str(target))
            return real_exists(self, **kwargs)

        monkeypatch.setattr(Path, "exists", _deny)

        result = runner.invoke(app, ["files", str(target), "--output", "json"])

        assert result.exit_code == 1, result.output
        assert result.exception is None or isinstance(result.exception, SystemExit), (
            f"unhandled exception leaked: {result.exception!r}"
        )
        import json

        payload = json.loads(result.stdout)
        assert payload["status"] == "error"
        assert payload["error"]["code"] == "infra_permission_denied"
        assert "Full Disk Access" in payload["error"]["hint"]

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
            confirm=False,
            actor_kind="human",
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
            confirm=False,
            actor_kind="human",
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
        mock_import_file: MagicMock,
        mock_get_database: MagicMock,
        tmp_path: Path,
    ) -> None:
        """--output json emits the envelope shape with batch fields under data."""
        import json

        # A replayed sign override on the imported file: the JSON must carry the
        # signal's VALUE, not merely the key. A key that is always present and
        # always False would satisfy a presence check while telling a scripted
        # caller nothing.
        mock_import_file.return_value = ImportResult(
            file_path="test.ofx",
            file_type="ofx",
            accounts=2,
            transactions=15,
            sign_override_replayed=True,
        )
        test_file = tmp_path / "test.ofx"
        test_file.touch()
        result = runner.invoke(app, ["files", str(test_file), "--output", "json"])
        assert result.exit_code == 0, result.output
        # stdout, not output: the replay note goes to stderr, and parsing here
        # doubles as the proof that it stays out of the JSON document.
        payload = json.loads(result.stdout)
        assert payload["data"]["imported_count"] == 1
        assert payload["data"]["total_count"] == 1
        assert "files" in payload["data"]
        assert payload["summary"]["sensitivity"] == "low"
        # The single-file success entry must carry both sign signals, matching
        # the batch path — JSON-output agents see the same shape regardless of
        # single-vs-multi-file invocation. sign_override_replayed is the only
        # channel a scripted caller has for "a saved --sign override replayed and
        # the card detector was skipped"; the TTY path echoes it to stderr. The
        # two signals are distinct — the False proves they aren't crossed.
        assert payload["data"]["files"][0]["sign_override_replayed"] is True
        assert payload["data"]["files"][0]["sign_correction_suggested"] is False

    def test_batch_envelope_sensitivity_medium_when_confirmation_payload_present(
        self,
        runner: CliRunner,
        mock_import_files: MagicMock,
        mock_get_database: MagicMock,
        tmp_path: Path,
    ) -> None:
        """Batch envelope is sensitivity=medium with any confirmation_payload.

        Those payloads include detector samples (description / merchant
        cells) and must match the single-file confirmation_required
        envelope's medium tier so agents apply the same consent gate to
        batch proposals.
        """
        import json

        a = tmp_path / "a.csv"
        b = tmp_path / "b.ofx"
        a.touch()
        b.touch()
        mock_import_files.return_value = BatchImportResult(
            per_file=[
                PerFileResult(
                    path=str(a),
                    status="confirmation_required",
                    source_type=None,
                    rows_loaded=0,
                    import_id=None,
                    confirmation_payload={
                        "channel": "tabular",
                        "samples": {"Memo": ["Coffee", "Lunch"]},
                    },
                ),
                PerFileResult(
                    path=str(b),
                    status="imported",
                    source_type="ofx",
                    rows_loaded=1,
                    import_id="x",
                ),
            ],
            transforms_applied=True,
            transforms_duration_seconds=None,
        )
        result = runner.invoke(app, ["files", str(a), str(b), "--output", "json"])
        assert result.exit_code == 0, result.output
        payload = json.loads(result.output)
        assert payload["summary"]["sensitivity"] == "medium"

    def test_failed_file_json_carries_error_code_and_hint(
        self,
        runner: CliRunner,
        mock_import_files: MagicMock,
        mock_get_database: MagicMock,
        tmp_path: Path,
    ) -> None:
        """A failed file's JSON row carries error, error_code, and hint.

        The batch stays "ok" here on purpose: one file imported, so this
        isolates the per-file projection from the all-failed gate below.
        `error_code` is what a scripted caller branches on and `hint` is the
        only thing that tells it how to recover — the classified message alone
        names the problem without naming the fix.
        """
        import json

        a = tmp_path / "a.csv"
        b = tmp_path / "b.csv"
        a.touch()
        b.touch()
        mock_import_files.return_value = BatchImportResult(
            per_file=[
                PerFileResult(
                    path=str(a),
                    status="failed",
                    source_type=None,
                    error="Operation not permitted: a.csv",
                    error_code="infra_permission_denied",
                    hint="💡 Grant Full Disk Access, then restart.",
                ),
                PerFileResult(
                    path=str(b),
                    status="imported",
                    source_type="csv",
                    rows_loaded=3,
                    import_id="x",
                ),
            ],
            transforms_applied=True,
            transforms_duration_seconds=None,
        )
        result = runner.invoke(app, ["files", str(a), str(b), "--output", "json"])
        assert result.exit_code == 0, result.output
        payload = json.loads(result.stdout)
        assert payload["status"] == "ok"
        failed_row = payload["data"]["files"][0]
        assert failed_row["error"] == "Operation not permitted: a.csv"
        assert failed_row["error_code"] == "infra_permission_denied"
        assert failed_row["hint"] == "💡 Grant Full Disk Access, then restart."
        # The imported row must not sprout empty error keys — a scripted caller
        # tests for the key's presence, not its truthiness.
        imported_row = payload["data"]["files"][1]
        assert "error" not in imported_row
        assert "error_code" not in imported_row
        assert "hint" not in imported_row

    def test_unclassified_failure_json_omits_error_code_and_hint(
        self,
        runner: CliRunner,
        mock_import_files: MagicMock,
        mock_get_database: MagicMock,
        tmp_path: Path,
    ) -> None:
        """An unclassified failure reports the class name with no code or hint.

        The privacy invariant: raw ``str(e)`` can embed file contents, so an
        exception MoneyBin doesn't recognize surfaces only its type name — and
        must not acquire a code or a hint it was never classified with.
        """
        import json

        a = tmp_path / "a.csv"
        b = tmp_path / "b.csv"
        a.touch()
        b.touch()
        mock_import_files.return_value = BatchImportResult(
            per_file=[
                PerFileResult(
                    path=str(a),
                    status="failed",
                    source_type=None,
                    error="RuntimeError",
                ),
                PerFileResult(
                    path=str(b),
                    status="imported",
                    source_type="csv",
                    rows_loaded=3,
                    import_id="x",
                ),
            ],
            transforms_applied=True,
            transforms_duration_seconds=None,
        )
        result = runner.invoke(app, ["files", str(a), str(b), "--output", "json"])
        assert result.exit_code == 0, result.output
        failed_row = json.loads(result.stdout)["data"]["files"][0]
        assert failed_row["error"] == "RuntimeError"
        assert "error_code" not in failed_row
        assert "hint" not in failed_row

    def test_all_failed_batch_reports_error_status_and_exits_nonzero(
        self,
        runner: CliRunner,
        mock_import_files: MagicMock,
        mock_get_database: MagicMock,
        tmp_path: Path,
    ) -> None:
        """A batch where every file failed is a failure on the CLI too.

        Parity with the MCP ``import_files`` tool: a nightly script running
        ``moneybin import files *.csv --output json`` must not read ``ok`` and
        exit 0 when nothing landed.
        """
        import json

        a = tmp_path / "a.csv"
        b = tmp_path / "b.csv"
        a.touch()
        b.touch()
        mock_import_files.return_value = BatchImportResult(
            per_file=[
                PerFileResult(
                    path=str(a),
                    status="failed",
                    source_type=None,
                    error="Operation not permitted: a.csv",
                    error_code="infra_permission_denied",
                    hint="💡 Grant Full Disk Access, then restart.",
                ),
                PerFileResult(
                    path=str(b),
                    status="failed",
                    source_type=None,
                    error="Operation not permitted: b.csv",
                    error_code="infra_permission_denied",
                    hint="💡 Grant Full Disk Access, then restart.",
                ),
            ],
            transforms_applied=False,
            transforms_duration_seconds=None,
        )
        result = runner.invoke(app, ["files", str(a), str(b), "--output", "json"])
        assert result.exit_code == 1, result.output
        payload = json.loads(result.stdout)
        assert payload["status"] == "error"
        assert payload["error"]["code"] == "infra_permission_denied"
        # The batch message counts rather than hoisting one file's reason; the
        # per-file detail stays in data.files[].
        assert "all 2 file(s)" in payload["error"]["message"]
        assert len(payload["data"]["files"]) == 2

    def test_all_failed_batch_exits_nonzero_in_text_mode(
        self,
        runner: CliRunner,
        mock_import_files: MagicMock,
        mock_get_database: MagicMock,
        tmp_path: Path,
    ) -> None:
        """The non-zero exit is not conditional on --output json.

        A shell pipeline checking ``$?`` gets the same verdict as an agent
        parsing the envelope.
        """
        a = tmp_path / "a.csv"
        b = tmp_path / "b.csv"
        a.touch()
        b.touch()
        mock_import_files.return_value = BatchImportResult(
            per_file=[
                PerFileResult(path=str(a), status="failed", source_type=None),
                PerFileResult(path=str(b), status="failed", source_type=None),
            ],
            transforms_applied=False,
            transforms_duration_seconds=None,
        )
        result = runner.invoke(app, ["files", str(a), str(b)])
        assert result.exit_code == 1, result.output

    def test_text_mode_batch_shows_why_a_file_failed(
        self,
        runner: CliRunner,
        mock_import_files: MagicMock,
        mock_get_database: MagicMock,
        tmp_path: Path,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Text mode is the CLI default, so it cannot be the silent one.

        The batch renderer printed only `❌ <path> [?] — 0 rows`, dropping the
        per-file `error` and `hint` the rest of this change adds. That made the
        recovery advice reachable only via `--output json` — the headline
        scenario said nothing useful to a human running the bare command.
        """
        a = tmp_path / "a.csv"
        b = tmp_path / "b.csv"
        a.touch()
        b.touch()
        mock_import_files.return_value = BatchImportResult(
            per_file=[
                PerFileResult(
                    path=str(a),
                    status="failed",
                    source_type=None,
                    error="Operation not permitted: a.csv",
                    error_code="infra_permission_denied",
                    hint="💡 Grant Full Disk Access, then restart.",
                ),
                PerFileResult(
                    path=str(b), status="imported", source_type="tabular", rows_loaded=3
                ),
            ],
            transforms_applied=False,
            transforms_duration_seconds=None,
        )

        with caplog.at_level("INFO"):
            result = runner.invoke(app, ["files", str(a), str(b)])

        assert result.exit_code == 0, result.output  # partial success
        assert "Operation not permitted" in caplog.text
        assert "Grant Full Disk Access" in caplog.text

    def test_batch_with_a_failed_file_declares_medium_sensitivity(
        self,
        runner: CliRunner,
        mock_import_files: MagicMock,
        mock_get_database: MagicMock,
        tmp_path: Path,
    ) -> None:
        """`error`/`hint` are DESCRIPTION-tier prose, so the batch is medium.

        `ImportPerFileRow` annotates both as `DataClass.DESCRIPTION`, which the
        MCP path derives MEDIUM from. The CLI derives its tier inline and only
        looked at `confirmation_payload`, so once this change started putting
        `error`/`hint` on the wire the CLI under-declared the same data as
        `low` — and the paired privacy-audit row inherited that.
        """
        import json

        a = tmp_path / "a.csv"
        b = tmp_path / "b.csv"
        a.touch()
        b.touch()
        mock_import_files.return_value = BatchImportResult(
            per_file=[
                PerFileResult(
                    path=str(a),
                    status="failed",
                    source_type=None,
                    error="Operation not permitted: a.csv",
                    error_code="infra_permission_denied",
                    hint="💡 Grant Full Disk Access, then restart.",
                ),
                PerFileResult(
                    path=str(a), status="imported", source_type="tabular", rows_loaded=1
                ),
            ],
            transforms_applied=False,
            transforms_duration_seconds=None,
        )

        result = runner.invoke(app, ["files", str(a), str(b), "--output", "json"])

        payload = json.loads(result.stdout)
        assert payload["summary"]["sensitivity"] == "medium"

    def test_clean_batch_stays_low_sensitivity(
        self,
        runner: CliRunner,
        mock_import_files: MagicMock,
        mock_get_database: MagicMock,
        tmp_path: Path,
    ) -> None:
        """The bump is conditional — an all-imported batch carries no prose.

        Pairs with the test above so the fix can't be satisfied by declaring
        everything medium, which would over-gate every successful import.
        """
        import json

        a = tmp_path / "a.csv"
        b = tmp_path / "b.csv"
        a.touch()
        b.touch()
        mock_import_files.return_value = BatchImportResult(
            per_file=[
                PerFileResult(
                    path=str(a), status="imported", source_type="tabular", rows_loaded=1
                )
            ],
            transforms_applied=False,
            transforms_duration_seconds=None,
        )

        result = runner.invoke(app, ["files", str(a), str(b), "--output", "json"])

        payload = json.loads(result.stdout)
        assert payload["summary"]["sensitivity"] == "low"

    def test_text_mode_batch_stays_quiet_under_quiet_flag(
        self,
        runner: CliRunner,
        mock_import_files: MagicMock,
        mock_get_database: MagicMock,
        tmp_path: Path,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """`-q` suppresses the per-file block, error lines included."""
        a = tmp_path / "a.csv"
        b = tmp_path / "b.csv"
        a.touch()
        b.touch()
        mock_import_files.return_value = BatchImportResult(
            per_file=[
                PerFileResult(
                    path=str(a),
                    status="failed",
                    source_type=None,
                    error="Operation not permitted: a.csv",
                    hint="💡 Grant Full Disk Access, then restart.",
                ),
                PerFileResult(
                    path=str(a), status="imported", source_type="tabular", rows_loaded=1
                ),
            ],
            transforms_applied=False,
            transforms_duration_seconds=None,
        )

        with caplog.at_level("INFO"):
            runner.invoke(app, ["files", str(a), str(b), "--quiet"])

        assert "Grant Full Disk Access" not in caplog.text


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


class TestConfirmationEnvelopeData:
    """`_confirmation_envelope_data` is the single CLI confirmation_required builder.

    It must produce the canonical `confirmation_payload_dict` shape (so the CLI
    and MCP `confirmation_required` envelopes cannot drift) plus a leading
    `status` field — including `bridge_payload` for the PDF bridge channel.
    """

    @staticmethod
    def _bridge_outcome() -> Any:
        from moneybin.extractors.confidence import Confidence
        from moneybin.services.import_confirmation import (
            BridgePayload,
            ConfirmationRequired,
        )

        return ConfirmationRequired(
            channel="pdf",
            confidence=Confidence(
                score=0.4, tier="low", flagged=(), missing_required=()
            ),
            proposed=BridgePayload(payload={"ir": "request"}),
            reason="validation_failure",
        )

    def test_bridge_payload_is_carried(self) -> None:
        from moneybin.cli.commands.import_cmd import _confirmation_envelope_data

        data = _confirmation_envelope_data(self._bridge_outcome())
        assert data["bridge_payload"] == {"ir": "request"}

    def test_matches_canonical_helper_plus_status(self) -> None:
        from moneybin.cli.commands.import_cmd import _confirmation_envelope_data
        from moneybin.services.import_confirmation import confirmation_payload_dict

        outcome = self._bridge_outcome()
        data = _confirmation_envelope_data(outcome)
        assert data == {
            "status": "confirmation_required",
            **confirmation_payload_dict(outcome),
        }
