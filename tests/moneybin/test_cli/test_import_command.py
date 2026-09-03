"""Tests for the `moneybin import files` CLI command."""

from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest
from typer.testing import CliRunner

from moneybin.cli.commands.import_cmd import app
from moneybin.services.import_service import ImportRefreshError, ImportResult

runner = CliRunner()


@contextmanager
def _fake_db_ctx(*_args: object, **_kwargs: object) -> Generator[object, None, None]:
    """Stands in for both ``handle_cli_errors`` and ``get_database``.

    Accepts any argument because it replaces two functions with different
    signatures — ``handle_cli_errors(cli_actor=...)`` and
    ``get_database(read_only=...)``. Matches the doubles in
    ``test_cli_import_inbox.py`` and ``test_transactions_list.py``, which had
    already widened for the same reason.
    """
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


def test_import_files_single_no_knobs_surfaces_sign_correction_warning(
    csv_path: Path,
) -> None:
    """Single-file import (no per-file knobs) emits the sign-inversion warning.

    Single-path invocations always route through import_file (not import_files)
    so ImportConfirmationRequiredError can bubble.  The sign-correction warning
    still surfaces through this path.
    """

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
            ["files", str(csv_path)],
            catch_exceptions=False,
        )

    assert result.exit_code == 0, result.output
    assert "Sign convention may be inverted" in result.output


def test_import_files_single_surfaces_the_retirement_its_refresh_caused(
    csv_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """A single-file import names transfers its own refresh reversed.

    The single-file invocation routes through ``import_file``, whose refresh
    runs the same match step the batch path does. ``_single_file_success`` has
    to carry the count onto the batch it synthesizes, or the shared warning
    below it can never fire for the most common way to run this command.
    """

    def fake_run_import(**kwargs: Any) -> ImportResult:
        return ImportResult(
            file_path=str(kwargs["file_path"]),
            file_type="tabular",
            core_tables_rebuilt=True,
            transfers_retired=2,
        )

    with (
        patch("moneybin.cli.utils.handle_cli_errors", _fake_db_ctx),
        patch("moneybin.database.get_database", _fake_db_ctx),
        patch(
            "moneybin.services.import_service.ImportService.import_file",
            side_effect=fake_run_import,
        ),
    ):
        with caplog.at_level("WARNING"):
            result = runner.invoke(
                app, ["files", str(csv_path)], catch_exceptions=False
            )

    assert result.exit_code == 0, result.output
    # The helper reports through the project logger, which targets stderr.
    assert "Retired 2 previously accepted transfer(s)" in caplog.text
    assert "moneybin system audit undo" in caplog.text


def test_import_files_reports_the_best_effort_steps_its_refresh_ran(
    csv_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """An import runs four best-effort steps and reported none of them.

    ``import_file``'s refresh runs a matcher, a categorizer, an identity pass
    and a network-touching rate backfill. ``transforms_error`` covers only the
    SQLMesh apply, so a provider outage mid-import read as a clean import — and
    each of these outcomes carries a remedy the others do not.
    """
    from moneybin.services.refresh_outcome import RefreshStepOutcome

    def fake_run_import(**kwargs: Any) -> ImportResult:
        return ImportResult(
            file_path=str(kwargs["file_path"]),
            file_type="tabular",
            core_tables_rebuilt=True,
            refresh_steps=RefreshStepOutcome(
                categorization_error="categorizer blew up",
                rate_pairs_unsupported=("EUR/XTS",),
            ),
        )

    with (
        patch("moneybin.cli.utils.handle_cli_errors", _fake_db_ctx),
        patch("moneybin.database.get_database", _fake_db_ctx),
        patch(
            "moneybin.services.import_service.ImportService.import_file",
            side_effect=fake_run_import,
        ),
    ):
        with caplog.at_level("WARNING"):
            result = runner.invoke(
                app, ["files", str(csv_path)], catch_exceptions=False
            )

    assert result.exit_code == 0, result.output
    assert "Categorization step failed" in caplog.text
    assert "EUR/XTS" in caplog.text
    assert "moneybin fx set" in caplog.text


def test_import_files_retirement_warning_survives_quiet(
    csv_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """``-q`` drops informational output; it must not drop this.

    ``moneybin refresh``, ``moneybin sync pull``, and the inbox drain all emit
    this warning regardless of ``--quiet`` — the reversal is a decision the
    *user* made being undone, not a status line. Leaving ``import files`` as
    the one surface that swallows it would be the second pattern for one job.
    """

    def fake_run_import(**kwargs: Any) -> ImportResult:
        return ImportResult(
            file_path=str(kwargs["file_path"]),
            file_type="tabular",
            core_tables_rebuilt=True,
            transfers_retired=3,
        )

    with (
        patch("moneybin.cli.utils.handle_cli_errors", _fake_db_ctx),
        patch("moneybin.database.get_database", _fake_db_ctx),
        patch(
            "moneybin.services.import_service.ImportService.import_file",
            side_effect=fake_run_import,
        ),
    ):
        with caplog.at_level("INFO"):
            result = runner.invoke(
                app, ["files", str(csv_path), "--quiet"], catch_exceptions=False
            )

    assert result.exit_code == 0, result.output
    assert "Retired 3 previously accepted transfer(s)" in caplog.text
    # -q still suppresses the per-file status line it is meant to suppress.
    assert "✅" not in caplog.text


def test_import_files_reports_a_retirement_its_failed_refresh_committed(
    csv_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """A failed transform must not swallow a reversal the match step committed.

    The refresh reconciles inside its *match* step and commits there, so a
    transform apply that dies afterwards leaves the reversal on disk. Neither
    of the two paths that report it can see this one: the raise discards
    ``ImportResult``, and the exception escapes past the success-path warning
    entirely rather than landing in ``_single_file_failure`` (which catches
    only ``ValueError``/``PermissionError``). So the count rides on the
    exception, the way ``MatchRunError`` carries a partial run.
    """

    def fake_run_import(**kwargs: Any) -> ImportResult:
        _ = kwargs
        raise ImportRefreshError(
            "SQLMesh transforms failed: apply reported no plan",
            transfers_retired=2,
        )

    with (
        patch("moneybin.cli.utils.handle_cli_errors", _fake_db_ctx),
        patch("moneybin.database.get_database", _fake_db_ctx),
        patch(
            "moneybin.services.import_service.ImportService.import_file",
            side_effect=fake_run_import,
        ),
    ):
        with caplog.at_level("WARNING"):
            result = runner.invoke(app, ["files", str(csv_path)])

    # The failure still fails: this discloses the reversal, it does not absolve
    # the transform.
    assert result.exit_code != 0
    assert "Retired 2 previously accepted transfer(s)" in caplog.text
    assert "moneybin system audit undo" in caplog.text


def test_import_file_surfaces_ratified_sign_replay_note(csv_path: Path) -> None:
    """A replayed `--sign` override must be visible, not silent.

    The override disarms the card-marker guard for this format on every future
    statement. That is a durable decision acting without re-asking, so the import
    says so — the user's condition for having a durable override at all.
    """

    def fake_run_import(**kwargs: Any) -> ImportResult:
        return ImportResult(
            file_path=str(kwargs["file_path"]),
            file_type="pdf",
            sign_override_replayed=True,
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
        result = runner.invoke(app, ["files", str(csv_path)], catch_exceptions=False)

    assert result.exit_code == 0, result.output
    assert "saved --sign override" in result.output


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


_HISTORY_RECORDS = [
    {
        "import_id": "imp_0123456789ab",
        "status": "completed",
        "rows_imported": 1204,
        "rows_rejected": 0,
        "source_file": "/data/statements/january.csv",
    },
    {
        "import_id": "imp_ba9876543210",
        "status": "failed",
        "rows_imported": 0,
        "rows_rejected": 17,
        "source_file": "/data/statements/february.csv",
    },
]


def _patched_history() -> Any:
    return patch(
        "moneybin.extractors.tabular.TabularExtractor.get_import_history",
        return_value=_HISTORY_RECORDS,
    )


def test_import_history_renders_a_table_not_a_padded_rule(
    wide_terminal: None,
) -> None:
    """Requirement 1: the roll drew its own header and a 100-character rule.

    The rule was a fixed width regardless of the terminal or the values, so a
    long source path ran past it and a short one left it dangling.
    """
    with patch("moneybin.database.get_database", _fake_db_ctx), _patched_history():
        result = runner.invoke(app, ["history", "--wide"], catch_exceptions=False)

    assert result.exit_code == 0, result.output
    assert "┃" in result.stdout
    assert "-" * 100 not in result.stdout
    for header in ("import", "status", "imported", "rejected", "source file"):
        assert header in result.stdout
    # The whole path, which is what tells two same-named imports apart.
    assert "/data/statements/january.csv" in result.stdout


def test_import_history_distinguishes_same_named_files(wide_terminal: None) -> None:
    """The same file name under two directories is two imports, not one.

    `source_file` is part of `raw.tabular_transactions`' dedup key, so the same
    content read from a different path is a different import. The column exists
    to answer which one, and a basename cannot.
    """
    records = [
        dict(_HISTORY_RECORDS[0], source_file="/checking/january.csv"),
        dict(_HISTORY_RECORDS[1], source_file="/savings/january.csv"),
    ]
    with (
        patch("moneybin.database.get_database", _fake_db_ctx),
        patch(
            "moneybin.extractors.tabular.TabularExtractor.get_import_history",
            return_value=records,
        ),
    ):
        result = runner.invoke(app, ["history", "--wide"], catch_exceptions=False)

    assert result.exit_code == 0, result.output
    assert "/checking/january.csv" in result.stdout
    assert "/savings/january.csv" in result.stdout


def test_import_history_renders_one_row_per_import(wide_terminal: None) -> None:
    """Requirement 35: two imports are two rows, whatever they have in common."""
    with patch("moneybin.database.get_database", _fake_db_ctx), _patched_history():
        result = runner.invoke(app, ["history"], catch_exceptions=False)

    assert result.exit_code == 0, result.output
    assert "imp_0123456789ab" in result.stdout
    assert "imp_ba9876543210" in result.stdout
    # A row count is not an amount, so it renders unseparated. Accepting either
    # spelling made this assertion inert; `1,204` would mean a count had been
    # routed through `format_money`.
    assert "1204" in result.stdout
    assert "1,204" not in result.stdout
