"""A committed confirmation must retain planner failures and archive its file."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock

import pytest
import typer
from pytest_mock import MockerFixture
from typer.testing import CliRunner

from moneybin import error_codes
from moneybin.cli.commands.import_cmd import (
    _complete_import_confirmation,  # pyright: ignore[reportPrivateUsage]  # shared confirmation boundary
    app,
)
from moneybin.cli.output import OutputFormat
from moneybin.extractors.confidence import Confidence
from moneybin.services.import_confirmation import (
    ConfirmationRequired,
    ImportConfirmationRequiredError,
    ProposedMapping,
)
from moneybin.services.import_service import ImportResult
from moneybin.services.inbox_service import InboxService
from moneybin.services.refresh_outcome import RefreshStepOutcome, StageOutcome


@pytest.fixture
def pending_file(tmp_path: Path, mocker: MockerFixture) -> Path:
    inbox = InboxService(db=None, settings=MagicMock(profile_inbox_dir=tmp_path))
    path = inbox.pending_dir / "2026-10" / "statement.csv"
    path.parent.mkdir(parents=True)
    path.write_text("Date,Amount,Memo\n2026-10-01,-3,Example\n")
    path.with_name(path.name + ".pending.yml").write_text("reason: unknown_layout\n")
    mocker.patch(
        "moneybin.services.inbox_service.InboxService.for_active_profile_no_db",
        return_value=inbox,
    )
    mocker.patch("moneybin.database.get_database", return_value=MagicMock())
    return path


def _committed_result(path: Path, *, bounded: bool) -> ImportResult:
    return ImportResult(
        file_path=str(path),
        file_type="csv",
        transactions=3,
        import_id="saved-import",
        refresh_steps=RefreshStepOutcome(
            requested_steps=("investment_match", "transform"),
            stages=(
                StageOutcome(
                    "investment_match",
                    ran=True,
                    error="Component exceeds safe bound"
                    if bounded
                    else "Planner failed",
                    error_code=(
                        error_codes.INVESTMENT_MATCH_COMPONENT_TOO_LARGE
                        if bounded
                        else None
                    ),
                ),
                StageOutcome("transform", ran=False),
            ),
        ),
    )


def _assert_archived(path: Path) -> None:
    assert not path.exists()
    assert not path.with_name(path.name + ".pending.yml").exists()
    archived = path.parents[2] / "processed" / "2026-10" / path.name
    assert archived.read_text() == "Date,Amount,Memo\n2026-10-01,-3,Example\n"


@pytest.mark.parametrize("bounded", [False, True])
@pytest.mark.parametrize("output", [OutputFormat.TEXT, OutputFormat.JSON])
@pytest.mark.parametrize("quiet", [False, True])
def test_confirmation_preserves_committed_planner_block(
    pending_file: Path,
    mocker: MockerFixture,
    capsys: pytest.CaptureFixture[str],
    bounded: bool,
    output: OutputFormat,
    quiet: bool,
) -> None:
    imported = mocker.patch(
        "moneybin.services.import_service.ImportService.import_file",
        return_value=_committed_result(pending_file, bounded=bounded),
    )

    with pytest.raises(typer.Exit) as failure:
        _complete_import_confirmation(
            cli_actor="import_files_command",
            file_path=pending_file,
            accept=True,
            mapping=[],
            format_name=None,
            bridge_response=None,
            confirm=False,
            confirm_sign=False,
            sign=None,
            date_format=None,
            number_format=None,
            sheet=None,
            delimiter=None,
            encoding=None,
            no_row_limit=False,
            no_size_limit=False,
            institution=None,
            account_id=None,
            account_name=None,
            account_binding=[],
            account_meta=[],
            save_format=False,
            output=output,
            quiet=quiet,
            refresh=True,
        )

    assert failure.value.exit_code == 1
    imported.assert_called_once()
    assert imported.call_args.kwargs["refresh"] is True
    _assert_archived(pending_file)
    captured = capsys.readouterr()
    assert "moneybin transform apply" not in captured.out
    if output == OutputFormat.JSON:
        payload = json.loads(captured.out)
        assert payload["data"]["status"] == "imported"
        assert payload["data"]["import_id"] == "saved-import"
        assert payload["data"]["rows_loaded"] == 3
        stages = {stage["step"]: stage for stage in payload["data"]["stages"]}
        assert stages["investment_match"]["error"]
        assert stages["transform"]["ran"] is False
        retry_steps = [
            action["arguments"]["steps"]
            for action in payload.get("recovery_actions") or []
            if action["tool"] == "refresh_run"
        ]
        assert retry_steps == ([] if bounded else [["transform"]])
        if bounded:
            assert any("unchanged" in hint for hint in payload["actions"])
    else:
        assert "3 rows" in captured.out
        assert "saved-import" in captured.out
        assert "refresh blocked" in captured.out.lower()
        assert "Import complete" not in captured.out
        if bounded:
            assert "unchanged" in captured.out
            assert "moneybin refresh" not in captured.out
        else:
            assert "moneybin refresh --steps transform" in captured.out


def test_interactive_import_files_archives_after_planner_block(
    pending_file: Path, mocker: MockerFixture
) -> None:
    mapping = {"transaction_date": "Date", "amount": "Amount", "description": "Memo"}
    imported = mocker.patch(
        "moneybin.services.import_service.ImportService.import_file",
        side_effect=[
            ImportConfirmationRequiredError(
                ConfirmationRequired(
                    channel="tabular",
                    reason="unknown_layout",
                    confidence=Confidence(
                        score=0.55, tier="medium", flagged=(), missing_required=()
                    ),
                    proposed=ProposedMapping(
                        field_mapping=mapping, sample_values={}, unmapped_columns=()
                    ),
                    samples={},
                )
            ),
            _committed_result(pending_file, bounded=False),
        ],
    )
    mocker.patch(
        "moneybin.services.import_service.ImportService.prepare_tabular_import",
        return_value=MagicMock(
            confirmation=None,
            plan=MagicMock(
                field_mapping=mapping, sign_convention="negative_is_expense"
            ),
        ),
    )
    mocker.patch(
        "moneybin.cli.commands.import_cmd.get_terminal_policy",
        return_value=MagicMock(
            interactive=True,
            width=160,
            height=24,
            output="text",
            page=False,
            style=False,
            color=False,
        ),
    )
    mocker.patch(
        "moneybin.cli.commands.import_cmd.choose_required",
        side_effect=["accept", "confirm"],
    )

    result = CliRunner().invoke(app, ["files", str(pending_file)])

    assert result.exit_code == 1, result.output
    assert imported.call_count == 2
    assert imported.call_args.kwargs["refresh"] is True
    assert "refresh blocked" in result.output.lower()
    _assert_archived(pending_file)
