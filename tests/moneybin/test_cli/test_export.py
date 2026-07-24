"""CLI coverage for export delivery and saved destinations."""

from __future__ import annotations

import json
import subprocess  # noqa: S404  # shell-level artifact inspection is the contract
import sys
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
import typer
from typer.testing import CliRunner

from moneybin.cli.main import app
from moneybin.errors import UserError
from moneybin.exports.models import ExportDestination, ExportReceipt
from moneybin.exports.service import (
    ExportDestinationReadiness,
    ExportReadinessStatus,
)
from moneybin.services.audit_service import AuditEvent
from moneybin.services.entity_reference import AmbiguousEntity

runner = CliRunner()


def _local_destination(path: Path, *, name: str = "local:exports") -> ExportDestination:
    return ExportDestination(
        destination_id=None,
        name=name,
        kind="local",
        local_path=path.resolve(),
        spreadsheet_id=None,
        managed_tab_prefix=None,
    )


def _sheets_destination(*, name: str = "dashboard") -> ExportDestination:
    return ExportDestination(
        destination_id="dst_sheet_1",
        name=name,
        kind="sheets",
        local_path=None,
        spreadsheet_id="sheet_abc",
        managed_tab_prefix="MoneyBin",
    )


def _receipt(
    destination: ExportDestination, artifact: Path | None = None
) -> ExportReceipt:
    return ExportReceipt(
        subject={"kind": "bundle"},
        format="csv" if artifact is not None else "sheets",
        redaction_mode="redacted",
        destination=destination,
        artifact_path=artifact.resolve() if artifact is not None else None,
        compressed_artifact_path=None,
        sheets_identity=("MoneyBin:20260721T120000Z" if artifact is None else None),
        row_counts={"accounts": 2, "transactions": 4},
        output_classes={
            "accounts": {"account_id": "record_id"},
            "transactions": {"amount": "txn_amount"},
        },
        checksums={"accounts": "abc123", "transactions": "def456"},
        recovery_actions=(),
    )


def _settings(exports_dir: Path) -> SimpleNamespace:
    return SimpleNamespace(
        profile="test",
        profile_exports_dir=exports_dir.resolve(),
        mcp=SimpleNamespace(max_rows=1_000),
    )


def _completed_run(
    receipt: ExportReceipt,
    destination: ExportDestination,
):
    def run(
        _command: object,
        *,
        actor: str,
        on_destination_resolved: object,
    ) -> ExportReceipt:
        assert actor == "cli"
        assert callable(on_destination_resolved)
        on_destination_resolved(destination)
        return receipt

    return run


def test_export_help_exposes_the_public_command_grammar() -> None:
    result = runner.invoke(app, ["export", "--help"])

    assert result.exit_code == 0, result.output
    assert "bundle" in result.stdout
    assert "report" in result.stdout
    assert "destination" in result.stdout


def test_export_bundle_defaults_to_redacted_csv_and_local_exports(
    tmp_path: Path,
) -> None:
    destination = _local_destination(tmp_path / "exports")
    receipt = _receipt(destination, tmp_path / "exports" / "export-1")
    mock_db = MagicMock()

    with (
        patch("moneybin.database.get_database") as get_database,
        patch(
            "moneybin.config.get_settings", return_value=_settings(tmp_path / "exports")
        ),
        patch(
            "moneybin.exports.service.ExportService.run",
            side_effect=_completed_run(receipt, destination),
        ) as run,
    ):
        get_database.return_value.__enter__.return_value = mock_db
        result = runner.invoke(app, ["export", "bundle"])

    assert result.exit_code == 0, result.output
    request = run.call_args.args[0]
    assert request.subject_kind == "bundle"
    assert request.report_id is None
    assert request.report_parameters == {}
    assert request.destination_reference == "local:exports"
    assert request.format == "csv"
    assert request.redaction_mode == "redacted"
    assert request.compress_zip is False
    assert run.call_args.kwargs["actor"] == "cli"
    assert callable(run.call_args.kwargs["on_destination_resolved"])
    assert str(destination.local_path) in result.stderr
    assert str(receipt.artifact_path) in result.stdout


def test_export_report_parses_parameters_with_the_report_types(tmp_path: Path) -> None:
    destination = _local_destination(tmp_path / "exports")
    receipt = _receipt(destination, tmp_path / "exports" / "export-1")

    with (
        patch("moneybin.database.get_database") as get_database,
        patch(
            "moneybin.config.get_settings", return_value=_settings(tmp_path / "exports")
        ),
        patch(
            "moneybin.exports.service.ExportService.run", return_value=receipt
        ) as run,
    ):
        get_database.return_value.__enter__.return_value = MagicMock()
        result = runner.invoke(
            app,
            [
                "export",
                "report",
                "core:large_transactions",
                "--param",
                "top=5",
                "--param",
                "anomaly=account",
            ],
        )

    assert result.exit_code == 0, result.output
    request = run.call_args.args[0]
    assert request.subject_kind == "report"
    assert request.report_id == "core:large_transactions"
    assert request.report_parameters == {"top": 5, "anomaly": "account"}


def test_export_report_parses_optional_string_parameters(tmp_path: Path) -> None:
    destination = _local_destination(tmp_path / "exports")
    receipt = _receipt(destination, tmp_path / "exports" / "export-1")

    with (
        patch("moneybin.database.get_database") as get_database,
        patch(
            "moneybin.config.get_settings", return_value=_settings(tmp_path / "exports")
        ),
        patch(
            "moneybin.exports.service.ExportService.run", return_value=receipt
        ) as run,
    ):
        get_database.return_value.__enter__.return_value = MagicMock()
        result = runner.invoke(
            app,
            [
                "export",
                "report",
                "core:networth",
                "--param",
                "as_of=2026-07-01",
            ],
        )

    assert result.exit_code == 0, result.output
    assert run.call_args.args[0].report_parameters["as_of"] == "2026-07-01"


@pytest.mark.parametrize(
    "parameters",
    [
        ["top=1", "top=2"],
        ["top"],
        ["top=not-an-integer"],
    ],
)
def test_export_report_rejects_invalid_parameter_bindings(
    parameters: list[str],
    tmp_path: Path,
) -> None:
    """Malformed, duplicate, and mistyped --param values fail before delivery."""
    with patch("moneybin.exports.service.ExportService.run") as run:
        result = runner.invoke(
            app,
            [
                "export",
                "report",
                "core:large_transactions",
                *[item for parameter in parameters for item in ("--param", parameter)],
            ],
        )

    assert result.exit_code == 2
    run.assert_not_called()


def test_export_rejects_implicit_path_destinations() -> None:
    result = runner.invoke(app, ["export", "bundle", "--to", "./exports"])

    assert result.exit_code == 2
    assert "local:<name>" in result.output
    assert "sheets:<name>" in result.output


def test_export_resolves_custom_local_path_before_service_call(tmp_path: Path) -> None:
    configured = ExportDestination(
        destination_id="dst_local_1",
        name="archive",
        kind="local",
        local_path=Path("relative-exports"),
        spreadsheet_id=None,
        managed_tab_prefix=None,
    )
    receipt = _receipt(_local_destination(tmp_path / "published", name="archive"))
    resolved = replace(configured, local_path=Path("relative-exports").resolve())

    with (
        patch("moneybin.database.get_database") as get_database,
        patch(
            "moneybin.repositories.export_destinations_repo.ExportDestinationsRepo.resolve",
            return_value=configured,
        ),
        patch(
            "moneybin.exports.service.ExportService.run",
            side_effect=_completed_run(receipt, resolved),
        ) as run,
    ):
        get_database.return_value.__enter__.return_value = MagicMock()
        result = runner.invoke(
            app,
            ["export", "bundle", "--to", "local:archive"],
        )

    assert result.exit_code == 0, result.output
    assert run.call_args.args[0].destination_reference == "local:archive"
    assert str(resolved.local_path) in result.stderr


def test_export_report_parser_errors_are_safe_stderr() -> None:
    result = runner.invoke(app, ["export", "report", "core:not_a_report"])

    assert result.exit_code == 1
    assert "Report not found." in result.stderr
    assert "Traceback" not in result.output


def test_export_sheets_rejects_local_format_and_compression_options() -> None:
    destination = _sheets_destination()

    with (
        patch("moneybin.database.get_database") as get_database,
        patch(
            "moneybin.repositories.export_destinations_repo.ExportDestinationsRepo.resolve",
            return_value=destination,
        ),
        patch("moneybin.exports.service.ExportService.run") as run,
    ):
        get_database.return_value.__enter__.return_value = MagicMock()
        format_result = runner.invoke(
            app,
            ["export", "bundle", "--to", "sheets:dashboard", "--format", "csv"],
        )
        compress_result = runner.invoke(
            app,
            ["export", "bundle", "--to", "sheets:dashboard", "--compress", "zip"],
        )

    assert format_result.exit_code == 2
    assert "Sheets destinations do not accept" in format_result.output
    assert compress_result.exit_code == 2
    assert "Sheets destinations do not accept" in compress_result.output
    run.assert_not_called()


def test_export_unredacted_requires_the_explicit_flag(tmp_path: Path) -> None:
    destination = _local_destination(tmp_path / "exports")
    receipt = _receipt(destination, tmp_path / "exports" / "export-1")

    with (
        patch("moneybin.database.get_database") as get_database,
        patch(
            "moneybin.config.get_settings", return_value=_settings(tmp_path / "exports")
        ),
        patch(
            "moneybin.exports.service.ExportService.run", return_value=receipt
        ) as run,
    ):
        get_database.return_value.__enter__.return_value = MagicMock()
        result = runner.invoke(app, ["export", "bundle", "--unredacted"])

    assert result.exit_code == 0, result.output
    assert run.call_args.args[0].redaction_mode == "unredacted"


def test_export_yes_selects_only_the_redacted_default(tmp_path: Path) -> None:
    destination = _local_destination(tmp_path / "exports")
    receipt = _receipt(destination, tmp_path / "exports" / "export-1")

    with (
        patch("moneybin.database.get_database") as get_database,
        patch(
            "moneybin.config.get_settings", return_value=_settings(tmp_path / "exports")
        ),
        patch(
            "moneybin.exports.service.ExportService.run", return_value=receipt
        ) as run,
    ):
        get_database.return_value.__enter__.return_value = MagicMock()
        result = runner.invoke(app, ["export", "bundle", "--yes"])

    assert result.exit_code == 0, result.output
    assert run.call_args.args[0].redaction_mode == "redacted"


def test_interactive_decline_never_infers_unredacted_output(tmp_path: Path) -> None:
    with (
        patch(
            "moneybin.cli.commands.export._is_interactive_terminal",
            return_value=True,
        ),
        patch(
            "moneybin.config.get_settings", return_value=_settings(tmp_path / "exports")
        ),
        patch("moneybin.exports.service.ExportService.run") as run,
    ):
        result = runner.invoke(app, ["export", "bundle"], input="n\n")

    assert result.exit_code == 1
    assert "--unredacted" in result.stderr
    run.assert_not_called()


def test_interactive_json_prompt_keeps_stdout_machine_readable(tmp_path: Path) -> None:
    artifact = tmp_path / "exports" / "export-1"
    destination = _local_destination(tmp_path / "exports")
    receipt = _receipt(destination, artifact)

    with (
        patch(
            "moneybin.cli.commands.export._is_interactive_terminal",
            return_value=True,
        ),
        patch("moneybin.database.get_database") as get_database,
        patch(
            "moneybin.config.get_settings", return_value=_settings(tmp_path / "exports")
        ),
        patch("moneybin.exports.service.ExportService.run", return_value=receipt),
    ):
        get_database.return_value.__enter__.return_value = MagicMock()
        result = runner.invoke(
            app,
            ["export", "bundle", "--output", "json"],
            input="\n",
        )

    assert result.exit_code == 0, result.output
    assert json.loads(result.stdout)["status"] == "ok"
    assert "Export redacted output?" in result.stderr


def test_export_json_is_a_typed_standard_envelope(tmp_path: Path) -> None:
    artifact = tmp_path / "exports" / "export-1"
    artifact.mkdir(parents=True)
    destination = _local_destination(tmp_path / "exports")
    receipt = _receipt(destination, artifact)

    with (
        patch("moneybin.database.get_database") as get_database,
        patch(
            "moneybin.config.get_settings", return_value=_settings(tmp_path / "exports")
        ),
        patch("moneybin.exports.service.ExportService.run", return_value=receipt),
    ):
        get_database.return_value.__enter__.return_value = MagicMock()
        result = runner.invoke(app, ["export", "bundle", "--output", "json"])

    assert result.exit_code == 0, result.output
    envelope = json.loads(result.stdout)
    assert envelope["status"] == "ok"
    assert set(envelope) >= {"status", "summary", "data", "actions"}
    assert envelope["summary"]["sensitivity"] == "medium"
    assert envelope["data"]["redaction_mode"] == "redacted"
    assert envelope["data"]["format"] == receipt.format
    assert envelope["data"]["destination"]["name"] == "local:exports"
    assert envelope["data"]["artifact_path"] == str(artifact.resolve())
    assert envelope["data"]["export_id"] == receipt.export_id
    assert envelope["data"]["output_classes"] == {
        "accounts": {"account_id": "record_id"},
        "transactions": {"amount": "txn_amount"},
    }


@pytest.mark.integration
def test_export_json_path_is_absolute_and_shell_inspectable(tmp_path: Path) -> None:
    from tests.moneybin.test_exports.test_renderers import make_snapshot

    with (
        patch("moneybin.database.get_database") as get_database,
        patch(
            "moneybin.config.get_settings",
            return_value=_settings(tmp_path / "exports"),
        ),
        patch(
            "moneybin.exports.service.ExportService.prepare_bundle",
            return_value=make_snapshot(),
        ),
    ):
        get_database.return_value.__enter__.return_value = MagicMock()
        result = runner.invoke(app, ["export", "bundle", "--output", "json"])

    assert result.exit_code == 0, result.output
    returned_path = Path(json.loads(result.stdout)["data"]["artifact_path"])
    inspected = subprocess.run(  # noqa: S603  # fixed executable, test-owned path
        [
            sys.executable,
            "-c",
            (
                "import csv,json,sys; "
                "root=sys.argv[1]; "
                "manifest=json.load(open(root + '/manifest.json')); "
                "rows=list(csv.DictReader(open(root + '/tables/activity.csv'))); "
                "print(json.dumps({'kind': manifest['subject']['kind'], "
                "'rows': len(rows)}, sort_keys=True))"
            ),
            str(returned_path),
        ],
        capture_output=True,
        check=False,
        text=True,
    )
    assert returned_path.is_absolute()
    assert inspected.returncode == 0
    assert inspected.stdout == '{"kind": "bundle", "rows": 2}\n'


def test_export_destination_list_hides_sheets_target_and_shows_local_absolute_path(
    tmp_path: Path,
) -> None:
    local = ExportDestination(
        destination_id="dst_local_1",
        name="archive",
        kind="local",
        local_path=tmp_path / "relative-looking",
        spreadsheet_id=None,
        managed_tab_prefix=None,
    )
    sheets = _sheets_destination()
    readiness = ExportReadinessStatus(
        destinations=(
            ExportDestinationReadiness("local:exports", "local", True, True, ()),
            ExportDestinationReadiness("archive", "local", True, True, ()),
            ExportDestinationReadiness("dashboard", "sheets", True, True, ()),
        )
    )

    with (
        patch("moneybin.database.get_database") as get_database,
        patch(
            "moneybin.config.get_settings", return_value=_settings(tmp_path / "exports")
        ),
        patch(
            "moneybin.repositories.export_destinations_repo.ExportDestinationsRepo.list",
            return_value=[local, sheets],
        ),
        patch("moneybin.exports.service.ExportService.status", return_value=readiness),
    ):
        get_database.return_value.__enter__.return_value = MagicMock()
        result = runner.invoke(app, ["export", "destination", "list"])

    assert result.exit_code == 0, result.output
    assert local.local_path is not None
    assert str(local.local_path.resolve()) in result.stdout
    assert "dashboard" in result.stdout
    assert "dst_sheet_1" in result.stdout
    assert "sheet_abc" not in result.stdout
    assert "docs.google.com" not in result.stdout


def test_export_destination_add_local_resolves_the_saved_path(tmp_path: Path) -> None:
    configured_path = tmp_path / "archive"

    with (
        patch("moneybin.database.get_database") as get_database,
        patch(
            "moneybin.repositories.export_destinations_repo.ExportDestinationsRepo.set_local"
        ) as set_local,
    ):
        get_database.return_value.__enter__.return_value = MagicMock()
        result = runner.invoke(
            app,
            ["export", "destination", "add", "local", "archive", str(configured_path)],
        )

    assert result.exit_code == 0, result.output
    set_local.assert_called_once_with(
        name="archive",
        local_path=configured_path.resolve(),
        actor="cli",
    )
    assert str(configured_path.resolve()) in result.stdout
    assert "✅" not in result.stdout


def test_export_destination_add_local_json_matches_the_mutation_envelope(
    tmp_path: Path,
) -> None:
    configured_path = tmp_path / "archive"
    event = AuditEvent(
        audit_id="audit_1",
        occurred_at="",
        actor="cli",
        action="export_destination.set_local",
        target_schema="app",
        target_table="export_destinations",
        target_id="dst_local_1",
        before_value=None,
        after_value=None,
        parent_audit_id=None,
        operation_id="operation_1",
    )

    with (
        patch("moneybin.database.get_database") as get_database,
        patch(
            "moneybin.repositories.export_destinations_repo.ExportDestinationsRepo.set_local",
            return_value=event,
        ),
    ):
        get_database.return_value.__enter__.return_value = MagicMock()
        result = runner.invoke(
            app,
            [
                "export",
                "destination",
                "add",
                "local",
                "archive",
                str(configured_path),
                "--output",
                "json",
            ],
        )

    assert result.exit_code == 0, result.output
    assert json.loads(result.stdout)["data"] == {
        "destination": {
            "destination_id": "dst_local_1",
            "kind": "local",
            "name": "archive",
            "state": "present",
        },
        "operation_id": "operation_1",
    }


def test_export_destination_add_sheets_uses_service_write_oauth() -> None:
    oauth_client = MagicMock()
    url = "https://docs.google.com/spreadsheets/d/sheet_abc/edit#gid=0"

    with (
        patch("moneybin.database.get_database") as get_database,
        patch(
            "moneybin.connectors.gsheet.service_factory.build_oauth_client",
            return_value=oauth_client,
        ),
        patch(
            "moneybin.exports.service.ExportService.set_sheets_destination"
        ) as set_sheets,
    ):
        get_database.return_value.__enter__.return_value = MagicMock()
        result = runner.invoke(
            app,
            ["export", "destination", "add", "sheets", "dashboard", url],
        )

    assert result.exit_code == 0, result.output
    set_sheets.assert_called_once_with(
        name="dashboard",
        spreadsheet_id="sheet_abc",
        managed_tab_prefix="MoneyBin",
        actor="cli",
        oauth_client=oauth_client,
    )
    assert "dashboard" in result.stdout
    assert url not in result.output
    assert "sheet_abc" not in result.output
    assert "✅" not in result.stdout


def test_export_destination_add_sheets_accepts_workbook_url_without_gid() -> None:
    oauth_client = MagicMock()
    url = "https://docs.google.com/spreadsheets/d/sheet_abc/edit"

    with (
        patch("moneybin.database.get_database") as get_database,
        patch(
            "moneybin.connectors.gsheet.service_factory.build_oauth_client",
            return_value=oauth_client,
        ),
        patch(
            "moneybin.exports.service.ExportService.set_sheets_destination"
        ) as set_sheets,
    ):
        get_database.return_value.__enter__.return_value = MagicMock()
        result = runner.invoke(
            app,
            ["export", "destination", "add", "sheets", "dashboard", url],
        )

    assert result.exit_code == 0, result.output
    assert set_sheets.call_args.kwargs["spreadsheet_id"] == "sheet_abc"


def test_export_destination_add_sheets_json_matches_the_mutation_envelope() -> None:
    oauth_client = MagicMock()
    event = AuditEvent(
        audit_id="audit_1",
        occurred_at="",
        actor="cli",
        action="export_destination.set_sheets",
        target_schema="app",
        target_table="export_destinations",
        target_id="dst_sheet_1",
        before_value=None,
        after_value=None,
        parent_audit_id=None,
        operation_id="operation_1",
    )
    url = "https://docs.google.com/spreadsheets/d/sheet_abc/edit#gid=0"

    with (
        patch(
            "moneybin.connectors.gsheet.service_factory.build_oauth_client",
            return_value=oauth_client,
        ),
        patch(
            "moneybin.exports.service.ExportService.set_sheets_destination",
            return_value=event,
        ),
    ):
        result = runner.invoke(
            app,
            [
                "export",
                "destination",
                "add",
                "sheets",
                "dashboard",
                url,
                "--output",
                "json",
            ],
        )

    assert result.exit_code == 0, result.output
    assert json.loads(result.stdout)["data"] == {
        "destination": {
            "destination_id": "dst_sheet_1",
            "kind": "sheets",
            "name": "dashboard",
            "state": "present",
        },
        "operation_id": "operation_1",
    }


def test_export_destination_remove_requires_confirmation() -> None:
    with (
        patch("moneybin.database.get_database") as get_database,
        patch(
            "moneybin.repositories.export_destinations_repo.ExportDestinationsRepo.remove",
        ) as remove,
    ):
        get_database.return_value.__enter__.return_value = MagicMock()
        result = runner.invoke(
            app,
            ["export", "destination", "remove", "dashboard"],
            input="n\n",
        )

    assert result.exit_code == 0, result.output
    remove.assert_not_called()


def test_export_destination_remove_deletes_configuration_only_with_yes() -> None:
    with (
        patch("moneybin.database.get_database") as get_database,
        patch(
            "moneybin.repositories.export_destinations_repo.ExportDestinationsRepo.remove",
            return_value=MagicMock(),
        ) as remove,
    ):
        get_database.return_value.__enter__.return_value = MagicMock()
        result = runner.invoke(
            app,
            ["export", "destination", "remove", "dashboard", "--yes"],
        )

    assert result.exit_code == 0, result.output
    remove.assert_called_once_with("dashboard", actor="cli")
    assert "configuration" in result.stdout.lower()
    assert "✅" not in result.stdout


def test_export_destination_remove_json_matches_the_mutation_envelope() -> None:
    event = AuditEvent(
        audit_id="audit_1",
        occurred_at="",
        actor="cli",
        action="export_destination.remove",
        target_schema="app",
        target_table="export_destinations",
        target_id="dst_local_1",
        before_value=None,
        after_value=None,
        parent_audit_id=None,
        operation_id="operation_1",
    )
    with (
        patch("moneybin.database.get_database") as get_database,
        patch(
            "moneybin.repositories.export_destinations_repo.ExportDestinationsRepo.remove",
            return_value=event,
        ),
    ):
        get_database.return_value.__enter__.return_value = MagicMock()
        result = runner.invoke(
            app,
            [
                "export",
                "destination",
                "remove",
                "archive",
                "--yes",
                "--output",
                "json",
            ],
        )

    assert result.exit_code == 0, result.output
    assert json.loads(result.stdout)["data"] == {
        "destination": {
            "destination_id": "dst_local_1",
            "kind": "local",
            "name": "archive",
            "state": "absent",
        },
        "operation_id": "operation_1",
    }


def test_export_destination_remove_json_preserves_the_removed_kind() -> None:
    event = AuditEvent(
        audit_id="audit_1",
        occurred_at="",
        actor="cli",
        action="export_destination.remove",
        target_schema="app",
        target_table="export_destinations",
        target_id="dst_sheet_1",
        before_value={"kind": "sheets"},
        after_value=None,
        parent_audit_id=None,
        operation_id="operation_1",
    )
    with (
        patch("moneybin.database.get_database") as get_database,
        patch(
            "moneybin.repositories.export_destinations_repo.ExportDestinationsRepo.remove",
            return_value=event,
        ),
    ):
        get_database.return_value.__enter__.return_value = MagicMock()
        result = runner.invoke(
            app,
            [
                "export",
                "destination",
                "remove",
                "dashboard",
                "--yes",
                "--output",
                "json",
            ],
        )

    assert result.exit_code == 0, result.output
    assert json.loads(result.stdout)["data"]["destination"]["kind"] == "sheets"


def test_export_destination_remove_reports_ambiguous_reference() -> None:
    ambiguous = AmbiguousEntity(
        reference="archive",
        candidate_ids=("dst_local_1", "dst_local_2"),
    )
    with (
        patch("moneybin.database.get_database") as get_database,
        patch(
            "moneybin.repositories.export_destinations_repo.ExportDestinationsRepo.remove",
            return_value=ambiguous,
        ),
    ):
        get_database.return_value.__enter__.return_value = MagicMock()
        result = runner.invoke(
            app,
            [
                "export",
                "destination",
                "remove",
                "archive",
                "--yes",
                "--output",
                "json",
            ],
        )

    assert result.exit_code == 1, result.output
    error = json.loads(result.stdout)["error"]
    assert error["code"] == "mutation_ambiguous"
    assert error["details"] == {"candidate_ids": ["dst_local_1", "dst_local_2"]}


def test_export_service_errors_are_safe_stderr_with_nonzero_exit(
    tmp_path: Path,
) -> None:
    destination = _local_destination(tmp_path / "exports")

    def fail(
        _command: object,
        *,
        actor: str,
        on_destination_resolved: object,
    ) -> None:
        assert actor == "cli"
        assert callable(on_destination_resolved)
        on_destination_resolved(destination)
        raise UserError("Export could not be published.", code="EXPORT_FAILED")

    with (
        patch("moneybin.database.get_database") as get_database,
        patch(
            "moneybin.config.get_settings", return_value=_settings(tmp_path / "exports")
        ),
        patch(
            "moneybin.exports.service.ExportService.run",
            side_effect=fail,
        ),
    ):
        get_database.return_value.__enter__.return_value = MagicMock()
        result = runner.invoke(app, ["export", "bundle"])

    assert result.exit_code == 1
    assert "Export could not be published." in result.stderr
    assert result.stdout == ""
    assert str(destination.local_path) in result.stderr
    assert "Traceback" not in result.output


def test_local_export_oserror_discloses_destination_without_logging_filename(
    tmp_path: Path,
) -> None:
    destination_path = (tmp_path / "custom-destination").resolve()
    failed_filename = destination_path / ".publish.lock"
    destination = _local_destination(destination_path)

    def echo_log(message: str) -> None:
        typer.echo(message, err=True)

    def fail(
        _command: object,
        *,
        actor: str,
        on_destination_resolved: object,
    ) -> None:
        assert actor == "cli"
        assert callable(on_destination_resolved)
        on_destination_resolved(destination)
        raise OSError(13, "Permission denied", failed_filename)

    with (
        patch("moneybin.database.get_database") as get_database,
        patch(
            "moneybin.config.get_settings",
            return_value=_settings(destination_path),
        ),
        patch(
            "moneybin.exports.service.ExportService.run",
            side_effect=fail,
        ),
        patch(
            "moneybin.cli.utils.logger.error",
            side_effect=echo_log,
        ) as log_error,
    ):
        get_database.return_value.__enter__.return_value = MagicMock()
        result = runner.invoke(app, ["export", "bundle"])

    assert result.exit_code == 1
    assert result.stdout == ""
    assert f"Exporting to {destination_path}" in result.stderr
    assert "Local export could not be published." in result.stderr
    log_error.assert_called_once_with("❌ Local export could not be published.")
    assert str(failed_filename) not in result.stderr
    assert str(failed_filename) not in str(log_error.call_args)
    assert "Permission denied" not in str(log_error.call_args)
