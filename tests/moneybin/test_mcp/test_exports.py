"""MCP export delivery and destination-state parity."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastmcp.server.elicitation import AcceptedElicitation, DeclinedElicitation
from mcp.types import TextContent
from typer.testing import CliRunner

from moneybin.cli.main import app
from moneybin.errors import UserError
from moneybin.exports.models import ExportDestination, ExportReceipt
from tests.moneybin.test_mcp.schema_assertions import (
    call_tool_raw,
    isolated_server,
    listed_tool,
)


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


def _receipt(destination: ExportDestination, artifact: Path) -> ExportReceipt:
    return ExportReceipt(
        subject={"kind": "bundle"},
        format="csv",
        redaction_mode="redacted",
        destination=destination,
        artifact_path=artifact.resolve(),
        compressed_artifact_path=None,
        sheets_identity=None,
        row_counts={"accounts": 2, "transactions": 4},
        output_classes={
            "accounts": {"account_id": "record_id"},
            "transactions": {"amount": "txn_amount"},
        },
        checksums={"accounts": "abc123", "transactions": "def456"},
        recovery_actions=(),
    )


def _sheets_report_receipt(destination: ExportDestination) -> ExportReceipt:
    return ExportReceipt(
        subject={"kind": "report", "report_id": "core:networth", "parameters": {}},
        format="sheets",
        redaction_mode="redacted",
        destination=destination,
        artifact_path=None,
        compressed_artifact_path=None,
        sheets_identity="MoneyBin:20260721T120000Z",
        row_counts={"core:networth": 1},
        output_classes={"core:networth": {"net_worth": "balance"}},
        checksums={"core:networth": "sum123"},
        recovery_actions=(),
    )


def _structured(response: Any) -> dict[str, Any]:
    text = next(
        block.text for block in response.content if isinstance(block, TextContent)
    )
    assert response.structuredContent is not None
    assert json.loads(text) == response.structuredContent
    return response.structuredContent


def _observable_delivery(data: dict[str, Any], request: Any) -> dict[str, Any]:
    """Project stable parity fields without paths or timing."""
    return {
        "subject": data["subject"],
        "destination": {
            "kind": data["destination"]["kind"],
            "name": data["destination"]["name"],
        },
        "redaction_mode": data["redaction_mode"],
        "format": data["format"],
        "row_counts": data["row_counts"],
        "checksums": data["checksums"],
        "receipt_identity": data["sheets_identity"],
    }


async def test_export_tools_render_two_narrow_discriminated_contracts() -> None:
    from moneybin.mcp.tools.exports import register_export_tools

    mcp = isolated_server(register_export_tools)
    export = await listed_tool(mcp, "export_run")
    destinations = await listed_tool(mcp, "exports_set")

    assert export.outputSchema is None
    assert destinations.outputSchema is None
    assert export.annotations is not None
    assert export.annotations.readOnlyHint is False
    assert export.annotations.idempotentHint is False
    assert destinations.annotations is not None
    assert destinations.annotations.readOnlyHint is False
    assert destinations.annotations.idempotentHint is True
    assert {tool.name for tool in await mcp._list_tools()} == {  # pyright: ignore[reportPrivateUsage]
        "export_run",
        "exports_set",
    }

    def variants(schema: dict[str, Any], field: str) -> dict[str, set[str]]:
        return {
            branch["properties"]["kind"]["const"]: set(branch["required"])
            for branch in schema["properties"][field]["oneOf"]
        }

    assert variants(export.inputSchema, "subject") == {
        "bundle": {"kind"},
        "report": {"kind", "report_id"},
    }
    assert variants(export.inputSchema, "destination") == {
        "local": {"kind", "name"},
        "sheets": {"kind", "name"},
    }
    assert variants(destinations.inputSchema, "target") == {
        "local": {"kind", "state", "name"},
        "sheets": {"kind", "state", "name"},
    }

    rendered = json.dumps({
        "export_run": export.inputSchema,
        "exports_set": destinations.inputSchema,
    })
    assert '"operation"' not in rendered
    assert '"action"' not in rendered
    assert "local_path" in rendered
    assert "spreadsheet_id" in rendered
    assert "compression" in rendered


@pytest.mark.parametrize(
    "arguments",
    [
        {
            "subject": {"kind": "bundle"},
            "destination": {
                "kind": "local",
                "name": "exports",
                "format": "csv",
            },
            "redaction_mode": "redacted",
        },
        {
            "subject": {
                "kind": "report",
                "report_id": "core:networth",
                "parameters": {"as_of": "2026-07-01"},
            },
            "destination": {"kind": "sheets", "name": "dashboard"},
            "redaction_mode": "unredacted",
        },
    ],
)
async def test_export_run_builds_one_typed_service_request(
    arguments: dict[str, Any],
    tmp_path: Path,
    mcp_db: object,
) -> None:
    from moneybin.exports.service import ExportService
    from moneybin.mcp.tools.exports import register_export_tools

    destination_kind = arguments["destination"]["kind"]
    destination = (
        _local_destination(tmp_path / "exports")
        if destination_kind == "local"
        else ExportDestination(
            destination_id="dst_sheet_1",
            name="dashboard",
            kind="sheets",
            local_path=None,
            spreadsheet_id="sheet_abc",
            managed_tab_prefix="MoneyBin",
        )
    )
    artifact = tmp_path / "exports" / "export-1"
    receipt = _receipt(destination, artifact)
    if destination.kind == "sheets":
        receipt = ExportReceipt(
            subject={"kind": "report", "report_id": "core:networth"},
            format="sheets",
            redaction_mode="unredacted",
            destination=destination,
            artifact_path=None,
            compressed_artifact_path=None,
            sheets_identity="MoneyBin:20260721T120000Z",
            row_counts={"core:networth": 1},
            output_classes={"core:networth": {"net_worth": "balance"}},
            checksums={"core:networth": "sum123"},
            recovery_actions=(),
        )

    with patch.object(ExportService, "run", return_value=receipt) as run:
        response = await call_tool_raw(
            isolated_server(register_export_tools),
            "export_run",
            arguments,
        )

    structured = _structured(response)
    request = run.call_args.args[0]
    assert request.subject_kind == arguments["subject"]["kind"]
    assert request.report_id == arguments["subject"].get("report_id")
    assert request.report_parameters == arguments["subject"].get("parameters", {})
    assert request.destination_reference == (
        f"{arguments['destination']['kind']}:{arguments['destination']['name']}"
    )
    assert request.format == (arguments["destination"].get("format", "sheets"))
    assert request.redaction_mode == arguments["redaction_mode"]
    assert request.compress_zip is (
        arguments["destination"].get("compression") == "zip"
    )
    assert run.call_args.kwargs == {"actor": "mcp"}
    assert structured["data"]["row_counts"] == dict(receipt.row_counts)
    assert structured["data"]["checksums"] == dict(receipt.checksums)
    assert structured["data"]["format"] == receipt.format
    assert structured["summary"]["sensitivity"] == "medium"


async def test_export_run_elicits_redaction_choice_when_omitted(
    tmp_path: Path,
    mcp_db: object,
) -> None:
    from moneybin.exports.service import ExportService
    from moneybin.mcp.tools import exports as exports_mcp

    destination = _local_destination(tmp_path / "exports")
    receipt = _receipt(destination, tmp_path / "exports" / "export-1")
    ctx = MagicMock()
    ctx.elicit = AsyncMock(return_value=AcceptedElicitation(data="redacted"))

    with (
        patch.object(exports_mcp, "get_context", return_value=ctx),
        patch.object(exports_mcp, "supports_elicitation", return_value=True),
        patch.object(ExportService, "resolve_destination", return_value=destination),
        patch.object(ExportService, "run", return_value=receipt) as run,
    ):
        response = await call_tool_raw(
            isolated_server(exports_mcp.register_export_tools),
            "export_run",
            {
                "subject": {"kind": "bundle"},
                "destination": {"kind": "local", "name": "exports"},
            },
        )

    assert _structured(response)["data"]["redaction_mode"] == "redacted"
    assert run.call_args.args[0].redaction_mode == "redacted"
    elicitation = ctx.elicit.await_args
    assert elicitation.kwargs["response_type"] == ["redacted", "unredacted"]
    assert "redacted" in elicitation.kwargs["response_description"]


async def test_export_run_without_redaction_or_elicitation_returns_refusal(
    mcp_db: object,
) -> None:
    from moneybin.exports.service import ExportService
    from moneybin.mcp.tools import exports as exports_mcp

    with (
        patch.object(exports_mcp, "get_context", return_value=MagicMock()),
        patch.object(exports_mcp, "supports_elicitation", return_value=False),
        patch.object(ExportService, "run") as run,
    ):
        response = await call_tool_raw(
            isolated_server(exports_mcp.register_export_tools),
            "export_run",
            {
                "subject": {"kind": "bundle"},
                "destination": {"kind": "local", "name": "exports"},
            },
        )

    structured = _structured(response)
    assert structured["status"] == "error"
    assert structured["error"]["code"] == "mutation_redaction_choice_required"
    assert structured["error"]["details"] == {
        "default": "redacted",
        "options": ["redacted", "unredacted"],
        "reason": "client_unsupported",
    }
    run.assert_not_called()


@pytest.mark.parametrize(
    ("elicitation_result", "reason"),
    [
        (DeclinedElicitation(), "declined"),
        (AcceptedElicitation(data="unexpected"), "invalid_response"),
    ],
)
async def test_export_run_refuses_nonaccepted_or_invalid_redaction_choice(
    elicitation_result: object,
    reason: str,
    mcp_db: object,
) -> None:
    from moneybin.exports.service import ExportService
    from moneybin.mcp.tools import exports as exports_mcp

    ctx = MagicMock()
    ctx.elicit = AsyncMock(return_value=elicitation_result)
    with (
        patch.object(exports_mcp, "get_context", return_value=ctx),
        patch.object(exports_mcp, "supports_elicitation", return_value=True),
        patch.object(ExportService, "run") as run,
    ):
        response = await call_tool_raw(
            isolated_server(exports_mcp.register_export_tools),
            "export_run",
            {
                "subject": {"kind": "bundle"},
                "destination": {"kind": "local", "name": "exports"},
            },
        )

    assert _structured(response)["error"]["details"]["reason"] == reason
    run.assert_not_called()


@pytest.mark.parametrize("legacy", ["safe", "full"])
async def test_export_run_rejects_legacy_redaction_selectors(legacy: str) -> None:
    from moneybin.mcp.tools.exports import register_export_tools

    response = await call_tool_raw(
        isolated_server(register_export_tools),
        "export_run",
        {
            "subject": {"kind": "bundle"},
            "destination": {"kind": "local", "name": "exports"},
            "redaction_mode": legacy,
        },
    )

    assert response.isError is True


@pytest.mark.parametrize(
    ("target", "owner", "method"),
    [
        (
            {
                "kind": "local",
                "state": "present",
                "name": "archive",
                "local_path": "/Users/test/archive",
            },
            "moneybin.repositories.export_destinations_repo.ExportDestinationsRepo",
            "set_local",
        ),
        (
            {
                "kind": "sheets",
                "state": "present",
                "name": "dashboard",
                "spreadsheet_id": "sheet_abc",
                "managed_tab_prefix": "MoneyBin",
            },
            "moneybin.exports.service.ExportService",
            "set_sheets_destination",
        ),
    ],
)
async def test_exports_set_delegates_target_state_to_normal_owner(
    target: dict[str, Any],
    owner: str,
    method: str,
    mcp_db: object,
) -> None:
    from moneybin.mcp.tools.exports import register_export_tools
    from moneybin.services.audit_service import AuditEvent

    event = AuditEvent(
        audit_id="audit_1",
        occurred_at="",
        actor="mcp",
        action="export_destination.set",
        target_schema="app",
        target_table="export_destinations",
        target_id="dst_1",
        before_value=None,
        after_value=None,
        parent_audit_id=None,
        operation_id="operation_1",
    )
    with patch(f"{owner}.{method}", return_value=event) as mutate:
        response = await call_tool_raw(
            isolated_server(register_export_tools),
            "exports_set",
            {"target": target},
        )

    structured = _structured(response)
    assert structured["data"] == {
        "destination": {
            "destination_id": "dst_1",
            "kind": target["kind"],
            "name": target["name"],
            "state": target["state"],
        },
        "operation_id": "operation_1",
    }
    assert mutate.call_count == 1


async def test_exports_set_canonicalizes_local_destination_path_before_persisting(
    tmp_path: Path,
    mcp_db: object,
) -> None:
    from moneybin.mcp.tools.exports import register_export_tools
    from moneybin.services.audit_service import AuditEvent

    supplied_path = tmp_path / "exports" / ".." / "archive"
    event = AuditEvent(
        audit_id="audit_1",
        occurred_at="",
        actor="mcp",
        action="export_destination.set_local",
        target_schema="app",
        target_table="export_destinations",
        target_id="dst_local_1",
        before_value=None,
        after_value=None,
        parent_audit_id=None,
        operation_id="operation_1",
    )
    with patch(
        "moneybin.repositories.export_destinations_repo.ExportDestinationsRepo.set_local",
        return_value=event,
    ) as set_local:
        response = await call_tool_raw(
            isolated_server(register_export_tools),
            "exports_set",
            {
                "target": {
                    "kind": "local",
                    "state": "present",
                    "name": "archive",
                    "local_path": str(supplied_path),
                }
            },
        )

    assert response.isError is False
    assert set_local.call_args.kwargs["local_path"] == supplied_path.resolve()


async def test_exports_set_requires_confirmation_before_removing_configuration(
    mcp_db: object,
) -> None:
    from moneybin.mcp.tools import exports as exports_mcp
    from moneybin.services.audit_service import AuditEvent

    destination = _local_destination(Path.cwd() / "archive", name="archive")
    destination = ExportDestination(
        destination_id="dst_1",
        name=destination.name,
        kind=destination.kind,
        local_path=destination.local_path,
        spreadsheet_id=None,
        managed_tab_prefix=None,
    )
    event = MagicMock(spec=AuditEvent)
    event.target_id = "dst_1"
    event.operation_id = "operation_1"

    with (
        patch.object(exports_mcp, "_preview_removal", return_value=destination),
        patch.object(
            exports_mcp,
            "grant_confirmation_or_raise",
            new=AsyncMock(return_value=MagicMock()),
        ) as confirm,
        patch.object(
            exports_mcp, "_remove_destination_confirmed", return_value=event
        ) as remove,
    ):
        response = await call_tool_raw(
            isolated_server(exports_mcp.register_export_tools),
            "exports_set",
            {"target": {"kind": "local", "state": "absent", "name": "archive"}},
        )

    assert _structured(response)["data"]["destination"]["state"] == "absent"
    assert confirm.await_count == 1
    remove.assert_called_once()


@pytest.mark.parametrize(
    "target",
    [
        {"kind": "local", "state": "present", "name": "archive"},
        {
            "kind": "local",
            "state": "absent",
            "name": "archive",
            "local_path": str(Path.cwd() / "archive"),
        },
        {
            "kind": "sheets",
            "state": "absent",
            "name": "dashboard",
            "spreadsheet_id": "sheet_abc",
        },
        {
            "kind": "local",
            "state": "present",
            "name": "archive",
            "local_path": "relative/path",
        },
    ],
)
def test_destination_target_validation_rejects_invalid_state_shapes(
    target: dict[str, Any],
) -> None:
    from pydantic import TypeAdapter, ValidationError

    from moneybin.mcp.tools.exports import ExportDestinationTarget

    with pytest.raises(ValidationError):
        TypeAdapter(ExportDestinationTarget).validate_python(target)


async def test_export_run_returns_sanitized_service_error(mcp_db: object) -> None:
    from moneybin.exports.service import ExportService
    from moneybin.mcp.tools.exports import register_export_tools

    with patch.object(
        ExportService,
        "run",
        side_effect=UserError(
            "Export destination not found.",
            code="MUTATION_NOT_FOUND",
        ),
    ) as run:
        response = await call_tool_raw(
            isolated_server(register_export_tools),
            "export_run",
            {
                "subject": {"kind": "bundle"},
                "destination": {"kind": "local", "name": "private-drive"},
                "redaction_mode": "redacted",
            },
        )

    structured = _structured(response)
    assert structured["error"]["code"] == "MUTATION_NOT_FOUND"
    assert "private-drive" not in structured["error"]["message"]
    run.assert_called_once()


@pytest.mark.parametrize("subject_kind", ["bundle", "report"])
async def test_cli_and_mcp_export_receipts_have_same_observable_outcome(
    subject_kind: str,
    tmp_path: Path,
    mcp_db: object,
) -> None:
    from moneybin.exports.service import ExportService
    from moneybin.mcp.tools.exports import register_export_tools

    mcp_arguments: dict[str, Any]
    if subject_kind == "bundle":
        destination = _local_destination(tmp_path / "exports")
        receipt = _receipt(destination, tmp_path / "exports" / "export-1")
        cli_arguments = [
            "export",
            "bundle",
            "--to",
            "local:exports",
            "--format",
            "csv",
            "--yes",
            "--output",
            "json",
        ]
        mcp_arguments = {
            "subject": {"kind": "bundle"},
            "destination": {"kind": "local", "name": "exports", "format": "csv"},
            "redaction_mode": "redacted",
        }
    else:
        destination = _sheets_destination()
        receipt = _sheets_report_receipt(destination)
        cli_arguments = [
            "export",
            "report",
            "core:networth",
            "--to",
            "sheets:dashboard",
            "--yes",
            "--output",
            "json",
        ]
        mcp_arguments = {
            "subject": {
                "kind": "report",
                "report_id": "core:networth",
                "parameters": {},
            },
            "destination": {"kind": "sheets", "name": "dashboard"},
            "redaction_mode": "redacted",
        }
    requests: list[tuple[Any, str]] = []

    def run(
        request: Any,
        *,
        actor: str,
        on_destination_resolved: Any = None,
    ) -> ExportReceipt:
        if on_destination_resolved is not None:
            on_destination_resolved(destination)
        requests.append((request, actor))
        return receipt

    with patch.object(ExportService, "run", side_effect=run):
        cli_result = CliRunner().invoke(app, cli_arguments)
    assert cli_result.exit_code == 0, cli_result.output

    with patch.object(ExportService, "run", side_effect=run):
        mcp_result = await call_tool_raw(
            isolated_server(register_export_tools),
            "export_run",
            mcp_arguments,
        )

    cli_data = json.loads(cli_result.stdout)["data"]
    mcp_data = _structured(mcp_result)["data"]
    cli_request, cli_actor = requests[0]
    mcp_request, mcp_actor = requests[1]
    assert _observable_delivery(cli_data, cli_request) == _observable_delivery(
        mcp_data,
        mcp_request,
    )
    assert cli_request.subject_kind == mcp_request.subject_kind == subject_kind
    assert cli_request.redaction_mode == mcp_request.redaction_mode == "redacted"
    if subject_kind == "report":
        assert cli_data["sheets_identity"] == "MoneyBin:20260721T120000Z"
    assert (cli_actor, mcp_actor) == ("cli", "mcp")


@pytest.mark.parametrize("subject_kind", ["bundle", "report"])
async def test_cli_and_mcp_export_failures_are_equally_safe(
    subject_kind: str,
    tmp_path: Path,
    mcp_db: object,
) -> None:
    from moneybin.exports.service import ExportService
    from moneybin.mcp.tools.exports import register_export_tools

    mcp_arguments: dict[str, Any]
    if subject_kind == "bundle":
        cli_arguments = ["export", "bundle", "--yes", "--output", "json"]
        mcp_arguments = {
            "subject": {"kind": "bundle"},
            "destination": {"kind": "local", "name": "exports"},
            "redaction_mode": "redacted",
        }
    else:
        cli_arguments = [
            "export",
            "report",
            "core:networth",
            "--to",
            "sheets:dashboard",
            "--yes",
            "--output",
            "json",
        ]
        mcp_arguments = {
            "subject": {"kind": "report", "report_id": "core:networth"},
            "destination": {"kind": "sheets", "name": "dashboard"},
            "redaction_mode": "redacted",
        }
    unsafe_detail = str(tmp_path / "private-ledger.csv")
    failure = UserError("Export could not be published.", code="EXPORT_FAILED")

    with patch.object(ExportService, "run", side_effect=failure):
        cli_result = CliRunner().invoke(app, cli_arguments)

    with patch.object(ExportService, "run", side_effect=failure):
        mcp_result = await call_tool_raw(
            isolated_server(register_export_tools),
            "export_run",
            mcp_arguments,
        )

    cli_error = json.loads(cli_result.stdout)["error"]
    mcp_error = _structured(mcp_result)["error"]
    assert cli_result.exit_code == 1
    assert cli_error["code"] == mcp_error["code"] == "EXPORT_FAILED"
    assert cli_error["message"] == mcp_error["message"]
    assert unsafe_detail not in json.dumps(cli_error)
    assert unsafe_detail not in json.dumps(mcp_error)


async def test_report_export_reuses_the_registered_reports_catalog_result(
    mcp_db: object,
) -> None:
    from moneybin.database import get_database
    from moneybin.exports.service import ExportService
    from moneybin.mcp.tools.reports import register_reports_tools
    from moneybin.privacy.taxonomy import DataClass
    from moneybin.reports._framework.catalog import ReportCatalog, ServiceReportSpec
    from moneybin.reports._framework.contract import OutputColumn, ReportSemantics
    from moneybin.reports._framework.execute import build_catalog_execution

    calls: list[tuple[dict[str, Any], int | None]] = []
    semantics = ReportSemantics(
        unit="count",
        currency=None,
        sign="non-negative",
        kind="count",
        valuation_basis=None,
        fx_basis=None,
        time_basis="point-in-time query result",
        denominator=None,
        comparison_window=None,
        exclusions=(),
        provenance=("reports.parity_export",),
    )

    def execute(_: Any, parameters: Any, limit: int | None) -> Any:
        calls.append((dict(parameters), limit))
        return build_catalog_execution(
            spec,
            parameters=parameters,
            sql=None,
            records=[{"count": 7}],
            columns=["count"],
            column_types=["BIGINT"],
            max_rows=limit,
        )

    spec = ServiceReportSpec(
        report_id="test:parity_export",
        name="parity_export",
        description="A registered report reused by export.",
        parameters=(),
        columns=(OutputColumn("count", "Row count.", DataClass.AGGREGATE),),
        semantics=semantics,
        classes={"count": DataClass.AGGREGATE},
        examples=(),
        executor=execute,
    )
    catalog = ReportCatalog((spec,))

    with (
        patch("moneybin.mcp.tools.reports.get_report_catalog", return_value=catalog),
        patch("moneybin.exports.service.get_report_catalog", return_value=catalog),
    ):
        report_response = await call_tool_raw(
            isolated_server(register_reports_tools),
            "reports",
            {"report_id": "test:parity_export", "parameters": {}, "limit": 10},
        )
        with get_database(read_only=True) as db:
            snapshot = ExportService(db).prepare_report(
                profile="test",
                report_id="test:parity_export",
                report_parameters={},
                redaction_mode="redacted",
            )

    report_data = _structured(report_response)["data"]
    assert report_data["rows"] == [{"count": 7}]
    assert snapshot.tables[0].rows == ((7,),)
    assert calls == [({}, 10), ({}, None)]
    assert snapshot.provenance is not None
    assert snapshot.provenance.report_id == report_data["report_id"]
