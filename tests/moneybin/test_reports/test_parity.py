"""Surface parity: catalog reports match the documented CLI and MCP contracts.

Locks the migrated surface — one generic MCP runner, ergonomic CLI commands,
and stable catalog IDs — against regression. Runner logic is covered by
test_definitions; masking/tier by test_execute.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import typer
from fastmcp import Client, FastMCP
from typer.testing import CliRunner

from moneybin.cli.commands import reports as reports_commands
from moneybin.privacy.taxonomy import DataClass, Tier
from moneybin.reports._framework.catalog import get_report_catalog
from moneybin.reports._framework.execute import ReportResult
from moneybin.reports._framework.registry import (
    register_generic_reports_tool,
    register_reports_cli,
    spec_of,
)
from moneybin.reports.definitions import ALL_REPORTS

REPORTS_APP = reports_commands.app

_EXPECTED_CLI = {
    "cashflow",
    "spending",
    "recurring",
    "merchants",
    "large-transactions",
    "balance-drift",
}
_EXPECTED_CATALOG_CLI = {
    "core:balance_drift": "balance-drift",
    "core:cashflow": "cashflow",
    "core:large_transactions": "large-transactions",
    "core:merchants": "merchants",
    "core:networth": "networth",
    "core:networth_history": "networth-history",
    "core:recurring": "recurring",
    "core:spending": "spending",
}


def registered_report_command_names(app: typer.Typer) -> set[str]:
    """Return the public report command names registered on one Typer app."""
    return {command.name for command in app.registered_commands if command.name}


def _result(records: list[dict[str, object]]) -> ReportResult:
    return ReportResult(
        records=records,
        columns=list(records[0]) if records else [],
        output_classes={"value": DataClass.AGGREGATE},
        tier=Tier.LOW,
        total_count=len(records),
        truncated=False,
    )


async def test_mcp_surface_matches_expected_set() -> None:
    mcp = FastMCP("parity")
    register_generic_reports_tool(mcp)
    async with Client(mcp) as client:
        tools = {t.name: t for t in await client.list_tools()}
    assert set(tools) == {"reports"}
    assert set(tools["reports"].inputSchema["properties"]) == {
        "report_id",
        "parameters",
        "limit",
    }


def test_cli_surface_matches_expected_set() -> None:
    app = typer.Typer()
    register_reports_cli(ALL_REPORTS, app)
    names = registered_report_command_names(app)
    assert names == _EXPECTED_CLI


def test_every_catalog_report_has_an_ergonomic_cli_command() -> None:
    assert registered_report_command_names(REPORTS_APP) == set(
        _EXPECTED_CATALOG_CLI.values()
    )


def test_catalog_ids_map_one_to_one_to_public_cli_commands() -> None:
    mapping = {
        report.report_id: report.name.replace("_", "-")
        for report in get_report_catalog().list()
    }
    assert mapping == _EXPECTED_CATALOG_CLI
    assert len(set(mapping.values())) == len(mapping)


def test_networth_preserves_flags_and_executes_through_catalog() -> None:
    help_result = CliRunner().invoke(REPORTS_APP, ["networth", "--help"])
    assert help_result.exit_code == 0, help_result.output
    assert "--as-of" in help_result.output
    assert "--account" in help_result.output
    assert "--as-of-date" not in help_result.output

    database = MagicMock()
    database_context = MagicMock()
    database_context.__enter__.return_value = database
    with (
        patch(
            "moneybin.cli.commands.reports.networth.get_database",
            return_value=database_context,
        ),
        patch("moneybin.reports._framework.catalog.get_report_catalog") as mock_catalog,
        patch("moneybin.cli.commands.reports.networth.render_or_json"),
    ):
        mock_catalog.return_value.execute.return_value = _result([{"value": 1}])
        result = CliRunner().invoke(
            REPORTS_APP,
            [
                "networth",
                "--as-of",
                "2026-07-01",
                "--account",
                "acct-a",
                "--account",
                "acct-b",
                "--output",
                "json",
            ],
        )

    assert result.exit_code == 0, result.output
    mock_catalog.return_value.execute.assert_called_once_with(
        database,
        report_id="core:networth",
        parameters={
            "as_of": "2026-07-01",
            "account_ids": ["acct-a", "acct-b"],
        },
        limit=1_000_000,
    )


def test_networth_history_preserves_flags_and_executes_through_catalog() -> None:
    help_result = CliRunner().invoke(REPORTS_APP, ["networth-history", "--help"])
    assert help_result.exit_code == 0, help_result.output
    assert "--from" in help_result.output
    assert "--to" in help_result.output
    assert "--interval" in help_result.output
    assert "--from-date" not in help_result.output
    assert "--to-date" not in help_result.output

    database = MagicMock()
    database_context = MagicMock()
    database_context.__enter__.return_value = database
    with (
        patch(
            "moneybin.cli.commands.reports.networth.get_database",
            return_value=database_context,
        ),
        patch("moneybin.reports._framework.catalog.get_report_catalog") as mock_catalog,
        patch("moneybin.cli.commands.reports.networth.render_or_json"),
    ):
        mock_catalog.return_value.execute.return_value = _result([{"value": 1}])
        result = CliRunner().invoke(
            REPORTS_APP,
            [
                "networth-history",
                "--from",
                "2026-01-01",
                "--to",
                "2026-07-01",
                "--interval",
                "weekly",
                "--output",
                "json",
            ],
        )

    assert result.exit_code == 0, result.output
    mock_catalog.return_value.execute.assert_called_once_with(
        database,
        report_id="core:networth_history",
        parameters={
            "from_date": "2026-01-01",
            "to_date": "2026-07-01",
            "interval": "weekly",
        },
        limit=1_000_000,
    )


def test_every_report_targets_a_reports_view() -> None:
    for runner in ALL_REPORTS:
        assert spec_of(runner).view.schema == "reports"
