"""Surface parity: the framework-generated reports match the documented set.

Locks the migrated surface — MCP tool names, CLI command names, and per-report
parameter schemas — against regression. Runner logic is covered by
test_definitions; masking/tier by test_execute; this is the surface contract.
"""

from __future__ import annotations

import typer
from fastmcp import Client, FastMCP

from moneybin.reports._framework.registry import register_reports_cli, spec_of
from moneybin.reports.definitions import ALL_REPORTS

# The view-backed reports the framework owns (networth/networth_history/budget
# stay hand-written and are not in ALL_REPORTS).
_EXPECTED_MCP = {
    "reports_cashflow",
    "reports_spending",
    "reports_recurring",
    "reports_merchants",
    "reports_large_transactions",
    "reports_balance_drift",
}
_EXPECTED_CLI = {
    "cashflow",
    "spending",
    "recurring",
    "merchants",
    "large-transactions",
    "balance-drift",
}
# Per-report parameter sets, independently derived from the runner signatures.
_EXPECTED_PARAMS = {
    "reports_cashflow": {"from_month", "to_month", "by"},
    "reports_spending": {"from_month", "to_month", "category", "compare"},
    "reports_recurring": {"min_confidence", "status", "cadence"},
    "reports_merchants": {"top", "sort"},
    "reports_large_transactions": {"top", "anomaly"},
    "reports_balance_drift": {"account", "status", "since"},
}


async def test_mcp_surface_matches_expected_set() -> None:
    mcp = FastMCP("parity")
    for runner in ALL_REPORTS:
        from moneybin.reports._framework.mcp_register import register_report_mcp

        register_report_mcp(spec_of(runner), mcp)
    async with Client(mcp) as client:
        tools = {t.name: t for t in await client.list_tools()}
    assert _EXPECTED_MCP <= set(tools)
    for name, expected_params in _EXPECTED_PARAMS.items():
        assert set(tools[name].inputSchema["properties"]) == expected_params


def test_cli_surface_matches_expected_set() -> None:
    app = typer.Typer()
    register_reports_cli(ALL_REPORTS, app)
    names = {c.name for c in app.registered_commands}
    assert names == _EXPECTED_CLI


def test_every_report_targets_a_reports_view() -> None:
    for runner in ALL_REPORTS:
        assert spec_of(runner).view.schema == "reports"
