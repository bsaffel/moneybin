"""Report extensions join the catalog and CLI without growing MCP."""

from __future__ import annotations

import pytest
import typer

from moneybin.database import Database
from moneybin.errors import UserError
from moneybin.mcp.surface import STANDARD_TOOL_COUNT
from moneybin.privacy.taxonomy import DataClass
from moneybin.reports._framework import registry
from moneybin.reports._framework.catalog import get_report_catalog
from moneybin.reports._framework.contract import (
    OutputColumn,
    ReportQuery,
    ReportSemantics,
    report,
)
from moneybin.reports._framework.registry import (
    register_extension_report,
    register_extension_reports,
    spec_of,
)
from moneybin.tables import TableRef

_VIEW = TableRef("reports", "retirement_forecast")
_CLASSES = {"projected_balance": DataClass.BALANCE}
_COLUMNS = (
    OutputColumn(
        "projected_balance",
        "Projected retirement balance.",
        DataClass.BALANCE,
    ),
)
_SEMANTICS = ReportSemantics(
    unit="currency",
    currency="summary.display_currency",
    sign="projected balances are positive positions",
    kind="position",
    valuation_basis="deterministic test projection",
    fx_basis="no FX conversion",
    time_basis="point-in-time projection",
    denominator=None,
    comparison_window=None,
    exclusions=(),
    provenance=("reports.retirement_forecast",),
)


@report(
    report_id="retirement:forecast",
    name="forecast",
    view=_VIEW,
    classes=_CLASSES,
    parameter_classes={},
    columns=_COLUMNS,
    semantics=_SEMANTICS,
)
def retirement_runner(db: Database) -> ReportQuery:
    """Forecast retirement savings.

    Args:
        db: Open read-only database connection.
    """
    return ReportQuery("SELECT 1 AS projected_balance")


RETIREMENT_RUNNER = retirement_runner


@pytest.fixture(autouse=True)
def _isolated_extension_reports(  # pyright: ignore[reportUnusedFunction]
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(registry, "_extension_reports", {})


def registered_report_command_names(app: typer.Typer) -> set[str]:
    """Return the named report commands registered on ``app``."""
    return {command.name for command in app.registered_commands if command.name}


async def listed_tools():  # noqa: ANN201 — FastMCP's component type is internal
    """Return the frozen live FastMCP registry."""
    from moneybin.mcp.server import mcp, register_core_tools

    register_core_tools()
    return tuple(await mcp.list_tools())


async def test_extension_report_joins_catalog_and_cli_without_mcp_growth() -> None:
    before_tools = await listed_tools()
    before = {tool.name for tool in before_tools}
    app = typer.Typer()

    register_extension_reports([RETIREMENT_RUNNER], app)

    after_tools = await listed_tools()
    after = {tool.name for tool in after_tools}
    assert after == before
    assert len(after) == STANDARD_TOOL_COUNT
    assert all(tool.output_schema is None for tool in after_tools)
    assert "reports" in after
    assert "reports_forecast" not in after
    assert get_report_catalog().resolve("retirement:forecast") is spec_of(
        RETIREMENT_RUNNER
    )
    assert registered_report_command_names(app) == {"forecast"}


def test_extension_duplicate_is_rejected_before_cli_mutation() -> None:
    register_extension_report(spec_of(RETIREMENT_RUNNER))
    app = typer.Typer()

    with pytest.raises(ValueError, match="duplicate extension report_id"):
        register_extension_reports([RETIREMENT_RUNNER], app)

    assert registered_report_command_names(app) == set()


def test_extension_batch_duplicate_is_rejected_before_any_mutation() -> None:
    app = typer.Typer()

    with pytest.raises(ValueError, match="duplicate extension report_id"):
        register_extension_reports([RETIREMENT_RUNNER, RETIREMENT_RUNNER], app)

    assert registered_report_command_names(app) == set()
    with pytest.raises(UserError, match="Report not found"):
        get_report_catalog().resolve("retirement:forecast")
