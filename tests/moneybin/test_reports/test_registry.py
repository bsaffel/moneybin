"""Tests for report discovery and dual-surface registration."""

from __future__ import annotations

from types import ModuleType

import pytest
import typer
from fastmcp import Client, FastMCP

from moneybin.database import Database
from moneybin.privacy.taxonomy import DataClass
from moneybin.reports._framework.contract import ReportQuery, report
from moneybin.reports._framework.registry import (
    discover_reports,
    register_report,
    register_reports,
)
from moneybin.tables import TableRef

_VIEW = TableRef("reports", "test_summary")
_CLASSES = {"value": DataClass.AGGREGATE}


@report(name="alpha", view=_VIEW, classes=_CLASSES)
def _alpha(db: Database, *, top: int = 5) -> ReportQuery:
    """Alpha report.

    Args:
        db: Open read-only database connection.
        top: Maximum rows to return.
    """
    return ReportQuery("SELECT 1", [])


@report(name="beta", view=_VIEW, classes=_CLASSES)
def _beta(db: Database) -> ReportQuery:
    """Beta report.

    Args:
        db: Open read-only database connection.
    """
    return ReportQuery("SELECT 1", [])


def _not_a_report(db: Database) -> ReportQuery:
    """Plain function — valid runner shape but never decorated with @report."""
    return ReportQuery("SELECT 1", [])


async def test_register_reports_wires_both_surfaces() -> None:
    mcp = FastMCP("reg-test")
    app = typer.Typer()
    specs = register_reports([_alpha, _beta], mcp, app)

    assert {s.name for s in specs} == {"alpha", "beta"}
    cli_names = {c.name for c in app.registered_commands}
    assert cli_names == {"alpha", "beta"}
    async with Client(mcp) as client:
        tool_names = {t.name for t in await client.list_tools()}
    assert {"reports_alpha", "reports_beta"} <= tool_names


def test_register_report_rejects_plain_function() -> None:
    mcp = FastMCP("reg-test")
    app = typer.Typer()
    with pytest.raises(ValueError, match="not a @report runner"):
        register_report(_not_a_report, mcp, app)


def test_discover_reports_finds_decorated_runners() -> None:
    module = ModuleType("fake_definitions")
    module.alpha = _alpha  # type: ignore[attr-defined]
    module.beta = _beta  # type: ignore[attr-defined]
    module.helper = _not_a_report  # type: ignore[attr-defined]
    found = discover_reports(module)
    assert found == [_alpha, _beta]  # only decorated, definition order, no helper
