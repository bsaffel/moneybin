"""Tests for report discovery and explicit surface registration."""

from __future__ import annotations

from types import ModuleType

import pytest
from fastmcp import FastMCP

from moneybin.database import Database
from moneybin.privacy.taxonomy import DataClass
from moneybin.reports._framework.contract import ReportQuery, report
from moneybin.reports._framework.registry import (
    discover_reports,
    register_generic_reports_tool,
    spec_of,
)
from moneybin.tables import TableRef
from tests.moneybin.test_reports._metadata import TEST_SEMANTICS, output_columns

_VIEW = TableRef("reports", "test_summary")
_CLASSES = {"value": DataClass.AGGREGATE}


@report(
    report_id="test:alpha",
    name="alpha",
    view=_VIEW,
    classes=_CLASSES,
    parameter_classes={"top": DataClass.AGGREGATE},
    columns=output_columns(_CLASSES),
    semantics=TEST_SEMANTICS,
)
def _alpha(db: Database, *, top: int = 5) -> ReportQuery:
    """Alpha report.

    Args:
        db: Open read-only database connection.
        top: Maximum rows to return.
    """
    return ReportQuery("SELECT 1", [])


@report(
    report_id="test:beta",
    name="beta",
    view=_VIEW,
    classes=_CLASSES,
    parameter_classes={},
    columns=output_columns(_CLASSES),
    semantics=TEST_SEMANTICS,
)
def _beta(db: Database) -> ReportQuery:
    """Beta report.

    Args:
        db: Open read-only database connection.
    """
    return ReportQuery("SELECT 1", [])


def _not_a_report(db: Database) -> ReportQuery:
    """Plain function — valid runner shape but never decorated with @report."""
    return ReportQuery("SELECT 1", [])


async def test_generic_registrar_registers_one_tool_in_isolation() -> None:
    mcp = FastMCP("reports-contract")
    register_generic_reports_tool(mcp)

    tools = await mcp.list_tools()

    assert {tool.name for tool in tools} == {"reports"}
    assert tools[0].output_schema is None


def test_spec_of_rejects_plain_function() -> None:
    with pytest.raises(ValueError, match="not a @report runner"):
        spec_of(_not_a_report)


def test_discover_reports_finds_decorated_runners() -> None:
    module = ModuleType("fake_definitions")
    module.alpha = _alpha  # type: ignore[attr-defined]
    module.beta = _beta  # type: ignore[attr-defined]
    module.helper = _not_a_report  # type: ignore[attr-defined]
    found = discover_reports(module)
    assert found == [_alpha, _beta]  # only decorated, definition order, no helper
