"""Tests for the dynamic MCP tool registrar."""

from __future__ import annotations

import asyncio
import inspect
from unittest.mock import MagicMock, patch

from fastmcp import Client, FastMCP

from moneybin import error_codes
from moneybin.database import Database
from moneybin.mcp.decorator import mcp_tool
from moneybin.mcp.privacy import tier_to_sensitivity
from moneybin.privacy.taxonomy import DataClass, Tier
from moneybin.reports._framework.contract import ReportQuery
from moneybin.reports._framework.execute import ReportResult
from moneybin.reports._framework.introspect import build_spec
from moneybin.reports._framework.mcp_register import make_tool_fn, register_report_mcp
from moneybin.tables import TableRef

_VIEW = TableRef("reports", "test_summary")


def _runner(db: Database, *, month: str | None = None, top: int = 25) -> ReportQuery:
    """Per-account summary.

    Args:
        db: Open read-only database connection.
        month: Inclusive month filter (YYYY-MM).
        top: Maximum rows to return.

    Examples:
        reports_summary(top=5)
    """
    return ReportQuery("SELECT 1", [])


def _spec():  # noqa: ANN202 — test helper
    return build_spec(_runner, name="summary", view=_VIEW, domain="cashflow")


def test_make_tool_fn_signature_matches_params() -> None:
    fn = make_tool_fn(_spec())
    sig = inspect.signature(fn)
    assert list(sig.parameters) == ["month", "top"]  # db excluded
    assert all(
        p.kind is inspect.Parameter.KEYWORD_ONLY for p in sig.parameters.values()
    )
    assert sig.parameters["top"].default == 25
    assert sig.parameters["month"].default is None


def test_make_tool_fn_builds_envelope_from_result() -> None:
    result = ReportResult(
        records=[{"account_id": "****2222", "txn_count": 2}],
        columns=["account_id", "txn_count"],
        output_classes={
            "account_id": DataClass.ACCOUNT_IDENTIFIER,
            "txn_count": DataClass.AGGREGATE,
        },
        tier=Tier.CRITICAL,
        total_count=1,
        truncated=False,
    )
    fn = make_tool_fn(_spec())
    with (
        patch("moneybin.reports._framework.mcp_register.get_database", MagicMock()),
        patch(
            "moneybin.reports._framework.mcp_register.run_report",
            return_value=result,
        ) as mock_run,
    ):
        env = fn(top=5)

    assert env.data == result.records
    assert env.summary.sensitivity == tier_to_sensitivity(Tier.CRITICAL).value
    assert env.summary.total_count == 1
    assert sorted(env.classes_returned or []) == ["account_identifier", "aggregate"]
    # params forwarded to run_report
    assert mock_run.call_args.kwargs["top"] == 5


def test_generated_tool_value_error_yields_error_envelope() -> None:
    # A runner ValueError must surface as an INFRA_INVALID_INPUT error envelope
    # through the @mcp_tool decorator chain. One test covers the error path for
    # every generated report tool (they share make_tool_fn + the decorator).
    decorated = mcp_tool(dynamic_classification=True, domain="cashflow")(
        make_tool_fn(_spec())
    )
    with (
        patch("moneybin.reports._framework.mcp_register.get_database", MagicMock()),
        patch(
            "moneybin.reports._framework.mcp_register.run_report",
            side_effect=ValueError("Unknown compare: bogus"),
        ),
        # The audit sink is a filesystem dependency; stub it so this stays a unit test.
        patch("moneybin.mcp.decorator.write_privacy_event"),
    ):
        env = asyncio.run(decorated(top=5))

    assert env.error is not None
    assert env.error.code == error_codes.INFRA_INVALID_INPUT


async def test_register_report_mcp_registers_tool() -> None:
    mcp = FastMCP("reports-test")
    register_report_mcp(_spec(), mcp)
    async with Client(mcp) as client:
        tools = {t.name: t for t in await client.list_tools()}
    assert "reports_summary" in tools
    schema = tools["reports_summary"].inputSchema
    assert set(schema["properties"]) == {"month", "top"}
    assert "Per-account summary." in (tools["reports_summary"].description or "")
