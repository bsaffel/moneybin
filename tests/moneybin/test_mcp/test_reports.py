"""Contract tests for the dormant generic ``reports`` MCP tool."""

from __future__ import annotations

import json
from collections.abc import Mapping
from contextlib import AbstractContextManager
from datetime import date
from decimal import Decimal
from typing import Any, cast
from unittest.mock import MagicMock, patch

import pytest
from fastmcp import Client, FastMCP
from mcp.types import TextContent
from pydantic import JsonValue

from moneybin.database import Database
from moneybin.mcp.tools.reports import reports
from moneybin.privacy.taxonomy import DataClass, Tier
from moneybin.reports._framework.catalog import ReportCatalog, ServiceReportSpec
from moneybin.reports._framework.contract import (
    OutputColumn,
    ParamSpec,
    ReportSemantics,
)
from moneybin.reports._framework.execute import (
    CatalogReportResult,
    build_catalog_result,
)
from moneybin.reports._framework.registry import register_generic_reports_tool

_SEMANTICS = ReportSemantics(
    unit="currency",
    currency="summary.display_currency",
    sign="signed accounting amount",
    kind="flow",
    valuation_basis="transaction amount",
    fx_basis="no FX conversion",
    time_basis="calendar date",
    denominator=None,
    comparison_window=None,
    exclusions=(),
    provenance=("reports.transport_test",),
)
_COLUMNS = (
    OutputColumn("period_date", "Report date.", DataClass.TXN_DATE),
    OutputColumn("amount", "Signed amount.", DataClass.TXN_AMOUNT),
    OutputColumn(
        "account_id",
        "Account identifier.",
        DataClass.ACCOUNT_IDENTIFIER,
    ),
)
_CLASSES = {column.name: column.data_class for column in _COLUMNS}


def _transport_report() -> ServiceReportSpec:
    def execute(
        db: Database,  # noqa: ARG001  # contract handle
        parameters: Mapping[str, JsonValue],
        limit: int,
    ) -> CatalogReportResult:
        return build_catalog_result(
            spec,
            parameters=parameters,
            records=[
                {
                    "period_date": date(2026, 7, 1),
                    "amount": Decimal("12.34"),
                    "account_id": "acct_11112222",
                },
                {
                    "period_date": date(2026, 7, 2),
                    "amount": Decimal("-5.67"),
                    "account_id": "acct_99998888",
                },
            ],
            columns=[column.name for column in _COLUMNS],
            max_rows=limit,
            actions=["Inspect another registered report."],
            period="2026-07-01 to 2026-07-02",
        )

    spec = ServiceReportSpec(
        report_id="test:transport",
        name="transport",
        description="Transport fidelity report.",
        parameters=(
            ParamSpec(
                "account_filters",
                dict[str, str],
                None,
                True,
                "Sensitive account-reference mapping.",
                DataClass.USER_NOTE,
            ),
        ),
        columns=_COLUMNS,
        semantics=_SEMANTICS,
        classes=_CLASSES,
        examples=(),
        executor=execute,
    )
    return spec


def _database_context(
    db: Database,
) -> MagicMock:
    context = MagicMock(spec=AbstractContextManager)
    context.__enter__.return_value = db
    context.__exit__.return_value = None
    return context


@pytest.mark.unit
async def test_reports_without_id_returns_catalog_with_runtime_classification() -> None:
    captured: list[dict[str, Any]] = []

    def capture_event(event: dict[str, Any]) -> None:
        captured.append(event)

    with patch(
        "moneybin.mcp.decorator.write_privacy_event",
        capture_event,
    ):
        response = await reports()

    assert response.error is None
    assert response.data.kind == "catalog"
    assert "core:spending" in {entry.report_id for entry in response.data.reports}
    assert response.summary.sensitivity == "low"
    assert response.classes_returned == ["aggregate"]
    assert response.summary.returned_count == len(response.data.reports)
    assert len(captured) == 1
    assert captured[0]["sensitivity"] == "low"
    assert captured[0]["classes_returned"] == ["aggregate"]
    assert captured[0]["row_count"] == len(response.data.reports)


@pytest.mark.unit
@pytest.mark.parametrize(
    ("parameters", "limit"),
    [
        ({}, None),
        ({"from_month": "2026-06"}, None),
        (None, 0),
    ],
)
async def test_reports_without_id_rejects_execution_arguments(
    parameters: dict[str, JsonValue] | None,
    limit: int | None,
) -> None:
    with patch("moneybin.mcp.decorator.write_privacy_event"):
        response = await reports(parameters=parameters, limit=limit)

    assert response.to_dict()["status"] == "error"
    assert response.error is not None
    assert response.error.code == "REPORT_ID_REQUIRED"


@pytest.mark.unit
async def test_reports_with_id_opens_one_read_only_database_and_executes() -> None:
    result = CatalogReportResult(
        report_id="core:spending",
        parameters={"from_month": "2026-06", "to_month": "2026-06"},
        semantics=_SEMANTICS,
        provenance=_SEMANTICS.provenance,
        records=[
            {
                "period_date": date(2026, 6, 1),
                "amount": Decimal("12.34"),
                "account_id": "****2222",
            }
        ],
        columns=[column.name for column in _COLUMNS],
        output_classes=_CLASSES,
        tier=Tier.CRITICAL,
        total_count=3,
        truncated=True,
        actions=["Inspect another registered report."],
        period="2026-06",
        display_currency="CAD",
    )
    catalog = MagicMock(spec=ReportCatalog)
    catalog.execute.return_value = result
    db = cast(Database, MagicMock(spec=Database))
    database_context = _database_context(db)

    with (
        patch(
            "moneybin.mcp.tools.reports.get_report_catalog",
            return_value=catalog,
        ),
        patch(
            "moneybin.mcp.tools.reports.get_database",
            return_value=database_context,
        ) as get_database,
        patch("moneybin.mcp.tools.reports.get_max_rows", return_value=50),
        patch("moneybin.mcp.decorator.write_privacy_event"),
    ):
        response = await reports(
            report_id="core:spending",
            parameters={"from_month": "2026-06", "to_month": "2026-06"},
        )

    get_database.assert_called_once_with(read_only=True)
    catalog.execute.assert_called_once_with(
        db,
        report_id="core:spending",
        parameters={"from_month": "2026-06", "to_month": "2026-06"},
        limit=50,
    )
    assert response.data.kind == "result"
    assert response.data.report_id == "core:spending"
    assert response.summary.sensitivity == "critical"
    assert response.summary.total_count == 3
    assert response.summary.returned_count == 1
    assert response.summary.has_more is True
    assert response.summary.period == "2026-06"
    assert response.summary.display_currency == "CAD"
    assert response.actions == ["Inspect another registered report."]
    assert response.classes_returned == [
        "account_identifier",
        "txn_amount",
        "txn_date",
    ]


@pytest.mark.unit
@pytest.mark.parametrize(
    ("requested", "expected"),
    [
        (None, 1),
        (0, 0),
        (1, 1),
        (100, 1),
    ],
)
async def test_reports_preserves_zero_and_caps_only_positive_limits(
    requested: int | None,
    expected: int,
) -> None:
    catalog = ReportCatalog((_transport_report(),))
    db = cast(Database, MagicMock(spec=Database))

    with (
        patch(
            "moneybin.mcp.tools.reports.get_report_catalog",
            return_value=catalog,
        ),
        patch(
            "moneybin.mcp.tools.reports.get_database",
            return_value=_database_context(db),
        ),
        patch("moneybin.mcp.tools.reports.get_max_rows", return_value=1),
        patch("moneybin.mcp.decorator.write_privacy_event"),
    ):
        response = await reports(
            report_id="test:transport",
            parameters={"account_filters": {"primary": "acct_11112222"}},
            limit=requested,
        )

    assert response.error is None
    assert response.summary.returned_count == expected


@pytest.mark.unit
async def test_reports_negative_limit_reaches_catalog_validation() -> None:
    catalog = ReportCatalog((_transport_report(),))
    db = cast(Database, MagicMock(spec=Database))

    with (
        patch(
            "moneybin.mcp.tools.reports.get_report_catalog",
            return_value=catalog,
        ),
        patch(
            "moneybin.mcp.tools.reports.get_database",
            return_value=_database_context(db),
        ),
        patch("moneybin.mcp.tools.reports.get_max_rows", return_value=50),
        patch("moneybin.mcp.decorator.write_privacy_event"),
    ):
        response = await reports(
            report_id="test:transport",
            parameters={"account_filters": {"primary": "acct_11112222"}},
            limit=-1,
        )

    assert response.to_dict()["status"] == "error"
    assert response.error is not None
    assert response.error.code == "REPORT_LIMIT_INVALID"


@pytest.mark.unit
async def test_generic_reports_fastmcp_schema_and_catalog_transport() -> None:
    mcp = FastMCP("reports-contract")
    register_generic_reports_tool(mcp)
    captured: list[dict[str, Any]] = []

    def capture_event(event: dict[str, Any]) -> None:
        captured.append(event)

    with patch("moneybin.mcp.decorator.write_privacy_event", capture_event):
        async with Client(mcp) as client:
            tools = await client.list_tools()
            result = await client.call_tool("reports", {})

    assert {tool.name for tool in tools} == {"reports"}
    tool = tools[0]
    assert tool.outputSchema is None
    assert set(tool.inputSchema["properties"]) == {
        "report_id",
        "parameters",
        "limit",
    }
    properties = tool.inputSchema["properties"]
    assert {branch.get("type") for branch in properties["report_id"]["anyOf"]} == {
        "string",
        "null",
    }
    assert {branch.get("type") for branch in properties["parameters"]["anyOf"]} == {
        "object",
        "null",
    }
    assert {branch.get("type") for branch in properties["limit"]["anyOf"]} == {
        "integer",
        "null",
    }
    assert "sql" not in tool.inputSchema["properties"]
    assert "catalog" in (tool.description or "").lower()
    assert "registered read-only report" in (tool.description or "").lower()
    assert "never accepts sql" in (tool.description or "").lower()
    assert "sql_query" in (tool.description or "")

    text = next(
        block.text for block in result.content if isinstance(block, TextContent)
    )
    assert result.structured_content is not None
    assert json.loads(text) == result.structured_content
    assert result.structured_content["data"]["kind"] == "catalog"
    report_count = len(result.structured_content["data"]["reports"])
    assert len(captured) == 1
    assert captured[0]["sensitivity"] == "low"
    assert captured[0]["classes_returned"] == ["aggregate"]
    assert captured[0]["row_count"] == report_count


@pytest.mark.unit
async def test_generic_reports_fastmcp_result_transport_and_dynamic_audit() -> None:
    mcp = FastMCP("reports-contract")
    register_generic_reports_tool(mcp)
    catalog = ReportCatalog((_transport_report(),))
    db = cast(Database, MagicMock(spec=Database))
    captured: list[dict[str, Any]] = []
    sensitive_key = "acct_key_11112222"
    sensitive_value = "acct_value_99998888"

    def capture_event(event: dict[str, Any]) -> None:
        captured.append(event)

    with (
        patch(
            "moneybin.mcp.tools.reports.get_report_catalog",
            return_value=catalog,
        ),
        patch(
            "moneybin.mcp.tools.reports.get_database",
            return_value=_database_context(db),
        ),
        patch("moneybin.mcp.tools.reports.get_max_rows", return_value=1),
        patch(
            "moneybin.mcp.decorator.write_privacy_event",
            capture_event,
        ),
    ):
        async with Client(mcp) as client:
            result = await client.call_tool(
                "reports",
                {
                    "report_id": "test:transport",
                    "parameters": {
                        "account_filters": {sensitive_key: sensitive_value},
                    },
                    "limit": 100,
                },
            )

    text = next(
        block.text for block in result.content if isinstance(block, TextContent)
    )
    structured = result.structured_content
    assert structured is not None
    assert json.loads(text) == structured
    assert structured["data"]["kind"] == "result"
    assert structured["data"]["rows"] == [
        {
            "period_date": "2026-07-01",
            "amount": 12.34,
            "account_id": "****2222",
        }
    ]
    assert isinstance(structured["data"]["rows"][0]["amount"], (int, float))
    assert structured["data"]["parameters"] == {
        "account_filters": {"entry_count": 1, "redacted": True},
    }
    assert sensitive_key not in text
    assert sensitive_value not in text
    assert structured["summary"]["sensitivity"] == "critical"
    assert structured["summary"]["total_count"] == 2
    assert structured["summary"]["returned_count"] == 1
    assert structured["summary"]["has_more"] is True
    assert structured["summary"]["period"] == "2026-07-01 to 2026-07-02"
    assert structured["summary"]["display_currency"] == "USD"
    assert structured["data"]["truncated"] is True
    assert len(captured) == 1
    assert captured[0]["sensitivity"] == "critical"
    assert captured[0]["classes_returned"] == [
        "account_identifier",
        "txn_amount",
        "txn_date",
    ]
    assert captured[0]["row_count"] == 1


@pytest.mark.integration
async def test_live_registry_uses_generic_reports_tool(
    mcp_db: object,
) -> None:
    from moneybin.mcp.server import init_db, mcp
    from moneybin.mcp.surface import STANDARD_TOOL_NAMES

    init_db()
    async with Client(mcp) as client:
        names = {tool.name for tool in await client.list_tools()}

    assert names == set(STANDARD_TOOL_NAMES)
    assert "reports" in names
    assert "reports_spending" not in names
