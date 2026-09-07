"""Contract tests for the standard generic ``reports`` MCP tool."""

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

from moneybin.database import Database, DatabaseNotInitializedError
from moneybin.mcp.tools.reports import reports
from moneybin.privacy.taxonomy import CLASSIFICATION, DataClass, Tier
from moneybin.reports._framework.catalog import (
    DEGRADED_PENDING_DEDUP,
    ReportCatalog,
    ServiceReportSpec,
)
from moneybin.reports._framework.contract import (
    OutputColumn,
    ParamSpec,
    ReportSemantics,
)
from moneybin.reports._framework.dynamic import DEGRADED_STALE_CLASSIFICATION
from moneybin.reports._framework.execute import (
    CatalogReportExecution,
    CatalogReportResult,
    build_catalog_execution,
)
from moneybin.reports._framework.registry import register_generic_reports_tool
from moneybin.services.currency_service import ResolvedRate
from moneybin.services.matching_service import PENDING_MATCHES_HINT
from tests.database_mocks import without_a_profile
from tests.moneybin.db_helpers import create_core_tables_raw, seed_pending_dedup_pair

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
        limit: int | None,
    ) -> CatalogReportExecution:
        return build_catalog_execution(
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
            column_types=["DATE", "DECIMAL(18,2)", "VARCHAR"],
            max_rows=limit,
            actions=["Inspect another registered report."],
            period="2026-07-01 to 2026-07-02",
            sql=None,
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


def _mock_database() -> Database:
    """A mock connection that answers the profile's home-currency read.

    ``reports`` reads it before executing, and a mock answers that read with a
    child mock the repository cannot unpack — see ``tests/database_mocks.py``.
    Tests taking the real ``db`` fixture answer the read for themselves.
    """
    return cast(Database, without_a_profile(MagicMock(spec=Database)))


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

    # The catalog spans all three tiers, so listing opens a read-only database
    # for the user tier — it is no longer a pure-metadata call. The open lives in
    # `open_report_catalog` (catalog.py), which resolves its own `get_database`,
    # so the patch has to name *that* module: patching this tool's import left
    # the real open running, and its `DatabaseNotInitializedError` degraded the
    # catalog to the packaged tiers — making the `low` assertions below pass
    # because no user report existed rather than because the tier was computed.
    # Stated as the degradation it is, so the fixture isolates one condition.
    with (
        patch(
            "moneybin.reports._framework.catalog.get_database",
            side_effect=DatabaseNotInitializedError("no database yet"),
        ),
        patch(
            "moneybin.mcp.decorator.write_privacy_event",
            capture_event,
        ),
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
async def test_reports_catalog_elevates_to_medium_when_a_user_report_is_listed(
    db: Database,
) -> None:
    """A listing carrying a user-authored name reports MEDIUM, not LOW.

    The counterpart to the degraded test above, and the one arm no test reached:
    every other catalog test runs on a profile with no database, where the user
    tier is absent and ``low`` is correct for the wrong reason. This saves a
    report through the real service so the tier is present in the rows
    ``catalog_sensitivity`` actually reads.
    """
    from moneybin.services.user_reports_service import UserReportsService

    create_core_tables_raw(db.conn)
    UserReportsService(db).create(
        name="my_accounts",
        query_sql="SELECT account_id FROM core.dim_accounts",
        description="Accounts I care about.",
        actor="cli",
    )

    captured: list[dict[str, Any]] = []

    with (
        patch(
            "moneybin.reports._framework.catalog.get_database",
            return_value=_database_context(db),
        ),
        patch("moneybin.mcp.decorator.write_privacy_event", captured.append),
    ):
        response = await reports()

    assert response.error is None
    assert "user" in {entry.tier for entry in response.data.reports}
    assert response.summary.sensitivity == "medium"
    assert response.classes_returned == ["aggregate", "user_note"]
    assert captured[0]["sensitivity"] == "medium"
    assert captured[0]["classes_returned"] == ["aggregate", "user_note"]


@pytest.mark.unit
async def test_reports_run_of_a_drifted_saved_report_says_so_in_the_envelope(
    db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """R4's verdict has to reach the agent, not only the CLI and the receipt.

    The CLI prints the reason and the export receipt carries its code, each with
    its own test; ``summary.degraded`` / ``degraded_reason`` is how the agent
    learns the same thing, and nothing exercised it through an actual tool call.
    A drifted report returns ``*****`` either way, so without this an agent reads
    masked cells as the honest answer.

    Driven through the real service and a real reclassification rather than a
    mocked result: the wiring under test is exactly the hand-off from catalog
    status to envelope, which a stubbed ``CatalogReportResult`` would supply.
    """
    from moneybin.services.user_reports_service import UserReportsService

    create_core_tables_raw(db.conn)
    db.execute(
        "INSERT INTO core.dim_accounts (account_id) VALUES (?)", ["acct_11112222"]
    )
    UserReportsService(db).create(
        name="my_accounts",
        query_sql="SELECT account_id FROM core.dim_accounts",
        actor="cli",
    )
    reclassified = dict(CLASSIFICATION)
    reclassified[("core", "dim_accounts")] = {
        **CLASSIFICATION[("core", "dim_accounts")],
        "account_id": DataClass.ROUTING_NUMBER,
    }
    monkeypatch.setattr("moneybin.privacy.sql_lineage.CLASSIFICATION", reclassified)

    with (
        patch(
            "moneybin.reports._framework.catalog.get_database",
            return_value=_database_context(db),
        ),
        patch(
            "moneybin.mcp.tools.reports.get_database",
            return_value=_database_context(db),
        ),
        patch("moneybin.mcp.tools.reports.get_max_rows", return_value=50),
        patch("moneybin.mcp.decorator.write_privacy_event"),
    ):
        response = await reports(report_id="my_accounts")

    assert response.error is None
    assert response.summary.degraded is True
    assert response.summary.degraded_reason is not None
    assert response.summary.degraded_reason.startswith(DEGRADED_STALE_CLASSIFICATION)
    assert [row["account_id"] for row in response.data.rows] == ["*****"]


@pytest.mark.unit
async def test_reports_marks_a_total_provisional_while_a_duplicate_pair_is_undecided(
    db: Database,
) -> None:
    """Issue #409: the agent reading the number is the one owed the caveat.

    Dedup escalates a low-confidence duplicate to the review queue rather than
    merging it, so both rows stay in ``core.fct_transactions`` and every total
    covering them is overstated. ``summary.degraded_reason`` carries the caveat,
    ``actions`` the CLI hint, and ``recovery_actions`` the queue an agent can
    read and decide without parsing that hint.
    """
    from moneybin.services.user_reports_service import UserReportsService

    create_core_tables_raw(db.conn)
    seed_pending_dedup_pair(db)
    UserReportsService(db).create(
        name="my_total",
        query_sql="SELECT SUM(amount) AS total FROM core.fct_transactions",
        actor="cli",
    )

    with (
        patch(
            "moneybin.mcp.tools.reports.get_database",
            return_value=_database_context(db),
        ),
        patch("moneybin.mcp.tools.reports.get_max_rows", return_value=50),
        patch("moneybin.mcp.decorator.write_privacy_event"),
    ):
        response = await reports(report_id="my_total")

    assert response.error is None
    assert response.summary.degraded is True
    assert response.summary.degraded_reason is not None
    assert response.summary.degraded_reason.startswith(f"{DEGRADED_PENDING_DEDUP}: 1 ")
    assert PENDING_MATCHES_HINT in response.actions
    assert response.recovery_actions is not None
    assert [action.tool for action in response.recovery_actions] == ["reviews"]


@pytest.mark.unit
async def test_reports_returns_not_found_for_an_unknown_report_id(
    db: Database,
) -> None:
    """The likeliest real MCP error for this feature: a hallucinated handle.

    An agent mistyping or inventing a ``user:r…`` id is asserted at the catalog
    layer, but nothing checked that the code survives the tool wrapper's own error
    translation — which is the only place the agent reads it. Driven through the
    real catalog so the wrapper's translation is what is under test.
    """
    create_core_tables_raw(db.conn)

    with (
        patch(
            "moneybin.reports._framework.catalog.get_database",
            return_value=_database_context(db),
        ),
        patch(
            "moneybin.mcp.tools.reports.get_database",
            return_value=_database_context(db),
        ),
        patch("moneybin.mcp.tools.reports.get_max_rows", return_value=50),
        patch("moneybin.mcp.decorator.write_privacy_event"),
    ):
        response = await reports(report_id="user:rnosuchreport")

    assert response.to_dict()["status"] == "error"
    assert response.error is not None
    assert response.error.code == "report_id_not_found"


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
    assert response.error.code == "report_id_required"


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
        applied_rates=(
            ResolvedRate(
                from_currency="USD",
                to_currency="CAD",
                requested_date=date(2026, 6, 1),
                rate_date=date(2026, 5, 29),
                rate=Decimal("1.37"),
                source="frankfurter",
            ),
        ),
    )
    catalog = MagicMock(spec=ReportCatalog)
    catalog.execute.return_value = result
    db = _mock_database()
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
        display_currency=None,
        home_currency=None,
    )
    assert response.data.kind == "result"
    assert response.data.report_id == "core:spending"
    assert response.summary.sensitivity == "critical"
    assert response.summary.total_count == 3
    assert response.summary.returned_count == 1
    assert response.summary.has_more is True
    assert response.summary.period == "2026-06"
    assert response.summary.display_currency == "CAD"
    # The tool's own registered description promises this field. It builds its
    # envelope by hand rather than through `ReportResult.to_envelope`, because
    # it publishes a typed payload rather than raw records — which is exactly
    # how the field went missing on the one report-reading surface an agent
    # actually calls.
    assert response.summary.applied_rates == [
        {
            "from_currency": "USD",
            "to_currency": "CAD",
            "requested_date": "2026-06-01",
            "rate_date": "2026-05-29",
            "rate": Decimal("1.37"),
            "source": "frankfurter",
        }
    ]
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
        (1, 1),
        (100, 1),
    ],
)
async def test_reports_caps_positive_limits(
    requested: int | None,
    expected: int,
) -> None:
    catalog = ReportCatalog((_transport_report(),))
    db = _mock_database()

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
@pytest.mark.parametrize("limit", [0, -1])
async def test_reports_rejects_non_positive_limit(limit: int) -> None:
    catalog = ReportCatalog((_transport_report(),))
    db = _mock_database()

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
            limit=limit,
        )

    assert response.to_dict()["status"] == "error"
    assert response.error is not None
    assert response.error.code == "report_limit_invalid"


@pytest.mark.unit
async def test_generic_reports_fastmcp_schema_and_catalog_transport() -> None:
    mcp = FastMCP("reports-contract")
    register_generic_reports_tool(mcp)
    captured: list[dict[str, Any]] = []

    def capture_event(event: dict[str, Any]) -> None:
        captured.append(event)

    # Same patch target as the catalog test above, for the same reason: the open
    # this needs to control belongs to `open_report_catalog`, not to this tool.
    with (
        patch(
            "moneybin.reports._framework.catalog.get_database",
            side_effect=DatabaseNotInitializedError("no database yet"),
        ),
        patch("moneybin.mcp.decorator.write_privacy_event", capture_event),
    ):
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
        "display_currency",
    }
    properties = tool.inputSchema["properties"]
    assert {
        branch.get("type") for branch in properties["display_currency"]["anyOf"]
    } == {
        "string",
        "null",
    }
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
    integer_limit = next(
        branch
        for branch in properties["limit"]["anyOf"]
        if branch.get("type") == "integer"
    )
    assert integer_limit["minimum"] == 1
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
    db = _mock_database()
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
    # This report's columns are period_date / amount / account_id — it never
    # states a currency, so the wire must not either. The assertion read "USD"
    # while the dataclass default supplied one for free, which is what shipped a
    # confident denomination to agents for every currency-less report.
    assert structured["summary"]["display_currency"] is None
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
