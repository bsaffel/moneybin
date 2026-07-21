"""Self-tests for rendered MCP schema compatibility assertions."""

from __future__ import annotations

import json
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from fastmcp import FastMCP
from mcp.types import TextContent
from pydantic import StrictBool

from moneybin.database import get_database
from moneybin.mcp._registration import register
from moneybin.mcp.decorator import mcp_tool
from moneybin.mcp.privacy import Sensitivity
from moneybin.mcp.tools.accounts import register_accounts_coarse_reads
from moneybin.mcp.tools.gsheet import register_gsheet_coarse_reads
from moneybin.mcp.tools.import_tools import register_import_coarse_reads
from moneybin.mcp.tools.investments import register_investment_coarse_reads
from moneybin.mcp.tools.privacy import register_privacy_coarse_reads
from moneybin.mcp.tools.system import register_system_coarse_reads
from moneybin.mcp.tools.transactions import register_transaction_coarse_reads
from moneybin.protocol.envelope import ResponseEnvelope, build_envelope

from .schema_assertions import (
    assert_discriminated_variants,
    assert_literal_values,
    call_tool_raw,
    isolated_server,
    listed_tool,
)


def register_strict_probe(mcp: FastMCP) -> None:
    """Register a standard probe through MoneyBin's normal adapter."""

    @mcp_tool(dynamic_classification=True, maximum_sensitivity=Sensitivity.HIGH)
    async def strict_probe(enabled: StrictBool) -> ResponseEnvelope[Any]:
        return build_envelope(data={"enabled": enabled})

    register(mcp, strict_probe, "strict_probe", "Reject non-boolean input.")


def test_literal_helper_reads_ref_resolved_schema() -> None:
    schema = {
        "$defs": {"view": {"type": "string", "enum": ["list", "detail"]}},
        "properties": {"view": {"$ref": "#/$defs/view"}},
    }

    assert_literal_values(schema, ("properties", "view"), {"list", "detail"})


def test_discriminated_helper_reads_ref_resolved_variants() -> None:
    json_schema = {
        "$defs": {
            "list": {
                "type": "object",
                "properties": {"view": {"const": "list"}},
                "required": ["view"],
            },
            "detail": {
                "type": "object",
                "properties": {
                    "view": {"const": "detail"},
                    "account_id": {"type": "string"},
                },
                "required": ["view", "account_id"],
            },
        },
        "discriminator": {"propertyName": "view"},
        "oneOf": [{"$ref": "#/$defs/list"}, {"$ref": "#/$defs/detail"}],
    }

    assert_discriminated_variants(
        json_schema,
        {"list": {"view"}, "detail": {"view", "account_id"}},
    )


def test_discriminated_helper_finds_nested_variants() -> None:
    schema = {
        "$defs": {
            "list": {
                "type": "object",
                "properties": {"view": {"const": "list"}},
                "required": ["view"],
            },
            "detail": {
                "type": "object",
                "properties": {
                    "view": {"const": "detail"},
                    "account_id": {"type": "string"},
                },
                "required": ["view", "account_id"],
            },
        },
        "properties": {
            "request": {
                "discriminator": {"propertyName": "view"},
                "oneOf": [{"$ref": "#/$defs/list"}, {"$ref": "#/$defs/detail"}],
            }
        },
    }

    assert_discriminated_variants(
        schema,
        {"list": {"view"}, "detail": {"view", "account_id"}},
    )


async def test_listed_tool_reads_rendered_input_schema() -> None:
    mcp = isolated_server(register_strict_probe)

    tool = await listed_tool(mcp, "strict_probe")

    assert tool.inputSchema["properties"]["enabled"]["type"] == "boolean"


@pytest.mark.integration
async def test_live_standard_read_selectors_render_exactly() -> None:
    from moneybin.mcp.server import init_db, mcp

    init_db()
    expected_literals = {
        ("system_status", "detail"): {"summary", "full"},
        ("system_audit", "view"): {"events", "history", "detail"},
        ("accounts", "view"): {"list", "detail", "summary", "resolve"},
        ("accounts_balances", "view"): {
            "latest",
            "history",
            "assertions",
            "reconcile",
        },
        ("investments", "view"): {
            "events",
            "holdings",
            "lots",
            "gains",
            "securities",
        },
        ("transactions_categorize_rules", "view"): {
            "active",
            "inactive",
            "history",
        },
        ("reviews", "kind"): {
            "summary",
            "categorization",
            "auto_rules",
            "matches",
            "account_links",
            "merchant_links",
            "security_links",
        },
        ("reviews", "status"): {"pending", "history"},
        ("taxonomy", "view"): {"categories", "merchants"},
        ("gsheet", "view"): {"connections", "status"},
        ("privacy", "view"): {"status", "log"},
    }
    for (name, field), expected in expected_literals.items():
        tool = await listed_tool(mcp, name)
        assert_literal_values(
            tool.inputSchema,
            ("properties", field),
            expected,
        )

    status = await listed_tool(mcp, "system_status")
    assert_literal_values(
        status.inputSchema["properties"]["sections"]["anyOf"][0],
        ("items",),
        {"overview", "doctor", "categorization", "exports"},
    )
    import_status = await listed_tool(mcp, "import_status")
    assert_literal_values(
        import_status.inputSchema["properties"]["sections"]["anyOf"][0],
        ("items",),
        {"imports", "formats", "inbox"},
    )


@pytest.mark.parametrize("bad", ["false", "0", "[]", "{}"])
async def test_strict_probe_does_not_coerce(bad: str) -> None:
    mcp = isolated_server(register_strict_probe)

    response = await call_tool_raw(mcp, "strict_probe", {"enabled": bad})

    assert response.isError is True


async def _assert_canonical_variant(
    mcp: FastMCP,
    name: str,
    arguments: dict[str, Any],
    expected_kind: str,
) -> dict[str, Any]:
    response = await call_tool_raw(mcp, name, arguments)
    text = next(
        block.text for block in response.content if isinstance(block, TextContent)
    )
    assert response.structuredContent is not None
    assert json.loads(text) == response.structuredContent
    assert response.structuredContent["data"]["kind"] == expected_kind
    return response.structuredContent


async def _assert_canonical_error(
    mcp: FastMCP,
    name: str,
    arguments: dict[str, Any],
    expected_code: str,
) -> dict[str, Any]:
    response = await call_tool_raw(mcp, name, arguments)
    text = next(
        block.text for block in response.content if isinstance(block, TextContent)
    )
    assert response.isError is False
    assert response.structuredContent is not None
    assert json.loads(text) == response.structuredContent
    assert response.structuredContent["status"] == "error"
    assert response.structuredContent["error"]["code"] == expected_code
    return response.structuredContent


async def test_system_coarse_tools_render_schema_contract() -> None:
    mcp = isolated_server(register_system_coarse_reads)

    status = await listed_tool(mcp, "system_status")
    audit = await listed_tool(mcp, "system_audit")

    assert status.outputSchema is None
    assert audit.outputSchema is None
    assert status.annotations is not None
    assert status.annotations.readOnlyHint is False
    assert audit.annotations is not None
    assert audit.annotations.readOnlyHint is True
    sections_schema = status.inputSchema["properties"]["sections"]["anyOf"][0]
    assert_literal_values(
        sections_schema,
        ("items",),
        {"overview", "doctor", "categorization", "exports"},
    )
    assert_literal_values(
        status.inputSchema,
        ("properties", "detail"),
        {"summary", "full"},
    )
    assert_literal_values(
        audit.inputSchema,
        ("properties", "view"),
        {"events", "history", "detail"},
    )


@pytest.mark.parametrize("section", ["overview", "doctor", "categorization", "exports"])
async def test_system_status_coarse_transport_variants(
    section: str,
    mcp_db: object,
) -> None:
    mcp = isolated_server(register_system_coarse_reads)

    structured = await _assert_canonical_variant(
        mcp,
        "system_status",
        {"sections": [section]},
        expected_kind="sections",
    )

    assert structured["data"]["sections"][0]["kind"] == section


@pytest.mark.parametrize(
    ("name", "arguments", "sensitivity"),
    [
        ("system_status", {"sections": ["overview"]}, "low"),
        ("system_status", {"sections": []}, "low"),
        ("system_audit", {"view": "events"}, "high"),
        ("system_audit", {"view": "detail"}, "low"),
    ],
)
async def test_system_coarse_call_emits_public_privacy_actor(
    name: str,
    arguments: dict[str, Any],
    sensitivity: str,
    mcp_db: object,
) -> None:
    captured: list[dict[str, Any]] = []
    mcp = isolated_server(register_system_coarse_reads)

    with patch(
        "moneybin.mcp.decorator.write_privacy_event",
        captured.append,
    ):
        await call_tool_raw(mcp, name, arguments)

    assert len(captured) == 1
    assert captured[0]["actor"] == f"mcp.{name}"
    assert captured[0]["sensitivity"] == sensitivity


async def test_system_audit_coarse_transport_variants(mcp_db: object) -> None:
    from moneybin.database import get_database
    from moneybin.repositories.transaction_tags_repo import TransactionTagsRepo
    from moneybin.services.audit_service import AuditService
    from moneybin.services.mutation_context import operation

    with get_database(read_only=False) as db, operation() as operation_id:
        TransactionTagsRepo(db).add(
            transaction_id="txn_1",
            tag="schema-contract",
            actor="cli",
        )
    with get_database(read_only=True) as db:
        audit_id = AuditService(db).events_for_operation(operation_id)[0].audit_id

    mcp = isolated_server(register_system_coarse_reads)
    await _assert_canonical_variant(mcp, "system_audit", {}, "events")
    await _assert_canonical_variant(
        mcp,
        "system_audit",
        {"view": "history"},
        "history",
    )
    await _assert_canonical_variant(
        mcp,
        "system_audit",
        {"view": "detail", "audit_id": audit_id},
        "detail",
    )


@pytest.mark.parametrize(
    ("name", "arguments"),
    [
        ("system_status", {"sections": ["health"]}),
        ("system_status", {"detail": "verbose"}),
        ("system_audit", {"view": "list"}),
        ("system_audit", {"limit": "50"}),
        ("system_audit", {"unknown": "value"}),
    ],
)
async def test_system_coarse_tools_reject_invalid_raw_arguments(
    name: str,
    arguments: dict[str, Any],
) -> None:
    mcp = isolated_server(register_system_coarse_reads)

    response = await call_tool_raw(mcp, name, arguments)

    assert response.isError is True


async def test_accounts_coarse_tools_render_schema_contract() -> None:
    mcp = isolated_server(register_accounts_coarse_reads)

    accounts = await listed_tool(mcp, "accounts")
    balances = await listed_tool(mcp, "accounts_balances")

    assert accounts.outputSchema is None
    assert balances.outputSchema is None
    assert accounts.annotations is not None
    assert accounts.annotations.readOnlyHint is True
    assert balances.annotations is not None
    assert balances.annotations.readOnlyHint is True
    assert_literal_values(
        accounts.inputSchema,
        ("properties", "view"),
        {"list", "detail", "summary", "resolve"},
    )
    assert_literal_values(
        balances.inputSchema,
        ("properties", "view"),
        {"latest", "history", "assertions", "reconcile"},
    )
    assert accounts.inputSchema["properties"]["include_closed"]["type"] == "boolean"
    threshold_schema = json.dumps(balances.inputSchema["properties"]["threshold"])
    assert '"number"' in threshold_schema
    assert '"string"' not in threshold_schema
    for field in ("start", "end"):
        date_schema = balances.inputSchema["properties"][field]["anyOf"][0]
        assert date_schema["type"] == "string"
        assert date_schema["format"] == "date"


@pytest.mark.parametrize(
    ("name", "arguments", "kind", "sensitivity"),
    [
        ("accounts", {}, "list", "critical"),
        (
            "accounts",
            {"view": "detail", "reference": "CHECKING"},
            "detail",
            "critical",
        ),
        ("accounts", {"view": "summary"}, "summary", "low"),
        (
            "accounts",
            {"view": "resolve", "query": "checking"},
            "resolve",
            "medium",
        ),
        ("accounts_balances", {}, "latest", "high"),
        (
            "accounts_balances",
            {
                "view": "history",
                "reference": "CHECKING",
                "start": "2025-01-01",
                "end": "2025-12-31",
            },
            "history",
            "high",
        ),
        (
            "accounts_balances",
            {"view": "assertions"},
            "assertions",
            "high",
        ),
        (
            "accounts_balances",
            {"view": "reconcile", "threshold": 0},
            "reconcile",
            "high",
        ),
    ],
)
async def test_accounts_coarse_transport_variants(
    name: str,
    arguments: dict[str, Any],
    kind: str,
    sensitivity: str,
    mcp_db: object,
) -> None:
    mcp = isolated_server(register_accounts_coarse_reads)

    structured = await _assert_canonical_variant(mcp, name, arguments, kind)

    assert structured["summary"]["sensitivity"] == sensitivity


@pytest.mark.parametrize(
    ("name", "arguments", "sensitivity", "classes"),
    [
        (
            "accounts",
            {},
            "critical",
            {
                "balance",
                "currency",
                "institution",
                "institution_account_number",
                "record_id",
                "txn_type",
                "user_note",
            },
        ),
        (
            "accounts",
            {"view": "detail", "reference": "CHECKING"},
            "critical",
            {
                "balance",
                "currency",
                "institution",
                "institution_account_number",
                "record_id",
                "routing_number",
                "txn_type",
                "user_note",
            },
        ),
        ("accounts", {"view": "summary"}, "low", {"aggregate"}),
        (
            "accounts",
            {"view": "resolve", "query": "checking"},
            "medium",
            {"aggregate", "institution", "record_id", "txn_type", "user_note"},
        ),
        (
            "accounts_balances",
            {},
            "high",
            {"balance", "record_id", "txn_date", "txn_type"},
        ),
        (
            "accounts_balances",
            {"view": "history", "reference": "CHECKING"},
            "high",
            {"balance", "record_id", "txn_date", "txn_type"},
        ),
        (
            "accounts_balances",
            {"view": "assertions"},
            "high",
            {
                "balance",
                "record_id",
                "timestamp_observability",
                "txn_date",
                "user_note",
            },
        ),
        (
            "accounts_balances",
            {"view": "reconcile", "threshold": 0},
            "high",
            {"balance", "record_id", "txn_date", "txn_type"},
        ),
    ],
)
async def test_accounts_coarse_call_emits_one_public_privacy_event(
    name: str,
    arguments: dict[str, Any],
    sensitivity: str,
    classes: set[str],
    mcp_db: object,
) -> None:
    captured: list[dict[str, Any]] = []
    mcp = isolated_server(register_accounts_coarse_reads)

    with patch(
        "moneybin.mcp.decorator.write_privacy_event",
        captured.append,
    ):
        await call_tool_raw(mcp, name, arguments)

    assert len(captured) == 1
    assert captured[0]["actor"] == f"mcp.{name}"
    assert captured[0]["sensitivity"] == sensitivity
    assert set(captured[0]["classes_returned"]) == classes


async def test_accounts_coarse_masks_sensitive_account_fields(
    mcp_db: object,
) -> None:
    from moneybin.database import get_database

    with get_database(read_only=False) as db:
        db.execute(
            """
            UPDATE core.dim_accounts
            SET last_four = '1234'
            WHERE account_id = 'ACC001'
            """
        )
    mcp = isolated_server(register_accounts_coarse_reads)

    listed = await _assert_canonical_variant(mcp, "accounts", {}, "list")
    detail = await _assert_canonical_variant(
        mcp,
        "accounts",
        {"view": "detail", "reference": "ACC001"},
        "detail",
    )

    account_row = next(
        row for row in listed["data"]["rows"] if row["account_id"] == "ACC001"
    )
    assert account_row["last_four"] == "****1234"
    assert detail["data"]["account"]["last_four"] == "****1234"
    assert detail["data"]["account"]["routing_number"] == "*****"


async def test_accounts_coarse_raw_errors_are_canonical_and_sanitized(
    mcp_db: object,
) -> None:
    from moneybin.database import get_database

    with get_database(read_only=False) as db:
        db.execute(
            """
            UPDATE core.dim_accounts
            SET display_name = 'Savings'
            WHERE account_id IN ('ACC001', 'ACC002')
            """
        )
    mcp = isolated_server(register_accounts_coarse_reads)

    ambiguous = await _assert_canonical_error(
        mcp,
        "accounts",
        {"view": "detail", "reference": "Savings"},
        "ENTITY_REFERENCE_AMBIGUOUS",
    )
    missing_reference = "secret-account-123456789"
    missing = await _assert_canonical_error(
        mcp,
        "accounts",
        {"view": "detail", "reference": missing_reference},
        "ENTITY_REFERENCE_NOT_FOUND",
    )

    assert ambiguous["error"]["details"]["candidate_ids"] == ["ACC001", "ACC002"]
    assert "Savings" not in ambiguous["error"]["message"]
    assert missing["error"]["details"]["candidate_ids"] == []
    assert missing_reference not in missing["error"]["message"]


async def test_accounts_coarse_raw_cursors_reject_cross_filter_reuse(
    mcp_db: object,
) -> None:
    from moneybin.database import get_database

    with get_database(read_only=False) as db:
        db.execute(
            """
            INSERT INTO core.fct_balances_daily (
                account_id, balance_date, balance, is_observed,
                observation_source, reconciliation_delta
            ) VALUES
                ('ACC001', '2025-06-29', 100.00, TRUE, 'ofx', NULL),
                ('ACC001', '2025-06-30', 125.00, TRUE, 'ofx', NULL)
            """
        )
    mcp = isolated_server(register_accounts_coarse_reads)

    account_page = await _assert_canonical_variant(
        mcp,
        "accounts",
        {"view": "list", "include_closed": True, "limit": 1},
        "list",
    )
    await _assert_canonical_error(
        mcp,
        "accounts",
        {
            "view": "list",
            "include_closed": False,
            "limit": 1,
            "cursor": account_page["next_cursor"],
        },
        "ACCOUNT_CURSOR_INVALID",
    )

    resolve_page = await _assert_canonical_variant(
        mcp,
        "accounts",
        {"view": "resolve", "query": "bank", "limit": 1},
        "resolve",
    )
    assert "next_cursor" not in resolve_page
    assert resolve_page["summary"]["has_more"] is True
    await _assert_canonical_error(
        mcp,
        "accounts",
        {
            "view": "resolve",
            "query": "checking",
            "limit": 1,
            "cursor": "opaque",
        },
        "ACCOUNT_CURSOR_NOT_ALLOWED",
    )

    balance_page = await _assert_canonical_variant(
        mcp,
        "accounts_balances",
        {
            "view": "history",
            "reference": "ACC001",
            "start": "2025-06-29",
            "end": "2025-06-30",
            "limit": 1,
        },
        "history",
    )
    await _assert_canonical_error(
        mcp,
        "accounts_balances",
        {
            "view": "history",
            "reference": "ACC001",
            "start": "2025-06-30",
            "end": "2025-06-30",
            "limit": 1,
            "cursor": balance_page["next_cursor"],
        },
        "BALANCE_CURSOR_INVALID",
    )


@pytest.mark.parametrize(
    ("arguments", "code"),
    [
        (
            {"view": "summary", "include_closed": True},
            "ACCOUNT_INCLUDE_CLOSED_NOT_ALLOWED",
        ),
        (
            {"view": "resolve", "query": "checking", "include_closed": True},
            "ACCOUNT_INCLUDE_CLOSED_NOT_ALLOWED",
        ),
        (
            {"view": "detail", "reference": "ACC001", "limit": 1},
            "ACCOUNT_LIMIT_NOT_ALLOWED",
        ),
        ({"view": "summary", "limit": 1}, "ACCOUNT_LIMIT_NOT_ALLOWED"),
    ],
)
async def test_accounts_coarse_raw_rejects_unused_arguments(
    arguments: dict[str, Any],
    code: str,
    mcp_db: object,
) -> None:
    mcp = isolated_server(register_accounts_coarse_reads)

    await _assert_canonical_error(mcp, "accounts", arguments, code)


@pytest.mark.parametrize(
    ("name", "arguments"),
    [
        ("accounts", {"view": "all"}),
        ("accounts", {"include_closed": "false"}),
        ("accounts", {"limit": "50"}),
        ("accounts", {"unknown": "value"}),
        ("accounts_balances", {"limit": "50"}),
        ("accounts_balances", {"start": 20250101}),
        ("accounts_balances", {"view": "reconcile", "threshold": "0"}),
        ("accounts_balances", {"unknown": "value"}),
    ],
)
async def test_accounts_coarse_tools_reject_invalid_raw_arguments(
    name: str,
    arguments: dict[str, Any],
) -> None:
    mcp = isolated_server(register_accounts_coarse_reads)

    response = await call_tool_raw(mcp, name, arguments)

    assert response.isError is True


async def test_investment_and_transaction_coarse_tools_render_schema_contract() -> None:
    investments_mcp = isolated_server(register_investment_coarse_reads)
    transactions_mcp = isolated_server(register_transaction_coarse_reads)

    investments = await listed_tool(investments_mcp, "investments")
    transactions = await listed_tool(transactions_mcp, "transactions")

    assert investments.outputSchema is None
    assert transactions.outputSchema is None
    assert investments.annotations is not None
    assert investments.annotations.readOnlyHint is True
    assert transactions.annotations is not None
    assert transactions.annotations.readOnlyHint is True
    assert_literal_values(
        investments.inputSchema,
        ("properties", "view"),
        {"events", "holdings", "lots", "gains", "securities"},
    )
    open_only_schema = investments.inputSchema["properties"]["open_only"]
    assert open_only_schema["anyOf"][0]["type"] == "boolean"
    for field in ("start", "end"):
        investment_date = investments.inputSchema["properties"][field]["anyOf"][0]
        transaction_date = transactions.inputSchema["properties"][field]["anyOf"][0]
        assert investment_date["type"] == "string"
        assert investment_date["format"] == "date"
        assert transaction_date["type"] == "string"
        assert transaction_date["format"] == "date"
    for field in ("min_amount", "max_amount"):
        amount_schema = json.dumps(transactions.inputSchema["properties"][field])
        assert '"number"' in amount_schema
        assert '"string"' not in amount_schema


async def test_transaction_coarse_transport_is_canonical_and_numeric(
    mcp_db: object,
) -> None:
    with get_database(read_only=False) as db:
        db.execute(
            """
            INSERT INTO core.fct_transactions (
                transaction_id, account_id, transaction_date, amount,
                amount_absolute, transaction_direction, description,
                transaction_type, is_pending, currency_code, source_type,
                source_extracted_at, loaded_at, transaction_year,
                transaction_month, transaction_day, transaction_day_of_week,
                transaction_year_month, transaction_year_quarter,
                notes, tags, splits
            ) VALUES (
                'txn_schema', 'ACC001', '2025-06-01', -25.50,
                25.50, 'expense', 'Schema probe', 'DEBIT', false, 'USD',
                'ofx', '2025-06-01', CURRENT_TIMESTAMP, 2025, 6, 1, 0,
                '2025-06', '2025-Q2', NULL, NULL, NULL
            )
            """
        )
    mcp = isolated_server(register_transaction_coarse_reads)

    response = await call_tool_raw(
        mcp,
        "transactions",
        {"min_amount": -100.0},
    )
    text = next(
        block.text for block in response.content if isinstance(block, TextContent)
    )

    assert response.isError is False
    assert response.structuredContent is not None
    assert json.loads(text) == response.structuredContent
    amount = response.structuredContent["data"]["transactions"][0]["amount"]
    assert isinstance(amount, int | float)
    assert amount == -25.5


@pytest.mark.parametrize(
    ("view", "sensitivity"),
    [
        ("events", "high"),
        ("holdings", "high"),
        ("lots", "high"),
        ("gains", "high"),
        ("securities", "low"),
    ],
)
async def test_investment_coarse_transport_variants(
    view: str,
    sensitivity: str,
    mcp_db: object,
) -> None:
    from tests.moneybin.db_helpers import create_core_dim_stub_views

    with get_database(read_only=False) as db:
        create_core_dim_stub_views(db)
    mcp = isolated_server(register_investment_coarse_reads)

    structured = await _assert_canonical_variant(
        mcp,
        "investments",
        {"view": view},
        view,
    )

    assert structured["summary"]["sensitivity"] == sensitivity


@pytest.mark.parametrize(
    ("name", "registrar", "arguments", "sensitivity", "classes"),
    [
        (
            "investments",
            register_investment_coarse_reads,
            {"view": "events"},
            "high",
            {
                "aggregate",
                "currency",
                "description",
                "record_id",
                "txn_amount",
                "txn_date",
                "txn_type",
            },
        ),
        (
            "investments",
            register_investment_coarse_reads,
            {"view": "holdings"},
            "high",
            {"aggregate", "balance", "currency", "record_id", "txn_amount"},
        ),
        (
            "investments",
            register_investment_coarse_reads,
            {"view": "lots"},
            "high",
            {
                "aggregate",
                "balance",
                "currency",
                "record_id",
                "txn_amount",
                "txn_date",
                "txn_type",
            },
        ),
        (
            "investments",
            register_investment_coarse_reads,
            {"view": "gains"},
            "high",
            {
                "aggregate",
                "balance",
                "currency",
                "record_id",
                "txn_amount",
                "txn_date",
                "txn_type",
            },
        ),
        (
            "investments",
            register_investment_coarse_reads,
            {"view": "securities"},
            "low",
            {"aggregate", "currency", "record_id", "txn_type"},
        ),
        (
            "transactions",
            register_transaction_coarse_reads,
            {},
            "high",
            {
                "aggregate",
                "category",
                "description",
                "record_id",
                "txn_amount",
                "txn_date",
                "txn_type",
                "user_note",
            },
        ),
    ],
)
async def test_investment_and_transaction_coarse_emit_one_public_privacy_event(
    name: str,
    registrar: Any,
    arguments: dict[str, Any],
    sensitivity: str,
    classes: set[str],
    mcp_db: object,
) -> None:
    captured: list[dict[str, Any]] = []
    if name == "investments":
        from tests.moneybin.db_helpers import create_core_dim_stub_views

        with get_database(read_only=False) as db:
            create_core_dim_stub_views(db)
    mcp = isolated_server(registrar)

    with patch(
        "moneybin.mcp.decorator.write_privacy_event",
        captured.append,
    ):
        await call_tool_raw(mcp, name, arguments)

    assert len(captured) == 1
    assert captured[0]["actor"] == f"mcp.{name}"
    assert captured[0]["sensitivity"] == sensitivity
    assert set(captured[0]["classes_returned"]) == classes


async def test_investment_and_transaction_raw_errors_are_sanitized(
    mcp_db: object,
) -> None:
    from tests.moneybin.db_helpers import create_core_dim_stub_views

    with get_database(read_only=False) as db:
        create_core_dim_stub_views(db)
        db.execute(
            """
            UPDATE core.dim_accounts
            SET display_name = 'Shared Account'
            WHERE account_id IN ('ACC001', 'ACC002')
            """
        )
    investments_mcp = isolated_server(register_investment_coarse_reads)
    transactions_mcp = isolated_server(register_transaction_coarse_reads)

    investments_error = await _assert_canonical_error(
        investments_mcp,
        "investments",
        {"view": "events", "account": "Shared Account"},
        "ENTITY_REFERENCE_AMBIGUOUS",
    )
    missing_merchant = "secret-merchant-123456789"
    transactions_error = await _assert_canonical_error(
        transactions_mcp,
        "transactions",
        {"merchant": missing_merchant},
        "ENTITY_REFERENCE_NOT_FOUND",
    )

    assert investments_error["error"]["details"]["candidate_ids"] == [
        "ACC001",
        "ACC002",
    ]
    assert "Shared Account" not in investments_error["error"]["message"]
    assert transactions_error["error"]["details"]["candidate_ids"] == []
    assert missing_merchant not in transactions_error["error"]["message"]


@pytest.mark.parametrize(
    ("name", "registrar", "arguments"),
    [
        ("investments", register_investment_coarse_reads, {"view": "portfolio"}),
        ("investments", register_investment_coarse_reads, {"limit": "50"}),
        ("investments", register_investment_coarse_reads, {"start": 20250101}),
        ("investments", register_investment_coarse_reads, {"unknown": "value"}),
        ("transactions", register_transaction_coarse_reads, {"limit": "50"}),
        ("transactions", register_transaction_coarse_reads, {"min_amount": "-50.00"}),
        ("transactions", register_transaction_coarse_reads, {"start": 20250101}),
        ("transactions", register_transaction_coarse_reads, {"unknown": "value"}),
    ],
)
async def test_investment_and_transaction_coarse_reject_invalid_raw_arguments(
    name: str,
    registrar: Any,
    arguments: dict[str, Any],
) -> None:
    mcp = isolated_server(registrar)

    response = await call_tool_raw(mcp, name, arguments)

    assert response.isError is True


async def test_import_gsheet_privacy_coarse_render_schema_contract() -> None:
    import_mcp = isolated_server(register_import_coarse_reads)
    gsheet_mcp = isolated_server(register_gsheet_coarse_reads)
    privacy_mcp = isolated_server(register_privacy_coarse_reads)

    import_tool = await listed_tool(import_mcp, "import_status")
    gsheet_tool = await listed_tool(gsheet_mcp, "gsheet")
    privacy_tool = await listed_tool(privacy_mcp, "privacy")

    assert import_tool.outputSchema is None
    assert gsheet_tool.outputSchema is None
    assert privacy_tool.outputSchema is None
    assert_literal_values(
        import_tool.inputSchema["properties"]["sections"]["anyOf"][0],
        ("items",),
        {"imports", "formats", "inbox"},
    )
    assert_literal_values(
        gsheet_tool.inputSchema,
        ("properties", "view"),
        {"connections", "status"},
    )
    assert_literal_values(
        privacy_tool.inputSchema,
        ("properties", "view"),
        {"status", "log"},
    )
    for tool in (import_tool, gsheet_tool, privacy_tool):
        assert tool.annotations is not None
        assert tool.annotations.readOnlyHint is True


async def test_import_coarse_transport_variants(mcp_db: object) -> None:
    mcp = isolated_server(register_import_coarse_reads)

    for sections, expected in (
        (["imports"], ["imports"]),
        (["formats"], ["formats"]),
        (["inbox"], ["inbox"]),
        (["inbox", "formats", "imports"], ["imports", "formats", "inbox"]),
        (["inbox", "imports"], ["imports", "inbox"]),
        (None, ["imports", "formats", "inbox"]),
    ):
        arguments = {} if sections is None else {"sections": sections}
        structured = await _assert_canonical_variant(
            mcp,
            "import_status",
            arguments,
            "sections",
        )
        assert [section["kind"] for section in structured["data"]["sections"]] == (
            expected
        )


async def test_gsheet_coarse_transport_variants() -> None:
    connection = MagicMock(
        connection_id="conn_a",
        spreadsheet_id="sheet_a",
        sheet_gid=0,
        sheet_name="Transactions",
        workbook_name="Budget",
        adapter="transactions",
        alias=None,
        account_id=None,
        account_name=None,
        status="healthy",
        last_pull_at=None,
        last_success_at=None,
        last_status_reason=None,
        consecutive_failure_count=0,
    )
    service = MagicMock()
    service.list_connections.return_value = [connection]
    service.get.return_value = connection
    mcp = isolated_server(register_gsheet_coarse_reads)

    with patch("moneybin.mcp.tools.gsheet._build_connection_service") as build_service:
        build_service.return_value.__enter__.return_value = service
        await _assert_canonical_variant(mcp, "gsheet", {}, "connections")
        await _assert_canonical_variant(
            mcp,
            "gsheet",
            {"view": "status"},
            "status",
        )
        await _assert_canonical_variant(
            mcp,
            "gsheet",
            {"view": "status", "connection_id": "conn_a"},
            "status",
        )


async def test_privacy_coarse_transport_variants(mcp_db: object) -> None:
    mcp = isolated_server(register_privacy_coarse_reads)

    await _assert_canonical_variant(mcp, "privacy", {}, "status")
    await _assert_canonical_variant(
        mcp,
        "privacy",
        {"view": "log", "limit": 1},
        "log",
    )


@pytest.mark.parametrize(
    ("name", "registrar", "arguments"),
    [
        ("import_status", register_import_coarse_reads, {"sections": "imports"}),
        ("import_status", register_import_coarse_reads, {"limit": "50"}),
        ("import_status", register_import_coarse_reads, {"unknown": "value"}),
        ("gsheet", register_gsheet_coarse_reads, {"view": "list"}),
        ("gsheet", register_gsheet_coarse_reads, {"connection_id": 123}),
        ("privacy", register_privacy_coarse_reads, {"view": "events"}),
        ("privacy", register_privacy_coarse_reads, {"limit": "50"}),
        ("privacy", register_privacy_coarse_reads, {"unknown": "value"}),
    ],
)
async def test_import_gsheet_privacy_coarse_reject_invalid_raw_arguments(
    name: str,
    registrar: Any,
    arguments: dict[str, Any],
) -> None:
    mcp = isolated_server(registrar)

    response = await call_tool_raw(mcp, name, arguments)

    assert response.isError is True


@pytest.mark.parametrize(
    ("name", "registrar", "arguments", "sensitivity", "classes"),
    [
        (
            "import_status",
            register_import_coarse_reads,
            {"sections": ["imports"]},
            "low",
            {"aggregate", "txn_type"},
        ),
        (
            "import_status",
            register_import_coarse_reads,
            {"sections": ["formats"]},
            "medium",
            {
                "aggregate",
                "description",
                "institution",
                "record_id",
                "timestamp_observability",
                "txn_type",
            },
        ),
        (
            "gsheet",
            register_gsheet_coarse_reads,
            {},
            "medium",
            {
                "aggregate",
                "description",
                "institution",
                "record_id",
                "timestamp_observability",
                "txn_type",
            },
        ),
        (
            "privacy",
            register_privacy_coarse_reads,
            {},
            "low",
            {
                "category",
                "institution",
                "timestamp_observability",
                "txn_type",
            },
        ),
        (
            "privacy",
            register_privacy_coarse_reads,
            {"view": "log", "limit": 1},
            "low",
            {
                "aggregate",
                "category",
                "institution",
                "timestamp_observability",
                "txn_type",
            },
        ),
    ],
)
async def test_import_gsheet_privacy_coarse_emit_one_public_privacy_event(
    name: str,
    registrar: Any,
    arguments: dict[str, Any],
    sensitivity: str,
    classes: set[str],
    mcp_db: object,
) -> None:
    captured: list[dict[str, Any]] = []
    mcp = isolated_server(registrar)
    service = MagicMock()
    service.list_connections.return_value = []

    with (
        patch("moneybin.mcp.decorator.write_privacy_event", captured.append),
        patch("moneybin.mcp.tools.gsheet._build_connection_service") as build_service,
    ):
        build_service.return_value.__enter__.return_value = service
        await call_tool_raw(mcp, name, arguments)

    assert len(captured) == 1
    assert captured[0]["actor"] == f"mcp.{name}"
    assert captured[0]["sensitivity"] == sensitivity
    assert set(captured[0]["classes_returned"]) == classes


async def test_import_gsheet_privacy_incompatible_errors_are_canonical(
    mcp_db: object,
) -> None:
    import_mcp = isolated_server(register_import_coarse_reads)
    gsheet_mcp = isolated_server(register_gsheet_coarse_reads)
    privacy_mcp = isolated_server(register_privacy_coarse_reads)

    import_error = await _assert_canonical_error(
        import_mcp,
        "import_status",
        {"sections": ["imports", "formats"], "import_id": "secret-import-id"},
        "IMPORT_ID_NOT_ALLOWED",
    )
    gsheet_error = await _assert_canonical_error(
        gsheet_mcp,
        "gsheet",
        {"view": "connections", "connection_id": "secret-connection-id"},
        "GSHEET_CONNECTION_ID_NOT_ALLOWED",
    )
    privacy_error = await _assert_canonical_error(
        privacy_mcp,
        "privacy",
        {"view": "status", "limit": 99},
        "PRIVACY_PAGINATION_NOT_ALLOWED",
    )

    assert "secret-import-id" not in import_error["error"]["message"]
    assert "secret-connection-id" not in gsheet_error["error"]["message"]
    assert privacy_error["summary"]["sensitivity"] == "low"
