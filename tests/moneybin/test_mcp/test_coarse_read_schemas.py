"""Self-tests for rendered MCP schema compatibility assertions."""

from __future__ import annotations

import json
from typing import Any
from unittest.mock import patch

import pytest
from fastmcp import FastMCP
from mcp.types import TextContent
from pydantic import StrictBool

from moneybin.mcp._registration import register
from moneybin.mcp.decorator import mcp_tool
from moneybin.mcp.tools.system import register_system_coarse_reads
from moneybin.protocol.envelope import ResponseEnvelope, build_envelope

from .schema_assertions import (
    assert_discriminated_variants,
    assert_literal_values,
    call_tool_raw,
    isolated_server,
    listed_tool,
)


def register_strict_probe(mcp: FastMCP) -> None:
    """Register a dormant probe through MoneyBin's normal adapter."""

    @mcp_tool(dynamic_classification=True)
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
        {"overview", "doctor", "categorization"},
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


@pytest.mark.parametrize("section", ["overview", "doctor", "categorization"])
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
