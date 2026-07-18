"""Self-tests for rendered MCP schema compatibility assertions."""

from __future__ import annotations

from typing import Any

import pytest
from fastmcp import FastMCP
from pydantic import StrictBool

from moneybin.mcp._registration import register
from moneybin.mcp.decorator import mcp_tool
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
