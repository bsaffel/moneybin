"""Verify mcp_tool decorator converts domain exceptions to error envelopes."""

from __future__ import annotations

import asyncio

import pytest

from moneybin.database import DatabaseKeyError
from moneybin.errors import UserError
from moneybin.mcp.decorator import mcp_tool
from moneybin.protocol.envelope import ResponseEnvelope


def test_mcp_tool_converts_user_error_to_envelope() -> None:
    """A UserError raised inside a tool becomes an error envelope."""

    @mcp_tool(sensitivity="low")
    def failing_tool() -> ResponseEnvelope:
        raise UserError("not found", code="NOT_FOUND")

    result = asyncio.run(failing_tool())
    assert isinstance(result, ResponseEnvelope)
    assert result.error is not None
    assert result.error.code == "NOT_FOUND"
    assert result.data == []


def test_mcp_tool_converts_database_key_error_to_envelope() -> None:
    """DatabaseKeyError is a recognised classified exception."""

    @mcp_tool(sensitivity="low")
    def failing_tool() -> ResponseEnvelope:
        raise DatabaseKeyError("missing key")

    result = asyncio.run(failing_tool())
    assert isinstance(result, ResponseEnvelope)
    assert result.error is not None
    assert result.error.code == "database_locked"


def test_mcp_tool_lets_unclassified_exceptions_propagate() -> None:
    """Non-domain exceptions propagate; the decorator does NOT swallow them.

    fastmcp's mask_error_details wraps them into masked ToolErrors at the
    server boundary.
    """

    @mcp_tool(sensitivity="low")
    def failing_tool() -> ResponseEnvelope:
        raise RuntimeError("internal detail leak")

    with pytest.raises(RuntimeError):
        asyncio.run(failing_tool())


def test_mcp_tool_returns_response_envelope_directly() -> None:
    """Decorator returns ResponseEnvelope directly, not a JSON string.

    fastmcp 3.x serializes the model to both content and structured_content.
    """
    from moneybin.protocol.envelope import build_envelope

    @mcp_tool(sensitivity="low")
    def ok_tool() -> ResponseEnvelope:
        return build_envelope(data=[{"x": 1}], sensitivity="low")

    result = asyncio.run(ok_tool())
    assert isinstance(result, ResponseEnvelope)  # NOT a str
    assert result.data == [{"x": 1}]
