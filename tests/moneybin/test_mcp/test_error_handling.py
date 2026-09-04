"""Verify mcp_tool decorator converts domain exceptions to error envelopes."""

from __future__ import annotations

import logging
from typing import Any

import pytest

from moneybin import error_codes
from moneybin.database import DatabaseKeyError
from moneybin.errors import UserError
from moneybin.mcp.decorator import mcp_tool
from moneybin.privacy.sensitivity import Sensitivity
from moneybin.protocol.envelope import ResponseEnvelope


async def test_mcp_tool_converts_user_error_to_envelope() -> None:
    """A UserError raised inside a tool becomes an error envelope."""

    @mcp_tool(dynamic_classification=True, maximum_sensitivity=Sensitivity.HIGH)
    def failing_tool() -> ResponseEnvelope[Any]:
        raise UserError("not found", code="NOT_FOUND")

    result = await failing_tool()
    assert isinstance(result, ResponseEnvelope)
    assert result.error is not None
    assert result.error.code == "NOT_FOUND"
    assert result.data == []  # pyright: ignore[reportUnknownMemberType]


async def test_mcp_tool_converts_database_key_error_to_envelope() -> None:
    """DatabaseKeyError is a recognised classified exception."""

    @mcp_tool(dynamic_classification=True, maximum_sensitivity=Sensitivity.HIGH)
    def failing_tool() -> ResponseEnvelope[Any]:
        raise DatabaseKeyError("missing key")

    result = await failing_tool()
    assert isinstance(result, ResponseEnvelope)
    assert result.error is not None
    assert result.error.code == error_codes.INFRA_WRONG_KEY


async def test_mcp_tool_returns_structured_envelope_for_unclassified_exception() -> (
    None
):
    """An unclassifiable exception still reaches the wire with a branchable code.

    Previously the decorator re-raised, so fastmcp's mask_error_details
    reduced the failure to ``str(exc)`` — no code, no hint, nothing for an
    agent to branch on. Drives the tool return path directly (not middleware
    calling ``to_dict()``), which is where the bare string actually escaped.
    """

    @mcp_tool(dynamic_classification=True, maximum_sensitivity=Sensitivity.HIGH)
    def failing_tool() -> ResponseEnvelope[Any]:
        raise RuntimeError("internal detail leak")

    result = await failing_tool()
    assert isinstance(result, ResponseEnvelope)
    assert result.error is not None
    assert result.error.code == error_codes.INFRA_UNCLASSIFIED_ERROR


async def test_unclassified_envelope_does_not_leak_exception_message() -> None:
    """The envelope names the exception type, never its message.

    Exception messages embed file paths, SQL fragments, and user financial
    data (the reason transform_service logs ``type(e).__name__``). The
    structured envelope must not become a new leak channel.
    """

    @mcp_tool(dynamic_classification=True, maximum_sensitivity=Sensitivity.HIGH)
    def failing_tool() -> ResponseEnvelope[Any]:
        raise RuntimeError("card 4111111111111111 balance -2412.55")

    result = await failing_tool()
    assert result.error is not None
    assert "4111111111111111" not in result.error.message
    assert "2412.55" not in result.error.message
    assert "RuntimeError" in result.error.message


async def test_unclassified_failure_does_not_leak_the_message_to_the_log(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """The local log is not exempt from "no financial data in logs".

    `logger.exception` writes the whole traceback, whose last line is
    `<Type>: <str(exc)>`. SanitizedLogFormatter is not a backstop: its money
    pattern requires a literal `$`, so the bare `-2412.55` below survives it
    unmasked. Breaks if this path goes back to logging the traceback — the
    wire stays clean while the amount persists to the session log.
    """

    @mcp_tool(dynamic_classification=True, maximum_sensitivity=Sensitivity.HIGH)
    def failing_tool() -> ResponseEnvelope[Any]:
        raise RuntimeError("card 4111111111111111 balance -2412.55")

    with caplog.at_level(logging.ERROR):
        await failing_tool()

    logged = "\n".join(
        record.getMessage() + (record.exc_text or "") for record in caplog.records
    )
    assert "2412.55" not in logged
    assert "4111111111111111" not in logged
    # Still diagnosable: the type and the frame it came from survive.
    assert "RuntimeError" in logged
    assert "test_error_handling.py:" in logged


async def test_mcp_tool_returns_response_envelope_directly() -> None:
    """Decorator returns ResponseEnvelope directly, not a JSON string.

    fastmcp 3.x serializes the model to both content and structured_content.
    """
    from moneybin.protocol.envelope import build_envelope

    @mcp_tool(dynamic_classification=True, maximum_sensitivity=Sensitivity.HIGH)
    def ok_tool() -> ResponseEnvelope[Any]:
        return build_envelope(data=[{"x": 1}])

    result = await ok_tool()
    assert isinstance(result, ResponseEnvelope)  # NOT a str
    assert result.data == [{"x": 1}]  # pyright: ignore[reportUnknownMemberType]
