"""ValidationErrorMiddleware translates pydantic ValidationError to envelopes.

Also covers MCP_TOOL_CALLS_TOTAL / MCP_TOOL_DURATION_SECONDS instrumentation,
which lives in this middleware as the single touchpoint for every tool call
(see docs/specs/observability.md).
"""

from __future__ import annotations

import json

import mcp.types as mt
import pytest
from fastmcp import Client, FastMCP
from fastmcp.server.middleware import MiddlewareContext
from fastmcp.tools import ToolResult
from prometheus_client import REGISTRY
from pydantic import ValidationError

from moneybin.mcp.middleware import ValidationErrorMiddleware


def _tool_call_count(tool_name: str) -> float:
    """Public read of the call counter — no private attribute access.

    Matches tests/moneybin/test_services/test_price_service.py::_rows_written;
    the alternative ``.labels(...)._value.get()`` needs a pyright suppression
    for protected access.
    """
    return (
        REGISTRY.get_sample_value(
            "moneybin_mcp_tool_calls_total", {"tool_name": tool_name}
        )
        or 0.0
    )


def _tool_duration_observation_count(tool_name: str) -> float:
    """Total histogram observations for ``tool_name`` (the ``_count`` sample)."""
    return (
        REGISTRY.get_sample_value(
            "moneybin_mcp_tool_duration_seconds_count", {"tool_name": tool_name}
        )
        or 0.0
    )


def _make_test_server() -> FastMCP:
    server = FastMCP("middleware-test")

    # output_schema=None: middleware returns an envelope shape, not whatever
    # the underlying tool would have. Disabling output validation on the test
    # tool avoids a schema mismatch between the envelope and the auto-derived
    # `{result: str}` schema fastmcp would otherwise expect. Real MoneyBin
    # tools all return ResponseEnvelope so their schemas already match.
    @server.tool(output_schema=None)
    def echo(  # pyright: ignore[reportUnusedFunction]
        x: str, y: int = 0
    ) -> str:
        return f"{x}-{y}"

    server.add_middleware(ValidationErrorMiddleware(server=server))
    return server


async def test_unknown_kwarg_becomes_invalid_arguments_envelope() -> None:
    """An unexpected kwarg yields an envelope listing accepted params."""
    server = _make_test_server()
    async with Client(server) as client:
        result = await client.call_tool("echo", {"wrong_arg": "value"})
        envelope = json.loads(result.content[0].text)  # type: ignore[attr-defined]
        assert envelope["status"] == "error"
        assert envelope["error"]["code"] == "infra_invalid_arguments"
        hint = envelope["error"]["hint"]
        assert "x" in hint and "y" in hint
        details = envelope["error"]["details"]
        assert details["unexpected"] == ["wrong_arg"]
        assert set(details["accepted"]) == {"x", "y"}


async def test_missing_required_kwarg_becomes_envelope() -> None:
    """A missing required arg is surfaced as 'Provide required:'."""
    server = _make_test_server()
    async with Client(server) as client:
        result = await client.call_tool("echo", {})
        envelope = json.loads(result.content[0].text)  # type: ignore[attr-defined]
        assert envelope["error"]["code"] == "infra_invalid_arguments"
        assert envelope["error"]["details"]["missing"] == ["x"]


async def test_unrelated_errors_pass_through() -> None:
    """Non-ValidationError exceptions are not intercepted by this middleware."""
    server = FastMCP("middleware-test")

    @server.tool(output_schema=None)
    def boom() -> str:  # pyright: ignore[reportUnusedFunction]
        raise RuntimeError("kaboom")

    server.add_middleware(ValidationErrorMiddleware(server=server))
    # mask_error_details defaults False here, so the inner error leaks through.
    async with Client(server) as client:
        with pytest.raises(Exception, match="kaboom"):
            await client.call_tool("boom", {})


async def test_middleware_unit_returns_tool_result_with_accepted_list() -> None:
    """Direct invocation: ValidationError → ToolResult containing the envelope."""
    server = _make_test_server()
    mw = ValidationErrorMiddleware(server=server)
    msg = mt.CallToolRequestParams(name="echo", arguments={"wrong_arg": "v"})
    ctx = MiddlewareContext(
        message=msg,
        fastmcp_context=None,
        source="client",
        type="request",
        method="tools/call",
    )

    async def call_next(
        context: MiddlewareContext[mt.CallToolRequestParams],  # noqa: ARG001
    ) -> ToolResult:
        # Trigger the same ValidationError fastmcp would raise on bad kwargs.
        raise ValidationError.from_exception_data(
            "call[echo]",
            [
                {
                    "type": "unexpected_keyword_argument",
                    "loc": ("wrong_arg",),
                    "input": "v",
                },
            ],
        )

    result = await mw.on_call_tool(ctx, call_next)
    body = json.loads(result.content[0].text)  # type: ignore[attr-defined]
    assert body["error"]["code"] == "infra_invalid_arguments"
    assert "wrong_arg" in body["error"]["details"]["unexpected"]
    assert set(body["error"]["details"]["accepted"]) == {"x", "y"}


async def test_successful_call_records_tool_metrics() -> None:
    """A clean call increments the call counter and observes a duration."""
    server = _make_test_server()
    before_calls = _tool_call_count("echo")
    before_duration = _tool_duration_observation_count("echo")
    async with Client(server) as client:
        await client.call_tool("echo", {"x": "hi"})
    assert _tool_call_count("echo") == before_calls + 1
    assert _tool_duration_observation_count("echo") == before_duration + 1


async def test_validation_error_still_records_tool_metrics() -> None:
    """A call the middleware itself intercepts is still counted."""
    server = _make_test_server()
    before_calls = _tool_call_count("echo")
    before_duration = _tool_duration_observation_count("echo")
    async with Client(server) as client:
        await client.call_tool("echo", {"wrong_arg": "value"})
    assert _tool_call_count("echo") == before_calls + 1
    assert _tool_duration_observation_count("echo") == before_duration + 1


async def test_unrelated_error_still_records_tool_metrics() -> None:
    """A tool exception this middleware doesn't intercept is still counted.

    The metric touchpoint must not depend on the middleware's own error
    handling — instrumentation and the ValidationError translation are
    independent concerns wrapping the same call.
    """
    server = FastMCP("middleware-test")

    @server.tool(output_schema=None)
    def boom() -> str:  # pyright: ignore[reportUnusedFunction]
        raise RuntimeError("kaboom")

    server.add_middleware(ValidationErrorMiddleware(server=server))
    before_calls = _tool_call_count("boom")
    before_duration = _tool_duration_observation_count("boom")
    async with Client(server) as client:
        with pytest.raises(Exception, match="kaboom"):
            await client.call_tool("boom", {})
    assert _tool_call_count("boom") == before_calls + 1
    assert _tool_duration_observation_count("boom") == before_duration + 1


async def test_unregistered_tool_name_is_not_used_as_a_metric_label() -> None:
    """A request naming no registered tool must not mint a new label series.

    The request name is client-supplied and unbounded. Recording it verbatim
    would let a buggy or hostile client mint arbitrarily many
    moneybin_mcp_tool_calls_total / moneybin_mcp_tool_duration_seconds label
    series just by spraying distinct nonexistent tool names.
    """
    server = _make_test_server()
    garbage_name = "does_not_exist_" + "x" * 40
    before_unknown_calls = _tool_call_count("unknown")
    before_unknown_duration = _tool_duration_observation_count("unknown")
    async with Client(server) as client:
        with pytest.raises(Exception, match="Unknown tool"):
            await client.call_tool(garbage_name, {})
    assert _tool_call_count(garbage_name) == 0
    assert _tool_duration_observation_count(garbage_name) == 0
    assert _tool_call_count("unknown") == before_unknown_calls + 1
    assert _tool_duration_observation_count("unknown") == before_unknown_duration + 1
