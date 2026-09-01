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
from pydantic import ValidationError

from moneybin.mcp.middleware import ValidationErrorMiddleware
from moneybin.metrics.registry import MCP_TOOL_CALLS_TOTAL, MCP_TOOL_DURATION_SECONDS


def _tool_call_count(tool_name: str) -> float:
    counter = MCP_TOOL_CALLS_TOTAL.labels(tool_name=tool_name)
    return counter._value.get()  # type: ignore[reportPrivateUsage] — prometheus internals


def _tool_duration_observation_count(tool_name: str) -> float:
    """Total histogram observations for ``tool_name`` (the ``_count`` sample)."""
    for metric in MCP_TOOL_DURATION_SECONDS.collect():
        for sample in metric.samples:
            if (
                sample.name.endswith("_count")
                and sample.labels.get("tool_name") == tool_name
            ):
                return sample.value
    return 0.0


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
