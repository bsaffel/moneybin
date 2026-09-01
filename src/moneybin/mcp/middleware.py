"""FastMCP middleware: ValidationError translation and per-call tool metrics.

Without the ValidationError translation, an agent that calls a tool with a
wrong kwarg name gets a raw pydantic_core.ValidationError stringified back
("Unexpected keyword argument ... For further information visit
https://errors.pydantic.dev/..."), with no hint of what the accepted
parameter names are. That forces guess-and-retry.

This middleware intercepts the ValidationError at the call-tool boundary,
looks up the tool's accepted parameter names from its JSON schema, and
returns the standard MoneyBin response envelope with ``error.hint`` set to
the accepted parameter list and ``error.details`` recording which arguments
were unexpected and which were missing. The tool-author experience is
unchanged — body code never sees the bad call.

The same ``on_call_tool`` boundary is also the single touchpoint for
MCP_TOOL_CALLS_TOTAL / MCP_TOOL_DURATION_SECONDS (docs/specs/observability.md):
every tool call is timed and counted here, regardless of whether it succeeds,
raises a ValidationError this middleware translates, or raises something else
that propagates. No per-tool opt-in.
"""

from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING, Any

from fastmcp.server.middleware import CallNext, Middleware, MiddlewareContext
from fastmcp.tools import ToolResult
from pydantic import ValidationError

from moneybin import error_codes
from moneybin.errors import UserError
from moneybin.metrics.registry import MCP_TOOL_CALLS_TOTAL, MCP_TOOL_DURATION_SECONDS
from moneybin.protocol.envelope import build_error_envelope

if TYPE_CHECKING:
    import mcp.types as mt
    from fastmcp import FastMCP

logger = logging.getLogger(__name__)


class ValidationErrorMiddleware(Middleware):
    """Wrap pydantic ValidationError raised during tool-arg binding."""

    def __init__(self, server: FastMCP | None = None) -> None:
        """Init.

        Args:
            server: The FastMCP instance to use for tool lookup. When None,
                the middleware falls back to ``context.fastmcp_context.fastmcp``
                (which is set on real requests but absent in unit tests).
                Passing the server explicitly makes hints reliable in tests.
        """
        super().__init__()
        self._server = server

    async def on_call_tool(
        self,
        context: MiddlewareContext[mt.CallToolRequestParams],
        call_next: CallNext[mt.CallToolRequestParams, ToolResult],
    ) -> ToolResult:
        """Translate ValidationError on arg binding into a response envelope.

        Records MCP_TOOL_CALLS_TOTAL / MCP_TOOL_DURATION_SECONDS for every
        call in a ``finally`` block, so a call that raises an exception this
        middleware does not translate is still counted before it propagates.
        """
        tool_name = context.message.name
        started = time.monotonic()
        try:
            return await call_next(context)
        except ValidationError as exc:
            accepted = await _accepted_params(context, tool_name, self._server)
            envelope = _build_validation_envelope(exc, tool_name, accepted)
            logger.info(
                f"Tool {tool_name} rejected with validation error; "
                f"returning friendly envelope ({len(exc.errors())} issue(s))"
            )
            # Provide both text content and structured_content so the result
            # passes the tool's outputSchema check (when one is declared) and
            # still renders as text on hosts that ignore structured output.
            return ToolResult(
                content=envelope.to_json(),
                structured_content=envelope.to_dict(),
            )
        finally:
            MCP_TOOL_CALLS_TOTAL.labels(tool_name=tool_name).inc()
            MCP_TOOL_DURATION_SECONDS.labels(tool_name=tool_name).observe(
                time.monotonic() - started
            )


def _build_validation_envelope(
    exc: ValidationError,
    tool_name: str,
    accepted: list[str],
) -> Any:
    """Map a ValidationError onto a MoneyBin error envelope."""
    unexpected: list[str] = []
    missing: list[str] = []
    other: list[dict[str, Any]] = []
    for err in exc.errors():
        loc = err.get("loc", ())
        field = loc[0] if loc else "?"
        err_type = err.get("type", "")
        if err_type == "unexpected_keyword_argument":
            unexpected.append(str(field))
        elif err_type == "missing_argument":
            missing.append(str(field))
        else:
            other.append({
                "field": str(field),
                "type": err_type,
                "msg": err.get("msg", ""),
            })

    hint_parts: list[str] = []
    if accepted:
        hint_parts.append(f"Accepted parameters: {', '.join(accepted)}")
    if unexpected:
        hint_parts.append(
            f"Remove unrecognized: {', '.join(repr(p) for p in unexpected)}"
        )
    if missing:
        hint_parts.append(f"Provide required: {', '.join(repr(p) for p in missing)}")

    details: dict[str, Any] = {"tool": tool_name}
    if unexpected:
        details["unexpected"] = unexpected
    if missing:
        details["missing"] = missing
    if accepted:
        details["accepted"] = accepted
    if other:
        details["other"] = other

    message = f"Invalid arguments for {tool_name}"
    return build_error_envelope(
        error=UserError(
            message,
            code=error_codes.INFRA_INVALID_ARGUMENTS,
            hint="; ".join(hint_parts) if hint_parts else None,
            details=details,
        ),
        sensitivity="low",
    )


async def _accepted_params(
    context: MiddlewareContext[mt.CallToolRequestParams],
    tool_name: str,
    server: FastMCP | None,
) -> list[str]:
    """Best-effort lookup of the tool's accepted parameter names.

    Prefers the server passed at middleware init; falls back to the
    per-request ``fastmcp_context.fastmcp`` reference.
    """
    try:
        resolved_server = server
        if resolved_server is None:
            fastmcp_ctx = context.fastmcp_context
            resolved_server = (
                getattr(fastmcp_ctx, "fastmcp", None) if fastmcp_ctx else None
            )
        if resolved_server is None:
            return []
        tool = await resolved_server.get_tool(tool_name)
        if tool is None:
            return []
        params: dict[str, Any] = tool.parameters or {}
        props: dict[str, Any] = params.get("properties") or {}
        return sorted(props.keys())
    except Exception:  # noqa: BLE001 — middleware must not raise on lookup
        logger.debug(
            f"Could not resolve accepted params for {tool_name}", exc_info=True
        )
        return []
