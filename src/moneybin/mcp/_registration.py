"""Shared helpers for registering MCP tools with the FastMCP server.

The ``mcp_tool`` decorator (see ``decorator.py``) sets ``_mcp_*`` attributes
on each wrapped function. ``register`` translates those attributes into the
arguments expected by ``mcp.tool()`` — ``tags`` for progressive disclosure
and ``annotations`` for protocol-standard tool hints (readOnlyHint,
destructiveHint, idempotentHint, openWorldHint).
"""

from __future__ import annotations

import functools
import inspect
import json
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, cast

from fastmcp.tools import ToolResult
from mcp.types import ToolAnnotations

from moneybin.mcp.decorator import privacy_actor_scope
from moneybin.protocol.envelope import ResponseEnvelope

if TYPE_CHECKING:
    from fastmcp import FastMCP


def _wire_result_adapter(
    fn: Callable[..., Any],
    *,
    privacy_actor: str | None = None,
) -> Callable[..., Any]:
    """Return a FastMCP adapter that emits the canonical envelope wire shape."""
    signature = inspect.signature(fn)

    @functools.wraps(fn)
    async def wire_result(*args: Any, **kwargs: Any) -> ToolResult:
        with privacy_actor_scope(privacy_actor):
            envelope = cast(ResponseEnvelope[Any], await fn(*args, **kwargs))
        content_json = envelope.to_json()
        return ToolResult(
            content=content_json,
            structured_content=json.loads(content_json),
        )

    wire_result.__annotations__ = {  # pyright: ignore[reportFunctionMemberAccess]
        **getattr(fn, "__annotations__", {}),
        "return": ToolResult,
    }
    wire_result.__signature__ = signature.replace(  # type: ignore[attr-defined]
        return_annotation=ToolResult
    )
    return wire_result


def register(
    mcp: FastMCP,
    fn: Callable[..., Any],
    name: str,
    description: str,
    *,
    privacy_actor: str | None = None,
) -> None:
    """Register an mcp_tool-decorated function with FastMCP.

    Reads ``_mcp_domain`` for tag-based visibility and the four annotation
    attrs (``_mcp_read_only``, ``_mcp_destructive``, ``_mcp_idempotent``,
    ``_mcp_open_world``) for the protocol-standard ``ToolAnnotations``.
    ``privacy_actor`` explicitly attributes a replacement callback to its
    public tool identity without changing existing registrations by default.
    """
    domain = getattr(fn, "_mcp_domain", None)
    tags = {domain} if domain else None
    annotations = ToolAnnotations(
        readOnlyHint=getattr(fn, "_mcp_read_only", True),
        destructiveHint=getattr(fn, "_mcp_destructive", False),
        idempotentHint=getattr(fn, "_mcp_idempotent", True),
        openWorldHint=getattr(fn, "_mcp_open_world", False),
    )
    mcp.tool(
        name=name,
        description=description,
        tags=tags,
        annotations=annotations,
    )(_wire_result_adapter(fn, privacy_actor=privacy_actor))
