"""Shared helpers for registering MCP tools with the FastMCP server.

The ``mcp_tool`` decorator (see ``decorator.py``) sets ``_mcp_*`` attributes
on each wrapped function. ``register`` translates those attributes into the
arguments expected by ``mcp.tool()`` — ``tags`` for progressive disclosure
and ``annotations`` for protocol-standard tool hints (readOnlyHint,
destructiveHint, idempotentHint, openWorldHint).
"""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from mcp.types import ToolAnnotations

if TYPE_CHECKING:
    from fastmcp import FastMCP


def register(mcp: FastMCP, fn: Callable[..., Any], name: str, description: str) -> None:
    """Register an mcp_tool-decorated function with FastMCP.

    Reads ``_mcp_domain`` for tag-based visibility and the four annotation
    attrs (``_mcp_read_only``, ``_mcp_destructive``, ``_mcp_idempotent``,
    ``_mcp_open_world``) for the protocol-standard ``ToolAnnotations``.
    """
    domain = getattr(fn, "_mcp_domain", None)
    tags = {domain} if domain else None
    annotations = ToolAnnotations(
        readOnlyHint=getattr(fn, "_mcp_read_only", True),
        destructiveHint=getattr(fn, "_mcp_destructive", False),
        idempotentHint=getattr(fn, "_mcp_idempotent", True),
        openWorldHint=getattr(fn, "_mcp_open_world", False),
    )
    mcp.tool(name=name, description=description, tags=tags, annotations=annotations)(fn)
