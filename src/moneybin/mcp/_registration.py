"""Shared helpers for registering MCP tools with the FastMCP server.

The ``mcp_tool`` decorator (see ``decorator.py``) sets a ``_mcp_domain``
attribute on each wrapped function. ``register`` translates that attribute
into the ``tags=`` argument expected by ``mcp.tool()`` and, transitively,
by the ``Visibility`` transforms installed at server boot.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from fastmcp import FastMCP


def register(mcp: FastMCP, fn: Callable[..., Any], name: str, description: str) -> None:
    """Register an mcp_tool-decorated function with FastMCP.

    Reads the ``_mcp_domain`` attribute set by ``@mcp_tool`` and forwards
    it as a tag so the server's Visibility transform can hide
    extended-namespace tools at boot.
    """
    domain = getattr(fn, "_mcp_domain", None)
    tags = {domain} if domain else None
    mcp.tool(name=name, description=description, tags=tags)(fn)
