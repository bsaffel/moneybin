"""Shared helpers for registering MCP tools with the FastMCP server.

The ``mcp_tool`` decorator (see ``decorator.py``) sets a ``_mcp_domain``
attribute on each wrapped function. ``tags_for`` translates that attribute
into the ``tags=`` argument expected by ``mcp.tool()`` and, transitively,
by the ``Visibility`` transforms installed at server boot.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any


def tags_for(fn: Callable[..., Any]) -> set[str] | None:
    """Translate the mcp_tool decorator's _mcp_domain attribute into a tag set.

    Tools without a domain (core tools) return ``None`` so that
    ``mcp.tool(tags=None)`` adds no tag.
    """
    domain = getattr(fn, "_mcp_domain", None)
    return {domain} if domain else None
