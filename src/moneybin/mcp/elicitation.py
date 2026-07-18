"""Shared MCP elicitation capability probe."""

from __future__ import annotations

from typing import TYPE_CHECKING

from mcp.types import ClientCapabilities, ElicitationCapability

if TYPE_CHECKING:
    from fastmcp.server.context import Context


def supports_elicitation(ctx: Context) -> bool:
    """True when the connected client declared the elicitation capability."""
    return ctx.session.check_client_capability(
        ClientCapabilities(elicitation=ElicitationCapability())
    )
