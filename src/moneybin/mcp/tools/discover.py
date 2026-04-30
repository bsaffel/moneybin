# src/moneybin/mcp/tools/discover.py
"""Discover meta-tool — on-demand namespace loading.

Tools:
    - moneybin.discover — Load tools from an extended namespace (low sensitivity)
"""

from __future__ import annotations

import logging

from moneybin.mcp.decorator import mcp_tool
from moneybin.mcp.namespaces import NamespaceRegistry, ToolDefinition
from moneybin.protocol.envelope import ResponseEnvelope, build_envelope

logger = logging.getLogger(__name__)


@mcp_tool(sensitivity="low")
def moneybin_discover(namespace: str) -> ResponseEnvelope:
    """Load tools from an extended namespace on demand.

    Extended namespaces (categorize, budget, tax, privacy) are not
    registered at connection time. Call this tool to load them.
    Use the moneybin://tools resource to see available namespaces.

    Args:
        namespace: The namespace to load (e.g. 'categorize', 'budget', 'tax').
    """
    from moneybin.mcp.server import get_registry, mcp

    registry = get_registry()

    tools = registry.get_namespace_tools(namespace)
    if not tools:
        return build_envelope(
            data={
                "namespace": namespace,
                "error": f"Unknown namespace: {namespace}",
            },
            sensitivity="low",
        )

    if not registry.is_loaded(namespace):
        for tool in tools:
            mcp.tool(name=tool.name, description=tool.description)(tool.fn)
        registry.mark_loaded(namespace)

    return build_envelope(
        data={
            "namespace": namespace,
            "tools_loaded": [
                {"name": t.name, "description": t.description} for t in tools
            ],
        },
        sensitivity="low",
    )


def register_discover_tool(
    registry: NamespaceRegistry,
) -> list[ToolDefinition]:
    """Register the moneybin.discover meta-tool with the registry."""
    tools = [
        ToolDefinition(
            name="moneybin.discover",
            description=(
                "Load tools from an extended namespace on demand. "
                "Use moneybin://tools to see available namespaces."
            ),
            fn=moneybin_discover,
        ),
    ]
    for tool in tools:
        registry.register(tool)
    return tools
