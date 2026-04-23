# src/moneybin/mcp/namespaces.py
"""Namespace registry for MCP progressive disclosure.

Tools are organized into namespaces (``spending``, ``accounts``, etc.).
Core namespaces are registered at connection time (~19 tools). Extended
namespaces are loaded on demand via ``moneybin.discover``.

See ``mcp-architecture.md`` section 3 for design rationale.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

logger = logging.getLogger(__name__)

# Default core namespaces — registered at connection time.
# The user can override via ``mcp.core_namespaces`` in profile config.
CORE_NAMESPACES_DEFAULT: frozenset[str] = frozenset({
    "overview",
    "spending",
    "cashflow",
    "accounts",
    "transactions",
    "import",
    "sql",
})

# Extended namespaces — loaded on demand via moneybin.discover.
EXTENDED_NAMESPACES: frozenset[str] = frozenset({
    "categorize",
    "budget",
    "tax",
    "privacy",
    "transactions.matches",
})

# Descriptions for each namespace (used in moneybin://tools resource).
NAMESPACE_DESCRIPTIONS: dict[str, str] = {
    "overview": "Data status and financial health snapshot",
    "spending": "Expense analysis, trends, category breakdowns",
    "cashflow": "Income vs outflows, net cash position",
    "accounts": "Account listing, balances, net worth",
    "transactions": "Search, corrections, annotations, recurring",
    "import": "File import, status, format management",
    "sql": "Direct read-only SQL queries",
    "categorize": "Rules, merchant mappings, bulk categorization",
    "budget": "Budget targets, status, rollovers",
    "tax": "W-2 data, deductible expense search",
    "privacy": "Consent status, grants, revocations, audit log",
    "transactions.matches": "Match review workflow",
}


@dataclass(frozen=True, slots=True)
class ToolDefinition:
    """A registered MCP tool with its metadata.

    Attributes:
        name: Dot-separated tool name (e.g., ``spending.summary``).
        description: Tool description for AI consumers.
        fn: The tool function (decorated with ``@mcp_tool``).
    """

    name: str
    description: str
    fn: Callable[..., Any]

    def __post_init__(self) -> None:
        """Validate that the tool name contains a namespace separator."""
        if "." not in self.name:
            raise ValueError(
                f"Tool name '{self.name}' must contain a dot (namespace.action)"
            )

    @property
    def namespace(self) -> str:
        """Extract the namespace from the tool name.

        For two-level names like ``spending.summary``, returns ``spending``.
        For three-level names like ``transactions.matches.pending``,
        returns ``transactions.matches``.
        """
        parts = self.name.rsplit(".", 1)
        return parts[0]


class NamespaceRegistry:
    """Registry of all MCP tools organized by namespace.

    Tracks which namespaces are loaded (registered with FastMCP)
    vs available but unloaded.
    """

    def __init__(self) -> None:
        """Initialize registry with empty tool and loaded namespace sets."""
        self._tools: dict[str, list[ToolDefinition]] = {}
        self._loaded: set[str] = set()
        self._descriptions: dict[str, str] = dict(NAMESPACE_DESCRIPTIONS)

    def register(self, tool: ToolDefinition) -> None:
        """Register a tool definition (does not register with FastMCP)."""
        ns = tool.namespace
        if ns not in self._tools:
            self._tools[ns] = []
        self._tools[ns].append(tool)

    def all_namespaces(self) -> set[str]:
        """Return all registered namespace names."""
        return set(self._tools.keys())

    def get_namespace_tools(self, namespace: str) -> list[ToolDefinition]:
        """Get all tools in a namespace."""
        return self._tools.get(namespace, [])

    def get_core_tools(
        self, core_namespaces: set[str] | frozenset[str]
    ) -> list[ToolDefinition]:
        """Get all tools that belong to core namespaces."""
        tools: list[ToolDefinition] = []
        for ns in core_namespaces:
            tools.extend(self._tools.get(ns, []))
        return tools

    def get_extended_namespaces(
        self, core_namespaces: set[str] | frozenset[str]
    ) -> set[str]:
        """Get namespace names that are not in the core set."""
        return self.all_namespaces() - set(core_namespaces)

    def set_namespace_description(self, namespace: str, description: str) -> None:
        """Set the description for a namespace."""
        self._descriptions[namespace] = description

    def get_namespace_description(self, namespace: str) -> str:
        """Get the description for a namespace."""
        return self._descriptions.get(namespace, "")

    def is_loaded(self, namespace: str) -> bool:
        """Check if a namespace has been loaded (registered with FastMCP)."""
        return namespace in self._loaded

    def mark_loaded(self, namespace: str) -> None:
        """Mark a namespace as loaded."""
        self._loaded.add(namespace)

    def tools_resource_data(
        self, core_namespaces: set[str] | frozenset[str]
    ) -> dict[str, Any]:
        """Build the data payload for the ``moneybin://tools`` resource."""
        core_list: list[dict[str, Any]] = []
        extended_list: list[dict[str, Any]] = []

        for ns in sorted(self.all_namespaces()):
            entry = {
                "namespace": ns,
                "tools": len(self._tools.get(ns, [])),
                "loaded": self.is_loaded(ns),
                "description": self.get_namespace_description(ns),
            }
            if ns in core_namespaces:
                core_list.append(entry)
            else:
                extended_list.append(entry)

        return {
            "core": core_list,
            "extended": extended_list,
            "discover_tool": "moneybin.discover",
        }
