"""Test-synthetic MCP tool registration.

Exposes one tool (test_synthetic_status) used in framework tests to confirm
register_package wires tools through correctly.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from fastmcp import FastMCP

_REGISTERED: list[str] = []


def register(mcp: FastMCP) -> None:  # noqa: ARG001  # signature matches the register() contract
    """Called by register_package(); records the call for tests."""
    _REGISTERED.append("tools.register")


def calls() -> list[str]:
    """Test helper to introspect registration calls."""
    return list(_REGISTERED)


def reset() -> None:
    _REGISTERED.clear()
