"""Per-session visibility tests for MCP tools.

Replaces NamespaceRegistry-based progressive disclosure with fastmcp 3.x
tag-based visibility transforms.
"""

from __future__ import annotations

import pytest
from fastmcp import Client


@pytest.fixture(scope="module", autouse=True)
def _register_tools() -> None:
    """Ensure all tools and visibility transforms are installed before tests run."""
    from moneybin.mcp.server import register_core_tools

    register_core_tools()


@pytest.mark.asyncio
async def test_core_tools_visible_at_connect() -> None:
    """Tools without a domain are listed by default."""
    from moneybin.mcp.server import mcp

    async with Client(mcp) as client:
        names = {t.name for t in await client.list_tools()}
        assert "spending.summary" in names
        assert "accounts.list" in names


@pytest.mark.asyncio
async def test_extended_tools_hidden_at_connect() -> None:
    """Tools with a domain are not listed by default — Visibility transforms hide them."""
    from moneybin.mcp.server import mcp

    async with Client(mcp) as client:
        names = {t.name for t in await client.list_tools()}
        assert "categorize.bulk" not in names
        assert "budget.set" not in names


@pytest.mark.asyncio
async def test_discover_reveals_namespace_tools() -> None:
    """Discover reveals namespace tools for the calling session.

    Calling moneybin.discover('categorize') enables every tool tagged
    'categorize' for the calling session.
    """
    from moneybin.mcp.server import mcp

    async with Client(mcp) as client:
        await client.call_tool("moneybin.discover", {"domain": "categorize"})
        names = {t.name for t in await client.list_tools()}
        assert "categorize.bulk" in names


@pytest.mark.asyncio
async def test_unknown_domain_returns_error_envelope() -> None:
    """Calling discover('not-a-real-namespace') returns an error envelope."""
    from moneybin.mcp.server import mcp

    async with Client(mcp) as client:
        result = await client.call_tool("moneybin.discover", {"domain": "nope"})
        envelope = result.structured_content
        assert envelope.get("error") is not None or "Unknown domain" in str(envelope)


@pytest.mark.asyncio
async def test_hidden_tool_is_uncallable_via_tools_call() -> None:
    """Hidden tools must be uncallable.

    Verified safe by spike (3.2.4 raises ToolError: Unknown tool:
    '<name>'). This test guards against regression if fastmcp's behavior
    ever changes.
    """
    from fastmcp.exceptions import ToolError

    from moneybin.mcp.server import mcp

    async with Client(mcp) as client:
        with pytest.raises(ToolError):
            await client.call_tool("categorize.bulk", {})
