"""Per-session visibility tests for MCP tools."""

from __future__ import annotations

import pytest
from fastmcp import Client


@pytest.fixture(scope="module", autouse=True)
def _register_tools() -> None:  # pyright: ignore[reportUnusedFunction]
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
    import json

    from moneybin.mcp.server import mcp

    async with Client(mcp) as client:
        result = await client.call_tool("moneybin.discover", {"domain": "nope"})
        envelope = json.loads(result.content[0].text)  # type: ignore[attr-defined]
        assert envelope["data"] == []
        assert envelope["error"] is not None
        assert "Unknown domain" in str(envelope["error"])


@pytest.mark.asyncio
async def test_per_session_discover_isolated() -> None:
    """Two clients connected to the same server have independent visibility.

    One client discovering 'categorize' must not affect the other client's
    tool list — fastmcp Client sessions are isolated by construction; this
    test guards against regression.
    """
    from moneybin.mcp.server import mcp

    async with Client(mcp) as client_a, Client(mcp) as client_b:
        before_a = {t.name for t in await client_a.list_tools()}
        assert "categorize.bulk" not in before_a

        await client_a.call_tool("moneybin.discover", {"domain": "categorize"})

        after_a = {t.name for t in await client_a.list_tools()}
        visible_b = {t.name for t in await client_b.list_tools()}

        assert "categorize.bulk" in after_a
        assert "categorize.bulk" not in visible_b, (
            "Client B's tool visibility leaked from Client A's discover call — "
            "session isolation is broken."
        )


@pytest.mark.asyncio
async def test_visibility_or_match_semantics() -> None:
    """A tool tagged with one extended domain stays hidden when only a different domain is enabled.

    The server installs a single ``Visibility(False, tags=set(EXTENDED_DOMAINS))``
    transform — this test pins the OR-match contract verified against fastmcp
    3.1.x. If a future SDK change flips this to AND-match semantics, every
    extended tool would become visible at boot, which would be a silent
    privacy regression.
    """
    from moneybin.mcp.server import mcp

    async with Client(mcp) as client:
        await client.call_tool("moneybin.discover", {"domain": "categorize"})
        names = {t.name for t in await client.list_tools()}
        assert "categorize.bulk" in names, "categorize tools should be enabled"
        assert "budget.set" not in names, (
            "budget tools must remain hidden when only categorize is enabled — "
            "Visibility transform OR-match contract is broken."
        )


@pytest.mark.asyncio
async def test_full_discover_reveals_every_extended_tool() -> None:
    """After discovering every extended domain, total visible tool count matches the unfiltered registry.

    Guards against an extended namespace silently dropping a tool during a
    future refactor — if the count diverges, either a tool lost its tag or a
    visible tool gained a stray tag.
    """
    from moneybin.mcp.server import EXTENDED_DOMAINS, mcp

    async with Client(mcp) as client:
        for domain in EXTENDED_DOMAINS:
            await client.call_tool("moneybin.discover", {"domain": domain})
        visible = {t.name for t in await client.list_tools()}
        all_registered = {
            t.name
            for t in await mcp._list_tools()  # noqa: SLF001  # pyright: ignore[reportPrivateUsage]
        }
        assert visible == all_registered, (
            f"Visible tool set diverged from registered tool set after full "
            f"discover. Missing: {all_registered - visible}; "
            f"unexpected: {visible - all_registered}"
        )


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
