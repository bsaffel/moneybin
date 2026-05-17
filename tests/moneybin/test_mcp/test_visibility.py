"""MCP surface-visibility guard tests.

Client-driven progressive disclosure was retired 2026-05-17 (see
``docs/specs/mcp-architecture.md`` §3). The full registered surface is
visible at connect. These tests guard against an accidental
re-introduction of hidden-tool semantics and pin tool-name conformance to
the portable Anthropic/OpenAI regex.
"""

from __future__ import annotations

import re

import pytest
from fastmcp import Client


@pytest.fixture(scope="module", autouse=True)
def _register_tools() -> None:  # pyright: ignore[reportUnusedFunction]
    from moneybin.config import reload_settings, set_current_profile
    from moneybin.mcp import server

    set_current_profile("test")
    reload_settings()
    server._tools_registered = False  # pyright: ignore[reportPrivateUsage]
    server.register_core_tools()


async def test_full_surface_visible_at_connect() -> None:
    """Every registered tool — including formerly-extended namespaces — is visible at connect.

    Guards against re-introduction of a ``Visibility(False, ...)`` transform
    or other hidden-tool semantics. ``list_tools()`` (public, visibility-filtered)
    must equal ``_list_tools()`` (unfiltered registry).
    """
    from moneybin.mcp.server import mcp

    async with Client(mcp) as client:
        visible = {t.name for t in await client.list_tools()}

    all_registered = {
        t.name
        for t in await mcp._list_tools()  # noqa: SLF001  # fastmcp internal — public list_tools() filters by visibility  # pyright: ignore[reportPrivateUsage]
    }

    assert visible == all_registered, (
        f"Visible tool set diverged from registered tool set — a Visibility "
        f"transform may have been reintroduced. "
        f"Missing from visible: {all_registered - visible}; "
        f"unexpected in visible: {visible - all_registered}"
    )


async def test_formerly_extended_tools_visible_at_connect() -> None:
    """Representative tools from each formerly-extended namespace are visible at connect.

    Belt-and-suspenders complement to ``test_full_surface_visible_at_connect``:
    asserts specific tools by name so a regression that drops one namespace
    entirely (rather than hiding it) is caught.
    """
    from moneybin.mcp.server import mcp

    async with Client(mcp) as client:
        names = {t.name for t in await client.list_tools()}

    expected = {
        "transactions_categorize_apply",
        "budget_set",
        "tax_w2",
    }
    missing = expected - names
    assert not missing, f"Expected tools missing from connect-time surface: {missing}"


async def test_every_tool_name_matches_anthropic_openai_pattern() -> None:
    """Every registered tool name must match ``^[a-zA-Z0-9_-]{1,64}$``.

    Anthropic and OpenAI clients reject tool definitions whose names don't
    match this regex (FastMCP/MCP SDK itself does not enforce it, so a bad
    name boots fine and only fails at the frontend on connect). See
    ``.claude/rules/mcp-server.md`` — "we use the portable subset."
    """
    from moneybin.mcp.server import mcp

    pattern = re.compile(r"^[a-zA-Z0-9_-]{1,64}$")
    names = [
        t.name
        for t in await mcp._list_tools()  # noqa: SLF001  # fastmcp internal — public list_tools() filters by visibility  # pyright: ignore[reportPrivateUsage]
    ]
    bad = [n for n in names if not pattern.match(n)]
    assert not bad, (
        f"Tool names violate ^[a-zA-Z0-9_-]{{1,64}}$ (Anthropic/OpenAI "
        f"frontend regex): {bad}"
    )
