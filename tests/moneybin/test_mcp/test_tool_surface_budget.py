"""The client-visible tool count is a public-surface fact with a hard client ceiling.

MoneyBin retired client-driven progressive disclosure (`mcp-architecture.md` §3):
every registered tool is visible at connect, so "registered" and "visible" are the
same number. That number is not free — Cascade (Windsurf), a client `mcp install`
supports, enforces a hard ceiling on how many tools it will hold at once.

These tests pin the count so crossing a client's limit is a conscious act recorded
in a diff, not something a user discovers when their tools silently stop working.
"""

import asyncio

import pytest

from moneybin.mcp.surface import VISIBLE_TOOL_COUNT, WINDSURF_ACTIVE_TOOL_CAP

# The declared counts live in `moneybin.mcp.surface` because `mcp install` cites them
# in its Windsurf warning and cannot afford to boot the server to compute them. This
# module is what keeps that declaration honest against the live registry — bump it
# deliberately, not reflexively: read `docs/guides/mcp-clients.md` → Windsurf first,
# and if the change pushes us further past the cap, say so in the PR.


def _visible_tool_names() -> set[str]:
    """Tool names a connecting client actually receives (visibility filters applied)."""
    from moneybin.mcp.server import init_db, mcp

    init_db()
    return {tool.name for tool in asyncio.run(mcp.list_tools())}


@pytest.mark.integration
def test_visible_tool_count_is_pinned() -> None:
    visible = _visible_tool_names()
    assert len(visible) == VISIBLE_TOOL_COUNT, (
        f"The client-visible MCP tool surface changed to {len(visible)} "
        f"(expected {VISIBLE_TOOL_COUNT}). This is a public-contract change: "
        "update VISIBLE_TOOL_COUNT, and re-check the Windsurf section of "
        "docs/guides/mcp-clients.md — we are already over Cascade's "
        f"{WINDSURF_ACTIVE_TOOL_CAP}-tool ceiling."
    )


@pytest.mark.integration
def test_nothing_is_hidden_from_connecting_clients() -> None:
    """Guards the §3 claim itself: no tool is quietly withheld at connect.

    If this ever fails, MoneyBin has grown a hidden-tool tier — which would change
    the Windsurf math below and mean the docs (and the cap arithmetic) are stale.
    """
    from moneybin.mcp.server import init_db, mcp

    init_db()
    registered = {tool.name for tool in asyncio.run(mcp._list_tools())}  # noqa: SLF001  # pyright: ignore[reportPrivateUsage]  # public API filters by visibility; we want the raw registry
    assert registered == _visible_tool_names()


@pytest.mark.integration
def test_windsurf_overflow_is_documented() -> None:
    """We exceed Cascade's ceiling. That is a shipped limitation, so it must be written down.

    Not a hypothetical: `mcp install --client windsurf` writes a config for a client
    that cannot hold our surface, and Windsurf gives the user no signal about which
    tools got dropped. The guide has to tell them.
    """
    from pathlib import Path

    overflow = VISIBLE_TOOL_COUNT - WINDSURF_ACTIVE_TOOL_CAP
    assert overflow > 0, (
        "MoneyBin now fits inside Cascade's tool ceiling — delete this test and the "
        "Windsurf overflow warning in docs/guides/mcp-clients.md."
    )
    guide = Path(__file__).parents[3] / "docs" / "guides" / "mcp-clients.md"
    assert str(WINDSURF_ACTIVE_TOOL_CAP) in guide.read_text(), (
        "docs/guides/mcp-clients.md must state Cascade's 100-tool ceiling and that "
        "MoneyBin exceeds it."
    )
