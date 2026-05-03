"""Tests for sync_* MCP tools.

Sync is currently a stub surface (mirrors `moneybin sync *` CLI which is
also stubbed pending docs/specs/sync-overview.md). These tests verify
taxonomy/wiring — every advertised sync tool registers and returns the
not_implemented envelope shape — not real sync behavior.
"""

from __future__ import annotations

import asyncio

import pytest
from fastmcp import FastMCP

from moneybin.mcp.tools.sync import (
    register_sync_tools,
    sync_connect,
    sync_disconnect,
    sync_login,
    sync_logout,
    sync_pull,
    sync_schedule_remove,
    sync_schedule_set,
    sync_schedule_show,
    sync_status,
)

_EXPECTED_TOOLS = {
    "sync_login",
    "sync_logout",
    "sync_connect",
    "sync_disconnect",
    "sync_pull",
    "sync_status",
    "sync_schedule_set",
    "sync_schedule_show",
    "sync_schedule_remove",
}


@pytest.mark.unit
def test_register_sync_tools_registers_all_nine() -> None:
    """All nine sync tools register; key rotate is excluded by design."""
    srv = FastMCP("test")
    register_sync_tools(srv)
    names = {t.name for t in asyncio.run(srv._list_tools())}  # noqa: SLF001  # pyright: ignore[reportPrivateUsage]
    assert _EXPECTED_TOOLS <= names
    assert "sync_key_rotate" not in names
    assert "sync_rotate_key" not in names


@pytest.mark.unit
@pytest.mark.parametrize(
    "fn",
    [
        lambda: sync_login(),
        lambda: sync_logout(),
        lambda: sync_connect(),
        lambda: sync_disconnect(institution="chase"),
        lambda: sync_pull(),
        lambda: sync_status(),
        lambda: sync_schedule_set(time="09:00"),
        lambda: sync_schedule_show(),
        lambda: sync_schedule_remove(),
    ],
)
def test_sync_tool_returns_not_implemented_envelope(fn) -> None:  # noqa: ANN001
    """Every sync tool returns a stub envelope pointing at the spec."""
    parsed = fn().to_dict()
    assert parsed["summary"]["sensitivity"] == "low"
    assert parsed["data"]["status"] == "not_implemented"
    assert parsed["data"]["spec"] == "docs/specs/sync-overview.md"
