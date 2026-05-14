"""Tests for sync_* MCP tools.

Verifies taxonomy/wiring — every registered sync stub returns the
not_implemented envelope shape — not real sync behavior. sync_pull, sync_status,
sync_connect, sync_connect_status, and sync_disconnect have live implementations
tested in test_mcp_sync.py. sync_login and sync_logout are CLI-only (browser
interaction + credential handling) and are intentionally absent from MCP.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import pytest
from fastmcp import FastMCP

from moneybin.mcp.tools.sync import (
    register_sync_tools,
    sync_schedule_remove,
    sync_schedule_set,
    sync_schedule_show,
)

_EXPECTED_TOOLS = {
    "sync_connect",
    "sync_connect_status",
    "sync_disconnect",
    "sync_pull",
    "sync_status",
    "sync_schedule_set",
    "sync_schedule_show",
    "sync_schedule_remove",
}


@pytest.mark.unit
async def test_register_sync_tools_registers_expected_tools() -> None:
    """Expected sync tools register; login/logout/key-rotate excluded by design."""
    srv = FastMCP("test")
    register_sync_tools(srv)
    names = {t.name for t in await srv._list_tools()}  # noqa: SLF001  # pyright: ignore[reportPrivateUsage]
    assert _EXPECTED_TOOLS <= names
    assert "sync_login" not in names
    assert "sync_logout" not in names
    assert "sync_key_rotate" not in names
    assert "sync_rotate_key" not in names


@pytest.mark.unit
@pytest.mark.parametrize(
    "fn",
    [
        lambda: sync_schedule_set(time="09:00"),
        sync_schedule_show,
        sync_schedule_remove,
    ],
)
async def test_sync_stub_tool_returns_not_implemented_envelope(
    fn: Callable[..., Any],
) -> None:
    """Stub sync tools return a not_implemented envelope pointing at the spec."""
    parsed = (await fn()).to_dict()
    assert parsed["status"] == "error"
    assert parsed["summary"]["sensitivity"] == "low"
    assert parsed["error"]["code"] == "not_implemented"
    assert (
        parsed["error"]["details"]["spec"]
        == "docs/specs/2026-05-13-plaid-sync-design.md"
    )
    assert any("moneybin sync" in a for a in parsed["actions"])
