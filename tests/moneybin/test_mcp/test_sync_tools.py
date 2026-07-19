"""Tests for sync_* MCP tools.

Verifies taxonomy/wiring — every registered sync tool is present and the
removed stubs (sync_schedule_*) and CLI-only tools are absent.
sync_pull, sync_status, sync_link, sync_link_status, and sync_disconnect have
live implementations tested in test_mcp_sync.py; sync_connect and
sync_connect_status are deprecated aliases retained for one minor release.
sync_login and sync_logout are CLI-only (browser interaction + credential
handling) and are intentionally absent from MCP.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from fastmcp import FastMCP

import moneybin.mcp.tools.sync as sync_module
from moneybin.mcp.tools.sync import register_sync_tools

_EXPECTED_TOOLS = {
    "sync_link",
    "sync_link_status",
    "sync_disconnect",
    "sync_pull",
    "sync_status",
}

# Deprecated aliases retained until next minor release; covered by
# test_mcp_sync.py alias-warning tests.
_DEPRECATED_ALIASES = {"sync_connect", "sync_connect_status"}


@pytest.mark.unit
async def test_register_sync_tools_registers_expected_tools() -> None:
    """Expected sync tools register; schedule stubs/login/logout/key-rotate excluded."""
    srv = FastMCP("test")
    register_sync_tools(srv)
    names = {t.name for t in await srv._list_tools()}  # noqa: SLF001  # pyright: ignore[reportPrivateUsage]
    assert _EXPECTED_TOOLS <= names
    # Deprecated aliases stay registered until the next minor release.
    assert _DEPRECATED_ALIASES <= names
    assert "sync_login" not in names
    assert "sync_logout" not in names
    assert "sync_key_rotate" not in names
    assert "sync_rotate_key" not in names
    assert "sync_schedule_set" not in names
    assert "sync_schedule_show" not in names
    assert "sync_schedule_remove" not in names


@pytest.mark.unit
async def test_register_sync_workflow_tools_excludes_live_aliases() -> None:
    srv = FastMCP("test")
    sync_module.register_sync_workflow_tools(srv)

    names = {t.name for t in await srv._list_tools()}  # noqa: SLF001  # pyright: ignore[reportPrivateUsage]

    assert names == {"sync_link", "sync_status", "sync_pull", "sync_disconnect"}
    assert "sync_link_status" not in names
    assert "sync_connect" not in names
    assert "sync_connect_status" not in names


@pytest.mark.unit
async def test_sync_workflow_status_accepts_optional_session_id() -> None:
    srv = FastMCP("test")
    sync_module.register_sync_workflow_tools(srv)
    tool = next(t for t in await srv._list_tools() if t.name == "sync_status")  # noqa: SLF001  # pyright: ignore[reportPrivateUsage]

    assert tool.parameters["properties"]["session_id"] == {
        "anyOf": [{"type": "string"}, {"type": "null"}],
        "default": None,
    }
    assert tool.output_schema is None


def test_sync_workflow_registrar_uses_public_privacy_actor_names() -> None:
    registered: list[tuple[str, str | None]] = []

    def capture(
        _mcp: object,
        _callback: object,
        name: str,
        _description: str,
        *,
        privacy_actor: str | None = None,
        **_kwargs: object,
    ) -> None:
        registered.append((name, privacy_actor))

    with patch.object(sync_module, "register", capture):
        sync_module.register_sync_workflow_tools(MagicMock())

    assert registered == [(name, name) for name, _ in registered]
