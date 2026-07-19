"""Tests for the consolidated sync MCP workflow."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from fastmcp import FastMCP

import moneybin.mcp.tools.sync as sync_module
from moneybin.mcp.tools.sync import register_sync_tools

_EXPECTED_TOOLS = {
    "sync_link",
    "sync_disconnect",
    "sync_pull",
    "sync_status",
}


@pytest.mark.unit
async def test_register_sync_tools_registers_expected_tools() -> None:
    """Expected sync tools register; schedule stubs/login/logout/key-rotate excluded."""
    srv = FastMCP("test")
    register_sync_tools(srv)
    names = {t.name for t in await srv._list_tools()}  # noqa: SLF001  # pyright: ignore[reportPrivateUsage]
    assert names == _EXPECTED_TOOLS
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
