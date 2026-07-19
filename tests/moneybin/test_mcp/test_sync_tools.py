"""Tests for the consolidated sync MCP workflow."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from fastmcp import FastMCP

import moneybin.mcp.tools.sync as sync_module
from moneybin.connectors.sync_auth import SyncAuthResult, SyncLogoutResult
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


@pytest.mark.unit
async def test_sync_workflow_renders_explicit_auth_variants() -> None:
    """Existing tools carry strict login/status/logout variants without new names."""
    srv = FastMCP("test")
    sync_module.register_sync_workflow_tools(srv)
    tools = {tool.name: tool for tool in await srv._list_tools()}  # noqa: SLF001  # pyright: ignore[reportPrivateUsage]

    assert tools["sync_link"].parameters["properties"]["mode"] == {
        "default": "institution",
        "enum": ["institution", "login"],
        "type": "string",
    }
    assert tools["sync_status"].parameters["properties"]["auth_session_id"] == {
        "anyOf": [{"type": "string"}, {"type": "null"}],
        "default": None,
    }
    assert tools["sync_disconnect"].parameters["properties"]["mode"] == {
        "default": "institution",
        "enum": ["institution", "logout"],
        "type": "string",
    }


@pytest.mark.unit
@patch("moneybin.mcp.tools.sync._build_sync_auth_service")
async def test_sync_link_login_begins_nonblocking_safe_session(
    mock_build: MagicMock,
) -> None:
    mock_build.return_value.begin.return_value = SyncAuthResult(
        auth_session_id="syncauth_abc",
        status="pending",
        user_code="ABCD-EFGH",
        verification_url="https://auth.example/activate?code=ABCD-EFGH",
        expiration="2026-07-19T00:15:00+00:00",
    )

    envelope = await sync_module.sync_link_coarse(mode="login")

    assert envelope.data.kind == "auth"
    assert envelope.data.status == "pending"
    assert envelope.data.auth_session_id == "syncauth_abc"
    assert "device_code" not in envelope.to_dict()["data"]
    assert any("sync_status" in action for action in envelope.actions)


@pytest.mark.unit
@patch("moneybin.mcp.tools.sync._build_sync_auth_service")
async def test_sync_status_completes_auth_session_idempotently(
    mock_build: MagicMock,
) -> None:
    mock_build.return_value.status.return_value = SyncAuthResult(
        auth_session_id="syncauth_abc",
        status="authenticated",
        user_code="ABCD-EFGH",
        verification_url="https://auth.example/activate?code=ABCD-EFGH",
        expiration="2026-07-19T00:15:00+00:00",
        replayed=True,
    )

    envelope = await sync_module.sync_status_coarse(auth_session_id="syncauth_abc")

    assert envelope.data.kind == "auth"
    assert envelope.data.status == "authenticated"
    assert envelope.data.replayed is True
    mock_build.return_value.status.assert_called_once_with("syncauth_abc")


@pytest.mark.unit
@patch("moneybin.mcp.tools.sync._build_sync_auth_service")
async def test_sync_disconnect_logout_clears_scoped_auth(
    mock_build: MagicMock,
) -> None:
    mock_build.return_value.logout.return_value = SyncLogoutResult(
        status="logged_out",
        cleared_auth_sessions=2,
    )

    envelope = await sync_module.sync_disconnect(mode="logout")

    assert envelope.data.kind == "auth"
    assert envelope.data.status == "logged_out"
    assert envelope.data.cleared_auth_sessions == 2
    assert any("sync_link" in action for action in envelope.actions)


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
