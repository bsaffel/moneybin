"""MCP tests for the privacy consent tools (grant/revoke/status/log)."""

from __future__ import annotations

import pytest

from moneybin.config import clear_settings_cache, set_current_profile

pytestmark = pytest.mark.usefixtures("mcp_db")


def _set_backend(monkeypatch: pytest.MonkeyPatch, backend: str = "anthropic") -> None:
    monkeypatch.setenv("MONEYBIN_AI__DEFAULT_BACKEND", backend)
    clear_settings_cache()
    set_current_profile("test")


async def test_register_privacy_tools_registers_expected() -> None:
    from fastmcp import FastMCP

    from moneybin.mcp.tools.privacy import register_privacy_tools

    srv = FastMCP("test")
    register_privacy_tools(srv)
    names = {t.name for t in await srv._list_tools()}  # noqa: SLF001  # pyright: ignore[reportPrivateUsage]
    assert {
        "privacy_grant_consent",
        "privacy_revoke_consent",
        "privacy_status",
        "privacy_log",
    } <= names


async def test_grant_consent_tool(
    mcp_db: object, monkeypatch: pytest.MonkeyPatch
) -> None:
    _set_backend(monkeypatch)
    from moneybin.mcp.tools.privacy import privacy_grant_consent

    env = await privacy_grant_consent(category="mcp-data-sharing")
    assert env.error is None
    assert env.data.action in ("granted", "noop")
    assert env.data.backend == "anthropic"


async def test_status_tool_reflects_grant(
    mcp_db: object, monkeypatch: pytest.MonkeyPatch
) -> None:
    _set_backend(monkeypatch)
    from moneybin.mcp.tools.privacy import privacy_grant_consent, privacy_status

    await privacy_grant_consent(category="mcp-data-sharing")
    env = await privacy_status()
    assert env.error is None
    cats = {g.feature_category for g in env.data.active_grants}
    assert "mcp-data-sharing" in cats


async def test_revoke_consent_tool(
    mcp_db: object, monkeypatch: pytest.MonkeyPatch
) -> None:
    _set_backend(monkeypatch)
    from moneybin.mcp.tools.privacy import (
        privacy_grant_consent,
        privacy_revoke_consent,
        privacy_status,
    )

    await privacy_grant_consent(category="mcp-data-sharing")
    env = await privacy_revoke_consent(category="mcp-data-sharing")
    assert env.error is None
    assert env.data.action == "revoked"
    status = await privacy_status()
    assert status.data.active_grants == []


async def test_log_tool_returns_events(
    mcp_db: object, monkeypatch: pytest.MonkeyPatch
) -> None:
    _set_backend(monkeypatch)
    from moneybin.mcp.tools.privacy import privacy_grant_consent, privacy_log

    await privacy_grant_consent(category="mcp-data-sharing")
    env = await privacy_log(last_n=20)
    assert env.error is None
    actions = {e.action for e in env.data.events}
    assert "consent.grant" in actions
