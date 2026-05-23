"""MCP tests for the privacy consent tools (grant/revoke/status/log)."""

from __future__ import annotations

import pytest

from moneybin import error_codes
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
        "privacy_consent_grant",
        "privacy_consent_revoke",
        "privacy_status",
        "privacy_log",
    } <= names


async def test_grant_consent_tool(
    mcp_db: object, monkeypatch: pytest.MonkeyPatch
) -> None:
    _set_backend(monkeypatch)
    from moneybin.mcp.tools.privacy import privacy_consent_grant

    env = await privacy_consent_grant(category="mcp-data-sharing")
    assert env.error is None
    assert env.data.action in ("granted", "noop")
    assert env.data.backend == "anthropic"


async def test_grant_no_backend_returns_error_envelope(
    mcp_db: object, monkeypatch: pytest.MonkeyPatch
) -> None:
    """No backend + no default → a well-formed error envelope, not an exception."""
    monkeypatch.delenv("MONEYBIN_AI__DEFAULT_BACKEND", raising=False)
    clear_settings_cache()
    set_current_profile("test")
    from moneybin.mcp.tools.privacy import privacy_consent_grant

    env = await privacy_consent_grant(category="mcp-data-sharing")
    assert env.error is not None
    assert env.error.code == error_codes.MUTATION_INVALID_INPUT


async def test_status_tool_reflects_grant(
    mcp_db: object, monkeypatch: pytest.MonkeyPatch
) -> None:
    _set_backend(monkeypatch)
    from moneybin.mcp.tools.privacy import privacy_consent_grant, privacy_status

    await privacy_consent_grant(category="mcp-data-sharing")
    env = await privacy_status()
    assert env.error is None
    cats = {g.feature_category for g in env.data.active_grants}
    assert "mcp-data-sharing" in cats


async def test_revoke_consent_tool(
    mcp_db: object, monkeypatch: pytest.MonkeyPatch
) -> None:
    _set_backend(monkeypatch)
    from moneybin.mcp.tools.privacy import (
        privacy_consent_grant,
        privacy_consent_revoke,
        privacy_status,
    )

    await privacy_consent_grant(category="mcp-data-sharing")
    env = await privacy_consent_revoke(category="mcp-data-sharing")
    assert env.error is None
    assert env.data.action == "revoked"
    status = await privacy_status()
    assert status.data.active_grants == []


async def test_log_tool_returns_events(
    mcp_db: object, monkeypatch: pytest.MonkeyPatch
) -> None:
    _set_backend(monkeypatch)
    from moneybin.mcp.tools.privacy import privacy_consent_grant, privacy_log

    await privacy_consent_grant(category="mcp-data-sharing")
    env = await privacy_log(last=20)
    assert env.error is None
    grants = [e for e in env.data.events if e.action == "consent.grant"]
    assert grants, "consent.grant event missing from privacy_log"
    # consent_mode must survive the round-trip — auditors need persistent vs one-time.
    assert grants[0].consent_mode == "persistent"
