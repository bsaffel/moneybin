"""MCP tests for the privacy consent tools (grant/revoke/status/log)."""

from __future__ import annotations

import pytest

from moneybin import error_codes
from moneybin.config import clear_settings_cache, set_current_profile
from moneybin.mcp.tools.privacy import (
    privacy_consent_set_coarse,
    register_privacy_coarse_writes,
)

from .schema_assertions import isolated_server, listed_tool

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


async def test_consent_revoke_confirms_exact_categories(
    mcp_db: object, monkeypatch: pytest.MonkeyPatch
) -> None:
    _set_backend(monkeypatch)
    from moneybin.mcp.tools.privacy import (
        privacy_consent_grant,
        privacy_consent_set_coarse,
    )

    await privacy_consent_grant(category="mcp-data-sharing")
    await privacy_consent_grant(category="matching-overview")

    first = await privacy_consent_set_coarse(
        categories=["matching-overview", "mcp-data-sharing"],
        state="revoked",
    )
    assert first.error is not None
    token = first.error.details["confirmation_token"]

    second = await privacy_consent_set_coarse(
        categories=["mcp-data-sharing", "matching-overview"],
        state="revoked",
        confirmation_token=token,
    )

    assert second.error is None
    assert second.data.state == "revoked"


async def test_consent_grant_normalizes_order_and_returns_effective_set(
    mcp_db: object, monkeypatch: pytest.MonkeyPatch
) -> None:
    _set_backend(monkeypatch)

    response = await privacy_consent_set_coarse(
        categories=["matching-overview", "mcp-data-sharing"],
        state="granted",
        mode="one-time",
    )

    assert response.error is None
    assert response.data.categories == [
        "matching-overview",
        "mcp-data-sharing",
    ]
    assert response.data.effective_categories == response.data.categories
    assert response.data.consent_mode == "one-time"

    from moneybin.database import get_database

    with get_database(read_only=True) as db:
        operation_ids = {
            row[0]
            for row in db.execute(
                "SELECT operation_id FROM app.audit_log WHERE action = 'consent.grant'"
            ).fetchall()
        }
    assert operation_ids == {response.data.operation_id}

    noop = await privacy_consent_set_coarse(
        categories=["mcp-data-sharing", "matching-overview"],
        state="granted",
        mode="persistent",
    )
    assert noop.error is not None
    assert noop.error.code == error_codes.MUTATION_NOTHING_TO_DO


async def test_consent_revoke_rejects_token_after_live_state_changes(
    mcp_db: object, monkeypatch: pytest.MonkeyPatch
) -> None:
    _set_backend(monkeypatch)
    from moneybin.mcp.tools.privacy import privacy_consent_grant, privacy_consent_revoke

    await privacy_consent_grant(category="mcp-data-sharing")
    first = await privacy_consent_set_coarse(
        categories=["mcp-data-sharing"],
        state="revoked",
    )
    assert first.error is not None

    await privacy_consent_revoke(category="mcp-data-sharing")
    stale = await privacy_consent_set_coarse(
        categories=["mcp-data-sharing"],
        state="revoked",
        confirmation_token=first.error.details["confirmation_token"],
    )

    assert stale.error is not None
    assert stale.error.code == error_codes.MUTATION_CONFIRMATION_MISMATCH


async def test_consent_revoke_rejects_one_time_mode(
    mcp_db: object, monkeypatch: pytest.MonkeyPatch
) -> None:
    _set_backend(monkeypatch)

    response = await privacy_consent_set_coarse(
        categories=["mcp-data-sharing"],
        state="revoked",
        mode="one-time",
    )

    assert response.error is not None
    assert response.error.code == error_codes.MUTATION_INVALID_INPUT


async def test_consent_dormant_registrar_advertises_closed_destructive_contract() -> (
    None
):
    mcp = isolated_server(register_privacy_coarse_writes)

    tools = await mcp._list_tools()  # noqa: SLF001  # pyright: ignore[reportPrivateUsage]
    assert {tool.name for tool in tools} == {"privacy_consent_set"}
    tool = await listed_tool(mcp, "privacy_consent_set")
    assert tool.outputSchema is None
    assert tool.annotations is not None
    assert tool.annotations.destructiveHint is True
    assert set(tool.inputSchema["properties"]["state"]["enum"]) == {
        "granted",
        "revoked",
    }


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


async def test_log_signals_truncation_on_full_page(
    mcp_db: object, monkeypatch: pytest.MonkeyPatch
) -> None:
    """has_more fires when `last` (not just the server cap) is the limiter."""
    _set_backend(monkeypatch)
    from moneybin.mcp.tools.privacy import privacy_consent_grant, privacy_log

    # Two grants → at least two log events; request fewer than exist.
    await privacy_consent_grant(category="mcp-data-sharing")
    await privacy_consent_grant(category="ml-categorization")
    env = await privacy_log(last=1)
    assert env.error is None
    assert len(env.data.events) == 1
    assert env.summary.has_more is True
