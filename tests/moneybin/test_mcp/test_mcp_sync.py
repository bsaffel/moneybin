"""Unit tests for sync_* MCP tools (envelope shape + service delegation)."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pytest
from fastmcp import FastMCP

from moneybin.connectors.sync_models import (
    ConnectedInstitution,
    InstitutionResult,
    LinkInitiateResponse,
    PullResult,
    SyncConnectionView,
)
from moneybin.mcp.tools.sync import register_sync_tools


@pytest.mark.unit
@patch("moneybin.mcp.tools.sync._build_sync_service")
async def test_sync_pull_returns_envelope_with_summary(mock_build: MagicMock) -> None:
    service = MagicMock()
    service.pull.return_value = PullResult(
        job_id="j1",
        transactions_loaded=5,
        accounts_loaded=1,
        balances_loaded=1,
        transactions_removed=0,
        institutions=[
            InstitutionResult(
                provider_item_id="item_a",
                institution_name="Chase",
                status="completed",
                transaction_count=5,
            ),
        ],
    )
    mock_build.return_value.__enter__.return_value = service
    from moneybin.mcp.tools.sync import sync_pull

    envelope = sync_pull()
    assert envelope.summary.sensitivity == "low"
    assert envelope.data.transactions_loaded == 5
    service.pull.assert_called_once()


@pytest.mark.unit
@patch("moneybin.mcp.tools.sync._build_sync_service")
async def test_sync_pull_surfaces_security_resolution_failure(
    mock_build: MagicMock,
) -> None:
    """A pull whose security resolution failed must NOT read as a clean success.

    There is no source-native fallback for security_id and cost_basis.py skips
    every NULL-security event, so a swallowed resolution failure silently drops
    every buy/sell on those securities from lots and realized gains. The CLI
    warns and exits non-zero; the MCP payload must carry the same outcome.
    """
    service = MagicMock()
    service.pull.return_value = PullResult(
        job_id="j1",
        transactions_loaded=0,
        accounts_loaded=1,
        balances_loaded=1,
        transactions_removed=0,
        securities_loaded=12,
        investment_transactions_loaded=7,
        holdings_loaded=3,
        holding_lots_loaded=2,
        institutions=[],
        opening_bootstrap_rows=4,
        investment_source_overlap_accounts=["acc_dup"],
        security_resolution={"minted": 2, "proposed": 1},
        security_resolution_error="database is locked",
    )
    mock_build.return_value.__enter__.return_value = service
    from moneybin.mcp.tools.sync import sync_pull

    envelope = sync_pull()

    data = envelope.data
    assert data.security_resolution_error == "database is locked"
    assert data.security_resolution == {"minted": 2, "proposed": 1}
    assert data.securities_loaded == 12
    assert data.investment_transactions_loaded == 7
    assert data.holdings_loaded == 3
    assert data.holding_lots_loaded == 2
    assert data.opening_bootstrap_rows == 4
    assert data.investment_source_overlap_accounts == ["acc_dup"]
    # The agent must be told the pull is NOT clean and what to do about it.
    actions_text = " ".join(envelope.actions)
    assert "security resolution failed" in actions_text.lower()
    assert "sync_pull" in actions_text


@pytest.mark.unit
@patch("moneybin.mcp.tools.sync._build_sync_service")
async def test_sync_pull_coarse_preserves_partial_failure_recovery_actions(
    mock_build: MagicMock,
) -> None:
    service = MagicMock()
    service.pull.return_value = PullResult(
        job_id="j1",
        transactions_loaded=0,
        accounts_loaded=0,
        balances_loaded=0,
        transactions_removed=0,
        institutions=[],
        transforms_error="apply failed",
        security_resolution={"pending": 1},
        security_resolution_error="resolution failed",
        investment_source_overlap_accounts=["acct_1"],
    )
    mock_build.return_value.__enter__.return_value = service
    from moneybin.mcp.tools.sync import sync_pull_coarse

    envelope = await sync_pull_coarse()

    actions = " ".join(envelope.actions)
    assert "security resolution failed" in actions
    assert "refresh_run" in actions
    assert "reviews(kind='security_links')" in actions
    assert "system_status(sections=['doctor'])" in actions
    assert "investments_securities_links_pending" not in actions
    assert "system_doctor" not in actions


@pytest.mark.unit
@patch("moneybin.mcp.tools.sync._build_sync_service")
async def test_sync_pull_flags_pending_security_review(mock_build: MagicMock) -> None:
    """Identities awaiting review must reach the agent, with the review tool named."""
    service = MagicMock()
    service.pull.return_value = PullResult(
        job_id="j1",
        transactions_loaded=0,
        accounts_loaded=1,
        balances_loaded=0,
        transactions_removed=0,
        institutions=[],
        security_resolution={"adopted": 3, "proposed": 2, "pending": 1},
    )
    mock_build.return_value.__enter__.return_value = service
    from moneybin.mcp.tools.sync import sync_pull

    envelope = sync_pull()

    assert envelope.data.security_resolution["proposed"] == 2
    actions_text = " ".join(envelope.actions)
    assert "reviews" in actions_text


@pytest.mark.unit
@patch("moneybin.mcp.tools.sync._build_sync_service")
async def test_sync_pull_flags_manual_plaid_overlap(mock_build: MagicMock) -> None:
    """The manual/Plaid overlap list reaches the agent — lots double-count until fixed."""
    service = MagicMock()
    service.pull.return_value = PullResult(
        job_id="j1",
        transactions_loaded=0,
        accounts_loaded=1,
        balances_loaded=0,
        transactions_removed=0,
        institutions=[],
        investment_source_overlap_accounts=["acc_a", "acc_b"],
    )
    mock_build.return_value.__enter__.return_value = service
    from moneybin.mcp.tools.sync import sync_pull

    envelope = sync_pull()

    assert envelope.data.investment_source_overlap_accounts == ["acc_a", "acc_b"]
    actions_text = " ".join(envelope.actions)
    assert "both manual and Plaid" in actions_text


@pytest.mark.unit
@patch("moneybin.mcp.tools.sync._build_sync_service")
async def test_sync_status_returns_low_sensitivity(mock_build: MagicMock) -> None:
    service = MagicMock()
    service.list_connections.return_value = [
        SyncConnectionView(
            id="u1",
            provider_item_id="item_a",
            institution_name="Chase",
            provider="plaid",
            status="active",
            last_sync=datetime(2026, 4, 7, tzinfo=UTC),
            guidance=None,
        ),
    ]
    mock_build.return_value.__enter__.return_value = service
    from moneybin.mcp.tools.sync import sync_status

    envelope = sync_status()
    assert envelope.summary.sensitivity == "low"
    assert envelope.data.connections[0].institution_name == "Chase"


@pytest.mark.unit
@patch("moneybin.mcp.tools.sync._build_sync_client")
async def test_sync_link_returns_link_url_with_medium_sensitivity(
    mock_client_builder: MagicMock,
) -> None:
    client = MagicMock()
    client.initiate_link.return_value = LinkInitiateResponse(
        session_id="sess_abc",
        link_url="https://hosted.plaid.com/link/xyz",
        link_type="widget_flow",
        expiration=datetime(2026, 5, 13, 13, 30, tzinfo=UTC),
    )
    mock_client_builder.return_value = client
    from moneybin.mcp.tools.sync import sync_link

    envelope = sync_link()
    assert envelope.summary.sensitivity == "low"
    assert envelope.data.session_id == "sess_abc"
    assert envelope.data.link_url == "https://hosted.plaid.com/link/xyz"
    # Agent should know about expiration to decide when to give up polling
    assert envelope.data.expiration is not None


@pytest.mark.unit
@patch("moneybin.mcp.tools.sync._build_sync_client")
async def test_sync_link_status_pending(mock_client_builder: MagicMock) -> None:
    from datetime import UTC, datetime

    from moneybin.connectors.sync_models import LinkStatusResponse

    client = MagicMock()
    # MCP sync_link_status uses the public get_link_status single-shot
    # method on the client (was reaching into _authed_request before).
    client.get_link_status.return_value = LinkStatusResponse(
        session_id="sess_abc",
        status="pending",
        expiration=datetime(2026, 5, 13, 13, 30, tzinfo=UTC),
    )
    mock_client_builder.return_value = client
    from moneybin.mcp.tools.sync import sync_link_status

    envelope = sync_link_status(session_id="sess_abc")
    assert envelope.data.status == "pending"
    assert envelope.data.expiration is not None


@pytest.mark.unit
async def test_sync_link_mcp_tool_registered() -> None:
    """The live schema preserves the bearer-credential handling warning."""
    srv = FastMCP("test")
    register_sync_tools(srv)
    tools = {
        tool.name: tool
        for tool in await srv._list_tools()  # noqa: SLF001  # pyright: ignore[reportPrivateUsage]
    }

    assert "sync_link" in tools
    assert "sensitive one-time credential" in (tools["sync_link"].description or "")


@pytest.mark.unit
async def test_sync_status_mcp_tool_registered() -> None:
    """The consolidated sync_status tool handles link-session polling."""
    srv = FastMCP("test")
    register_sync_tools(srv)
    names = {t.name for t in await srv._list_tools()}  # noqa: SLF001  # pyright: ignore[reportPrivateUsage]
    assert names == {"sync_link", "sync_status", "sync_pull", "sync_disconnect"}


@pytest.mark.unit
@patch("moneybin.mcp.tools.sync._build_sync_service")
async def test_sync_disconnect_calls_service(mock_build: MagicMock) -> None:
    service = MagicMock()
    connection = ConnectedInstitution(
        id="conn_uuid",
        provider_item_id="item_a",
        provider="plaid",
        institution_name="Chase",
        status="active",
        created_at=datetime(2026, 3, 15, tzinfo=UTC),
    )
    service.plan_disconnect.return_value = connection

    def disconnect_confirmed(
        *,
        institution: str,
        verify: object,
    ) -> ConnectedInstitution:
        assert institution == "Chase"
        verify(connection)  # type: ignore[operator]
        return connection

    service.disconnect_confirmed.side_effect = disconnect_confirmed
    mock_build.return_value.__enter__.return_value = service
    from moneybin.mcp.tools.sync import sync_disconnect

    required = await sync_disconnect(institution="Chase")

    assert required.error is not None
    assert required.error.code == "mutation_confirmation_required"
    assert required.error.details is not None
    service.disconnect_confirmed.assert_not_called()

    envelope = await sync_disconnect(
        institution="Chase",
        confirmation_token=str(required.error.details["confirmation_token"]),
    )

    service.disconnect_confirmed.assert_called_once()
    # SyncDisconnectPayload has only TXN_TYPE + INSTITUTION → Tier.LOW derived sensitivity
    assert envelope.summary.sensitivity == "low"


@pytest.mark.unit
@patch("moneybin.mcp.tools.sync._build_sync_service")
async def test_sync_disconnect_refuses_confirmation_after_live_target_changes(
    mock_build: MagicMock,
) -> None:
    original = ConnectedInstitution(
        id="conn_uuid",
        provider_item_id="item_a",
        provider="plaid",
        institution_name="Chase",
        status="active",
        created_at=datetime(2026, 3, 15, tzinfo=UTC),
    )
    changed = original.model_copy(update={"id": "conn_replaced"})
    service = MagicMock()
    service.plan_disconnect.return_value = original

    def disconnect_confirmed(
        *,
        institution: str,
        verify: object,
    ) -> ConnectedInstitution:
        assert institution == "Chase"
        verify(changed)  # type: ignore[operator]
        return changed

    service.disconnect_confirmed.side_effect = disconnect_confirmed
    mock_build.return_value.__enter__.return_value = service
    from moneybin.mcp.tools.sync import sync_disconnect

    required = await sync_disconnect(institution="Chase")
    assert required.error is not None
    assert required.error.details is not None

    result = await sync_disconnect(
        institution="Chase",
        confirmation_token=str(required.error.details["confirmation_token"]),
    )

    assert result.error is not None
    assert result.error.code == "mutation_confirmation_mismatch"


@pytest.mark.unit
def test_sync_review_prompt_content_includes_required_elements() -> None:
    """The sync_review prompt must guide an agent through a sync health check."""
    from moneybin.mcp.prompts import sync_review  # noqa: PLC0415

    text = sync_review()
    assert "sync_status" in text
    assert "reports(report_id='core:spending'" in text
    assert "spending_summary" not in text
    assert "stale" in text.lower()
    assert "error" in text.lower()
    # Privacy guard: prompt must direct agent NOT to surface PII
    assert (
        "account numbers" in text.lower()
        or "no pii" in text.lower()
        or "do not include" in text.lower()
    )
