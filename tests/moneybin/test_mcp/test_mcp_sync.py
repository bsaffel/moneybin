"""Unit tests for sync_* MCP tools (envelope shape + service delegation)."""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pytest
from fastmcp import FastMCP

from moneybin import error_codes
from moneybin.connectors.sync_models import (
    ConnectedInstitution,
    InstitutionResult,
    LinkInitiateResponse,
    PullResult,
    SyncConnectionView,
    SyncDisconnectResult,
)
from moneybin.errors import UserError
from moneybin.mcp.tools.sync import register_sync_tools
from moneybin.services.refresh_outcome import RefreshStepOutcome, StageOutcome
from moneybin.services.sync_service import SyncPurgePlan


def _disconnected(connection: ConnectedInstitution) -> SyncDisconnectResult:
    return SyncDisconnectResult(**connection.model_dump(), remote_revocation="revoked")


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
async def test_sync_pull_coarse_surfaces_mutual_exclusion_as_mutation_error(
    mock_build: MagicMock,
) -> None:
    """The service's mutual-exclusion guard must classify as mutation_invalid_input.

    A bare ValueError would classify as infra_invalid_input via the generic
    exception classifier — the service raises UserError directly instead.
    """
    service = MagicMock()
    service.pull.side_effect = UserError(
        "institution and provider_item_id are mutually exclusive — pass one or neither",
        code=error_codes.MUTATION_INVALID_INPUT,
    )
    mock_build.return_value.__enter__.return_value = service
    from moneybin.mcp.tools.sync import sync_pull_coarse

    envelope = await sync_pull_coarse(institution="Chase")

    assert envelope.error is not None
    assert envelope.error.code == error_codes.MUTATION_INVALID_INPUT


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
    assert "accounts_set(investment_source_type=...)" in actions_text
    assert "system_status(sections=['doctor'])" in actions_text


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
            created_at=datetime(2026, 1, 5, tzinfo=UTC),
            guidance=None,
        ),
    ]
    mock_build.return_value.__enter__.return_value = service
    from moneybin.mcp.tools.sync import sync_status

    envelope = sync_status()
    assert envelope.summary.sensitivity == "low"
    assert envelope.data.connections[0].institution_name == "Chase"


@pytest.mark.unit
@patch("moneybin.mcp.tools.sync._build_sync_service")
async def test_sync_link_returns_link_url_with_medium_sensitivity(
    mock_build: MagicMock,
) -> None:
    service = MagicMock()
    service.initiate_link.return_value = LinkInitiateResponse(
        session_id="sess_abc",
        link_url="https://hosted.plaid.com/link/xyz",
        link_type="widget_flow",
        expiration=datetime(2026, 5, 13, 13, 30, tzinfo=UTC),
    )
    mock_build.return_value.__enter__.return_value = service
    from moneybin.mcp.tools.sync import sync_link

    envelope = sync_link()
    assert envelope.summary.sensitivity == "low"
    assert envelope.data.session_id == "sess_abc"
    assert envelope.data.link_url == "https://hosted.plaid.com/link/xyz"
    # Agent should know about expiration to decide when to give up polling
    assert envelope.data.expiration is not None
    service.initiate_link.assert_called_once_with(
        institution=None, provider=None, connection_id=None
    )


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
        for tool in await srv._list_tools()  # pyright: ignore[reportPrivateUsage]
    }

    assert "sync_link" in tools
    assert "sensitive one-time credential" in (tools["sync_link"].description or "")


@pytest.mark.unit
async def test_sync_status_mcp_tool_registered() -> None:
    """The consolidated sync_status tool handles link-session polling."""
    srv = FastMCP("test")
    register_sync_tools(srv)
    names = {t.name for t in await srv._list_tools()}  # pyright: ignore[reportPrivateUsage]
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
        institution: str | None,
        provider_item_id: str | None,
        connection_id: str | None,
        verify: Callable[[ConnectedInstitution], None],
    ) -> SyncDisconnectResult:
        assert institution == "Chase"
        assert provider_item_id is None
        assert connection_id is None
        verify(connection)
        return _disconnected(connection)

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
    # Affected account names are USER_NOTE, so the disconnect view is medium.
    assert envelope.summary.sensitivity == "medium"
    assert envelope.data.remote_revocation == "revoked"


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
        institution: str | None,
        provider_item_id: str | None,
        connection_id: str | None,
        verify: Callable[[ConnectedInstitution], None],
    ) -> SyncDisconnectResult:
        assert institution == "Chase"
        assert provider_item_id is None
        assert connection_id is None
        verify(changed)
        return _disconnected(changed)

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
@patch("moneybin.mcp.tools.sync._build_sync_service")
async def test_sync_disconnect_by_provider_item_id_threads_through_to_service(
    mock_build: MagicMock,
) -> None:
    """provider_item_id resolves and disconnects one exact item, not by name."""
    connection = ConnectedInstitution(
        id="conn_b",
        provider_item_id="item_b",
        provider="plaid",
        institution_name="Chase",
        status="active",
        created_at=datetime(2026, 4, 1, tzinfo=UTC),
    )
    service = MagicMock()
    service.plan_disconnect.return_value = connection

    def disconnect_confirmed(
        *,
        institution: str | None,
        provider_item_id: str | None,
        connection_id: str | None,
        verify: Callable[[ConnectedInstitution], None],
    ) -> SyncDisconnectResult:
        assert institution is None
        assert provider_item_id == "item_b"
        assert connection_id is None
        verify(connection)
        return _disconnected(connection)

    service.disconnect_confirmed.side_effect = disconnect_confirmed
    mock_build.return_value.__enter__.return_value = service
    from moneybin.mcp.tools.sync import sync_disconnect

    required = await sync_disconnect(provider_item_id="item_b")
    service.plan_disconnect.assert_called_once_with(
        institution=None, provider_item_id="item_b", connection_id=None
    )
    assert required.error is not None

    envelope = await sync_disconnect(
        provider_item_id="item_b",
        confirmation_token=str(required.error.details["confirmation_token"]),
    )
    assert envelope.error is None
    assert envelope.data.institution == "Chase"
    assert envelope.data.provider_item_id == "item_b"


@pytest.mark.unit
@patch("moneybin.mcp.tools.sync._build_sync_service")
async def test_sync_disconnect_keeps_nameless_institution_null(
    mock_build: MagicMock,
) -> None:
    """A nameless connection's institution stays null, never the raw id.

    Regression: the result used to fall back to provider_item_id for
    institution when institution_name was None.
    """
    connection = ConnectedInstitution(
        id="conn_c",
        provider_item_id="item_c",
        provider="plaid",
        institution_name=None,
        status="active",
        created_at=datetime(2026, 4, 1, tzinfo=UTC),
    )
    service = MagicMock()
    service.plan_disconnect.return_value = connection

    def disconnect_confirmed(
        *,
        institution: str | None,
        provider_item_id: str | None,
        connection_id: str | None,
        verify: Callable[[ConnectedInstitution], None],
    ) -> SyncDisconnectResult:
        assert institution is None
        assert provider_item_id == "item_c"
        assert connection_id is None
        verify(connection)
        return _disconnected(connection)

    service.disconnect_confirmed.side_effect = disconnect_confirmed
    mock_build.return_value.__enter__.return_value = service
    from moneybin.mcp.tools.sync import sync_disconnect

    required = await sync_disconnect(provider_item_id="item_c")
    assert required.error is not None

    envelope = await sync_disconnect(
        provider_item_id="item_c",
        confirmation_token=str(required.error.details["confirmation_token"]),
    )
    assert envelope.error is None
    assert envelope.data.institution is None
    assert envelope.data.provider_item_id == "item_c"


@pytest.mark.unit
@patch("moneybin.mcp.tools.sync._build_sync_service")
async def test_sync_disconnect_result_distinguishes_same_named_connections(
    mock_build: MagicMock,
) -> None:
    """Two same-named connections (e.g. after a relink) must be distinguishable.

    Both share institution="Chase"; only provider_item_id tells them apart.
    """
    item_a = ConnectedInstitution(
        id="conn_a",
        provider_item_id="item_a",
        provider="plaid",
        institution_name="Chase",
        status="active",
        created_at=datetime(2026, 3, 1, tzinfo=UTC),
    )
    item_b = item_a.model_copy(update={"id": "conn_b", "provider_item_id": "item_b"})
    service = MagicMock()

    async def _run(target: ConnectedInstitution):
        service.plan_disconnect.return_value = target

        def disconnect_confirmed(
            *,
            institution: str | None,
            provider_item_id: str | None,
            connection_id: str | None,
            verify: Callable[[ConnectedInstitution], None],
        ) -> SyncDisconnectResult:
            assert institution is None
            assert provider_item_id == target.provider_item_id
            assert connection_id is None
            verify(target)
            return _disconnected(target)

        service.disconnect_confirmed.side_effect = disconnect_confirmed
        mock_build.return_value.__enter__.return_value = service
        from moneybin.mcp.tools.sync import sync_disconnect

        required = await sync_disconnect(provider_item_id=target.provider_item_id)
        assert required.error is not None
        assert required.error.details is not None
        return await sync_disconnect(
            provider_item_id=target.provider_item_id,
            confirmation_token=str(required.error.details["confirmation_token"]),
        )

    envelope_a = await _run(item_a)
    envelope_b = await _run(item_b)

    assert envelope_a.data.institution == envelope_b.data.institution == "Chase"
    assert envelope_a.data.provider_item_id == "item_a"
    assert envelope_b.data.provider_item_id == "item_b"


@pytest.mark.unit
async def test_sync_disconnect_rejects_both_institution_and_provider_item_id() -> None:
    """Mirrors pull()'s mutual-exclusion guard shape.

    The MCP tool now rejects this before planning, so the service is never
    reached — the mock's side_effect stands ready as defense in depth but
    should not fire.
    """
    from moneybin.mcp.tools.sync import sync_disconnect

    with patch("moneybin.mcp.tools.sync._build_sync_service") as mock_build:
        service = MagicMock()
        service.plan_disconnect.side_effect = UserError(
            "institution and provider_item_id are mutually exclusive — pass exactly one",
            code=error_codes.MUTATION_INVALID_INPUT,
        )
        mock_build.return_value.__enter__.return_value = service
        envelope = await sync_disconnect(institution="Chase", provider_item_id="item_a")

    assert envelope.error is not None
    assert envelope.error.code == error_codes.MUTATION_INVALID_INPUT
    service.plan_disconnect.assert_not_called()
    service.disconnect_confirmed.assert_not_called()


@pytest.mark.unit
@patch("moneybin.mcp.tools.sync._build_sync_service")
async def test_sync_disconnect_both_selectors_retry_does_not_consume_token(
    mock_build: MagicMock,
) -> None:
    """A malformed both-selectors retry must not burn a valid confirmation token.

    Regression: previously the both-selectors case skipped straight to
    `grant_confirmation_or_raise` (consuming the single-use token) and only
    the service's own guard caught it afterward — leaving a corrected retry
    unable to reuse the token. The tool must reject before token consumption.
    """
    connection = ConnectedInstitution(
        id="conn_x",
        provider_item_id="item_x",
        provider="plaid",
        institution_name="Chase",
        status="active",
        created_at=datetime(2026, 4, 1, tzinfo=UTC),
    )
    service = MagicMock()
    service.plan_disconnect.return_value = connection

    def disconnect_confirmed(
        *,
        institution: str | None,
        provider_item_id: str | None,
        connection_id: str | None,
        verify: Callable[[ConnectedInstitution], None],
    ) -> SyncDisconnectResult:
        assert institution is None
        assert provider_item_id == "item_x"
        assert connection_id is None
        verify(connection)
        return _disconnected(connection)

    service.disconnect_confirmed.side_effect = disconnect_confirmed
    mock_build.return_value.__enter__.return_value = service
    from moneybin.mcp.tools.sync import sync_disconnect

    required = await sync_disconnect(provider_item_id="item_x")
    assert required.error is not None
    assert required.error.details is not None
    token = str(required.error.details["confirmation_token"])

    malformed = await sync_disconnect(
        institution="Chase",
        provider_item_id="item_x",
        confirmation_token=token,
    )
    assert malformed.error is not None
    assert malformed.error.code == error_codes.MUTATION_INVALID_INPUT
    service.disconnect_confirmed.assert_not_called()

    envelope = await sync_disconnect(
        provider_item_id="item_x",
        confirmation_token=token,
    )
    assert envelope.error is None
    assert envelope.data.institution == "Chase"
    assert envelope.data.provider_item_id == "item_x"
    service.disconnect_confirmed.assert_called_once()


@pytest.mark.unit
async def test_sync_disconnect_surfaces_unknown_provider_item_id_as_not_found() -> None:
    """An unknown provider_item_id classifies as mutation_not_found, not infra."""
    from moneybin.mcp.tools.sync import sync_disconnect

    with patch("moneybin.mcp.tools.sync._build_sync_service") as mock_build:
        service = MagicMock()
        service.plan_disconnect.side_effect = UserError(
            "no connected institution with provider_item_id 'item_missing' — "
            "run `moneybin sync status` to list connected banks",
            code=error_codes.MUTATION_NOT_FOUND,
        )
        mock_build.return_value.__enter__.return_value = service
        envelope = await sync_disconnect(provider_item_id="item_missing")

    assert envelope.error is not None
    assert envelope.error.code == error_codes.MUTATION_NOT_FOUND
    service.disconnect_confirmed.assert_not_called()


@pytest.mark.unit
async def test_sync_disconnect_requires_institution_or_provider_item_id() -> None:
    """Neither target given must refuse before any service call is made."""
    from moneybin.mcp.tools.sync import sync_disconnect

    with patch("moneybin.mcp.tools.sync._build_sync_service") as mock_build:
        service = MagicMock()
        mock_build.return_value.__enter__.return_value = service
        envelope = await sync_disconnect()

    assert envelope.error is not None
    assert envelope.error.code == "sync_institution_required"
    service.plan_disconnect.assert_not_called()


@pytest.mark.unit
def test_sync_disconnect_binding_and_message_name_the_specific_item() -> None:
    """The confirmation binding and message must distinguish same-named items.

    Two connections at one institution (a relink) must not collapse into one
    indistinguishable confirmation — see design-principles.md "Magic stays
    visible".
    """
    from moneybin.mcp.tools.sync import (
        _sync_disconnect_binding,  # pyright: ignore[reportPrivateUsage]
        _sync_disconnect_message,  # pyright: ignore[reportPrivateUsage]
    )

    item_a = ConnectedInstitution(
        id="conn_a",
        provider_item_id="item_a",
        provider="plaid",
        institution_name="Chase",
        status="active",
        created_at=datetime(2026, 1, 5, tzinfo=UTC),
    )
    item_b = item_a.model_copy(
        update={
            "id": "conn_b",
            "provider_item_id": "item_b",
            "created_at": datetime(2026, 6, 1, tzinfo=UTC),
        }
    )

    message_a = _sync_disconnect_message(item_a)
    message_b = _sync_disconnect_message(item_b)
    assert "item_a" in message_a
    assert "2026-01-05" in message_a
    assert "item_b" in message_b
    assert "2026-06-01" in message_b
    assert message_a != message_b

    binding_a = _sync_disconnect_binding(item_a)
    binding_b = _sync_disconnect_binding(item_b)
    assert binding_a.resolved_ids == ("conn_a", "item_a")
    assert binding_b.resolved_ids == ("conn_b", "item_b")
    assert binding_a.canonical_bytes() != binding_b.canonical_bytes()


@pytest.mark.unit
def test_sync_review_prompt_content_includes_required_elements() -> None:
    """The sync_review prompt must guide an agent through a sync health check."""
    from moneybin.mcp.prompts import sync_review

    text = sync_review()
    assert "sync_status" in text
    assert "reports(report_id='core:spending_trend'" in text
    assert "spending_summary" not in text
    assert "stale" in text.lower()
    assert "error" in text.lower()
    # Privacy guard: prompt must direct agent NOT to surface PII
    assert (
        "account numbers" in text.lower()
        or "no pii" in text.lower()
        or "do not include" in text.lower()
    )


@pytest.mark.unit
@patch("moneybin.mcp.tools.sync._build_sync_service")
async def test_sync_pull_offers_the_embedded_refresh_its_own_retries(
    mock_build: MagicMock,
) -> None:
    """A pull runs the whole refresh cascade, so it owes that cascade's remedies.

    The fields alone are not the remedy. MCP has no stderr warning to carry one
    the way the CLI does, so a step that crashed inside this pull reaches the
    agent as a payload string with nothing naming the re-run that fixes it.
    """
    service = MagicMock()
    service.pull.return_value = PullResult(
        job_id="j1",
        transactions_loaded=5,
        accounts_loaded=1,
        balances_loaded=1,
        transactions_removed=0,
        institutions=[],
        refresh_steps=RefreshStepOutcome(
            stages=(
                StageOutcome(step="match", ran=True, error="matcher blew up"),
                StageOutcome(step="rates", ran=True),
            ),
            rate_pairs_failed=("EUR/USD",),
        ),
    )
    mock_build.return_value.__enter__.return_value = service
    from moneybin.mcp.tools.sync import sync_pull

    envelope = sync_pull()

    steps = [
        action.arguments.get("steps") for action in (envelope.recovery_actions or [])
    ]
    assert ["match"] in steps
    assert ["rates"] in steps


@pytest.mark.unit
@patch("moneybin.mcp.tools.sync._build_sync_service")
async def test_sync_pull_names_the_manual_remedy_for_an_unpublished_pair(
    mock_build: MagicMock,
) -> None:
    """A pull reaches the same permanent gap and owes the same next step."""
    service = MagicMock()
    service.pull.return_value = PullResult(
        job_id="j1",
        transactions_loaded=5,
        accounts_loaded=1,
        balances_loaded=1,
        transactions_removed=0,
        institutions=[],
        refresh_steps=RefreshStepOutcome(rate_pairs_unsupported=("XBT/USD",)),
    )
    mock_build.return_value.__enter__.return_value = service
    from moneybin.mcp.tools.sync import sync_pull

    envelope = sync_pull()

    assert any("fx set" in action for action in envelope.actions)
    assert not envelope.recovery_actions, "the futile retry stays withheld"


@pytest.mark.unit
@patch("moneybin.mcp.tools.sync._build_sync_service")
async def test_sync_pull_withholds_retries_when_the_apply_failed(
    mock_build: MagicMock,
) -> None:
    """The apply is the blocker; a step retry would run against the same wreck."""
    service = MagicMock()
    service.pull.return_value = PullResult(
        job_id="j1",
        transactions_loaded=5,
        accounts_loaded=1,
        balances_loaded=1,
        transactions_removed=0,
        institutions=[],
        transforms_applied=False,
        transforms_error="sqlmesh apply blew up",
        refresh_steps=RefreshStepOutcome(
            stages=(StageOutcome(step="match", ran=True, error="matcher blew up"),)
        ),
    )
    mock_build.return_value.__enter__.return_value = service
    from moneybin.mcp.tools.sync import sync_pull

    assert not sync_pull().recovery_actions


def _purge_plan(*, connected: bool = True, rows: int = 3) -> SyncPurgePlan:
    return SyncPurgePlan(
        provider_item_id="item_a",
        connection=(
            ConnectedInstitution(
                id="conn_a",
                provider_item_id="item_a",
                provider="plaid",
                institution_name="Chase",
                status="active",
                created_at=datetime(2026, 3, 15, tzinfo=UTC),
            )
            if connected
            else None
        ),
        table_counts=(
            ("raw.plaid_accounts", 1),
            ("raw.plaid_transactions", rows),
            ("raw.security_prices", 0),
        ),
        account_link_ids=("l1", "l2"),
        decision_ids=("d1",),
    )


@pytest.mark.unit
@patch("moneybin.mcp.tools.sync._build_sync_service")
async def test_sync_disconnect_absent_confirms_then_purges(
    mock_build: MagicMock,
) -> None:
    """state='absent' is a two-call confirm whose confirmed call purges."""
    service = MagicMock()
    plan = _purge_plan()
    service.plan_purge.return_value = plan

    def purge_confirmed(**kwargs: object) -> object:
        assert kwargs["institution"] == "Chase"
        assert kwargs["provider_item_id"] is None
        assert kwargs["actor"] == "mcp"
        kwargs["verify"](plan)  # type: ignore[operator]
        return plan

    service.purge_confirmed.side_effect = purge_confirmed
    mock_build.return_value.__enter__.return_value = service
    from moneybin.mcp.tools.sync import sync_disconnect

    required = await sync_disconnect(institution="Chase", state="absent")

    assert required.error is not None
    assert required.error.code == "mutation_confirmation_required"
    assert required.error.details is not None
    service.purge_confirmed.assert_not_called()
    service.disconnect_confirmed.assert_not_called()

    envelope = await sync_disconnect(
        institution="Chase",
        state="absent",
        confirmation_token=str(required.error.details["confirmation_token"]),
    )

    service.purge_confirmed.assert_called_once()
    assert envelope.error is None
    assert envelope.data.kind == "institution"
    assert envelope.data.status == "purged"
    assert envelope.data.purged is True
    assert envelope.data.rows_deleted == 4
    assert envelope.data.account_links_reversed == 2
    assert envelope.data.institution == "Chase"
    assert envelope.data.provider_item_id == "item_a"
    # The purge rebuilds the models it fed, so nothing is left to refresh.
    assert not any("refresh_run" in action for action in envelope.actions)


@pytest.mark.unit
@patch("moneybin.mcp.tools.sync._build_sync_service")
async def test_sync_disconnect_absent_refuses_when_the_counts_change(
    mock_build: MagicMock,
) -> None:
    """A pull between preview and confirm changes the digest; the confirm is refused."""
    service = MagicMock()
    service.plan_purge.return_value = _purge_plan(rows=3)

    def purge_confirmed(**kwargs: object) -> object:
        kwargs["verify"](_purge_plan(rows=9))  # type: ignore[operator]
        raise AssertionError("verify must have refused the changed plan")

    service.purge_confirmed.side_effect = purge_confirmed
    mock_build.return_value.__enter__.return_value = service
    from moneybin.mcp.tools.sync import sync_disconnect

    required = await sync_disconnect(provider_item_id="item_a", state="absent")
    assert required.error is not None
    assert required.error.details is not None

    result = await sync_disconnect(
        provider_item_id="item_a",
        state="absent",
        confirmation_token=str(required.error.details["confirmation_token"]),
    )

    assert result.error is not None
    assert result.error.code == "mutation_confirmation_mismatch"


@pytest.mark.unit
@patch("moneybin.mcp.tools.sync._build_sync_service")
async def test_sync_disconnect_absent_reaches_an_item_the_server_no_longer_lists(
    mock_build: MagicMock,
) -> None:
    service = MagicMock()
    plan = _purge_plan(connected=False)
    service.plan_purge.return_value = plan

    def purge_confirmed(**kwargs: object) -> object:
        kwargs["verify"](plan)  # type: ignore[operator]
        return plan

    service.purge_confirmed.side_effect = purge_confirmed
    mock_build.return_value.__enter__.return_value = service
    from moneybin.mcp.tools.sync import sync_disconnect

    required = await sync_disconnect(provider_item_id="item_a", state="absent")
    assert required.error is not None
    assert required.error.details is not None
    assert required.error.details["operation_kind"] == "sync_disconnect_absent"
    blast_radius = required.error.details["blast_radius"]
    assert isinstance(blast_radius, dict)
    assert blast_radius["institutions"] == 0

    envelope = await sync_disconnect(
        provider_item_id="item_a",
        state="absent",
        confirmation_token=str(required.error.details["confirmation_token"]),
    )

    assert envelope.error is None
    assert envelope.data.status == "purged"
    assert envelope.data.institution is None


@pytest.mark.unit
def test_sync_purge_binding_and_message_quote_the_exact_item_and_counts() -> None:
    from moneybin.mcp.tools.sync import (
        _sync_purge_binding,  # pyright: ignore[reportPrivateUsage]
        _sync_purge_message,  # pyright: ignore[reportPrivateUsage]
    )

    plan = _purge_plan()
    binding = _sync_purge_binding(plan)

    assert binding.operation_kind == "sync_disconnect_absent"
    assert binding.resolved_ids == ("item_a",)
    assert binding.arguments["state"] == "absent"
    assert binding.arguments["table_counts"] == [
        ["raw.plaid_accounts", 1],
        ["raw.plaid_transactions", 3],
        ["raw.security_prices", 0],
    ]
    assert binding.blast_radius["total_rows"] == 4
    assert binding.blast_radius["institutions"] == 1
    assert binding.blast_radius["account_links_reversed"] == 2
    assert binding.blast_radius["proposals_withdrawn"] == 1
    message = _sync_purge_message(plan)
    assert "Chase" in message
    assert "item_a" in message
    assert "4 locally pulled rows" in message
    assert "2 account links" in message
    assert _sync_purge_binding(_purge_plan(rows=9)) != binding
    orphan = _sync_purge_message(_purge_plan(connected=False))
    assert "no longer lists" in orphan
    assert "item_a" in orphan


@pytest.mark.unit
@patch("moneybin.mcp.tools.sync._build_sync_service")
async def test_sync_purge_connection_selector_reaches_both_phases(
    mock_build: MagicMock,
) -> None:
    from moneybin.mcp.tools.sync import sync_disconnect

    plan = _purge_plan()
    service = MagicMock()

    def plan_purge(**selectors: object) -> SyncPurgePlan:
        if selectors.get("connection_id") != "conn_a":
            raise ValueError("Missing connection selector")
        return plan

    def purge_confirmed(**selectors: object) -> SyncPurgePlan:
        selected = plan_purge(**selectors)
        selectors["verify"](selected)  # type: ignore[operator]
        return selected

    service.plan_purge.side_effect = plan_purge
    service.purge_confirmed.side_effect = purge_confirmed
    mock_build.return_value.__enter__.return_value = service
    preview = await sync_disconnect(connection_id="conn_a", state="absent")
    assert preview.error is not None
    assert preview.error.code == "mutation_confirmation_required"
    assert preview.error.details is not None
    result = await sync_disconnect(
        connection_id="conn_a",
        state="absent",
        confirmation_token=str(preview.error.details["confirmation_token"]),
    )
    assert result.error is None
    assert result.data.purged is True


@pytest.mark.unit
@pytest.mark.parametrize("changed", ["generation", "account_identity"])
@patch("moneybin.mcp.tools.sync._build_sync_service")
async def test_sync_purge_refuses_changed_grant_scope(
    mock_build: MagicMock, changed: str
) -> None:
    from dataclasses import replace

    from moneybin.connectors.sync_models import SyncAccountScope
    from moneybin.mcp.tools.sync import sync_disconnect

    plan = _purge_plan()
    assert plan.connection is not None
    account = SyncAccountScope.model_validate({
        "account_id": "acct_a",
        "name": "Checking",
        "native_scope": {"kind": "plaid_item", "provider_item_id": "item_a"},
    })
    plan = replace(
        plan, connection=plan.connection.model_copy(update={"accounts": [account]})
    )
    assert plan.connection is not None
    update = (
        {"credential_generation": 2}
        if changed == "generation"
        else {"accounts": [account.model_copy(update={"account_id": "acct_b"})]}
    )
    live = replace(plan, connection=plan.connection.model_copy(update=update))
    service = MagicMock()
    service.plan_purge.return_value = plan

    def purge_confirmed(**kwargs: object) -> SyncPurgePlan:
        kwargs["verify"](live)  # type: ignore[operator]
        return live

    service.purge_confirmed.side_effect = purge_confirmed
    mock_build.return_value.__enter__.return_value = service
    preview = await sync_disconnect(provider_item_id="item_a", state="absent")
    assert preview.error is not None
    assert preview.error.details is not None
    result = await sync_disconnect(
        provider_item_id="item_a",
        state="absent",
        confirmation_token=str(preview.error.details["confirmation_token"]),
    )
    assert result.error is not None
    assert result.error.code == "mutation_confirmation_mismatch"


@pytest.mark.unit
@patch("moneybin.mcp.tools.sync._build_sync_service")
@pytest.mark.parametrize(
    "change",
    [
        "duplicate_added",
        "duplicate_removed",
        "labels_order",
        "account_id",
        "native_scope",
        "generation",
    ],
)
async def test_sync_purge_confirmation_uses_native_identity_set(
    mock_build: MagicMock, change: str
) -> None:
    from dataclasses import replace

    from moneybin.connectors.sync_models import SyncAccountScope
    from moneybin.mcp.tools.sync import sync_disconnect

    plan = _purge_plan()
    assert plan.connection is not None
    first = SyncAccountScope.model_validate({
        "account_id": "acct_a",
        "name": "Checking",
        "native_scope": {"kind": "plaid_item", "provider_item_id": "item_a"},
    })
    second = first.model_copy(update={"account_id": "acct_b"})
    preview_accounts = [first, second]
    live_accounts = [second.model_copy(update={"name": "Renamed"}), first]
    if change == "duplicate_removed":
        preview_accounts.append(first.model_copy(update={"name": "Duplicate label"}))
    elif change == "duplicate_added":
        live_accounts.append(first.model_copy(update={"name": "Duplicate label"}))
    elif change == "account_id":
        live_accounts[1] = first.model_copy(update={"account_id": "acct_changed"})
    elif change == "native_scope":
        live_accounts[1] = SyncAccountScope.model_validate({
            **first.model_dump(),
            "native_scope": {"kind": "plaid_item", "provider_item_id": "item_changed"},
        })
    plan = replace(
        plan,
        connection=plan.connection.model_copy(update={"accounts": preview_accounts}),
    )
    assert plan.connection is not None
    live = replace(
        plan,
        connection=plan.connection.model_copy(
            update={
                "accounts": live_accounts,
                "credential_generation": (
                    2
                    if change == "generation"
                    else plan.connection.credential_generation
                ),
            }
        ),
    )
    applied: list[SyncPurgePlan] = []
    service = MagicMock()
    service.plan_purge.return_value = plan

    def purge_confirmed(**kwargs: object) -> SyncPurgePlan:
        kwargs["verify"](live)  # type: ignore[operator]  # exercise the real MCP confirmation grant
        applied.append(live)
        return live

    service.purge_confirmed.side_effect = purge_confirmed
    mock_build.return_value.__enter__.return_value = service
    preview = await sync_disconnect(provider_item_id="item_a", state="absent")
    assert preview.error is not None
    assert preview.error.code == "mutation_confirmation_required"
    assert preview.error.details is not None
    assert applied == []
    result = await sync_disconnect(
        provider_item_id="item_a",
        state="absent",
        confirmation_token=str(preview.error.details["confirmation_token"]),
    )
    if change in {"account_id", "native_scope", "generation"}:
        assert result.error is not None
        assert result.error.code == "mutation_confirmation_mismatch"
        assert applied == []
    else:
        assert result.error is None
        assert result.data.purged is True
        assert applied == [live]
    service.disconnect_confirmed.assert_not_called()


@pytest.mark.unit
@patch("moneybin.mcp.tools.sync._build_sync_service")
async def test_sync_purge_reports_actual_broker_outcome(mock_build: MagicMock) -> None:
    from dataclasses import replace

    from moneybin.connectors.sync_models import SyncDisconnectResponse
    from moneybin.mcp.tools.sync import sync_disconnect

    plan = _purge_plan()
    outcome = SyncDisconnectResponse.model_validate({
        "connection_id": "00000000-0000-0000-0000-000000000001",
        "credential_generation": 1,
        "accounts": [
            {
                "account_id": "acct_a",
                "name": "Checking",
                "native_scope": {"kind": "plaid_item", "provider_item_id": "item_a"},
            }
        ],
        "remote_revocation": "user_action_required",
    })
    service = MagicMock()
    service.plan_purge.return_value = plan

    def purge_confirmed(**kwargs: object) -> SyncPurgePlan:
        kwargs["verify"](plan)  # type: ignore[operator]
        return replace(plan, remote_result=outcome)

    service.purge_confirmed.side_effect = purge_confirmed
    mock_build.return_value.__enter__.return_value = service
    preview = await sync_disconnect(provider_item_id="item_a", state="absent")
    assert preview.error is not None
    assert preview.error.details is not None
    result = await sync_disconnect(
        provider_item_id="item_a",
        state="absent",
        confirmation_token=str(preview.error.details["confirmation_token"]),
    )
    assert result.error is None
    assert result.data.remote_revocation == "user_action_required"
    assert result.data.affected_accounts == ["Checking"]
    assert result.data.connection_id == "00000000-0000-0000-0000-000000000001"


@pytest.mark.unit
@patch("moneybin.mcp.tools.sync._build_sync_service")
async def test_disconnect_token_cannot_authorize_purge(mock_build: MagicMock) -> None:
    from moneybin.mcp.tools.sync import sync_disconnect

    plan = _purge_plan()
    service = MagicMock()
    service.plan_disconnect.return_value = plan.connection

    def purge_confirmed(**kwargs: object) -> SyncPurgePlan:
        kwargs["verify"](plan)  # type: ignore[operator]
        raise AssertionError("Plain disconnect token must not authorize a purge")

    service.purge_confirmed.side_effect = purge_confirmed
    mock_build.return_value.__enter__.return_value = service
    preview = await sync_disconnect(provider_item_id="item_a")
    assert preview.error is not None
    assert preview.error.details is not None
    result = await sync_disconnect(
        provider_item_id="item_a",
        state="absent",
        confirmation_token=str(preview.error.details["confirmation_token"]),
    )
    assert result.error is not None
    assert result.error.code == "mutation_confirmation_mismatch"
