"""Unit tests for gsheet_* MCP tools (envelope shape + service delegation).

The OAuth + connection services are mocked at the `_build_*` boundary inside
``moneybin.mcp.tools.gsheet``; these tests verify the tool layer (typed payload
shape, actions[] hints, derived sensitivity tiers) without exercising the real
Google Sheets API.
"""

from __future__ import annotations

import asyncio
import json
import shlex
from dataclasses import replace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastmcp import FastMCP

from moneybin.connectors.gsheet.adapters.base import (
    DetectionResult,
    GSheetConnection,
    LoadResult,
)
from moneybin.connectors.gsheet.connection_service import ConnectResult
from moneybin.connectors.gsheet.errors import GSheetSignConfirmationRequiredError
from moneybin.connectors.gsheet.pull_service import PullResult
from moneybin.errors import UserError
from moneybin.mcp.tools.gsheet import register_gsheet_tools


def _make_connection(
    *,
    connection_id: str = "conn_abc",
    status: str = "healthy",
    adapter: str = "transactions",
    last_status_reason: str | None = None,
) -> GSheetConnection:
    return GSheetConnection(
        connection_id=connection_id,
        spreadsheet_id="sheet_123",
        sheet_gid=0,
        sheet_name="Transactions",
        workbook_name="Budget 2026",
        adapter=adapter,
        alias=None,
        account_id=None,
        account_name="Chase Checking",
        column_mapping={"Date": "date", "Amount": "amount"},
        header_signature=["Date", "Amount"],
        date_format="%Y-%m-%d",
        sign_convention="negative_expense",
        number_format="plain",
        skip_rows=0,
        skip_trailing_patterns=[],
        status=status,
        last_pull_at="2026-05-20T00:00:00+00:00",
        last_pull_import_id="imp_1",
        last_success_at=(
            None if status == "drift_detected" else "2026-05-20T00:00:00+00:00"
        ),
        last_status_reason=last_status_reason,
        consecutive_failure_count=0 if status == "healthy" else 1,
    )


def _make_detection() -> DetectionResult:
    return DetectionResult(
        confidence="high",
        column_mapping={"Date": "date", "Amount": "amount"},
        header_signature=["Date", "Amount"],
        date_format="%Y-%m-%d",
        sign_convention="negative_expense",
        number_format="plain",
        notes=["auto-detected"],
    )


def _make_load_result() -> LoadResult:
    return LoadResult(rows_inserted=10, rows_upserted=2, rows_soft_deleted=1)


# ---------------------------------------------------------------------------
# Registration
# ---------------------------------------------------------------------------


_EXPECTED_GSHEET_TOOLS = {
    "gsheet_auth",
    "gsheet",
    "gsheet_connect",
    "gsheet_pull",
    "gsheet_status",
    "gsheet_reconnect",
    "gsheet_disconnect",
}


@pytest.mark.unit
async def test_register_gsheet_tools_registers_expected_tools() -> None:
    """All seven gsheet_* MCP tools (including gsheet_auth) register."""
    srv = FastMCP("test")
    register_gsheet_tools(srv)
    names = {t.name for t in await srv._list_tools()}  # noqa: SLF001  # pyright: ignore[reportPrivateUsage]
    assert _EXPECTED_GSHEET_TOOLS <= names


@pytest.mark.unit
async def test_gsheet_write_tool_schemas_hide_sign_confirmation_inputs() -> None:
    """Only a human elicitation can confirm sign; the agent-facing schema cannot."""
    srv = FastMCP("test")
    register_gsheet_tools(srv)
    tools = {t.name: t for t in await srv._list_tools()}  # noqa: SLF001  # pyright: ignore[reportPrivateUsage]

    assert set(tools["gsheet_connect"].parameters["properties"]) == {
        "url",
        "adapter",
        "alias",
        "account_name",
        "account_id",
        "column_mapping",
        "yes",
        "accept_seed_fallback",
        "no_initial_pull",
    }
    assert set(tools["gsheet_reconnect"].parameters["properties"]) == {
        "connection_id",
        "yes",
    }


@pytest.mark.unit
def test_gsheet_write_tools_allow_human_confirmation_timeout() -> None:
    """Both sign-gated write tools allow the same 180s human decision window."""
    from moneybin.mcp.tools.gsheet import gsheet_connect, gsheet_reconnect

    assert gsheet_connect._mcp_timeout_seconds == 180.0  # type: ignore[attr-defined]
    assert gsheet_reconnect._mcp_timeout_seconds == 180.0  # type: ignore[attr-defined]


# ---------------------------------------------------------------------------
# gsheet_auth — status string only → Tier.LOW
# ---------------------------------------------------------------------------


@pytest.mark.unit
@patch("moneybin.mcp.tools.gsheet._build_oauth_client")
async def test_gsheet_auth_short_circuits_when_already_authorized(
    mock_build: MagicMock,
) -> None:
    """Already-authorized + no force → no browser flow, returns short-circuit envelope."""
    oauth = MagicMock()
    oauth.is_authorized.return_value = True
    mock_build.return_value = oauth

    from moneybin.mcp.tools.gsheet import gsheet_auth

    envelope = await gsheet_auth()
    assert envelope.summary.sensitivity == "low"
    assert envelope.data.status == "already_authorized"
    oauth.authorize.assert_not_called()


@pytest.mark.unit
@patch("moneybin.mcp.tools.gsheet._build_oauth_client")
async def test_gsheet_auth_runs_authorize_when_not_authorized(
    mock_build: MagicMock,
) -> None:
    """Not-yet-authorized → authorize() runs, returns 'authorized' envelope."""
    oauth = MagicMock()
    oauth.is_authorized.return_value = False
    mock_build.return_value = oauth

    from moneybin.mcp.tools.gsheet import gsheet_auth

    envelope = await gsheet_auth()
    assert envelope.summary.sensitivity == "low"
    assert envelope.data.status == "authorized"
    oauth.authorize.assert_called_once()


@pytest.mark.unit
@patch("moneybin.mcp.tools.gsheet._build_oauth_client")
async def test_gsheet_auth_force_reauth_runs_authorize_even_when_authorized(
    mock_build: MagicMock,
) -> None:
    """force_reauth=True bypasses the short-circuit even when a token exists."""
    oauth = MagicMock()
    oauth.is_authorized.return_value = True
    mock_build.return_value = oauth

    from moneybin.mcp.tools.gsheet import gsheet_auth

    envelope = await gsheet_auth(force_reauth=True)
    assert envelope.data.status == "authorized"
    oauth.authorize.assert_called_once()


# ---------------------------------------------------------------------------
# gsheet_connect — connection row's highest class is DESCRIPTION → Tier.MEDIUM
# ---------------------------------------------------------------------------


@pytest.mark.unit
@patch("moneybin.mcp.tools.gsheet._build_connection_service")
async def test_gsheet_connect_returns_envelope_with_connection(
    mock_build: MagicMock,
) -> None:
    """gsheet_connect returns a medium-sensitivity envelope with connection details."""
    service = MagicMock()
    service.connect.return_value = ConnectResult(
        connection=_make_connection(),
        detection=_make_detection(),
        initial_pull=_make_load_result(),
    )
    mock_build.return_value.__enter__.return_value = service

    from moneybin.mcp.tools.gsheet import gsheet_connect

    envelope = await gsheet_connect(
        url="https://docs.google.com/spreadsheets/d/abc/edit#gid=0"
    )
    # account_id is RECORD_ID (spec D6); last_status_reason / column_mapping are
    # DESCRIPTION (MEDIUM) → tool derives medium tier.
    assert envelope.summary.sensitivity == "medium"
    assert envelope.data.connection.connection_id == "conn_abc"
    assert envelope.data.initial_pull is not None
    assert envelope.data.initial_pull.rows_inserted == 10
    # Agent should see how to pull again and check status next
    assert any("gsheet_pull" in a for a in envelope.actions)


@pytest.mark.unit
@patch(
    "moneybin.mcp.elicitation.confirm_or_raise",
    new_callable=AsyncMock,
)
@patch("moneybin.mcp.tools.gsheet._build_connection_service")
async def test_gsheet_connect_elicits_human_then_retries_sign_internally(
    mock_build: MagicMock,
    mock_confirm: AsyncMock,
) -> None:
    """An inferred inversion retries once only after the human confirms."""
    from moneybin.cli.commands.gsheet import (
        _parse_column_mapping,  # pyright: ignore[reportPrivateUsage]  # verifies fallback compatibility with the real CLI parser
    )

    evidence_header = "Card Purchases (+)"
    service = MagicMock()
    service.connect.side_effect = [
        GSheetSignConfirmationRequiredError(
            proposed_convention="negative_is_income",
            evidence_header=evidence_header,
        ),
        ConnectResult(
            connection=_make_connection(),
            detection=_make_detection(),
            initial_pull=_make_load_result(),
        ),
    ]
    mock_build.return_value.__enter__.return_value = service

    from moneybin.mcp.tools.gsheet import gsheet_connect

    url = "https://docs.google.com/spreadsheets/d/sheet name/edit#gid=7"
    column_mapping = {
        "Merchant's Name": "description",
        "Amount, USD": "amount",
    }
    envelope = await gsheet_connect(
        url=url,
        adapter="transactions",
        alias="card feed",
        account_name="Owner's Card",
        account_id="acct card",
        column_mapping=column_mapping,
        yes=True,
        accept_seed_fallback=True,
        no_initial_pull=True,
    )

    assert envelope.data.connection.connection_id == "conn_abc"
    assert service.connect.call_count == 2
    first_request = service.connect.call_args_list[0].args[0]
    retry_request = service.connect.call_args_list[1].args[0]
    assert first_request.human_sign_confirmation is False
    assert retry_request == replace(first_request, human_sign_confirmation=True)
    assert first_request.sign is None
    assert retry_request.sign is None
    assert "human_sign_confirmation" not in repr(envelope.to_dict())
    assert mock_confirm.await_args is not None
    message = mock_confirm.await_args.args[0]
    assert evidence_header in message
    assert (
        "This would invert every transaction amount: charges become expenses "
        "and payments become credits."
    ) in message
    cli_equivalent = mock_confirm.await_args.kwargs["cli_equivalent"]
    cli_tokens = shlex.split(cli_equivalent)
    assert cli_tokens[:4] == ["moneybin", "gsheet", "connect", url]
    assert cli_tokens[-2:] == ["--sign", "negative_is_income"]
    assert cli_tokens[cli_tokens.index("--adapter") + 1] == "transactions"
    assert cli_tokens[cli_tokens.index("--alias") + 1] == "card feed"
    assert cli_tokens[cli_tokens.index("--account-name") + 1] == "Owner's Card"
    assert cli_tokens[cli_tokens.index("--account-id") + 1] == "acct card"
    serialized_mapping = cli_tokens[cli_tokens.index("--column-mapping") + 1]
    assert json.loads(serialized_mapping) == column_mapping
    assert _parse_column_mapping(serialized_mapping) == column_mapping
    assert "--yes" in cli_tokens
    assert "--accept-seed-fallback" in cli_tokens
    assert "--no-initial-pull" in cli_tokens
    assert "human_sign_confirmation" not in cli_equivalent


@pytest.mark.unit
@patch(
    "moneybin.mcp.elicitation.confirm_or_raise",
    new_callable=AsyncMock,
)
@patch("moneybin.mcp.tools.gsheet._build_connection_service")
async def test_gsheet_connect_split_debit_override_uses_private_human_retry(
    mock_build: MagicMock,
    mock_confirm: AsyncMock,
) -> None:
    """The public source→destination override survives the one-shot retry."""
    service = MagicMock()
    service.connect.side_effect = [
        GSheetSignConfirmationRequiredError(
            proposed_convention="negative_is_income",
            evidence_header="Debit",
        ),
        ConnectResult(
            connection=_make_connection(),
            detection=_make_detection(),
            initial_pull=_make_load_result(),
        ),
    ]
    mock_build.return_value.__enter__.return_value = service

    from moneybin.mcp.tools.gsheet import gsheet_connect

    await gsheet_connect(
        url="https://docs.google.com/spreadsheets/d/card/edit#gid=0",
        adapter="transactions",
        account_id="acct_card",
        column_mapping={"Debit": "amount"},
        yes=True,
    )

    first_request = service.connect.call_args_list[0].args[0]
    retry_request = service.connect.call_args_list[1].args[0]
    assert first_request.column_mapping == {"Debit": "amount"}
    assert retry_request == replace(first_request, human_sign_confirmation=True)
    assert mock_confirm.await_args is not None
    assert "Debit" in mock_confirm.await_args.args[0]
    assert mock_confirm.await_args.kwargs["cli_equivalent"].endswith(
        "--sign negative_is_income"
    )
    assert (
        "human_sign_confirmation"
        not in mock_confirm.await_args.kwargs["cli_equivalent"]
    )


@pytest.mark.unit
@patch(
    "moneybin.mcp.elicitation.confirm_or_raise",
    new_callable=AsyncMock,
)
@patch("moneybin.mcp.tools.gsheet._build_connection_service")
async def test_gsheet_connect_yes_does_not_self_confirm_sign(
    mock_build: MagicMock,
    mock_confirm: AsyncMock,
) -> None:
    """Yes accepts a mapping only; a rejected sign elicitation cannot retry."""
    service = MagicMock()
    service.connect.side_effect = GSheetSignConfirmationRequiredError(
        proposed_convention="negative_is_income",
        evidence_header="Amount",
    )
    mock_build.return_value.__enter__.return_value = service
    mock_confirm.side_effect = UserError(
        "Sign confirmation declined.",
        code="mutation_confirmation_required",
    )

    from moneybin.mcp.tools.gsheet import gsheet_connect

    envelope = await gsheet_connect(
        url="https://docs.google.com/spreadsheets/d/abc/edit#gid=0",
        yes=True,
    )

    assert envelope.error is not None
    assert service.connect.call_count == 1
    request = service.connect.call_args.args[0]
    assert request.yes is True
    assert request.human_sign_confirmation is False
    mock_confirm.assert_awaited_once()


# ---------------------------------------------------------------------------
# gsheet_pull — counts + error_message → Tier.MEDIUM
# ---------------------------------------------------------------------------


@pytest.mark.unit
@patch("moneybin.mcp.tools.gsheet._build_pull_service")
async def test_gsheet_pull_returns_status_per_connection(
    mock_build: MagicMock,
) -> None:
    """gsheet_pull with no id returns per-connection results in data.pulls."""
    service = MagicMock()
    service.pull_all_healthy.return_value = [
        PullResult(
            connection_id="conn_a",
            status="complete",
            load_result=_make_load_result(),
        ),
        PullResult(
            connection_id="conn_b",
            status="drift_detected",
            drift_reason="Header reworded: 'Amount' -> 'Amt'",
        ),
    ]
    mock_build.return_value.__enter__.return_value = service

    from moneybin.mcp.tools.gsheet import gsheet_pull

    envelope = await gsheet_pull()
    assert envelope.summary.sensitivity == "medium"
    pulls = envelope.data.pulls
    assert {p.connection_id for p in pulls} == {"conn_a", "conn_b"}
    by_id = {p.connection_id: p for p in pulls}
    assert by_id["conn_a"].status == "complete"
    assert by_id["conn_a"].rows_inserted == 10
    assert by_id["conn_b"].status == "drift_detected"
    # Drift on conn_b must surface a reconnect hint to the agent.
    assert any("gsheet_reconnect" in a and "conn_b" in a for a in envelope.actions)


@pytest.mark.unit
@patch("moneybin.mcp.tools.gsheet._build_pull_service")
async def test_gsheet_pull_single_connection(mock_build: MagicMock) -> None:
    """gsheet_pull(connection_id=...) delegates to pull_connection."""
    service = MagicMock()
    service.pull_connection.return_value = PullResult(
        connection_id="conn_a",
        status="complete",
        load_result=_make_load_result(),
    )
    mock_build.return_value.__enter__.return_value = service

    from moneybin.mcp.tools.gsheet import gsheet_pull

    envelope = await gsheet_pull(connection_id="conn_a")
    service.pull_connection.assert_called_once_with("conn_a")
    service.pull_all_healthy.assert_not_called()
    pulls = envelope.data.pulls
    assert len(pulls) == 1
    assert pulls[0].connection_id == "conn_a"


# ---------------------------------------------------------------------------
# gsheet (collection) — connection rows' highest class is DESCRIPTION → Tier.MEDIUM
# ---------------------------------------------------------------------------


@pytest.mark.unit
@patch("moneybin.mcp.tools.gsheet._build_connection_service")
async def test_gsheet_collection_returns_actions_on_drift(
    mock_build: MagicMock,
) -> None:
    """The gsheet noun-only collection read surfaces reconnect hints for drift."""
    healthy = _make_connection(connection_id="conn_ok", status="healthy")
    drifted = _make_connection(
        connection_id="conn_drift",
        status="drift_detected",
        last_status_reason="Header reworded",
    )
    service = MagicMock()
    service.list_connections.return_value = [healthy, drifted]
    mock_build.return_value.__enter__.return_value = service

    from moneybin.mcp.tools.gsheet import gsheet

    envelope = await gsheet()
    # account_id is RECORD_ID (spec D6); last_status_reason is DESCRIPTION → medium tier.
    assert envelope.summary.sensitivity == "medium"
    rows = envelope.data.connections
    assert len(rows) == 2
    # Only the drifted connection should have a reconnect hint.
    assert any("gsheet_reconnect" in a and "conn_drift" in a for a in envelope.actions)
    assert not any("conn_ok" in a for a in envelope.actions)


# ---------------------------------------------------------------------------
# gsheet_status — connection rows' highest class is DESCRIPTION → Tier.MEDIUM
# ---------------------------------------------------------------------------


@pytest.mark.unit
@patch("moneybin.mcp.tools.gsheet._build_connection_service")
async def test_gsheet_status_single_connection(mock_build: MagicMock) -> None:
    """gsheet_status(connection_id=...) returns the single connection."""
    conn = _make_connection()
    service = MagicMock()
    service.get.return_value = conn
    mock_build.return_value.__enter__.return_value = service

    from moneybin.mcp.tools.gsheet import gsheet_status

    envelope = await gsheet_status(connection_id="conn_abc")
    assert envelope.summary.sensitivity == "medium"
    rows = envelope.data.connections
    assert rows[0].connection_id == "conn_abc"
    service.get.assert_called_once_with("conn_abc")


@pytest.mark.unit
@patch("moneybin.mcp.tools.gsheet._build_connection_service")
async def test_gsheet_status_unknown_connection_returns_error(
    mock_build: MagicMock,
) -> None:
    """gsheet_status on an unknown id returns an error envelope (does not raise)."""
    service = MagicMock()
    service.get.return_value = None
    mock_build.return_value.__enter__.return_value = service

    from moneybin.mcp.tools.gsheet import gsheet_status

    envelope = await gsheet_status(connection_id="bogus")
    parsed = envelope.to_dict()
    assert parsed["status"] == "error"
    assert parsed["error"]["code"] == "infra_not_found"
    assert parsed["data"] == []


@pytest.mark.unit
@patch("moneybin.mcp.tools.gsheet._build_connection_service")
async def test_gsheet_status_surfaces_drift_hint(mock_build: MagicMock) -> None:
    """gsheet_status surfaces gsheet_reconnect hint for drift_detected rows."""
    drifted = _make_connection(
        connection_id="conn_drift",
        status="drift_detected",
        last_status_reason="Header reworded",
    )
    service = MagicMock()
    service.get.return_value = drifted
    mock_build.return_value.__enter__.return_value = service

    from moneybin.mcp.tools.gsheet import gsheet_status

    envelope = await gsheet_status(connection_id="conn_drift")
    assert any("gsheet_reconnect" in a and "conn_drift" in a for a in envelope.actions)


# ---------------------------------------------------------------------------
# gsheet_reconnect — connection row's highest class is DESCRIPTION → Tier.MEDIUM
# ---------------------------------------------------------------------------


@pytest.mark.unit
@patch("moneybin.mcp.tools.gsheet._build_connection_service")
async def test_gsheet_reconnect_returns_envelope(mock_build: MagicMock) -> None:
    """gsheet_reconnect returns a medium-sensitivity envelope with new detection."""
    healed = replace(_make_connection(), status="healthy")
    service = MagicMock()
    service.reconnect.return_value = ConnectResult(
        connection=healed,
        detection=_make_detection(),
        initial_pull=_make_load_result(),
    )
    mock_build.return_value.__enter__.return_value = service

    from moneybin.mcp.tools.gsheet import gsheet_reconnect

    envelope = await gsheet_reconnect(connection_id="conn_abc", yes=True)
    assert envelope.summary.sensitivity == "medium"
    assert envelope.data.connection.status == "healthy"
    service.reconnect.assert_called_once_with("conn_abc", yes=True, actor="mcp")


@pytest.mark.unit
@patch("moneybin.mcp.tools.gsheet._build_connection_service")
async def test_gsheet_reconnect_passes_yes_flag_through(
    mock_build: MagicMock,
) -> None:
    """Yes parameter must reach the service layer for medium-confidence remaps."""
    service = MagicMock()
    service.reconnect.return_value = ConnectResult(
        connection=_make_connection(),
        detection=_make_detection(),
        initial_pull=None,
    )
    mock_build.return_value.__enter__.return_value = service

    from moneybin.mcp.tools.gsheet import gsheet_reconnect

    await gsheet_reconnect(connection_id="conn_abc")  # default yes=False
    service.reconnect.assert_called_once_with("conn_abc", yes=False, actor="mcp")


@pytest.mark.unit
@patch(
    "moneybin.mcp.elicitation.confirm_or_raise",
    new_callable=AsyncMock,
)
@patch("moneybin.mcp.tools.gsheet._build_connection_service")
async def test_gsheet_reconnect_elicits_human_then_retries_sign_internally(
    mock_build: MagicMock,
    mock_confirm: AsyncMock,
) -> None:
    """Reconnect retries once with a private flag after human confirmation."""
    evidence_header = "Debit/Credit Signal"
    healed = replace(_make_connection(), status="healthy")
    service = MagicMock()
    service.reconnect.side_effect = [
        GSheetSignConfirmationRequiredError(
            proposed_convention="negative_is_income",
            evidence_header=evidence_header,
        ),
        ConnectResult(
            connection=healed,
            detection=_make_detection(),
            initial_pull=_make_load_result(),
        ),
    ]
    mock_build.return_value.__enter__.return_value = service

    from moneybin.mcp.tools.gsheet import gsheet_reconnect

    envelope = await gsheet_reconnect(connection_id="conn_abc", yes=True)

    assert envelope.data.connection.status == "healthy"
    assert "human_sign_confirmation" not in repr(envelope.to_dict())
    assert service.reconnect.call_args_list == [
        (
            ("conn_abc",),
            {"yes": True, "actor": "mcp"},
        ),
        (
            ("conn_abc",),
            {
                "yes": True,
                "human_sign_confirmation": True,
                "actor": "mcp",
            },
        ),
    ]
    assert mock_confirm.await_args is not None
    message = mock_confirm.await_args.args[0]
    assert evidence_header in message
    assert (
        "This would invert every transaction amount: charges become expenses "
        "and payments become credits."
    ) in message
    assert mock_confirm.await_args.kwargs["cli_equivalent"] == (
        "moneybin gsheet reconnect conn_abc --yes --sign negative_is_income"
    )


@pytest.mark.unit
@patch(
    "moneybin.mcp.elicitation.confirm_or_raise",
    new_callable=AsyncMock,
)
@patch("moneybin.mcp.tools.gsheet._build_connection_service")
async def test_gsheet_reconnect_timeout_does_not_retry(
    mock_build: MagicMock,
    mock_confirm: AsyncMock,
) -> None:
    """A timed-out human confirmation leaves reconnect at its first attempt."""
    service = MagicMock()
    service.reconnect.side_effect = GSheetSignConfirmationRequiredError(
        proposed_convention="negative_is_income",
        evidence_header="Amount",
    )
    mock_build.return_value.__enter__.return_value = service
    mock_confirm.side_effect = asyncio.TimeoutError

    from moneybin.mcp.tools.gsheet import gsheet_reconnect

    with pytest.raises(asyncio.TimeoutError):
        await gsheet_reconnect(connection_id="conn_abc")

    service.reconnect.assert_called_once_with("conn_abc", yes=False, actor="mcp")


# ---------------------------------------------------------------------------
# gsheet_disconnect — ids + status only → Tier.LOW
# ---------------------------------------------------------------------------


@pytest.mark.unit
@patch("moneybin.mcp.tools.gsheet._build_connection_service")
async def test_gsheet_disconnect_with_purge_param(mock_build: MagicMock) -> None:
    """gsheet_disconnect(connection_id, purge=True) reports purged=True."""
    service = MagicMock()
    mock_build.return_value.__enter__.return_value = service

    from moneybin.mcp.tools.gsheet import gsheet_disconnect

    envelope = await gsheet_disconnect(connection_id="conn_abc", purge=True)
    service.disconnect.assert_called_once_with("conn_abc", purge=True, actor="mcp")
    assert envelope.summary.sensitivity == "low"
    assert envelope.data.purged is True
    assert envelope.data.status == "purged"


@pytest.mark.unit
@patch("moneybin.mcp.tools.gsheet._build_connection_service")
async def test_gsheet_disconnect_soft_default(mock_build: MagicMock) -> None:
    """gsheet_disconnect defaults to soft-disconnect."""
    service = MagicMock()
    mock_build.return_value.__enter__.return_value = service

    from moneybin.mcp.tools.gsheet import gsheet_disconnect

    envelope = await gsheet_disconnect(connection_id="conn_abc")
    service.disconnect.assert_called_once_with("conn_abc", purge=False, actor="mcp")
    assert envelope.data.purged is False
    assert envelope.data.status == "disconnected"


@pytest.mark.unit
@patch("moneybin.mcp.tools.gsheet._build_connection_service")
async def test_gsheet_disconnect_unknown_connection_returns_error(
    mock_build: MagicMock,
) -> None:
    """gsheet_disconnect on a non-existent id with purge=True returns error envelope."""
    service = MagicMock()
    service.disconnect.side_effect = ValueError("Unknown connection: bogus")
    mock_build.return_value.__enter__.return_value = service

    from moneybin.mcp.tools.gsheet import gsheet_disconnect

    envelope = await gsheet_disconnect(connection_id="bogus", purge=True)
    parsed = envelope.to_dict()
    # ValueError → UserError(code='infra_invalid_input') via classify_user_error.
    assert parsed["status"] == "error"
    assert parsed["error"]["code"] == "infra_invalid_input"
