"""Unit tests for gsheet_* MCP tools (envelope shape + service delegation).

The OAuth + connection services are mocked at the `_build_*` boundary inside
``moneybin.mcp.tools.gsheet``; these tests verify the tool layer (typed payload
shape, actions[] hints, derived sensitivity tiers) without exercising the real
Google Sheets API.
"""

from __future__ import annotations

from dataclasses import replace
from unittest.mock import MagicMock, patch

import pytest
from fastmcp import FastMCP

import moneybin.mcp.tools.gsheet as gsheet_module
from moneybin.connectors.gsheet.adapters.base import (
    DetectionResult,
    GSheetConnection,
    LoadResult,
)
from moneybin.connectors.gsheet.connection_service import ConnectResult
from moneybin.connectors.gsheet.pull_service import PullResult
from moneybin.mcp.tools.gsheet import (
    gsheet_coarse,
    register_gsheet_coarse_reads,
    register_gsheet_tools,
)


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
async def test_register_gsheet_coarse_reads_is_dormant_and_isolated() -> None:
    srv = FastMCP("test")
    register_gsheet_coarse_reads(srv)

    names = {t.name for t in await srv._list_tools()}  # noqa: SLF001  # pyright: ignore[reportPrivateUsage]

    assert names == {"gsheet"}


@pytest.mark.unit
async def test_register_gsheet_workflow_tools_excludes_fragmented_aliases() -> None:
    srv = FastMCP("test")
    gsheet_module.register_gsheet_workflow_tools(srv)

    names = {t.name for t in await srv._list_tools()}  # noqa: SLF001  # pyright: ignore[reportPrivateUsage]

    assert names == {"gsheet", "gsheet_connect", "gsheet_pull", "gsheet_disconnect"}
    assert "gsheet_auth" not in names
    assert "gsheet_status" not in names
    assert "gsheet_reconnect" not in names


@pytest.mark.unit
async def test_gsheet_workflow_schemas_are_strict_and_target_state_based() -> None:
    srv = FastMCP("test")
    gsheet_module.register_gsheet_workflow_tools(srv)
    tools = {t.name: t for t in await srv._list_tools()}  # noqa: SLF001  # pyright: ignore[reportPrivateUsage]

    connect = tools["gsheet_connect"]
    disconnect = tools["gsheet_disconnect"]

    assert set(connect.parameters["properties"]) == {
        "url",
        "connection_id",
        "force_reauth",
        "adapter",
        "alias",
        "account_name",
        "account_id",
        "column_mapping",
        "sign",
        "confirm_mapping",
        "accept_seed_fallback",
        "no_initial_pull",
    }
    for field in (
        "force_reauth",
        "confirm_mapping",
        "accept_seed_fallback",
        "no_initial_pull",
    ):
        assert connect.parameters["properties"][field]["type"] == "boolean"
    assert disconnect.parameters["required"] == ["connection_id"]
    assert set(disconnect.parameters["properties"]["state"]["enum"]) == {
        "disconnected",
        "absent",
    }
    assert "confirmation_token" in disconnect.parameters["properties"]
    assert connect.output_schema is None
    assert disconnect.output_schema is None


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


@pytest.mark.unit
@patch("moneybin.mcp.tools.gsheet._build_connection_service")
async def test_gsheet_coarse_connections_is_default_collection(
    mock_build: MagicMock,
) -> None:
    service = MagicMock()
    service.list_connections.return_value = [_make_connection()]
    mock_build.return_value.__enter__.return_value = service

    envelope = await gsheet_coarse()

    assert envelope.data.kind == "connections"
    assert [row.connection_id for row in envelope.data.connections] == ["conn_abc"]
    service.list_connections.assert_called_once_with()
    service.get.assert_not_called()


@pytest.mark.unit
@patch("moneybin.mcp.tools.gsheet._build_connection_service")
async def test_gsheet_coarse_status_supports_global_summary(
    mock_build: MagicMock,
) -> None:
    service = MagicMock()
    service.list_connections.return_value = [
        _make_connection(connection_id="conn_a"),
        _make_connection(connection_id="conn_b"),
    ]
    mock_build.return_value.__enter__.return_value = service

    envelope = await gsheet_coarse(view="status")

    assert envelope.data.kind == "status"
    assert [row.connection_id for row in envelope.data.connections] == [
        "conn_a",
        "conn_b",
    ]


@pytest.mark.unit
@patch("moneybin.mcp.tools.gsheet._build_connection_service")
async def test_gsheet_coarse_status_supports_one_connection(
    mock_build: MagicMock,
) -> None:
    service = MagicMock()
    service.get.return_value = _make_connection(connection_id="conn_a")
    mock_build.return_value.__enter__.return_value = service

    envelope = await gsheet_coarse(view="status", connection_id="conn_a")

    assert envelope.data.kind == "status"
    assert [row.connection_id for row in envelope.data.connections] == ["conn_a"]
    service.get.assert_called_once_with("conn_a")
    service.list_connections.assert_not_called()


@pytest.mark.unit
async def test_gsheet_coarse_connections_rejects_connection_id() -> None:
    envelope = await gsheet_coarse(
        view="connections",
        connection_id="secret-connection-id",
    )

    assert envelope.error is not None
    assert envelope.error.code == "GSHEET_CONNECTION_ID_NOT_ALLOWED"
    assert "secret-connection-id" not in envelope.error.message


@pytest.mark.unit
@patch("moneybin.mcp.tools.gsheet._build_connection_service")
async def test_gsheet_coarse_unknown_status_id_is_sanitized(
    mock_build: MagicMock,
) -> None:
    service = MagicMock()
    service.get.return_value = None
    mock_build.return_value.__enter__.return_value = service

    envelope = await gsheet_coarse(
        view="status",
        connection_id="secret-connection-id",
    )

    assert envelope.error is not None
    assert envelope.error.code == "infra_not_found"
    assert "secret-connection-id" not in envelope.error.message


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
    service.reconnect.assert_called_once_with(
        "conn_abc", yes=True, sign=None, actor="mcp"
    )


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
    service.reconnect.assert_called_once_with(
        "conn_abc", yes=False, sign=None, actor="mcp"
    )


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
