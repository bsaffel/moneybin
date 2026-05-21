"""gsheet_* MCP tools — Google Sheets connector.

User-controlled-storage `connect-*` family per
`.claude/rules/surface-design.md` verb vocabulary. Mirrors the CLI subgroup
1:1 in functional shape; `gsheet_auth` is intentionally CLI-only because
the OAuth installed-app flow opens a browser, which has no MCP equivalent.

Connections that surface in drift_detected status carry an `actions[]` hint
pointing at `gsheet_reconnect(connection_id=...)` so an agent inspecting
status sees the recovery path without a second tool call.
"""

from __future__ import annotations

import logging
from collections.abc import Generator
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any

from fastmcp import FastMCP

from moneybin.errors import UserError
from moneybin.mcp._registration import register
from moneybin.mcp.decorator import mcp_tool
from moneybin.protocol.envelope import (
    ResponseEnvelope,
    build_envelope,
    build_error_envelope,
)

if TYPE_CHECKING:
    from moneybin.connectors.gsheet.adapters.base import GSheetConnection
    from moneybin.connectors.gsheet.connection_service import GSheetConnectionService
    from moneybin.connectors.gsheet.pull_service import GSheetPullService

logger = logging.getLogger(__name__)


def _build_oauth_client() -> Any:
    """Construct a GoogleOAuthClient from current settings."""
    from moneybin.config import get_settings  # noqa: PLC0415
    from moneybin.connectors.gsheet.oauth_client import (  # noqa: PLC0415
        GoogleOAuthClient,
    )
    from moneybin.secrets import SecretStore  # noqa: PLC0415

    return GoogleOAuthClient(secrets=SecretStore(), settings=get_settings())


@contextmanager
def _build_connection_service() -> Generator[GSheetConnectionService, None, None]:
    """Yield a GSheetConnectionService with an active Database connection."""
    from moneybin.connectors.gsheet.connection_service import (  # noqa: PLC0415
        GSheetConnectionService,
    )
    from moneybin.connectors.gsheet.sheets_api import SheetsClient  # noqa: PLC0415
    from moneybin.database import get_database  # noqa: PLC0415

    oauth_client = _build_oauth_client()
    sheets_client = SheetsClient(oauth=oauth_client)
    with get_database(read_only=False) as db:
        yield GSheetConnectionService(
            db=db, sheets_client=sheets_client, oauth_client=oauth_client
        )


@contextmanager
def _build_pull_service() -> Generator[GSheetPullService, None, None]:
    """Yield a GSheetPullService with an active Database connection."""
    from moneybin.connectors.gsheet.pull_service import (  # noqa: PLC0415
        GSheetPullService,
    )
    from moneybin.connectors.gsheet.sheets_api import SheetsClient  # noqa: PLC0415
    from moneybin.database import get_database  # noqa: PLC0415

    oauth_client = _build_oauth_client()
    sheets_client = SheetsClient(oauth=oauth_client)
    with get_database(read_only=False) as db:
        yield GSheetPullService(
            db=db, sheets_client=sheets_client, oauth_client=oauth_client
        )


def _connection_to_dict(conn: GSheetConnection) -> dict[str, Any]:
    """Serialize a GSheetConnection dataclass for envelope output."""
    return {
        "connection_id": conn.connection_id,
        "spreadsheet_id": conn.spreadsheet_id,
        "sheet_gid": conn.sheet_gid,
        "sheet_name": conn.sheet_name,
        "workbook_name": conn.workbook_name,
        "adapter": conn.adapter,
        "alias": conn.alias,
        "account_id": conn.account_id,
        "account_name": conn.account_name,
        "status": conn.status,
        "last_pull_at": conn.last_pull_at,
        "last_success_at": conn.last_success_at,
        "last_drift_reason": conn.last_drift_reason,
        "consecutive_failure_count": conn.consecutive_failure_count,
    }


def _reconnect_hint(connection_id: str) -> str:
    """Build the actions[] hint that points an agent at the reconnect path."""
    return (
        f"Run gsheet_reconnect(connection_id='{connection_id}') to re-detect "
        "the sheet structure and re-pin the column mapping."
    )


def _drift_hints(connections: list[dict[str, Any]]) -> list[str]:
    """Append a reconnect hint per drifted connection."""
    return [
        _reconnect_hint(c["connection_id"])
        for c in connections
        if c.get("status") == "drift_detected"
    ]


@mcp_tool(sensitivity="medium", read_only=False, idempotent=False, open_world=True)
def gsheet_connect(
    url: str,
    adapter: str | None = None,
    alias: str | None = None,
    account_name: str | None = None,
    account_id: str | None = None,
    column_mapping: dict[str, str] | None = None,
    yes: bool = False,
    accept_seed_fallback: bool = False,
    no_initial_pull: bool = False,
) -> ResponseEnvelope:
    """Bind a Google Sheet to MoneyBin via direct OAuth (user-controlled storage).

    Detects sheet structure, persists the column mapping + header
    signature in app.gsheet_connections, and (by default) runs the
    initial pull. Use adapter='seed' with alias=<slug> to land arbitrary
    tabular data into raw.gsheet_<alias>.

    Amounts loaded via the transactions adapter follow MoneyBin's accounting
    convention: negative = expense, positive = income. The URL must include
    `#gid=N` so the tab is unambiguous.

    Mutation surface: writes app.gsheet_connections (audited). Revert with
    gsheet_disconnect(connection_id, purge=True). Requires prior CLI
    `moneybin gsheet auth` (the OAuth flow opens a browser and has no MCP
    equivalent).
    """
    from moneybin.connectors.gsheet.connection_service import (  # noqa: PLC0415
        ConnectionRequest,
    )

    req = ConnectionRequest(
        url=url,
        adapter=adapter,
        alias=alias,
        account_name=account_name,
        account_id=account_id,
        column_mapping=column_mapping,
        yes=yes,
        no_initial_pull=no_initial_pull,
        accept_seed_fallback=accept_seed_fallback,
    )
    with _build_connection_service() as service:
        result = service.connect(req)

    data: dict[str, Any] = {
        "connection": _connection_to_dict(result.connection),
        "detection": {
            "confidence": result.detection.confidence,
            "column_mapping": result.detection.column_mapping,
            "notes": result.detection.notes,
        },
        "initial_pull": (
            {
                "rows_inserted": result.initial_pull.rows_inserted,
                "rows_upserted": result.initial_pull.rows_upserted,
                "rows_soft_deleted": result.initial_pull.rows_soft_deleted,
            }
            if result.initial_pull is not None
            else None
        ),
    }
    actions = [
        f"Run gsheet_pull(connection_id='{result.connection.connection_id}') to refresh.",
        "Use gsheet_status to check connection health going forward.",
    ]
    return build_envelope(data=data, sensitivity="medium", actions=actions)


@mcp_tool(sensitivity="medium", read_only=False, idempotent=False, open_world=True)
def gsheet_pull(connection_id: str | None = None) -> ResponseEnvelope:
    """Pull one Google Sheets connection by ID, or every healthy connection.

    Amounts in loaded transactions data follow MoneyBin's accounting convention:
    negative = expense, positive = income. Drift-detected connections surface in
    the response with status='drift_detected' and a reconnect hint in actions[].

    Mutation surface: writes raw.tabular_transactions / raw.gsheet_seeds and
    updates app.gsheet_connections counters (audited). No paired undo — the
    sheet is the source of truth and a subsequent pull reconciles.
    """
    with _build_pull_service() as service:
        if connection_id is None:
            results = service.pull_all_healthy()
        else:
            results = [service.pull_connection(connection_id)]

    pulls = [
        {
            "connection_id": r.connection_id,
            "status": r.status,
            "rows_inserted": r.load_result.rows_inserted if r.load_result else 0,
            "rows_upserted": r.load_result.rows_upserted if r.load_result else 0,
            "rows_soft_deleted": (
                r.load_result.rows_soft_deleted if r.load_result else 0
            ),
            "drift_reason": r.drift_reason,
            "error_message": r.error_message,
        }
        for r in results
    ]
    actions: list[str] = []
    for r in results:
        if r.status == "drift_detected":
            actions.append(_reconnect_hint(r.connection_id))
        elif r.status == "auth_expired":
            actions.append(
                "Re-authenticate with the CLI: `moneybin gsheet auth` "
                "(OAuth flow opens a browser; not available via MCP)."
            )
    return build_envelope(
        data={"pulls": pulls},
        sensitivity="medium",
        actions=actions,
    )


@mcp_tool(sensitivity="low")
def gsheet() -> ResponseEnvelope:
    """List every Google Sheets connection.

    Shape 5 collection read. Each connection that surfaces in drift_detected
    status carries a paired gsheet_reconnect hint in actions[].
    """
    with _build_connection_service() as service:
        connections = service.list_connections()
    data = [_connection_to_dict(c) for c in connections]
    return build_envelope(
        data=data,
        sensitivity="low",
        actions=_drift_hints(data),
    )


@mcp_tool(sensitivity="low")
def gsheet_status(connection_id: str | None = None) -> ResponseEnvelope:
    """Show status for one connection, or a summary of all of them.

    Returns the connection row(s) with status, last_pull_at, last_success_at,
    last_drift_reason, and consecutive_failure_count. Drift-detected connections
    carry a paired gsheet_reconnect hint in actions[].
    """
    with _build_connection_service() as service:
        if connection_id is None:
            connections = service.list_connections()
        else:
            single = service.get(connection_id)
            if single is None:
                return build_error_envelope(
                    error=UserError(
                        f"Unknown gsheet connection: {connection_id}",
                        code="not_found",
                        hint="Run gsheet (collection read) to list known connection_ids.",
                    ),
                )
            connections = [single]
    data = [_connection_to_dict(c) for c in connections]
    return build_envelope(
        data=data,
        sensitivity="low",
        actions=_drift_hints(data),
    )


@mcp_tool(sensitivity="medium", read_only=False, idempotent=False, open_world=True)
def gsheet_reconnect(connection_id: str) -> ResponseEnvelope:
    """Re-detect the sheet structure, re-pin the mapping, and run a pull.

    Use after the source sheet changes shape (column added, header reworded)
    and a connection surfaces in drift_detected status. Amounts in loaded data
    follow MoneyBin's accounting convention: negative = expense, positive = income.

    Mutation surface: updates app.gsheet_connections.column_mapping +
    header_signature (audited) and writes raw rows via the pull side-effect.
    Revert: there is no revert — the connection re-binds to whatever the
    sheet currently looks like. Run gsheet_status afterwards to verify.
    """
    with _build_connection_service() as service:
        result = service.reconnect(connection_id)

    data = {
        "connection": _connection_to_dict(result.connection),
        "detection": {
            "confidence": result.detection.confidence,
            "column_mapping": result.detection.column_mapping,
        },
        "initial_pull": (
            {
                "rows_inserted": result.initial_pull.rows_inserted,
                "rows_upserted": result.initial_pull.rows_upserted,
                "rows_soft_deleted": result.initial_pull.rows_soft_deleted,
            }
            if result.initial_pull is not None
            else None
        ),
    }
    return build_envelope(
        data=data,
        sensitivity="medium",
        actions=["Run gsheet_status to verify the connection is healthy."],
    )


@mcp_tool(sensitivity="medium", read_only=False, destructive=True, open_world=True)
def gsheet_disconnect(connection_id: str, purge: bool = False) -> ResponseEnvelope:
    """Soft-disconnect (default) or purge a Google Sheets connection.

    Without purge: sets status='disconnected', retains raw rows for analytics.
    With purge=True: hard-deletes the connection row, drops the seed view
    (if any), and deletes raw rows. Purge is permanent — no revert path.

    Mutation surface: writes app.gsheet_connections.status (audited). With
    purge=True also deletes raw.gsheet_seeds rows (seed adapter) or
    raw.tabular_transactions rows (transactions adapter) for this connection.
    """
    with _build_connection_service() as service:
        service.disconnect(connection_id, purge=purge)
    return build_envelope(
        data={
            "connection_id": connection_id,
            "status": "purged" if purge else "disconnected",
            "purged": purge,
        },
        sensitivity="medium",
    )


def register_gsheet_tools(mcp: FastMCP) -> None:
    """Register all gsheet_* namespace tools with the FastMCP server."""
    for fn, desc in [
        (
            gsheet_connect,
            "Bind a Google Sheet to MoneyBin via direct OAuth (user-controlled "
            "storage). Detects column mapping, persists app.gsheet_connections, "
            "and runs the initial pull by default. Amounts use MoneyBin convention "
            "(negative = expense). Requires prior CLI `moneybin gsheet auth`.",
        ),
        (
            gsheet_pull,
            "Pull one Google Sheets connection by ID, or every healthy "
            "connection. Returns per-connection status (complete / drift_detected / "
            "auth_expired / unreachable / rate_limited / failed). Drift-detected "
            "connections surface a gsheet_reconnect hint in actions[].",
        ),
        (
            gsheet,
            "List every Google Sheets connection — id, adapter, status, "
            "last-pull/last-success timestamps. Drift-detected connections "
            "surface a gsheet_reconnect hint in actions[].",
        ),
        (
            gsheet_status,
            "Show status for one connection by ID, or a full summary. "
            "Drift-detected connections surface a gsheet_reconnect hint in actions[].",
        ),
        (
            gsheet_reconnect,
            "Re-detect sheet structure, re-pin column mapping, and run a pull. "
            "Use after drift_detected. No revert — re-binds to whatever the "
            "sheet currently looks like.",
        ),
        (
            gsheet_disconnect,
            "Soft-disconnect (default) or purge a connection. Purge=True is "
            "permanent: drops seed view (if any), deletes raw rows, hard-deletes "
            "the connection row. Soft-disconnect retains raw rows for analytics.",
        ),
    ]:
        register(mcp, fn, fn.__name__, desc)
