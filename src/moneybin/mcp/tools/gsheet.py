"""gsheet_* MCP tools — Google Sheets connector.

User-controlled-storage `connect-*` family per
`.claude/rules/surface-design.md` verb vocabulary. Mirrors the CLI subgroup
1:1: `gsheet_auth` runs the OAuth installed-app flow inside the local MCP
server process (opens the browser, listens on a 127.0.0.1 callback) so
agents can drive end-to-end onboarding without dropping the user to a
terminal. The MCP server is local-only today (per mcp-server.md
"Connection Model"); a hosted-MCP variant would need a return-URL shape
instead, but that's outside the launch trigger.

Connections that surface in drift_detected status carry an `actions[]` hint
pointing at `gsheet_reconnect(connection_id=...)` so an agent inspecting
status sees the recovery path without a second tool call.
"""

from __future__ import annotations

import logging
from typing import Any

from fastmcp import FastMCP

from moneybin.connectors.gsheet.service_factory import (
    build_connection_service as _build_connection_service,
)
from moneybin.connectors.gsheet.service_factory import (
    build_oauth_client as _build_oauth_client,
)
from moneybin.connectors.gsheet.service_factory import (
    build_pull_service as _build_pull_service,
)
from moneybin.error_codes import INFRA_NOT_FOUND
from moneybin.errors import UserError
from moneybin.mcp._registration import register
from moneybin.mcp.decorator import mcp_tool
from moneybin.protocol.envelope import (
    ResponseEnvelope,
    build_envelope,
    build_error_envelope,
)

logger = logging.getLogger(__name__)


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


@mcp_tool(
    sensitivity="medium",
    read_only=False,
    idempotent=False,
    open_world=True,
    # Interactive OAuth: user has to click Allow in their browser. Give the
    # full flow generous headroom — default 30s timeout would routinely
    # fire before a human completes the consent screen.
    timeout_seconds=180.0,
)
def gsheet_auth(force_reauth: bool = False) -> ResponseEnvelope:
    """Authenticate with Google Sheets via OAuth 2.0 PKCE (installed-app flow).

    Opens the Google consent screen in the user's browser and listens on a
    127.0.0.1 loopback port for the callback. Tokens are persisted to the
    system keychain via SecretStore — they never flow through the MCP wire
    or the LLM context window. Read-only `spreadsheets.readonly` scope only;
    Drive is not requested.

    Returns immediately with ``status='already_authorized'`` when a refresh
    token is already on file unless ``force_reauth=True`` is passed.

    Mutation surface: writes the OAuth refresh + access tokens to
    SecretStore. Reverse by visiting
    https://myaccount.google.com/permissions and revoking access, then
    re-running this tool.
    """
    oauth = _build_oauth_client()
    if oauth.is_authorized() and not force_reauth:
        return build_envelope(
            data={"status": "already_authorized"},
            sensitivity="medium",
            actions=[
                "Run gsheet_connect(url='https://docs.google.com/spreadsheets/...') "
                "to bind a sheet."
            ],
        )
    oauth.authorize()
    return build_envelope(
        data={"status": "authorized"},
        sensitivity="medium",
        actions=[
            "Run gsheet_connect(url='https://docs.google.com/spreadsheets/...') "
            "to bind a sheet."
        ],
    )


@mcp_tool(
    sensitivity="medium",
    read_only=False,
    idempotent=False,
    open_world=True,
    # First-run before gsheet_auth() lands tokens, connect() will trigger
    # the OAuth installed-app flow itself. Same 180s cap as gsheet_auth so
    # the user has headroom to click Allow without the default 30s firing.
    timeout_seconds=180.0,
)
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
    gsheet_disconnect(connection_id, purge=True). Requires prior gsheet_auth
    (MCP) or `moneybin gsheet auth` (CLI) — both drive the same in-process
    OAuth installed-app flow.
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
        result = service.connect(req, actor="mcp")

    data: dict[str, Any] = {
        "connection": result.connection.to_dict(),
        "detection": {
            "confidence": result.detection.confidence,
            "column_mapping": result.detection.column_mapping,
            "notes": result.detection.notes,
        },
        "initial_pull": (
            {
                "status": result.initial_pull_status,
                "rows_inserted": result.initial_pull.rows_inserted,
                "rows_upserted": result.initial_pull.rows_upserted,
                "rows_soft_deleted": result.initial_pull.rows_soft_deleted,
            }
            if result.initial_pull is not None
            # Pull ran but produced no rows (e.g. drift_detected,
            # auth_expired) — surface the failure status + reason so
            # agents distinguish this from --no-initial-pull.
            else (
                {
                    "status": result.initial_pull_status,
                    "error": result.initial_pull_error,
                }
                if result.initial_pull_status is not None
                else None
            )
        ),
    }
    actions = [
        f"Run gsheet_pull(connection_id='{result.connection.connection_id}') to refresh.",
        "Use gsheet_status to check connection health going forward.",
    ]
    if result.initial_pull_status not in (None, "complete"):
        actions.insert(
            0,
            f"Initial pull returned status={result.initial_pull_status!r}; "
            "run gsheet_status for detail before assuming data is loaded.",
        )
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
                "Re-authenticate: call gsheet_auth() (MCP) or run "
                "`moneybin gsheet auth` (CLI). Both drive the same "
                "in-process OAuth flow."
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
    data = [c.to_dict() for c in connections]
    return build_envelope(
        data=data,
        sensitivity="low",
        actions=_drift_hints(data),
    )


@mcp_tool(sensitivity="low")
def gsheet_status(connection_id: str | None = None) -> ResponseEnvelope:
    """Show status for one connection, or a summary of all of them.

    Returns the connection row(s) with status, last_pull_at, last_success_at,
    last_status_reason, and consecutive_failure_count. Drift-detected connections
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
                        code=INFRA_NOT_FOUND,
                        hint="Run gsheet (collection read) to list known connection_ids.",
                    ),
                )
            connections = [single]
    data = [c.to_dict() for c in connections]
    return build_envelope(
        data=data,
        sensitivity="low",
        actions=_drift_hints(data),
    )


@mcp_tool(sensitivity="medium", read_only=False, idempotent=False, open_world=True)
def gsheet_reconnect(connection_id: str, yes: bool = False) -> ResponseEnvelope:
    """Re-detect the sheet structure, re-pin the mapping, and run a pull.

    Use after the source sheet changes shape (column added, header reworded)
    and a connection surfaces in drift_detected status. Amounts in loaded data
    follow MoneyBin's accounting convention: negative = expense, positive = income.

    Pass yes=True to accept a medium-confidence column-mapping remap. Without
    it, an ambiguous remap raises AmbiguousDetectionError so the agent can
    confirm with the user before silently re-pinning the wrong mapping.

    Mutation surface: updates app.gsheet_connections.column_mapping +
    header_signature (audited) and writes raw rows via the pull side-effect.
    Revert: there is no revert — the connection re-binds to whatever the
    sheet currently looks like. Run gsheet_status afterwards to verify.
    """
    with _build_connection_service() as service:
        result = service.reconnect(connection_id, yes=yes, actor="mcp")

    data = {
        "connection": result.connection.to_dict(),
        "detection": {
            "confidence": result.detection.confidence,
            "column_mapping": result.detection.column_mapping,
        },
        "initial_pull": (
            {
                "status": result.initial_pull_status,
                "rows_inserted": result.initial_pull.rows_inserted,
                "rows_upserted": result.initial_pull.rows_upserted,
                "rows_soft_deleted": result.initial_pull.rows_soft_deleted,
            }
            if result.initial_pull is not None
            else (
                {
                    "status": result.initial_pull_status,
                    "error": result.initial_pull_error,
                }
                if result.initial_pull_status is not None
                else None
            )
        ),
    }
    return build_envelope(
        data=data,
        sensitivity="medium",
        actions=["Run gsheet_status to verify the connection is healthy."],
    )


@mcp_tool(
    sensitivity="medium",
    read_only=False,
    idempotent=False,
    destructive=True,
    open_world=True,
)
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
        service.disconnect(connection_id, purge=purge, actor="mcp")
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
            gsheet_auth,
            "Authenticate with Google Sheets via OAuth PKCE — opens a browser, "
            "completes consent, and stores tokens in the system keychain. "
            "Tokens never enter the MCP wire or LLM context. Call this once "
            "before gsheet_connect; subsequent calls short-circuit unless "
            "force_reauth=True.",
        ),
        (
            gsheet_connect,
            "Bind a Google Sheet to MoneyBin via direct OAuth (user-controlled "
            "storage). Detects column mapping, persists app.gsheet_connections, "
            "and runs the initial pull by default. Amounts use MoneyBin convention "
            "(negative = expense). Run gsheet_auth first if not yet authorized — "
            "this matters for timeout: the OAuth browser consent and the initial "
            "pull share one call window, so for a first-time/large-sheet connect, "
            "authorizing separately leaves the full window for the pull (or set "
            "no_initial_pull=True and pull afterward with gsheet_pull).",
        ),
        (
            gsheet_pull,
            "Pull one Google Sheets connection by ID, or every healthy "
            "connection. Returns per-connection status (complete / drift_detected / "
            "auth_expired / unreachable / rate_limited / failed). Drift-detected "
            "connections surface a gsheet_reconnect hint in actions[]. "
            "Mutation surface: writes raw.tabular_transactions (transactions "
            "adapter) or raw.gsheet_seeds (seed adapter) and soft-deletes rows "
            "no longer in the sheet; no revert — the next pull re-derives state "
            "from the live sheet.",
        ),
        (
            gsheet,
            "List every Google Sheets connection — id, adapter, status, "
            "last-pull/last-success timestamps. Drift-detected connections "
            "surface a gsheet_reconnect hint in actions[]. (The bare name, not "
            "gsheet_list, is intentional: it reads as the gsheet domain's "
            "default collection view per the noun-only read convention.)",
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
