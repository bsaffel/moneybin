"""gsheet_* MCP tools — Google Sheets connector.

User-controlled-storage `connect-*` family per
`.claude/rules/surface-design.md` verb vocabulary. Mirrors the CLI subgroup
1:1: `gsheet_auth` runs the OAuth installed-app flow inside the local MCP
server process (opens the browser, listens on a 127.0.0.1 callback) so
agents can drive end-to-end onboarding without dropping the user to a
terminal. The MCP server is local-only today (per mcp.md
"Connection Model"); a hosted-MCP variant would need a return-URL shape
instead, but that's outside the launch trigger.

Connections that surface in drift_detected status carry an `actions[]` hint
pointing at `gsheet_reconnect(connection_id=...)` so an agent inspecting
status sees the recovery path without a second tool call.
"""

from __future__ import annotations

import asyncio
import json
import logging
import shlex
from dataclasses import replace
from typing import TYPE_CHECKING

from fastmcp import FastMCP

from moneybin.connectors.gsheet.adapters.base import GSheetConnection
from moneybin.connectors.gsheet.errors import GSheetSignConfirmationRequiredError
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
from moneybin.privacy.payloads.gsheet import (
    GsheetAuthPayload,
    GsheetConnectionRow,
    GsheetConnectionsPayload,
    GsheetConnectPayload,
    GsheetDetection,
    GsheetDisconnectPayload,
    GsheetInitialPull,
    GsheetPullPayload,
    GsheetPullRow,
)
from moneybin.protocol.envelope import (
    ResponseEnvelope,
    build_envelope,
    build_error_envelope,
)

if TYPE_CHECKING:
    from moneybin.connectors.gsheet.connection_service import (
        ConnectionRequest,
        ConnectResult,
    )

logger = logging.getLogger(__name__)


def _initial_pull(result: ConnectResult) -> GsheetInitialPull | None:
    """Build the typed initial-pull sub-object from a connect/reconnect result.

    Populated rows on success; status+error on a failed pull; None when no
    pull ran (``no_initial_pull``).
    """
    if result.initial_pull is not None:
        return GsheetInitialPull(
            status=result.initial_pull_status,
            rows_inserted=result.initial_pull.rows_inserted,
            rows_upserted=result.initial_pull.rows_upserted,
            rows_soft_deleted=result.initial_pull.rows_soft_deleted,
        )
    # Pull ran but produced no rows (e.g. drift_detected, auth_expired) —
    # surface the failure status + reason so agents distinguish this from
    # --no-initial-pull (which leaves initial_pull None entirely).
    if result.initial_pull_status is not None:
        return GsheetInitialPull(
            status=result.initial_pull_status,
            error=result.initial_pull_error,
        )
    return None


def _connection_row(conn: GSheetConnection) -> GsheetConnectionRow:
    """Build the typed connection row from a GSheetConnection (mirrors to_dict())."""
    return GsheetConnectionRow(
        connection_id=conn.connection_id,
        spreadsheet_id=conn.spreadsheet_id,
        sheet_gid=conn.sheet_gid,
        sheet_name=conn.sheet_name,
        workbook_name=conn.workbook_name,
        adapter=conn.adapter,
        alias=conn.alias,
        account_id=conn.account_id,
        account_name=conn.account_name,
        status=conn.status,
        last_pull_at=conn.last_pull_at,
        last_success_at=conn.last_success_at,
        last_status_reason=conn.last_status_reason,
        consecutive_failure_count=conn.consecutive_failure_count,
    )


def _reconnect_hint(connection_id: str) -> str:
    """Build the actions[] hint that points an agent at the reconnect path."""
    return (
        f"Run gsheet_reconnect(connection_id='{connection_id}') to re-detect "
        "the sheet structure and re-pin the column mapping."
    )


def _drift_hints(connections: list[GsheetConnectionRow]) -> list[str]:
    """Append a reconnect hint per drifted connection."""
    return [
        _reconnect_hint(c.connection_id)
        for c in connections
        if c.status == "drift_detected"
    ]


def _connect(req: ConnectionRequest) -> ConnectResult:
    """Run a blocking connect attempt with its service lifetime on one thread."""
    with _build_connection_service() as service:
        return service.connect(req, actor="mcp")


def _reconnect(
    connection_id: str,
    *,
    yes: bool,
    human_sign_confirmation: bool = False,
) -> ConnectResult:
    """Run a blocking reconnect attempt with its service lifetime on one thread."""
    with _build_connection_service() as service:
        if human_sign_confirmation:
            return service.reconnect(
                connection_id,
                yes=yes,
                human_sign_confirmation=True,
                actor="mcp",
            )
        return service.reconnect(connection_id, yes=yes, actor="mcp")


def _sign_confirmation_message(
    error: GSheetSignConfirmationRequiredError,
) -> str:
    """Explain the exact header evidence and ledger-wide safety consequence."""
    return (
        f"Google Sheets detected amount header {error.evidence_header!r} and inferred "
        f"{error.proposed_convention!r}. This would invert every transaction amount: "
        "charges become expenses and payments become credits. Confirm this "
        "whole-ledger polarity?"
    )


def _connect_cli_equivalent(
    req: ConnectionRequest,
    error: GSheetSignConfirmationRequiredError,
) -> str:
    """Reproduce the public connect request with an explicit human sign choice."""
    parts = ["moneybin", "gsheet", "connect", req.url]
    for flag, value in (
        ("--adapter", req.adapter),
        ("--alias", req.alias),
        ("--account-name", req.account_name),
        ("--account-id", req.account_id),
    ):
        if value is not None:
            parts.extend((flag, value))
    if req.column_mapping is not None:
        parts.extend((
            "--column-mapping",
            json.dumps(req.column_mapping, separators=(",", ":"), sort_keys=True),
        ))
    if req.yes:
        parts.append("--yes")
    if req.accept_seed_fallback:
        parts.append("--accept-seed-fallback")
    if req.no_initial_pull:
        parts.append("--no-initial-pull")
    parts.extend(("--sign", error.proposed_convention))
    return shlex.join(parts)


def _reconnect_cli_equivalent(
    connection_id: str,
    *,
    yes: bool,
    error: GSheetSignConfirmationRequiredError,
) -> str:
    """Reproduce the public reconnect request with an explicit human sign choice."""
    parts = ["moneybin", "gsheet", "reconnect", connection_id]
    if yes:
        parts.append("--yes")
    parts.extend(("--sign", error.proposed_convention))
    return shlex.join(parts)


@mcp_tool(
    read_only=False,
    idempotent=False,
    open_world=True,
    # Interactive OAuth: user has to click Allow in their browser. Give the
    # full flow generous headroom — default 30s timeout would routinely
    # fire before a human completes the consent screen.
    timeout_seconds=180.0,
)
def gsheet_auth(force_reauth: bool = False) -> ResponseEnvelope[GsheetAuthPayload]:
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
            data=GsheetAuthPayload(status="already_authorized"),
            actions=[
                "Run gsheet_connect(url='https://docs.google.com/spreadsheets/...') "
                "to bind a sheet."
            ],
        )
    oauth.authorize()
    return build_envelope(
        data=GsheetAuthPayload(status="authorized"),
        actions=[
            "Run gsheet_connect(url='https://docs.google.com/spreadsheets/...') "
            "to bind a sheet."
        ],
    )


@mcp_tool(
    read_only=False,
    idempotent=False,
    open_world=True,
    # First-run before gsheet_auth() lands tokens, connect() will trigger
    # the OAuth installed-app flow itself. Same 180s cap as gsheet_auth so
    # the user has headroom to click Allow without the default 30s firing.
    timeout_seconds=180.0,
)
async def gsheet_connect(
    url: str,
    adapter: str | None = None,
    alias: str | None = None,
    account_name: str | None = None,
    account_id: str | None = None,
    column_mapping: dict[str, str] | None = None,
    yes: bool = False,
    accept_seed_fallback: bool = False,
    no_initial_pull: bool = False,
) -> ResponseEnvelope[GsheetConnectPayload]:
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

    When detection infers a whole-ledger sign inversion, MCP asks the human
    inline and retries internally only after explicit confirmation. The agent
    cannot ratify that inference through this tool's parameters.
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
    try:
        result = await asyncio.to_thread(_connect, req)
    except GSheetSignConfirmationRequiredError as error:
        from moneybin.mcp.elicitation import confirm_or_raise  # noqa: PLC0415

        await confirm_or_raise(
            _sign_confirmation_message(error),
            subject="This Google Sheets sign inversion",
            unchanged="the connection was not created and no initial pull ran",
            cli_equivalent=_connect_cli_equivalent(req, error),
            details={
                "evidence_header": error.evidence_header,
                "proposed_convention": error.proposed_convention,
            },
        )
        result = await asyncio.to_thread(
            _connect,
            replace(req, human_sign_confirmation=True),
        )

    data = GsheetConnectPayload(
        connection=_connection_row(result.connection),
        detection=GsheetDetection(
            confidence=result.detection.confidence,
            column_mapping=result.detection.column_mapping,
            detection_notes=result.detection.notes,
        ),
        initial_pull=_initial_pull(result),
    )
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
    return build_envelope(data=data, actions=actions)


@mcp_tool(read_only=False, idempotent=False, open_world=True)
def gsheet_pull(
    connection_id: str | None = None,
) -> ResponseEnvelope[GsheetPullPayload]:
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
        GsheetPullRow(
            connection_id=r.connection_id,
            status=r.status,
            rows_inserted=r.load_result.rows_inserted if r.load_result else 0,
            rows_upserted=r.load_result.rows_upserted if r.load_result else 0,
            rows_soft_deleted=r.load_result.rows_soft_deleted if r.load_result else 0,
            drift_reason=r.drift_reason,
            error_message=r.error_message,
        )
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
        data=GsheetPullPayload(pulls=pulls),
        actions=actions,
    )


@mcp_tool()
def gsheet() -> ResponseEnvelope[GsheetConnectionsPayload]:
    """List every Google Sheets connection.

    Shape 5 collection read. Each connection that surfaces in drift_detected
    status carries a paired gsheet_reconnect hint in actions[].
    """
    with _build_connection_service() as service:
        connections = service.list_connections()
    rows = [_connection_row(c) for c in connections]
    return build_envelope(
        data=GsheetConnectionsPayload(connections=rows),
        actions=_drift_hints(rows),
    )


@mcp_tool()
def gsheet_status(
    connection_id: str | None = None,
) -> ResponseEnvelope[GsheetConnectionsPayload]:
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
    rows = [_connection_row(c) for c in connections]
    return build_envelope(
        data=GsheetConnectionsPayload(connections=rows),
        actions=_drift_hints(rows),
    )


@mcp_tool(
    read_only=False,
    idempotent=False,
    open_world=True,
    # Reconnect can pause for a human to review an inferred ledger-wide sign
    # inversion. Give that decision the same headroom as connect/OAuth instead
    # of letting the default 30s expire while the user is reading the evidence.
    timeout_seconds=180.0,
)
async def gsheet_reconnect(
    connection_id: str, yes: bool = False
) -> ResponseEnvelope[GsheetConnectPayload]:
    """Re-detect the sheet structure, re-pin the mapping, and run a pull.

    Use after the source sheet changes shape (column added, header reworded)
    and a connection surfaces in drift_detected status. Amounts in loaded data
    follow MoneyBin's accounting convention: negative = expense, positive = income.

    Pass yes=True to accept a medium-confidence column-mapping remap. Without
    it, an ambiguous remap raises AmbiguousDetectionError so the agent can
    confirm with the user before silently re-pinning the wrong mapping.

    When re-detection infers a whole-ledger sign inversion, MCP asks the human
    inline and retries internally only after explicit confirmation. The agent
    cannot ratify that inference through this tool's parameters.

    Mutation surface: updates app.gsheet_connections.column_mapping +
    header_signature (audited) and writes raw rows via the pull side-effect.
    Revert: there is no revert — the connection re-binds to whatever the
    sheet currently looks like. Run gsheet_status afterwards to verify.
    """
    try:
        result = await asyncio.to_thread(
            _reconnect,
            connection_id,
            yes=yes,
        )
    except GSheetSignConfirmationRequiredError as error:
        from moneybin.mcp.elicitation import confirm_or_raise  # noqa: PLC0415

        await confirm_or_raise(
            _sign_confirmation_message(error),
            subject="This Google Sheets sign inversion",
            unchanged=(f"connection '{connection_id}' was not re-pinned or pulled"),
            cli_equivalent=_reconnect_cli_equivalent(
                connection_id,
                yes=yes,
                error=error,
            ),
            details={
                "connection_id": connection_id,
                "evidence_header": error.evidence_header,
                "proposed_convention": error.proposed_convention,
            },
        )
        result = await asyncio.to_thread(
            _reconnect,
            connection_id,
            yes=yes,
            human_sign_confirmation=True,
        )

    data = GsheetConnectPayload(
        connection=_connection_row(result.connection),
        # Re-detection re-pins silently — no first-time detection notes.
        detection=GsheetDetection(
            confidence=result.detection.confidence,
            column_mapping=result.detection.column_mapping,
        ),
        initial_pull=_initial_pull(result),
    )
    return build_envelope(
        data=data,
        actions=["Run gsheet_status to verify the connection is healthy."],
    )


@mcp_tool(
    read_only=False,
    idempotent=False,
    destructive=True,
    open_world=True,
)
def gsheet_disconnect(
    connection_id: str, purge: bool = False
) -> ResponseEnvelope[GsheetDisconnectPayload]:
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
        data=GsheetDisconnectPayload(
            connection_id=connection_id,
            status="purged" if purge else "disconnected",
            purged=purge,
        ),
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
            "(negative = expense). An inferred ledger-wide sign inversion is "
            "confirmed by the human through inline elicitation; the agent cannot "
            "self-confirm it. Run gsheet_auth first if not yet authorized — "
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
            "Use after drift_detected. An inferred ledger-wide sign inversion is "
            "confirmed by the human through inline elicitation; the agent cannot "
            "self-confirm it. No revert — re-binds to whatever the sheet currently "
            "looks like.",
        ),
        (
            gsheet_disconnect,
            "Soft-disconnect (default) or purge a connection. Purge=True is "
            "permanent: drops seed view (if any), deletes raw rows, hard-deletes "
            "the connection row. Soft-disconnect retains raw rows for analytics.",
        ),
    ]:
        register(mcp, fn, fn.__name__, desc)
