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
from typing import TYPE_CHECKING, Any, Literal, cast

from fastmcp import FastMCP
from pydantic import JsonValue, StrictBool

from moneybin.config import get_settings
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
from moneybin.mcp.confirmation import (
    ConfirmationBinding,
    ConfirmationGrant,
    grant_confirmation_or_raise,
)
from moneybin.mcp.decorator import internal_envelope_adapter, mcp_tool
from moneybin.mcp.privacy import Sensitivity, tier_to_sensitivity
from moneybin.privacy.introspection import extract_data_classes
from moneybin.privacy.payloads.gsheet import (
    GsheetAuthPayload,
    GsheetCoarsePayload,
    GsheetConnectAuthView,
    GsheetConnectBindingView,
    GsheetConnectCoarsePayload,
    GsheetConnectionRow,
    GsheetConnectionsPayload,
    GsheetConnectionsView,
    GsheetConnectPayload,
    GsheetDetection,
    GsheetDisconnectCoarsePayload,
    GsheetDisconnectPayload,
    GsheetInitialPull,
    GsheetPullPayload,
    GsheetPullRow,
    GsheetStatusView,
)
from moneybin.privacy.redaction import redact_typed
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
        f"Run gsheet_connect(connection_id='{connection_id}') to re-detect "
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


@internal_envelope_adapter(sensitivity=Sensitivity.LOW)
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


@internal_envelope_adapter(sensitivity=Sensitivity.MEDIUM)
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
        "Use gsheet(view='status') to check connection health going forward.",
    ]
    if result.initial_pull_status not in (None, "complete"):
        actions.insert(
            0,
            f"Initial pull returned status={result.initial_pull_status!r}; "
            "run gsheet(view='status') before assuming data is loaded.",
        )
    return build_envelope(data=data, actions=actions)


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
                "Re-authenticate with gsheet_connect(force_reauth=True), or run "
                "`moneybin gsheet auth` (CLI). Both drive the same "
                "in-process OAuth flow."
            )
    return build_envelope(
        data=GsheetPullPayload(pulls=pulls),
        actions=actions,
    )


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


def _gsheet_coarse_envelope(
    data: GsheetConnectionsView | GsheetStatusView,
) -> ResponseEnvelope[GsheetCoarsePayload]:
    """Build and redact a dynamically classified Google Sheets view."""
    contract_type = type(data)
    classes = extract_data_classes(contract_type)
    tier = max(data_class.tier for data_class in classes)
    redacted = cast(
        GsheetConnectionsView | GsheetStatusView,
        redact_typed(data, None),
    )
    return cast(
        ResponseEnvelope[GsheetCoarsePayload],
        build_envelope(
            data=redacted,
            sensitivity=cast(Any, tier_to_sensitivity(tier).value),
            total_count=len(redacted.connections),
            returned_count=len(redacted.connections),
            actions=[
                f"Use gsheet_connect(connection_id='{row.connection_id}') to "
                "re-detect and reconnect this binding."
                for row in redacted.connections
                if row.status == "drift_detected"
            ],
            classes_returned=sorted(data_class.value for data_class in classes),
        ),
    )


@mcp_tool(dynamic_classification=True, maximum_sensitivity=Sensitivity.MEDIUM)
def gsheet_coarse(
    view: Literal["connections", "status"] = "connections",
    connection_id: str | None = None,
) -> ResponseEnvelope[GsheetCoarsePayload]:
    """Read the Google Sheets connection collection or health status."""
    if view == "connections" and connection_id is not None:
        raise UserError(
            "connection_id is valid only for the status view.",
            code="GSHEET_CONNECTION_ID_NOT_ALLOWED",
        )

    with _build_connection_service() as service:
        if connection_id is None:
            connections = service.list_connections()
        else:
            single = service.get(connection_id)
            if single is None:
                raise UserError(
                    "No Google Sheets connection found for the supplied ID.",
                    code=INFRA_NOT_FOUND,
                    hint="Use gsheet(view='connections') to list known IDs.",
                )
            connections = [single]
    rows = [_connection_row(connection) for connection in connections]
    if view == "connections":
        return _gsheet_coarse_envelope(GsheetConnectionsView(connections=rows))
    return _gsheet_coarse_envelope(GsheetStatusView(connections=rows))


@internal_envelope_adapter(sensitivity=Sensitivity.MEDIUM)
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
        actions=["Run gsheet(view='status') to verify connection health."],
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


@mcp_tool(
    read_only=False,
    idempotent=False,
    open_world=True,
    timeout_seconds=180.0,
    dynamic_classification=True,
    maximum_sensitivity=Sensitivity.MEDIUM,
)
async def gsheet_connect_coarse(
    url: str | None = None,
    connection_id: str | None = None,
    force_reauth: StrictBool = False,
    adapter: str | None = None,
    alias: str | None = None,
    account_name: str | None = None,
    account_id: str | None = None,
    column_mapping: dict[str, str] | None = None,
    confirm_mapping: StrictBool = False,
    accept_seed_fallback: StrictBool = False,
    no_initial_pull: StrictBool = False,
) -> ResponseEnvelope[GsheetConnectCoarsePayload]:
    """Authenticate, connect, or reconnect through one mode-aware workflow."""
    if url is not None and connection_id is not None:
        raise UserError(
            "url and connection_id select different modes and cannot be combined.",
            code="GSHEET_CONNECT_MODE_CONFLICT",
        )
    if url is None and connection_id is None:
        auth_only = {
            "adapter": adapter,
            "alias": alias,
            "account_name": account_name,
            "account_id": account_id,
            "column_mapping": column_mapping,
            "confirm_mapping": bool(confirm_mapping),
            "accept_seed_fallback": bool(accept_seed_fallback),
            "no_initial_pull": bool(no_initial_pull),
        }
        supplied = sorted(key for key, value in auth_only.items() if value)
        if supplied:
            raise UserError(
                f"Authentication-only mode does not accept: {', '.join(supplied)}.",
                code="GSHEET_AUTH_ARGUMENT_CONFLICT",
            )
        response = await gsheet_auth(force_reauth=bool(force_reauth))
        if response.error is not None:
            return cast(ResponseEnvelope[GsheetConnectCoarsePayload], response)
        return _gsheet_connect_envelope(
            GsheetConnectAuthView(status=response.data.status),
            actions=[
                "Use gsheet_connect(url=...) to create a connection when ready.",
            ],
        )
    if force_reauth:
        oauth = _build_oauth_client()
        oauth.authorize()
    if url is not None:
        response = await gsheet_connect(
            url=url,
            adapter=adapter,
            alias=alias,
            account_name=account_name,
            account_id=account_id,
            column_mapping=column_mapping,
            yes=bool(confirm_mapping),
            accept_seed_fallback=bool(accept_seed_fallback),
            no_initial_pull=bool(no_initial_pull),
        )
        if response.error is not None:
            return cast(ResponseEnvelope[GsheetConnectCoarsePayload], response)
        return _gsheet_connect_envelope(
            GsheetConnectBindingView(
                kind="new",
                connection=response.data.connection,
                detection=response.data.detection,
                initial_pull=response.data.initial_pull,
            ),
            actions=[
                "Use gsheet(view='status', connection_id=...) to verify the "
                "connection.",
            ],
        )
    unsupported = {
        "adapter": adapter,
        "alias": alias,
        "account_name": account_name,
        "account_id": account_id,
        "column_mapping": column_mapping,
        "accept_seed_fallback": bool(accept_seed_fallback),
        "no_initial_pull": bool(no_initial_pull),
    }
    supplied = sorted(key for key, value in unsupported.items() if value)
    if supplied:
        raise UserError(
            f"Reconnect mode does not accept: {', '.join(supplied)}.",
            code="GSHEET_RECONNECT_ARGUMENT_CONFLICT",
        )
    response = await gsheet_reconnect(
        connection_id=cast(str, connection_id),
        yes=bool(confirm_mapping),
    )
    if response.error is not None:
        return cast(ResponseEnvelope[GsheetConnectCoarsePayload], response)
    return _gsheet_connect_envelope(
        GsheetConnectBindingView(
            kind="reconnect",
            connection=response.data.connection,
            detection=response.data.detection,
            initial_pull=response.data.initial_pull,
        ),
        actions=[
            "Use gsheet(view='status', connection_id=...) to verify the connection.",
        ],
    )


def _gsheet_connect_envelope(
    data: GsheetConnectAuthView | GsheetConnectBindingView,
    *,
    actions: list[str],
) -> ResponseEnvelope[GsheetConnectCoarsePayload]:
    """Build a typed, redacted, mode-specific consolidated connect result."""
    classes = extract_data_classes(type(data))
    tier = max(data_class.tier for data_class in classes)
    redacted = redact_typed(data, None)
    return cast(
        ResponseEnvelope[GsheetConnectCoarsePayload],
        build_envelope(
            data=redacted,
            sensitivity=cast(Any, tier_to_sensitivity(tier).value),
            actions=actions,
            classes_returned=sorted(data_class.value for data_class in classes),
        ),
    )


def _json_value(value: object) -> JsonValue:
    """Convert database scalars and nested rows into canonical JSON values."""
    import json

    return cast(JsonValue, json.loads(json.dumps(value, default=str, sort_keys=True)))


def _purge_binding(plan: Any) -> ConfirmationBinding:
    """Bind a purge to its complete live connection and raw-row before-state."""
    return ConfirmationBinding(
        arguments={
            "connection_id": plan.connection_id,
            "state": "absent",
            "connection_before_state": _json_value(plan.connection_before_state),
            "raw_before_state": _json_value(plan.raw_before_state),
        },
        resolved_ids=(plan.connection_id,),
        actor="mcp",
        profile=get_settings().profile,
        authorization_context="local-profile",
        operation_kind="gsheet_disconnect_absent",
        blast_radius=plan.blast_radius,
    )


@mcp_tool(
    read_only=False,
    idempotent=False,
    destructive=True,
    open_world=True,
    timeout_seconds=180.0,
    dynamic_classification=True,
    maximum_sensitivity=Sensitivity.MEDIUM,
)
async def gsheet_disconnect_coarse(
    connection_id: str,
    state: Literal["disconnected", "absent"] = "disconnected",
    confirmation_token: str | None = None,
) -> ResponseEnvelope[GsheetDisconnectCoarsePayload]:
    """Set a reversible disconnected state or exactly confirmed absence."""
    if state == "disconnected":
        if confirmation_token is not None:
            raise UserError(
                "confirmation_token is valid only for state='absent'.",
                code="GSHEET_CONFIRMATION_NOT_ALLOWED",
            )
        with _build_connection_service() as service:
            service.disconnect(connection_id, purge=False, actor="mcp")
        return _gsheet_disconnect_envelope(
            GsheetDisconnectCoarsePayload(
                kind="disconnected",
                connection_id=connection_id,
                status="disconnected",
                purged=False,
            ),
            actions=[
                "Use gsheet_connect(connection_id=...) to reconnect this binding.",
            ],
        )

    with _build_connection_service() as service:
        plan = service.plan_purge(connection_id)
    binding = _purge_binding(plan)
    grant: ConfirmationGrant = await grant_confirmation_or_raise(
        binding=binding if confirmation_token is None else None,
        message=(
            "Permanently remove this exact Google Sheets connection and all "
            f"{plan.blast_radius['raw_rows']} raw rows?"
        ),
        confirmation_token=confirmation_token,
    )
    with _build_connection_service() as service:
        service.purge_confirmed(
            connection_id,
            verify=lambda live: grant.verify(_purge_binding(live)),
            actor="mcp",
        )
    before = plan.connection_before_state
    return _gsheet_disconnect_envelope(
        GsheetDisconnectCoarsePayload(
            kind="absent",
            connection_id=connection_id,
            status="purged",
            purged=True,
            recovery={
                "reversible": False,
                "spreadsheet_id": before["spreadsheet_id"],
                "sheet_gid": before["sheet_gid"],
                "adapter": before["adapter"],
                "alias": before.get("alias"),
                "account_id": before.get("account_id"),
            },
        ),
        actions=[
            "This purge is permanent. Recreate the connection from its Google "
            "Sheets URL to resume future pulls.",
        ],
    )


def _gsheet_disconnect_envelope(
    data: GsheetDisconnectCoarsePayload,
    *,
    actions: list[str],
) -> ResponseEnvelope[GsheetDisconnectCoarsePayload]:
    """Build one typed consolidated disconnect result."""
    classes = extract_data_classes(type(data))
    tier = max(data_class.tier for data_class in classes)
    return build_envelope(
        data=cast(GsheetDisconnectCoarsePayload, redact_typed(data, None)),
        sensitivity=cast(Any, tier_to_sensitivity(tier).value),
        actions=actions,
        classes_returned=sorted(data_class.value for data_class in classes),
    )


@mcp_tool(read_only=False, idempotent=False, open_world=True)
def gsheet_pull_coarse(
    connection_id: str | None = None,
) -> ResponseEnvelope[GsheetPullPayload]:
    """Pull through the isolated workflow vocabulary."""
    response = gsheet_pull(connection_id=connection_id)
    actions: list[str] = []
    for row in response.data.pulls:
        if row.status == "drift_detected":
            actions.append(
                f"Use gsheet_connect(connection_id='{row.connection_id}') to "
                "re-detect and reconnect this binding."
            )
        elif row.status == "auth_expired":
            actions.append("Use gsheet_connect(force_reauth=True) to authenticate.")
    return replace(response, actions=actions)


def register_gsheet_coarse_reads(mcp: FastMCP) -> None:
    """Register the standard Google Sheets read."""
    register(
        mcp,
        gsheet_coarse,
        "gsheet",
        "Read every Google Sheets connection or inspect connection health. "
        "The status view accepts one connection ID; without it status covers all.",
        privacy_actor="gsheet",
    )


def register_gsheet_workflow_tools(mcp: FastMCP) -> None:
    """Register the standard four-boundary Google Sheets workflow."""
    for callback, name, description in (
        (gsheet_coarse, "gsheet", "Read connections or connection health."),
        (
            gsheet_connect_coarse,
            "gsheet_connect",
            "Authenticate, connect, or reconnect in one mode-aware workflow.",
        ),
        (gsheet_pull_coarse, "gsheet_pull", "Pull one or all connections."),
        (
            gsheet_disconnect_coarse,
            "gsheet_disconnect",
            "Soft-disconnect or exactly confirm a permanent purge.",
        ),
    ):
        register(
            mcp,
            callback,
            name,
            description,
            privacy_actor=name,
        )


def register_gsheet_tools(mcp: FastMCP) -> None:
    """Register the standard Google Sheets workflow."""
    register_gsheet_workflow_tools(mcp)
