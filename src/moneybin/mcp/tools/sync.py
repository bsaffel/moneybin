"""sync_* MCP tools — Phase 1 implementations.

Per docs/specs/2026-05-13-plaid-sync-design.md Section 11.

Device authentication is a nonblocking variant of sync_link + sync_status;
logout is the credential-state variant of sync_disconnect. sync_key_rotate
remains CLI-only because it handles passphrase material.
"""

from __future__ import annotations

import asyncio
from collections.abc import Generator
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, Literal, cast

from fastmcp import FastMCP

from moneybin.config import get_settings
from moneybin.errors import UserError
from moneybin.mcp._registration import register
from moneybin.mcp.confirmation import (
    ConfirmationBinding,
    ConfirmationGrant,
    grant_confirmation_or_raise,
)
from moneybin.mcp.decorator import mcp_tool
from moneybin.mcp.privacy import Sensitivity, tier_to_sensitivity
from moneybin.privacy.introspection import extract_data_classes
from moneybin.privacy.payloads.sync import (
    SyncAuthView,
    SyncConnectionRow,
    SyncDisconnectCoarsePayload,
    SyncGlobalStatusView,
    SyncInstitutionDisconnectView,
    SyncInstitutionLinkView,
    SyncLinkCoarsePayload,
    SyncLinkPayload,
    SyncLinkStatusPayload,
    SyncLogoutView,
    SyncPullInstitutionRow,
    SyncPullPayload,
    SyncSessionStatusView,
    SyncStatusCoarsePayload,
    SyncStatusPayload,
)
from moneybin.privacy.redaction import redact_typed
from moneybin.protocol.envelope import (
    ResponseEnvelope,
    build_envelope,
    build_error_envelope,
)

if TYPE_CHECKING:
    from moneybin.connectors.sync_models import ConnectedInstitution, PullResult


def _build_sync_client() -> Any:
    """Construct a SyncClient from current settings.

    Mirrors the CLI builder (``cli/commands/sync.py``) exactly — including the
    per-profile identity — so MCP and CLI read and write the same scoped token
    slot. (The two builders are duplicated; consolidating them is tracked as a
    follow-up.)
    """
    from moneybin.config import get_settings  # noqa: PLC0415
    from moneybin.connectors.sync_client import SyncClient  # noqa: PLC0415
    from moneybin.utils.user_config import get_or_create_profile_id  # noqa: PLC0415

    settings = get_settings()
    if settings.sync.server_url is None:
        raise ValueError(
            "sync.server_url is not configured. "
            "Set MONEYBIN_SYNC__SERVER_URL in your environment."
        )
    # Scope the broker identity to the active profile so each profile
    # authenticates as a distinct user.
    profile_id = get_or_create_profile_id(settings.profile_dir)
    return SyncClient(server_url=str(settings.sync.server_url), profile_id=profile_id)


def _build_sync_auth_service() -> Any:
    """Construct profile-scoped nonblocking authentication orchestration."""
    from moneybin.connectors.sync_auth import SyncAuthService  # noqa: PLC0415

    return SyncAuthService(client=_build_sync_client())


@contextmanager
def _build_sync_service() -> Generator[Any, None, None]:
    """Context manager yielding a SyncService with active Database connection."""
    from moneybin.database import get_database  # noqa: PLC0415
    from moneybin.extractors.plaid import PlaidExtractor  # noqa: PLC0415
    from moneybin.services.sync_service import SyncService  # noqa: PLC0415

    client = _build_sync_client()
    with get_database(read_only=False) as db:
        loader = PlaidExtractor(db)
        yield SyncService(client=client, db=db, loader=loader)


def sync_pull(
    institution: str | None = None, force: bool = False, refresh: bool = True
) -> ResponseEnvelope[SyncPullPayload]:
    """Pull transactions, accounts, balances from connected institutions via moneybin-sync.

    Amounts in loaded data follow MoneyBin accounting convention: negative = expense,
    positive = income; the Plaid sign flip happens during ingestion. Returns per-institution
    results including error_code for any failed institutions. Mutates raw.plaid_* tables and
    propagates through SQLMesh to core; idempotent on re-run (transactions upsert by
    (transaction_id, provider_item_id)).

    When refresh=True (default) and the sync changes raw state, runs the post-load
    refresh pipeline (matching + SQLMesh apply + categorization) so core.dim_accounts
    and friends reflect the new data before returning. Pass refresh=False to defer
    (raw rows still land durably; derived models become stale until the next refresh).
    High-frequency callers should pass refresh=False and schedule refresh separately —
    SQLMesh apply dominates pull latency (typically 5–30s).

    A pull can partially fail while still returning: `security_resolution_error`
    non-null means this pull's investment transactions were NOT attributed to
    securities (cost basis is incomplete until a retry — the pull is idempotent),
    and `transforms_error` non-null means raw rows landed but core.* is stale.
    Report those to the user; do not summarize such a pull as a clean success.
    `security_resolution` carries the per-outcome counts (adopted / auto_bound /
    minted / proposed / pending); a non-zero proposed/pending count means
    security identities are awaiting the user's review.
    """
    with _build_sync_service() as service:
        result = service.pull(institution=institution, force=force, refresh=refresh)
    institutions = [
        SyncPullInstitutionRow(
            provider_item_id=inst.provider_item_id,
            institution_name=inst.institution_name,
            status=inst.status,
            transaction_count=inst.transaction_count,
            error=inst.error,
            error_code=inst.error_code,
        )
        for inst in result.institutions
    ]
    return build_envelope(
        data=SyncPullPayload(
            job_id=result.job_id,
            transactions_loaded=result.transactions_loaded,
            accounts_loaded=result.accounts_loaded,
            balances_loaded=result.balances_loaded,
            transactions_removed=result.transactions_removed,
            institutions=institutions,
            transforms_applied=result.transforms_applied,
            transforms_duration_seconds=result.transforms_duration_seconds,
            transforms_error=result.transforms_error,
            securities_loaded=result.securities_loaded,
            investment_transactions_loaded=result.investment_transactions_loaded,
            holdings_loaded=result.holdings_loaded,
            holding_lots_loaded=result.holding_lots_loaded,
            opening_bootstrap_rows=result.opening_bootstrap_rows,
            investment_source_overlap_accounts=list(
                result.investment_source_overlap_accounts
            ),
            security_resolution=dict(result.security_resolution),
            security_resolution_error=result.security_resolution_error,
        ),
        actions=_pull_actions(result),
    )


def _pull_actions(result: PullResult) -> list[str]:
    """Next-step hints, including every partial-failure the CLI surfaces.

    The CLI warns and exits non-zero on ``security_resolution_error``; MCP has no
    exit code, so the equivalent signal has to be prose the agent will actually
    read — hence a leading, unambiguous action rather than a quiet payload field.
    """
    actions: list[str] = []
    if result.security_resolution_error:
        actions.append(
            "WARNING: security resolution failed — this pull's investment "
            "transactions are NOT attributed to securities, so cost basis, "
            "holdings and realized gains are incomplete. Raw data already "
            "landed; retry with sync_pull (idempotent). Report this to the "
            "user rather than reporting a successful sync."
        )
    if result.transforms_error:
        actions.append(
            "WARNING: transforms failed — raw rows landed but core.* models are "
            "stale. Retry with refresh_run."
        )
    resolution = result.security_resolution or {}
    awaiting = resolution.get("proposed", 0) + resolution.get("pending", 0)
    if awaiting:
        actions.append(
            f"{awaiting} security identity(ies) await review — use "
            "reviews(kind='security_links') to see them "
            "(unresolved securities are dropped from cost basis)."
        )
    if result.investment_source_overlap_accounts:
        actions.append(
            f"{len(result.investment_source_overlap_accounts)} account(s) have "
            "both manual and Plaid investment history — lots and gains "
            "double-count until one source is chosen per account "
            "(see system_status(sections=['doctor']))."
        )
    actions.append("Use sync_status to see connection health going forward.")
    return actions


def sync_status() -> ResponseEnvelope[SyncStatusPayload]:
    """Connected institutions, last-sync times, and error-state guidance."""
    with _build_sync_service() as service:
        connections = service.list_connections()
    rows = [
        SyncConnectionRow(
            id=c.id,
            provider_item_id=c.provider_item_id,
            institution_name=c.institution_name,
            provider=c.provider,
            status=c.status,
            last_sync=c.last_sync.isoformat() if c.last_sync else None,
            error_code=c.error_code,
            guidance=c.guidance,
        )
        for c in connections
    ]
    return build_envelope(data=SyncStatusPayload(connections=rows), actions=[])


def sync_link(
    institution: str | None = None,
) -> ResponseEnvelope[SyncLinkPayload]:
    """Link a bank account via Plaid (formerly: sync_connect).

    Initiates a bank-connection flow via moneybin-sync's Plaid Hosted Link.
    Returns a URL the user opens in their browser to complete the Plaid UI.
    Does NOT wait for completion — after the user confirms they've finished,
    call sync_link_status with the returned session_id to verify. The
    link_url is a sensitive one-time credential — present it to the user
    but do not include it in logs or summaries.

    Pass `institution` to re-authenticate an existing connection (Plaid update mode).
    """
    client = _build_sync_client()
    provider_item_id: str | None = None
    if institution:
        matches = [
            inst
            for inst in client.list_institutions()
            if inst.institution_name
            and inst.institution_name.lower() == institution.lower()
        ]
        if len(matches) > 1:
            ids = ", ".join(m.provider_item_id for m in matches)
            return build_error_envelope(
                error=UserError(
                    f"multiple connected institutions match '{institution}' ({ids})",
                    code="ambiguous",
                    hint="Run sync_status to identify them; the duplicate name "
                    "must be disambiguated before sync_link can target one.",
                ),
                actions=["Run sync_status to list connected institutions."],
            )
        if len(matches) == 1:
            provider_item_id = matches[0].provider_item_id
        # else: name doesn't match any existing connection → new-connection flow
        # per design Section 8; let the server's Link flow name the institution.
    initiate = client.initiate_link(provider_item_id=provider_item_id)
    return build_envelope(
        data=SyncLinkPayload(
            session_id=initiate.session_id,
            link_url=initiate.link_url,
            expiration=initiate.expiration.isoformat(),
        ),
        actions=[
            "Present link_url to the user and ask them to complete the connection in their browser.",
            "After confirmation, call sync_status with session_id to verify.",
            "Once verified, call sync_pull to fetch transactions.",
            "Session expires at the expiration timestamp — beyond that, start a new link flow.",
        ],
    )


def sync_link_status(
    session_id: str,
) -> ResponseEnvelope[SyncLinkStatusPayload]:
    """Poll a sync_link session for completion (formerly: sync_connect_status).

    Check whether a bank-connection session has completed. Call after the user
    indicates they've finished the Plaid Link flow in their browser. Returns
    linked, pending, or failed. Does NOT loop internally — the agent should
    invoke this when the user signals completion, not poll repeatedly.
    """
    client = _build_sync_client()
    status = client.get_link_status(session_id)
    actions: list[str] = []
    if status.status == "pending":
        actions = [
            "Connection has not completed yet. Ask the user to finish the flow in their browser, or wait and check again.",
            "If the session expiration has passed, start a new link flow with sync_link.",
        ]
    elif status.status == "linked":
        actions = ["Run sync_pull to fetch transactions from the new institution."]
    elif status.status == "failed":
        actions = ["Run sync_link to retry the connection."]
    return build_envelope(
        data=SyncLinkStatusPayload(
            session_id=status.session_id,
            status=status.status,
            provider_item_id=status.provider_item_id,
            institution_name=status.institution_name,
            error=status.error,
            expiration=status.expiration.isoformat(),
        ),
        actions=actions,
    )


@mcp_tool(
    read_only=False,
    idempotent=True,
    open_world=True,
    dynamic_classification=True,
    maximum_sensitivity=Sensitivity.MEDIUM,
)
def sync_status_coarse(
    session_id: str | None = None,
    auth_session_id: str | None = None,
) -> ResponseEnvelope[SyncStatusCoarsePayload]:
    """Read connection health, a link session, or advance one auth session."""
    if session_id is not None and auth_session_id is not None:
        raise UserError(
            "session_id and auth_session_id select different status modes.",
            code="SYNC_STATUS_MODE_CONFLICT",
        )
    if auth_session_id is not None:
        result = _build_sync_auth_service().status(auth_session_id)
        data: SyncStatusCoarsePayload = SyncAuthView(
            auth_session_id=result.auth_session_id,
            status=result.status,
            user_code=result.user_code,
            verification_url=result.verification_url,
            expiration=result.expiration,
            replayed=result.replayed,
            error_code=result.error_code,
        )
        if result.status == "pending":
            actions = [
                "Ask the user to complete the verification URL, then call "
                f"sync_status(auth_session_id='{result.auth_session_id}') again."
            ]
        elif result.status == "authenticated":
            actions = ["Use sync_link to connect an institution."]
        elif result.status in {"denied", "expired"}:
            actions = ["Use sync_link(mode='login') to start a new auth session."]
        else:
            actions = [
                "The sync provider could not be reached. Retry this exact "
                "auth_session_id; terminal states are idempotent."
            ]
    elif session_id is None:
        response = sync_status()
        data = SyncGlobalStatusView(connections=response.data.connections)
        actions = list(response.actions)
    else:
        response = sync_link_status(session_id=session_id)
        data = SyncSessionStatusView(
            session_id=response.data.session_id,
            status=response.data.status,
            provider_item_id=response.data.provider_item_id,
            institution_name=response.data.institution_name,
            error=response.data.error,
            expiration=response.data.expiration,
        )
        actions = list(response.actions)
    classes = extract_data_classes(type(data))
    tier = max(data_class.tier for data_class in classes)
    return cast(
        ResponseEnvelope[SyncStatusCoarsePayload],
        build_envelope(
            data=redact_typed(data, None),
            sensitivity=cast(Any, tier_to_sensitivity(tier).value),
            actions=actions,
            classes_returned=sorted(data_class.value for data_class in classes),
        ),
    )


@mcp_tool(read_only=False, idempotent=False, open_world=True)
def sync_link_coarse(
    institution: str | None = None,
    mode: Literal["institution", "login"] = "institution",
) -> ResponseEnvelope[SyncLinkCoarsePayload]:
    """Start an institution-link or nonblocking device-login session."""
    if mode == "login":
        if institution is not None:
            raise UserError(
                "institution is valid only when mode='institution'.",
                code="SYNC_LINK_MODE_CONFLICT",
            )
        result = _build_sync_auth_service().begin()
        data: SyncLinkCoarsePayload = SyncAuthView(
            auth_session_id=result.auth_session_id,
            status=result.status,
            user_code=result.user_code,
            verification_url=result.verification_url,
            expiration=result.expiration,
            replayed=result.replayed,
            error_code=result.error_code,
        )
        return build_envelope(
            data=data,
            actions=[
                "Present verification_url and user_code to the user.",
                f"Then call sync_status(auth_session_id='{result.auth_session_id}') "
                "after the user completes authorization.",
            ],
        )
    response = sync_link(institution=institution)
    if response.error is not None:
        return cast(ResponseEnvelope[SyncLinkCoarsePayload], response)
    return build_envelope(
        data=SyncInstitutionLinkView(
            session_id=response.data.session_id,
            link_url=response.data.link_url,
            expiration=response.data.expiration,
        ),
        actions=[
            "Present link_url to the user and ask them to complete the browser flow.",
            f"Then call sync_status(session_id='{response.data.session_id}') to "
            "check completion.",
        ],
    )


@mcp_tool(read_only=False, idempotent=False, open_world=True)
def sync_pull_coarse(
    institution: str | None = None,
) -> ResponseEnvelope[SyncPullPayload]:
    """Pull while keeping recovery actions inside the isolated cohort."""
    return sync_pull(institution=institution)


def _sync_disconnect_binding(
    institution: str,
    connection: ConnectedInstitution,
) -> ConfirmationBinding:
    """Bind confirmation to one exact live remote institution connection."""
    return ConfirmationBinding(
        arguments={
            "institution": institution.casefold(),
            "mode": "institution",
            "institution_name": connection.institution_name,
            "provider": connection.provider,
            "status": connection.status,
        },
        resolved_ids=(connection.id, connection.provider_item_id),
        actor="mcp",
        profile=get_settings().profile,
        authorization_context="local-profile",
        operation_kind="sync_disconnect_institution",
        blast_radius={"institutions": 1},
    )


def _sync_logout() -> Any:
    """Run blocking credential cleanup outside the MCP event loop."""
    return _build_sync_auth_service().logout()


def _plan_sync_disconnect(institution: str) -> ConnectedInstitution:
    """Resolve one live disconnect target outside the MCP event loop."""
    with _build_sync_service() as service:
        return service.plan_disconnect(institution=institution)


def _disconnect_sync_confirmed(
    institution: str,
    grant: ConfirmationGrant,
) -> ConnectedInstitution:
    """Re-resolve, verify, and delete one connection outside the event loop."""

    def verify(live: ConnectedInstitution) -> None:
        grant.verify(_sync_disconnect_binding(institution, live))

    with _build_sync_service() as service:
        return service.disconnect_confirmed(
            institution=institution,
            verify=verify,
        )


@mcp_tool(
    read_only=False,
    destructive=True,
    idempotent=False,
    open_world=True,
    timeout_seconds=180.0,
)
async def sync_disconnect(
    institution: str | None = None,
    mode: Literal["institution", "logout"] = "institution",
    confirmation_token: str | None = None,
) -> ResponseEnvelope[SyncDisconnectCoarsePayload]:
    """Disconnect an institution or clear scoped sync credentials.

    Institution disconnect is permanent on moneybin-sync; local pulled rows
    remain. Logout clears profile-scoped credentials and pending auth sessions
    but is recoverable through ``sync_link(mode="login")``.
    """
    if mode == "logout":
        if institution is not None:
            raise UserError(
                "institution is valid only when mode='institution'.",
                code="SYNC_DISCONNECT_MODE_CONFLICT",
            )
        if confirmation_token is not None:
            raise UserError(
                "confirmation_token is valid only when mode='institution'.",
                code="SYNC_CONFIRMATION_NOT_ALLOWED",
            )
        result = await asyncio.to_thread(_sync_logout)
        return build_envelope(
            data=SyncLogoutView(
                status=result.status,
                cleared_auth_sessions=result.cleared_auth_sessions,
            ),
            actions=[
                "Use sync_link(mode='login') to authenticate this profile again.",
            ],
        )
    if institution is None:
        raise UserError(
            "institution is required when mode='institution'.",
            code="SYNC_INSTITUTION_REQUIRED",
        )
    binding: ConfirmationBinding | None = None
    if confirmation_token is None:
        plan = await asyncio.to_thread(_plan_sync_disconnect, institution)
        binding = _sync_disconnect_binding(institution, plan)
    grant: ConfirmationGrant = await grant_confirmation_or_raise(
        binding=binding,
        message=(
            "Permanently disconnect this exact institution from future syncs? "
            "Previously pulled local rows remain."
        ),
        confirmation_token=confirmation_token,
    )

    disconnected = await asyncio.to_thread(
        _disconnect_sync_confirmed,
        institution,
        grant,
    )
    return build_envelope(
        data=SyncInstitutionDisconnectView(
            status="disconnected",
            institution=disconnected.institution_name or institution,
        ),
        actions=["Use sync_status to inspect remaining institution connections."],
    )


def register_sync_workflow_tools(mcp: FastMCP) -> None:
    """Register the standard four-boundary sync workflow."""
    for callback, name, description in (
        (
            sync_link_coarse,
            "sync_link",
            "Start a hosted institution-link or nonblocking device-login session.",
        ),
        (
            sync_status_coarse,
            "sync_status",
            "Read global health, one link session, or advance one device-login session.",
        ),
        (sync_pull_coarse, "sync_pull", "Pull connected financial data."),
        (
            sync_disconnect,
            "sync_disconnect",
            "Disconnect one institution or clear profile-scoped sync credentials.",
        ),
    ):
        register(
            mcp,
            callback,
            name,
            description,
            privacy_actor=name,
        )


def register_sync_tools(mcp: FastMCP) -> None:
    """Register the standard mediated-sync workflow."""
    register_sync_workflow_tools(mcp)
