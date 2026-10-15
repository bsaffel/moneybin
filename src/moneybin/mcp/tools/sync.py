"""sync_* MCP tools — Phase 1 implementations.

Per docs/specs/sync-plaid.md. The client speaks only moneybin-sync's public API.

Device authentication is a nonblocking variant of sync_link + sync_status;
logout is the credential-state variant of sync_disconnect. sync_key_rotate
remains CLI-only because it handles passphrase material.
"""

from __future__ import annotations

import asyncio
from collections.abc import Generator
from contextlib import contextmanager
from typing import Any, Literal, cast

from fastmcp import FastMCP

from moneybin import error_codes
from moneybin.adapters.refresh_adapters import (
    refresh_rate_gap_hints,
    refresh_step_actions,
)
from moneybin.adapters.rematch_report import retired_transfers_action
from moneybin.adapters.sync_adapters import (
    sync_account_label,
    sync_link_envelope,
    sync_link_status_actions,
    sync_link_status_envelope,
    sync_pull_envelope,
    sync_repair_actions,
    sync_status_envelope,
)
from moneybin.config import get_settings
from moneybin.connectors.sync_models import (
    ConnectedInstitution,
    PullResult,
    SyncDisconnectResult,
)
from moneybin.errors import UserError
from moneybin.mcp._registration import register
from moneybin.mcp.confirmation import (
    ConfirmationBinding,
    ConfirmationGrant,
    grant_confirmation_or_raise,
)
from moneybin.mcp.decorator import mcp_tool
from moneybin.privacy.classified_envelope import build_classified_envelope
from moneybin.privacy.payloads.sync import (
    SyncAuthView,
    SyncDisconnectCoarsePayload,
    SyncGlobalStatusView,
    SyncInstitutionDisconnectView,
    SyncInstitutionLinkView,
    SyncLinkCoarsePayload,
    SyncLinkPayload,
    SyncLinkStatusPayload,
    SyncLogoutView,
    SyncPullPayload,
    SyncSessionStatusView,
    SyncStatusCoarsePayload,
    SyncStatusPayload,
)
from moneybin.privacy.sensitivity import Sensitivity
from moneybin.protocol.envelope import (
    ResponseEnvelope,
    build_envelope,
)


def _build_sync_client() -> Any:
    """Construct a SyncClient from current settings.

    Mirrors the CLI builder (``cli/commands/sync.py``) exactly — including the
    per-profile identity — so MCP and CLI read and write the same scoped token
    slot. (The two builders are duplicated; consolidating them is tracked as a
    follow-up.)
    """
    from moneybin.config import get_settings
    from moneybin.connectors.sync_client import SyncClient
    from moneybin.utils.user_config import get_or_create_profile_id

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
    from moneybin.connectors.sync_auth import SyncAuthService

    return SyncAuthService(client=_build_sync_client())


@contextmanager
def _build_sync_service(*, local_only: bool = False) -> Generator[Any, None, None]:
    """Context manager yielding a SyncService with active Database connection."""
    from moneybin.config import get_settings
    from moneybin.database import get_database
    from moneybin.extractors.plaid import PlaidExtractor
    from moneybin.services.sync_service import SyncService
    from moneybin.utils.user_config import get_or_create_profile_id

    client = None if local_only else _build_sync_client()
    consumer_id = (
        get_or_create_profile_id(get_settings().profile_dir) if local_only else None
    )
    with get_database(read_only=False) as db:
        loader = PlaidExtractor(db)
        yield SyncService(client=client, db=db, loader=loader, consumer_id=consumer_id)


def sync_pull(
    institution: str | None = None,
    force: bool = False,
    refresh: bool = True,
    provider: str | None = None,
    connection_id: str | None = None,
    history_start: str | None = None,
    job_id: str | None = None,
    local_only: bool = False,
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
    from moneybin.services.sync_service import validate_pull_selectors

    history = validate_pull_selectors(
        institution=institution,
        provider=provider,
        connection_id=connection_id,
        history_start=history_start,
        job_id=job_id,
        local_only=local_only,
        force=force,
    )
    with _build_sync_service(local_only=local_only) as service:
        if job_id is not None:
            result = service.recover(job_id, local_only=local_only, refresh=refresh)
        else:
            result = service.pull(
                institution=institution,
                provider=provider,
                connection_id=connection_id,
                history_start=history,
                force=force,
                refresh=refresh,
            )
    return sync_pull_envelope(
        result,
        actions=_pull_actions(result),
        # `or None` to omit the key when empty, matching `refresh_envelope`:
        # this envelope's `error` is None, so there is no `error.recovery_actions`
        # for an empty list to fall through to.
        recovery_actions=refresh_step_actions(
            result.refresh_steps, apply_failed=result.transforms_error is not None
        )
        or None,
    )


def _pull_actions(result: PullResult) -> list[str]:
    """Next-step hints, including every partial-failure the CLI surfaces.

    The CLI warns and exits non-zero on ``security_resolution_error``; MCP has no
    exit code, so the equivalent signal has to be prose the agent will actually
    read — hence a leading, unambiguous action rather than a quiet payload field.
    """
    actions = sync_repair_actions(result)
    if result.security_resolution_error:
        actions.append(
            "WARNING: security resolution failed — this pull's investment "
            "transactions are NOT attributed to securities, so cost basis, "
            "holdings and realized gains are incomplete. Raw data already "
            "landed; use the job-specific recovery action above. Report this to the "
            "user rather than reporting a successful sync."
        )
    if result.transforms_error:
        actions.append(
            "WARNING: transforms failed — raw rows landed but core.* models are "
            "stale. Retry with refresh_run."
        )
    # A pull runs the full refresh, so its match step can reverse a transfer the
    # user accepted. Ahead of the routine review hints below: those are work the
    # user chose, this is a decision of theirs that was undone.
    if retired := retired_transfers_action(result.transfers_retired, operation="sync"):
        actions.append(retired)
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
            "double-count until one source is chosen per account with "
            "accounts_set(investment_source_type=...); "
            "system_status(sections=['doctor']) shows each source's trade counts."
        )
    actions.extend(refresh_rate_gap_hints(result.refresh_steps))
    actions.append("Use sync_status to see connection health going forward.")
    return actions


def sync_status() -> ResponseEnvelope[SyncStatusPayload]:
    """Connected institutions, last-sync times, and error-state guidance."""
    with _build_sync_service() as service:
        connections = service.list_connections()
        recovery_jobs = service.recovery_jobs()
    return sync_status_envelope(connections, actions=[], recovery_jobs=recovery_jobs)


def sync_link(
    institution: str | None = None,
    provider: Literal["plaid", "simplefin"] | None = None,
    connection_id: str | None = None,
) -> ResponseEnvelope[SyncLinkPayload]:
    """Start a browser-mediated provider link for a new or exact existing grant."""
    with _build_sync_service() as service:
        initiate = service.initiate_link(
            institution=institution, provider=provider, connection_id=connection_id
        )
    return sync_link_envelope(
        initiate,
        actions=[
            "Present link_url to the user and ask them to complete the connection in their browser.",
            "Keep the SimpleFIN setup token in the broker browser form; never paste it into chat.",
            f"After completion, call sync_status(session_id='{initiate.session_id}') to verify.",
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
    return sync_link_status_envelope(status, actions=sync_link_status_actions(status))


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
            code=error_codes.SYNC_STATUS_MODE_CONFLICT,
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
        data = SyncGlobalStatusView(
            connections=response.data.connections,
            recovery_jobs=response.data.recovery_jobs,
        )
        actions = list(response.actions)
    else:
        response = sync_link_status(session_id=session_id)
        data = SyncSessionStatusView(
            provider=response.data.provider,
            connection_id=response.data.connection_id,
            action=response.data.action,
            session_id=response.data.session_id,
            status=response.data.status,
            provider_item_id=response.data.provider_item_id,
            institution_name=response.data.institution_name,
            error=response.data.error,
            expiration=response.data.expiration,
        )
        actions = list(response.actions)
    return build_classified_envelope(data, actions=actions)


@mcp_tool(read_only=False, idempotent=False, open_world=True)
def sync_link_coarse(
    institution: str | None = None,
    provider: Literal["plaid", "simplefin"] | None = None,
    connection_id: str | None = None,
    mode: Literal["institution", "login"] = "institution",
) -> ResponseEnvelope[SyncLinkCoarsePayload]:
    """Start an institution-link or nonblocking device-login session."""
    if mode == "login":
        if institution is not None or provider is not None or connection_id is not None:
            raise UserError(
                "Institution selectors are valid only when mode='institution'.",
                code=error_codes.SYNC_LINK_MODE_CONFLICT,
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
    response = sync_link(
        institution=institution, provider=provider, connection_id=connection_id
    )
    if response.error is not None:
        return cast(ResponseEnvelope[SyncLinkCoarsePayload], response)
    return build_envelope(
        data=SyncInstitutionLinkView(
            session_id=response.data.session_id,
            link_url=response.data.link_url,
            expiration=response.data.expiration,
            link_type=response.data.link_type,
        ),
        actions=[
            "Present link_url to the user and ask them to complete the browser flow.",
            f"Then call sync_status(session_id='{response.data.session_id}') to "
            "check completion.",
        ],
    )


@mcp_tool(read_only=False, idempotent=False, open_world=True, timeout_seconds=180.0)
def sync_pull_coarse(
    institution: str | None = None,
    provider: Literal["plaid", "simplefin"] | None = None,
    connection_id: str | None = None,
    history_start: str | None = None,
    force: bool = False,
    refresh: bool = True,
    job_id: str | None = None,
    local_only: bool = False,
) -> ResponseEnvelope[SyncPullPayload]:
    """Pull while keeping recovery actions inside the isolated cohort."""
    return sync_pull(
        institution=institution,
        provider=provider,
        connection_id=connection_id,
        history_start=history_start,
        force=force,
        refresh=refresh,
        job_id=job_id,
        local_only=local_only,
    )


def _sync_disconnect_binding(connection: ConnectedInstitution) -> ConfirmationBinding:
    """Bind confirmation to one exact live remote institution connection.

    Bound on the resolved connection's own stable identity
    (``provider_item_id`` / ``id``), not the caller's raw ``institution``
    input — that stays correct whether the caller targeted by name or by
    item id, and it is what actually changes if the live connection set
    shifts between plan and confirm (see ``ConfirmationGrant.verify``).
    """
    return ConfirmationBinding(
        arguments={
            "mode": "institution",
            "provider_item_id": connection.provider_item_id,
            "institution_name": connection.institution_name,
            "provider": connection.provider,
            "status": connection.status,
            "credential_generation": connection.credential_generation,
            "accounts": [
                account.model_dump(mode="json", by_alias=True)
                for account in connection.accounts
            ],
        },
        resolved_ids=(connection.id, connection.provider_item_id or connection.id),
        actor="mcp",
        profile=get_settings().profile,
        authorization_context="local-profile",
        operation_kind="sync_disconnect_institution",
        blast_radius={"institutions": 1, "accounts": len(connection.accounts)},
    )


def _sync_disconnect_message(connection: ConnectedInstitution) -> str:
    """Name the exact connection a disconnect confirmation would remove.

    Two connections at the same institution (e.g. after a relink) render
    identically without this — the confirm must let the user tell them
    apart rather than trust a bare institution name (design-principles.md
    -> "Magic stays visible").
    """
    name = connection.institution_name or "this institution"
    accounts = (
        ", ".join(sync_account_label(account.name) for account in connection.accounts)
        or "No discovered accounts"
    )
    remote = (
        " Revoke this grant directly in SimpleFIN Bridge afterward."
        if connection.provider == "simplefin"
        else ""
    )
    return (
        f"Permanently disconnect {name} (connection_id={connection.id}, provider_item_id={connection.provider_item_id}, linked {connection.created_at.date().isoformat()})? "
        f"Affected accounts: {accounts}. Previously pulled local rows remain.{remote}"
    )


def _sync_logout() -> Any:
    """Run blocking credential cleanup outside the MCP event loop."""
    return _build_sync_auth_service().logout()


def _plan_sync_disconnect(
    institution: str | None, provider_item_id: str | None, connection_id: str | None
) -> ConnectedInstitution:
    """Resolve one live disconnect target outside the MCP event loop."""
    with _build_sync_service() as service:
        return service.plan_disconnect(
            institution=institution,
            provider_item_id=provider_item_id,
            connection_id=connection_id,
        )


def _disconnect_sync_confirmed(
    institution: str | None,
    provider_item_id: str | None,
    grant: ConfirmationGrant,
    connection_id: str | None,
) -> SyncDisconnectResult:
    """Re-resolve, verify, and delete one connection outside the event loop."""

    def verify(live: ConnectedInstitution) -> None:
        grant.verify(_sync_disconnect_binding(live))

    with _build_sync_service() as service:
        return service.disconnect_confirmed(
            institution=institution,
            provider_item_id=provider_item_id,
            verify=verify,
            connection_id=connection_id,
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
    connection_id: str | None = None,
    provider_item_id: str | None = None,
    mode: Literal["institution", "logout"] = "institution",
    confirmation_token: str | None = None,
) -> ResponseEnvelope[SyncDisconnectCoarsePayload]:
    """Disconnect one institution connection or clear scoped sync credentials.

    Institution disconnect is permanent on moneybin-sync; local pulled rows
    remain. Pass `provider_item_id` (from sync_status) to target one exact
    connection — required when an institution has more than one (e.g. after
    a relink), where `institution` alone is ambiguous and gets refused.
    `institution` and `provider_item_id` are mutually exclusive. Logout
    clears profile-scoped credentials and pending auth sessions but is
    recoverable through ``sync_link(mode="login")``.
    """
    if mode == "logout":
        if (
            institution is not None
            or provider_item_id is not None
            or connection_id is not None
        ):
            raise UserError(
                "institution and provider_item_id are valid only when "
                "mode='institution'.",
                code=error_codes.SYNC_DISCONNECT_MODE_CONFLICT,
            )
        if confirmation_token is not None:
            raise UserError(
                "confirmation_token is valid only when mode='institution'.",
                code=error_codes.SYNC_CONFIRMATION_NOT_ALLOWED,
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
    if institution is None and provider_item_id is None and connection_id is None:
        raise UserError(
            "institution or provider_item_id is required when mode='institution'.",
            code=error_codes.SYNC_INSTITUTION_REQUIRED,
        )
    if (
        sum(
            value is not None
            for value in (institution, provider_item_id, connection_id)
        )
        > 1
    ):
        raise UserError(
            "institution and provider_item_id are mutually exclusive — "
            "pass exactly one",
            code=error_codes.MUTATION_INVALID_INPUT,
        )
    binding: ConfirmationBinding | None = None
    message = (
        "Permanently disconnect this connection? Previously pulled local rows remain."
    )
    if confirmation_token is None:
        plan = await asyncio.to_thread(
            _plan_sync_disconnect, institution, provider_item_id, connection_id
        )
        binding = _sync_disconnect_binding(plan)
        message = _sync_disconnect_message(plan)
    grant: ConfirmationGrant = await grant_confirmation_or_raise(
        binding=binding,
        message=message,
        confirmation_token=confirmation_token,
    )

    disconnected = await asyncio.to_thread(
        _disconnect_sync_confirmed,
        institution,
        provider_item_id,
        grant,
        connection_id,
    )
    from moneybin.adapters.sync_adapters import sync_disconnect_actions

    return build_envelope(
        data=SyncInstitutionDisconnectView(
            status="disconnected",
            institution=disconnected.institution_name,
            provider_item_id=disconnected.provider_item_id,
            connection_id=disconnected.id,
            remote_revocation=disconnected.remote_revocation,
            affected_accounts=[
                sync_account_label(account.name) for account in disconnected.accounts
            ],
        ),
        actions=sync_disconnect_actions(
            disconnected.provider, disconnected.remote_revocation
        ),
    )


def register_sync_workflow_tools(mcp: FastMCP) -> None:
    """Register the standard four-boundary sync workflow."""
    for callback, name, description in (
        (
            sync_link_coarse,
            "sync_link",
            "Start a hosted institution-link or nonblocking device-login session. "
            "Returned link_url and user_code values are sensitive one-time "
            "credentials; present them to the user, but never include them in "
            "logs or summaries.",
        ),
        (
            sync_status_coarse,
            "sync_status",
            "Read global health, one link session, or advance one device-login session.",
        ),
        (
            sync_pull_coarse,
            "sync_pull",
            "Pull connected financial data. The pull runs the refresh cascade, "
            "whose match step can reverse a transfer the user already accepted: "
            "`transfers_retired` counts those, and "
            "system_audit_undo(operation_id=...) restores "
            "them.",
        ),
        (
            sync_disconnect,
            "sync_disconnect",
            "Disconnect one institution connection or clear profile-scoped "
            "sync credentials. Institution disconnect is permanent (no revert); "
            "logout is recoverable via sync_link(mode='login'). "
            "provider_item_id (from sync_status) targets one exact connection "
            "— required when an institution has more than one (e.g. after a "
            "relink), where institution alone is ambiguous. Mutually "
            "exclusive with institution.",
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
