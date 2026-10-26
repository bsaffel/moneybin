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
from typing import TYPE_CHECKING, Any, Literal, cast

from fastmcp import FastMCP

from moneybin import error_codes
from moneybin.adapters.refresh_adapters import (
    refresh_rate_gap_hints,
)
from moneybin.adapters.rematch_report import retired_transfers_action
from moneybin.adapters.sync_adapters import (
    sync_account_label,
    sync_auth_view,
    sync_link_envelope,
    sync_link_status_actions,
    sync_link_status_envelope,
    sync_pull_envelope,
    sync_repair_actions,
    sync_status_envelope,
)
from moneybin.config import get_settings
from moneybin.connectors.sync_auth import SyncAuthResult, SyncAuthService
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
from moneybin.metrics.registry import SYNC_SERVER_ADDRESS_OUTCOMES
from moneybin.privacy.classified_envelope import build_classified_envelope
from moneybin.privacy.payloads.sync import (
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
from moneybin.services import sync_server_address
from moneybin.services.sync_server_address import AddressPlan

if TYPE_CHECKING:
    from moneybin.services.sync_service import SyncPurgePlan


def _build_sync_client() -> Any:
    """Construct a SyncClient from current settings. Extracted for test mocking."""
    try:
        return sync_server_address.build_sync_client()
    except UserError as e:
        raise _agent_facing(e) from e


_ASK_FOR_ADDRESS = (
    "Ask the user for their moneybin-sync server's address, then call "
    "sync_link(mode='login', server_url=<that address>). Never choose "
    "or guess the address yourself."
)


def _agent_facing(error: UserError) -> UserError:
    """Swap the terminal-command hint for one an agent can act on."""
    if error.code != error_codes.SYNC_SERVER_NOT_CONFIGURED:
        return error
    return UserError(error.message, code=error.code, hint=_ASK_FOR_ADDRESS)


def _build_sync_auth_service() -> Any:
    """Construct profile-scoped nonblocking authentication orchestration."""
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
        data: SyncStatusCoarsePayload = sync_auth_view(result)
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


def _server_address_binding(plan: AddressPlan) -> ConfirmationBinding:
    return ConfirmationBinding(
        arguments={"mode": "login", "server_url": plan.url, "change": plan.change},
        resolved_ids=(plan.url,),
        actor="mcp",
        profile=get_settings().profile,
        authorization_context="local-profile",
        operation_kind="sync_server_address_set",
        blast_radius={"profiles": 1},
    )


def _server_address_message(plan: AddressPlan, *, suggested: bool) -> str:
    if suggested:
        lines = [
            "No sync server is set for this profile. Use the local default, "
            f"{plan.url}? You will be sent there to sign in, and bank linking "
            "runs through it. Confirm only if you run moneybin-sync on this "
            "machine; otherwise decline and give your assistant your server's "
            "address."
        ]
    else:
        lines = [
            f"Use {plan.url} as this profile's sync server? You will be sent "
            "there to sign in, and bank linking runs through it. Confirm only "
            "if you gave this address yourself."
        ]
    if plan.change == "replace":
        lines.append(
            "This replaces the profile's current server and signs it out of that one."
        )
    if plan.unencrypted:
        lines.append("The connection to this address is not encrypted.")
    return " ".join(lines)


async def _begin_login_at(
    server_url: str, *, suggested: bool = False
) -> SyncAuthResult:
    """Begin login at an address, saving it only with consent."""
    plan = await asyncio.to_thread(
        sync_server_address.plan_server_address, server_url, surface="mcp"
    )
    if plan.change != "unchanged":
        try:
            # elicitation_only: a fallback token would go back to the calling
            # agent, which could then approve its own choice of server.
            grant = await grant_confirmation_or_raise(
                binding=_server_address_binding(plan),
                message=_server_address_message(plan, suggested=suggested),
                confirmation_token=None,
                elicitation_only=True,
                cli_equivalent=(
                    "moneybin sync login"
                    if suggested
                    else sync_server_address.LOGIN_COMMAND
                ),
            )
        except UserError as e:
            # A timeout shares the declined code; nobody answered, so the
            # local address was not ruled out.
            timed_out = (e.details or {}).get("reason") == "timeout"
            declined = (
                e.code == error_codes.MUTATION_CONFIRMATION_DECLINED and not timed_out
            )
            outcome = (
                "timed_out"
                if timed_out
                else "declined"
                if declined
                else "prompt_unavailable"
            )
            SYNC_SERVER_ADDRESS_OUTCOMES.labels(surface="mcp", outcome=outcome).inc()
            if declined and suggested:
                # Declining the default means the server is elsewhere.
                raise UserError(
                    e.message, code=e.code, details=e.details, hint=_ASK_FOR_ADDRESS
                ) from e
            raise
        # The prompt can sit open while the CLI saves another address; what
        # the person approved must still be what saving does now.
        plan = await asyncio.to_thread(
            sync_server_address.plan_server_address, server_url, surface="mcp"
        )
        grant.verify(_server_address_binding(plan))
    start = await asyncio.to_thread(
        sync_server_address.start_login, plan, surface="mcp"
    )
    return await asyncio.to_thread(
        SyncAuthService(client=start.client).begin, challenge=start.challenge
    )


@mcp_tool(read_only=False, idempotent=False, open_world=True)
async def sync_link_coarse(
    institution: str | None = None,
    provider: Literal["plaid", "simplefin"] | None = None,
    connection_id: str | None = None,
    mode: Literal["institution", "login"] = "institution",
    server_url: str | None = None,
) -> ResponseEnvelope[SyncLinkCoarsePayload]:
    """Start an institution-link or nonblocking device-login session."""
    if server_url is not None and mode != "login":
        raise UserError(
            "server_url is valid only when mode='login'.",
            code=error_codes.SYNC_LINK_MODE_CONFLICT,
        )
    if mode == "login":
        if institution is not None or provider is not None or connection_id is not None:
            raise UserError(
                "Institution selectors are valid only when mode='institution'.",
                code=error_codes.SYNC_LINK_MODE_CONFLICT,
            )
        if server_url is not None:
            result = await _begin_login_at(server_url)
        else:
            try:
                result = await asyncio.to_thread(
                    lambda: _build_sync_auth_service().begin()
                )
            except UserError as e:
                if e.code != error_codes.SYNC_SERVER_NOT_CONFIGURED:
                    raise
                result = await _begin_login_at(
                    sync_server_address.SUGGESTED_SERVER_URL, suggested=True
                )
        data: SyncLinkCoarsePayload = sync_auth_view(result)
        return build_envelope(
            data=data,
            actions=[
                "Present verification_url and user_code to the user.",
                f"Then call sync_status(auth_session_id='{result.auth_session_id}') "
                "after the user completes authorization.",
            ],
        )
    response = await asyncio.to_thread(
        sync_link,
        institution=institution,
        provider=provider,
        connection_id=connection_id,
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
    account_identities = sorted({account.identity for account in connection.accounts})
    return ConfirmationBinding(
        arguments={
            "mode": "institution",
            "provider_item_id": connection.provider_item_id,
            "institution_name": connection.institution_name,
            "provider": connection.provider,
            "status": connection.status,
            "credential_generation": connection.credential_generation,
            "account_identities": [list(identity) for identity in account_identities],
        },
        resolved_ids=(connection.id, connection.provider_item_id or connection.id),
        actor="mcp",
        profile=get_settings().profile,
        authorization_context="local-profile",
        operation_kind="sync_disconnect_institution",
        blast_radius={"institutions": 1, "accounts": len(account_identities)},
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


def _sync_purge_binding(plan: SyncPurgePlan) -> ConfirmationBinding:
    """Bind a purge to the exact item, its live connection, and every count.

    The counts are part of the binding, so a pull that lands between the
    preview and the confirm — adding rows the user never saw quoted — changes
    the digest and the confirm is refused rather than deleting more than it
    described. The fields are exactly ``SyncPurgePlan.binding``'s, so the CLI
    and MCP surfaces bind one purge to the same facts and volatile connection
    metadata (``status``, ``last_sync``) cannot spuriously refuse a confirm.
    """
    connection = plan.connection
    return ConfirmationBinding(
        arguments={
            "mode": "institution",
            "state": "absent",
            "provider_item_id": plan.provider_item_id,
            "connection_id": None if connection is None else connection.id,
            "credential_generation": (
                None if connection is None else connection.credential_generation
            ),
            "account_identities": (
                []
                if connection is None
                else [
                    list(identity)
                    for identity in sorted({
                        account.identity for account in connection.accounts
                    })
                ]
            ),
            "table_counts": [[name, int(count)] for name, count in plan.table_counts],
            "account_link_ids": list(plan.account_link_ids),
            "decision_ids": list(plan.decision_ids),
        },
        resolved_ids=(plan.provider_item_id,),
        actor="mcp",
        profile=get_settings().profile,
        authorization_context="local-profile",
        operation_kind="sync_disconnect_absent",
        blast_radius=plan.blast_radius,
    )


def _sync_purge_message(plan: SyncPurgePlan) -> str:
    """Name the item, the server half, and the exact local rows a purge removes."""
    connection = plan.connection
    local = (
        f"delete its {plan.rows_to_delete} locally pulled rows, reverse "
        f"{len(plan.account_link_ids)} account links, and withdraw "
        f"{len(plan.decision_ids)} merge proposals? This is permanent."
    )
    if connection is None:
        return (
            f"The sync server no longer lists provider_item_id="
            f"{plan.provider_item_id}. Permanently {local}"
        )
    name = connection.institution_name or "this institution"
    linked = connection.created_at.date().isoformat()
    return (
        f"Permanently disconnect {name} (provider_item_id="
        f"{plan.provider_item_id}, linked {linked}) and {local}"
    )


def _plan_sync_purge(
    institution: str | None, provider_item_id: str | None, connection_id: str | None
) -> SyncPurgePlan:
    """Resolve one purge target outside the MCP event loop."""
    with _build_sync_service() as service:
        return service.plan_purge(
            institution=institution,
            provider_item_id=provider_item_id,
            connection_id=connection_id,
        )


def _purge_sync_confirmed(
    institution: str | None,
    provider_item_id: str | None,
    grant: ConfirmationGrant,
    connection_id: str | None,
) -> SyncPurgePlan:
    """Re-plan, verify, disconnect, and purge outside the event loop."""

    def verify(live: SyncPurgePlan) -> None:
        grant.verify(_sync_purge_binding(live))

    with _build_sync_service() as service:
        return service.purge_confirmed(
            institution=institution,
            provider_item_id=provider_item_id,
            connection_id=connection_id,
            verify=verify,
            actor="mcp",
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
    state: Literal["disconnected", "absent"] = "disconnected",
    confirmation_token: str | None = None,
) -> ResponseEnvelope[SyncDisconnectCoarsePayload]:
    """Disconnect one institution connection or clear scoped sync credentials.

    Institution disconnect is permanent on moneybin-sync; with the default
    ``state="disconnected"`` local pulled rows remain. ``state="absent"``
    is supported only for Plaid and deletes every locally pulled row of the item,
    reverses its account links, and withdraws merge proposals only its accounts
    describe — and,
    by ``provider_item_id``, reaches an item the server no longer lists.
    Pass `provider_item_id` (from sync_status) to target one exact
    connection — required when an institution has more than one (e.g. after
    a relink), where `institution` alone is ambiguous and gets refused.
    Pass exactly one of `institution`, `provider_item_id`, or `connection_id`.
    SimpleFIN disconnect keeps local history; its purge is refused. Logout
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
                "institution, provider_item_id, and connection_id are valid only when "
                "mode='institution'.",
                code=error_codes.SYNC_DISCONNECT_MODE_CONFLICT,
            )
        if state != "disconnected":
            raise UserError(
                "state='absent' is valid only when mode='institution'.",
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
            "institution, provider_item_id, or connection_id is required when mode='institution'.",
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
            "institution, provider_item_id, and connection_id are mutually exclusive — "
            "pass exactly one",
            code=error_codes.MUTATION_INVALID_INPUT,
        )
    if state == "absent":
        return await _sync_disconnect_absent(
            institution, provider_item_id, confirmation_token, connection_id
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


async def _sync_disconnect_absent(
    institution: str | None,
    provider_item_id: str | None,
    confirmation_token: str | None,
    connection_id: str | None,
) -> ResponseEnvelope[SyncDisconnectCoarsePayload]:
    """The state='absent' branch: confirm the exact purge, then apply it."""
    binding: ConfirmationBinding | None = None
    message = (
        "Permanently disconnect this connection and delete every locally "
        "pulled row it owns?"
    )
    if confirmation_token is None:
        plan = await asyncio.to_thread(
            _plan_sync_purge, institution, provider_item_id, connection_id
        )
        binding = _sync_purge_binding(plan)
        message = _sync_purge_message(plan)
    grant: ConfirmationGrant = await grant_confirmation_or_raise(
        binding=binding,
        message=message,
        confirmation_token=confirmation_token,
    )
    live = await asyncio.to_thread(
        _purge_sync_confirmed, institution, provider_item_id, grant, connection_id
    )
    connection = live.connection
    remote = live.remote_result
    actions = [
        "Use sync_status to inspect remaining institution connections.",
    ]
    if remote is not None and connection is not None:
        from moneybin.adapters.sync_adapters import sync_disconnect_actions

        actions.extend(
            sync_disconnect_actions(connection.provider, remote.remote_revocation)
        )
    return build_envelope(
        data=SyncInstitutionDisconnectView(
            status="purged",
            institution=None if connection is None else connection.institution_name,
            provider_item_id=live.provider_item_id,
            connection_id=None if remote is None else str(remote.connection_id),
            remote_revocation=None if remote is None else remote.remote_revocation,
            affected_accounts=(
                []
                if remote is None
                else [sync_account_label(account.name) for account in remote.accounts]
            ),
            purged=True,
            rows_deleted=live.rows_to_delete,
            account_links_reversed=len(live.account_link_ids),
        ),
        actions=actions,
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
            "logs or summaries. In login mode, server_url names this profile's "
            "sync server: pass only an address the user gave you; omit it and, "
            "if none is saved, the user is offered "
            f"{sync_server_address.SUGGESTED_SERVER_URL}. A new or "
            "changed address is saved to the profile's config.yaml only after "
            "the user confirms a prompt, and signs the profile out of the "
            "previous server; log in with another address to change it back.",
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
            "Choose exactly one of institution, provider_item_id, or connection_id "
            "from sync_status; institution is refused when ambiguous. "
            "state='absent' is Plaid-only: delete the item's locally pulled raw "
            "rows and broker-carried prices, reverse its account links, and "
            "withdraw merge proposals only its accounts describe. "
            "provider_item_id also reaches a Plaid item the server no longer "
            "lists. SimpleFIN purge is refused; ordinary disconnect retains "
            "local history and may require revocation in SimpleFIN Bridge. "
            "Purge confirmation binds the connection, credential generation, "
            "full account identities, and local deletion scope; a disconnect "
            "confirmation cannot authorize purge. ",
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
