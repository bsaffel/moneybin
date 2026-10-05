"""Adapters that turn sync service results into typed payload envelopes.

`sync_pull` maps twenty-five fields of `PullResult` one-for-one. Spelled twice —
once per surface — a field added to the result reaches whichever surface the
author was looking at, and the other silently reports a partially-failed pull
as a clean success. Written once here so both read the same answer.

Pure: no I/O, no side-effects. `actions` stay with the caller, because MCP names
tools and the CLI names commands.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING

from moneybin.adapters.refresh_adapters import refresh_steps_fields
from moneybin.extractors.account_identity import (
    UNNAMED_ACCOUNT_LABEL,
    mask_embedded_account_number,
)
from moneybin.privacy.payloads.sync import (
    SyncConnectionRow,
    SyncCoverageInterval,
    SyncDisconnectPayload,
    SyncHistoryAccountRow,
    SyncHistoryPayload,
    SyncLinkPayload,
    SyncLinkStatusPayload,
    SyncPullInstitutionRow,
    SyncPullPayload,
    SyncRecoveryJobRow,
    SyncScopeRow,
    SyncSourceRow,
    SyncStatusPayload,
)
from moneybin.protocol.envelope import ResponseEnvelope, build_envelope

if TYPE_CHECKING:
    from collections.abc import Sequence

    from moneybin.connectors.sync_models import (
        LinkInitiateResponse,
        LinkStatusResponse,
        PullResult,
        SyncConnectionView,
        SyncHistoryView,
        SyncSourceView,
    )
    from moneybin.errors import RecoveryAction


def sync_account_label(name: str) -> str:
    """Mask native labels only for display, leaving confirmation identity intact."""
    return mask_embedded_account_number(name.strip()) or UNNAMED_ACCOUNT_LABEL


def _history_payload(history: SyncHistoryView | None) -> SyncHistoryPayload | None:
    if history is None:
        return None
    return SyncHistoryPayload(
        observed_at=history.observed_at.isoformat(),
        replay_required=history.replay_required,
        discovery_history_target=history.discovery_history_target.isoformat()
        if history.discovery_history_target
        else None,
        accounts=[
            SyncHistoryAccountRow(
                # Accepted legacy targets do not prove safe minted-ID provenance.
                account_id=None,
                display_name=account.display_name,
                acknowledged_intervals=[
                    SyncCoverageInterval(
                        start=window.start.isoformat(), end=window.end.isoformat()
                    )
                    for window in account.acknowledged_intervals
                ],
                history_target=account.history_target.isoformat()
                if account.history_target
                else None,
            )
            for account in history.accounts
        ],
    )


def _source_row(source: SyncSourceView) -> SyncSourceRow:
    return SyncSourceRow(
        provider=source.provider,
        connection_id=source.connection_id,
        credential_generation=source.credential_generation,
        protocol_version=source.protocol_version,
        history_phase=source.history_phase,
        history=_history_payload(source.history),
        outcomes=[
            SyncScopeRow(
                # Accepted legacy targets do not prove safe minted-ID provenance.
                account_id=None,
                display_name=outcome.display_name,
                scope_kind=outcome.scope_kind,
                status=outcome.status,
                error_code=outcome.error_code,
                retry_at=outcome.retry_at.isoformat()
                if outcome.retry_at is not None
                else None,
                window=SyncCoverageInterval(
                    start=outcome.window.start.isoformat(),
                    end=outcome.window.end.isoformat(),
                )
                if outcome.window
                else None,
            )
            for outcome in source.outcomes
        ],
    )


def sync_pull_payload(result: PullResult) -> SyncPullPayload:
    """Project one pull result, refresh steps flattened onto the top level.

    Flattened rather than left in the nested field it travels in, so the four
    best-effort refresh steps read exactly as `refresh_envelope` spells them on
    every other surface. The carrier is internal transport; this is public.
    """
    return SyncPullPayload(
        job_id=result.job_id,
        sources=[_source_row(source) for source in result.sources],
        job_status=result.job_status,
        ingestion_state=result.ingestion_state,
        ack_state=result.ack_state,
        ack_error_code=result.ack_error_code,
        setup_state=result.setup_state,
        setup_error_code=result.setup_error_code,
        inspection_error_code=result.inspection_error_code,
        refresh_state=result.refresh_state,
        complete=result.complete,
        transactions_loaded=result.transactions_loaded,
        accounts_loaded=result.accounts_loaded,
        balances_loaded=result.balances_loaded,
        transactions_removed=result.transactions_removed,
        institutions=[
            SyncPullInstitutionRow(
                provider_item_id=inst.provider_item_id,
                institution_name=inst.institution_name,
                status=inst.status,
                transaction_count=inst.transaction_count,
                error=inst.error,
                error_code=inst.error_code,
            )
            for inst in result.institutions
        ],
        transforms_applied=result.transforms_applied,
        transforms_duration_seconds=result.transforms_duration_seconds,
        transforms_error=result.transforms_error,
        transfers_retired=result.transfers_retired,
        securities_loaded=result.securities_loaded,
        investment_transactions_loaded=result.investment_transactions_loaded,
        holdings_loaded=result.holdings_loaded,
        holding_lots_loaded=result.holding_lots_loaded,
        security_prices_loaded=result.security_prices_loaded,
        opening_bootstrap_rows=result.opening_bootstrap_rows,
        investment_source_overlap_accounts=list(
            result.investment_source_overlap_accounts
        ),
        security_resolution=dict(result.security_resolution),
        security_resolution_error=result.security_resolution_error,
        **refresh_steps_fields(result.refresh_steps),
    )


def sync_pull_envelope(
    result: PullResult,
    *,
    actions: list[str],
    recovery_actions: Sequence[RecoveryAction] | None = None,
) -> ResponseEnvelope[SyncPullPayload]:
    """Wrap one pull result."""
    return build_envelope(
        data=sync_pull_payload(result),
        actions=actions,
        recovery_actions=list(recovery_actions) if recovery_actions else None,
    )


def sync_link_envelope(
    initiate: LinkInitiateResponse,
    *,
    actions: list[str],
) -> ResponseEnvelope[SyncLinkPayload]:
    """Wrap one initiated link session."""
    return build_envelope(
        data=SyncLinkPayload(
            session_id=initiate.session_id,
            link_url=initiate.link_url,
            expiration=initiate.expiration.isoformat(),
            link_type=initiate.link_type,
        ),
        actions=actions,
    )


def sync_link_status_envelope(
    status: LinkStatusResponse,
    *,
    actions: list[str],
) -> ResponseEnvelope[SyncLinkStatusPayload]:
    """Wrap one link-session status check."""
    return build_envelope(
        data=SyncLinkStatusPayload(
            session_id=status.session_id,
            provider=status.provider,
            connection_id=status.connection_id,
            action=status.action,
            status=status.status,
            provider_item_id=status.provider_item_id,
            institution_name=status.institution_name,
            error=status.error,
            expiration=status.expiration.isoformat(),
        ),
        actions=actions,
    )


def sync_connection_row(connection: SyncConnectionView) -> SyncConnectionRow:
    """Project one connected institution's health."""
    return SyncConnectionRow(
        id=connection.id,
        provider_item_id=connection.provider_item_id,
        institution_name=connection.institution_name,
        provider=connection.provider,
        status=connection.status,
        last_sync=(connection.last_sync.isoformat() if connection.last_sync else None),
        created_at=connection.created_at.isoformat(),
        error_code=connection.error_code,
        guidance=connection.guidance,
        credential_generation=connection.credential_generation,
        protocol_version=connection.protocol_version,
        history_phase=connection.history_phase,
        history=_history_payload(connection.safe_history),
        history_unavailable_reason=connection.history_unavailable_reason,
        provider_balance_at=connection.provider_balance_at.isoformat()
        if connection.provider_balance_at
        else None,
        setup_state=connection.setup_state,
        refresh_state=connection.refresh_state,
    )


def sync_status_envelope(
    connections: Sequence[SyncConnectionView],
    *,
    actions: list[str],
    recovery_jobs: Sequence[PullResult] = (),
    cli: bool = False,
) -> ResponseEnvelope[SyncStatusPayload]:
    """Wrap current grants and independent durable local recovery obligations."""
    return build_envelope(
        data=SyncStatusPayload(
            connections=[sync_connection_row(c) for c in connections],
            recovery_jobs=[
                SyncRecoveryJobRow(
                    job_id=job.job_id,
                    ingestion_state=job.ingestion_state,
                    ack_state=job.ack_state,
                    setup_state=job.setup_state,
                    refresh_state=job.refresh_state,
                    inspection_error_code=job.inspection_error_code,
                )
                for job in recovery_jobs
            ],
        ),
        actions=[
            *actions,
            *(
                action
                for job in recovery_jobs
                for action in sync_job_recovery_actions(job, cli=cli)
            ),
        ],
    )


def sync_disconnect_actions(provider: str, remote_revocation: str) -> list[str]:
    """Direct any remaining remote revocation to the provider that held access."""
    if remote_revocation != "user_action_required":
        return ["No remote revocation action required."]
    if provider == "simplefin":
        return [
            "Revoke this grant directly in SimpleFIN Bridge to finish remote revocation."
        ]
    return [
        "Remote revocation was not confirmed. Remove the linked access through Plaid or your institution's connected-app settings; contact the institution if needed."
    ]


def sync_disconnect_envelope(
    *,
    institution: str | None,
    provider_item_id: str | None,
    actions: list[str],
    connection_id: str | None = None,
    remote_revocation: str | None = None,
    affected_accounts: list[str] | None = None,
) -> ResponseEnvelope[SyncDisconnectPayload]:
    """Wrap the confirmation that one institution was disconnected."""
    return build_envelope(
        data=SyncDisconnectPayload(
            status="disconnected",
            connection_id=connection_id,
            remote_revocation=remote_revocation,
            affected_accounts=affected_accounts or [],
            institution=institution,
            provider_item_id=provider_item_id,
        ),
        actions=actions,
    )


def sync_job_recovery_actions(result: PullResult, *, cli: bool = False) -> list[str]:
    """Name the existing same-job acknowledgement and strictly local repairs."""
    job = (
        f"moneybin sync pull --job-id {result.job_id}"
        if cli
        else f"sync_pull(job_id='{result.job_id}')"
    )
    local = (
        f"{job} --local-only"
        if cli
        else f"sync_pull(job_id='{result.job_id}', local_only=true)"
    )
    actions: list[str] = []
    if result.ack_state in {"pending", "unknown"}:
        actions.append(
            f"Ingestion committed; acknowledgement is {result.ack_state}. Use {job} to check status/ack without provider retrieval."
        )
    if (
        result.setup_state != "complete"
        or result.refresh_state != "complete"
        or result.inspection_error_code is not None
    ):
        actions.append(
            f"Ingestion committed; local setup={result.setup_state}, refresh={result.refresh_state}. Use {local} for local repair without broker access."
        )
    return actions


def sync_repair_actions(result: PullResult, *, cli: bool = False) -> list[str]:
    """Name separately resumable acknowledgement, local work and new retrieval."""
    job = (
        f"moneybin sync pull --job-id {result.job_id}"
        if cli
        else f"sync_pull(job_id='{result.job_id}')"
    )
    local = (
        f"{job} --local-only"
        if cli
        else f"sync_pull(job_id='{result.job_id}', local_only=true)"
    )
    actions = sync_job_recovery_actions(result, cli=cli)
    now = datetime.now(UTC)
    waiting = {
        source.connection_id: max(deadlines)
        for source in result.sources
        if (
            deadlines := [
                outcome.retry_at
                for outcome in source.outcomes
                if outcome.retry_at is not None and outcome.retry_at > now
            ]
        )
    }
    for connection_id, retry_at in waiting.items():
        actions.append(
            f"Wait until {retry_at.isoformat()} before requesting another pull for connection {connection_id}."
        )
    if (
        result.ack_state == "unavailable"
        or result.job_status in {"partial", "failed"}
        or any(
            outcome.status != "complete"
            for source in result.source_results
            for outcome in source.outcomes
        )
    ):
        actions.append(
            "The original job cannot repair unavailable acknowledgement or missing upstream retrieval. Committed ingestion is preserved; choose a new pull explicitly."
        )
        for source in result.sources:
            if source.connection_id in waiting:
                continue
            command = (
                f"moneybin sync pull --connection-id {source.connection_id}"
                if cli
                else f"sync_pull(connection_id='{source.connection_id}')"
            )
            actions.append(
                f"Use {command} to request a new job for this connection; inspect sync status and relink first if credentials need attention."
            )
    for source in result.sources:
        if source.provider != "simplefin":
            continue
        if source.history is None:
            actions.append(
                "History coverage is unknown; inspect sync status. This is not confirmed complete history."
            )
        elif source.connection_id not in waiting and (
            source.history.replay_required
            or source.history.discovery_history_target is not None
            or any(
                account.history_target is not None
                or len(account.acknowledged_intervals) != 1
                for account in source.history.accounts
            )
        ):
            command = (
                f"moneybin sync pull --connection-id {source.connection_id}"
                if cli
                else f"sync_pull(connection_id='{source.connection_id}')"
            )
            actions.append(
                f"History work remains for this connection. Use {command} for the next explicitly requested retrieval; acknowledged intervals do not certify global completeness."
            )
        if source.history is not None and any(
            account.account_id is None for account in source.history.accounts
        ):
            actions.append(
                f"Unresolved account history remains visible. Use {local}, then review account identity and settings."
            )
    return actions


def sync_link_status_actions(
    status: LinkStatusResponse, *, cli: bool = False
) -> list[str]:
    """Keep an uncertain one-time claim distinct from retryable browser work."""
    if (
        status.status == "claim_outcome_unknown"
        or status.action == "disable_token_and_relink"
    ):
        return [
            "Claim outcome is unknown or this token cannot be reused. Disable the token directly in SimpleFIN Bridge, then start a new link with a fresh token. Do not retry the original claim."
        ]
    if status.status in {"pending", "claiming"}:
        command = (
            f"moneybin sync link-status --session-id {status.session_id}"
            if cli
            else f"sync_status(session_id='{status.session_id}')"
        )
        return [
            f"Complete the existing browser flow and use {command} to inspect this session. Do not submit the setup token again while the claim is pending."
        ]
    if status.status == "linked":
        if status.connection_id:
            command = (
                f"moneybin sync pull --connection-id {status.connection_id}"
                if cli
                else f"sync_pull(connection_id='{status.connection_id}')"
            )
        else:
            command = "moneybin sync pull" if cli else "sync_pull()"
        return [f"Use {command} to retrieve this linked connection."]
    return [
        "The link failed. Inspect the broker browser guidance and start a new link when ready; never paste a setup token into chat."
    ]
