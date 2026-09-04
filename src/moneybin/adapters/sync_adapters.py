"""Adapters that turn sync service results into typed payload envelopes.

`sync_pull` maps twenty-five fields of `PullResult` one-for-one. Spelled twice —
once per surface — a field added to the result reaches whichever surface the
author was looking at, and the other silently reports a partially-failed pull
as a clean success. Written once here so both read the same answer.

Pure: no I/O, no side-effects. `actions` stay with the caller, because MCP names
tools and the CLI names commands.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from moneybin.privacy.payloads.sync import (
    SyncConnectionRow,
    SyncDisconnectPayload,
    SyncLinkPayload,
    SyncLinkStatusPayload,
    SyncPullInstitutionRow,
    SyncPullPayload,
    SyncStatusPayload,
)
from moneybin.protocol.envelope import ResponseEnvelope, build_envelope
from moneybin.services.refresh_outcome import refresh_steps_fields

if TYPE_CHECKING:
    from collections.abc import Sequence

    from moneybin.connectors.sync_models import (
        LinkInitiateResponse,
        LinkStatusResponse,
        PullResult,
        SyncConnectionView,
    )
    from moneybin.errors import RecoveryAction


def sync_pull_payload(result: PullResult) -> SyncPullPayload:
    """Project one pull result, refresh steps flattened onto the top level.

    Flattened rather than left in the nested field it travels in, so the four
    best-effort refresh steps read exactly as `refresh_envelope` spells them on
    every other surface. The carrier is internal transport; this is public.
    """
    return SyncPullPayload(
        job_id=result.job_id,
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
        error_code=connection.error_code,
        guidance=connection.guidance,
    )


def sync_status_envelope(
    connections: Sequence[SyncConnectionView],
    *,
    actions: list[str],
) -> ResponseEnvelope[SyncStatusPayload]:
    """Wrap the connected-institution list."""
    return build_envelope(
        data=SyncStatusPayload(
            connections=[sync_connection_row(c) for c in connections]
        ),
        actions=actions,
    )


def sync_disconnect_envelope(
    *,
    institution: str,
    actions: list[str],
) -> ResponseEnvelope[SyncDisconnectPayload]:
    """Wrap the confirmation that one institution was disconnected."""
    return build_envelope(
        data=SyncDisconnectPayload(status="disconnected", institution=institution),
        actions=actions,
    )
