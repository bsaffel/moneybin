"""sync_* MCP tools — Phase 1 implementations.

Per docs/specs/2026-05-13-plaid-sync-design.md Section 11.

Excluded from MCP (CLI-only): sync_login, sync_logout (browser interaction +
credential handling). sync_key_rotate (Phase 3 stub; passphrase material is
CLI-only by convention).
"""

from __future__ import annotations

import logging
from collections.abc import Generator
from contextlib import contextmanager
from dataclasses import replace
from typing import TYPE_CHECKING, Any, cast

from fastmcp import FastMCP

from moneybin.errors import UserError
from moneybin.mcp._registration import register
from moneybin.mcp.decorator import mcp_tool
from moneybin.mcp.privacy import tier_to_sensitivity
from moneybin.privacy.introspection import extract_data_classes
from moneybin.privacy.payloads.sync import (
    SyncConnectionRow,
    SyncDisconnectPayload,
    SyncGlobalStatusView,
    SyncLinkPayload,
    SyncLinkStatusPayload,
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
    from moneybin.connectors.sync_models import PullResult

logger = logging.getLogger(__name__)


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


@mcp_tool(dynamic_classification=True)
def sync_status_coarse(
    session_id: str | None = None,
) -> ResponseEnvelope[SyncStatusCoarsePayload]:
    """Read global connection health or one link-session status."""
    if session_id is None:
        response = sync_status()
        data: SyncStatusCoarsePayload = SyncGlobalStatusView(
            connections=response.data.connections
        )
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
) -> ResponseEnvelope[SyncLinkPayload]:
    """Start a link session using only isolated workflow actions."""
    response = sync_link(institution=institution)
    if response.error is not None:
        return response
    return replace(
        response,
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
    response = sync_pull(institution=institution)
    return replace(
        response,
        actions=[
            "Use sync_status to inspect connection health and decide whether "
            "another pull is needed."
        ],
    )


@mcp_tool(
    read_only=False,
    destructive=True,
    idempotent=False,
    open_world=True,
)
def sync_disconnect(institution: str) -> ResponseEnvelope[SyncDisconnectPayload]:
    """Remove a bank connection on moneybin-sync. Permanent — no revert path.

    Local pulled transactions are preserved in raw.plaid_* and core.fct_transactions;
    the institution simply stops appearing in sync_status and can no longer be
    sync_pull'd. No local app.* state is mutated — connection state lives on the
    server per design Section 4.
    """
    with _build_sync_service() as service:
        service.disconnect(institution=institution)
    return build_envelope(
        data=SyncDisconnectPayload(status="disconnected", institution=institution),
        actions=[],
    )


SYNC_REVIEW_PROMPT = """\
Review my MoneyBin sync state and flag anything that needs attention.

Use these tools (in order):
1. sync_status — list connected institutions with last sync time, status, and any error guidance.
2. spending_summary detail=summary — optional, for context on recent transaction volume per institution.

Report concisely (bulleted, single paragraph if everything is healthy):

- **Errors:** any institutions with status='error' and the specific re-auth or reconnect action — quote the exact command from the actions hint.
- **Stale data:** any institution whose last_sync is more than 7 days ago, even if status='active'. Suggest running `moneybin sync pull`.
- **Anomalies:** institutions whose recent sync transaction counts are dramatically lower than typical volume (use spending_summary as a rough yardstick — a checking account that's been returning ~30/week suddenly returning 0 is worth flagging).
- **Recommended next action:** one specific command, or "no action needed."

Do not include account numbers, balances, individual transaction descriptions, or merchant names. Stick to counts, dates, status codes, and institution names.
"""


def register_sync_prompts(mcp: FastMCP) -> None:
    """Register sync-related FastMCP prompts."""

    @mcp.prompt(
        name="sync_review", description="Review sync health and suggest next steps."
    )
    def _sync_review() -> str:  # type: ignore[reportUnusedFunction]
        return SYNC_REVIEW_PROMPT


def register_sync_workflow_tools(mcp: FastMCP) -> None:
    """Register the standard four-boundary sync workflow."""
    for callback, name, description in (
        (sync_link_coarse, "sync_link", "Start a hosted bank-link session."),
        (
            sync_status_coarse,
            "sync_status",
            "Read global health or one link-session status.",
        ),
        (sync_pull_coarse, "sync_pull", "Pull connected financial data."),
        (sync_disconnect, "sync_disconnect", "Disconnect one institution."),
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
