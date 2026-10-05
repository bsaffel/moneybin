"""Typed payload dataclasses for the sync surface.

Each field carries ``Annotated[T, DataClass.X]`` metadata so the Phase 6
middleware can derive sensitivity via ``derive_tier`` without inspecting
tool source code directly.

Tier derivation summary:
  - ``SyncPullInstitutionRow``        → Tier.MEDIUM (error = DESCRIPTION)
  - ``SyncPullPayload``               → Tier.MEDIUM (transforms_error = DESCRIPTION;
                                        contains SyncPullInstitutionRow list)
  - ``SyncConnectionRow``             → Tier.MEDIUM (guidance = DESCRIPTION)
  - ``SyncStatusPayload``             → Tier.MEDIUM (via SyncConnectionRow)
  - ``SyncLinkPayload``               → Tier.MEDIUM (link_url = DESCRIPTION —
                                        link is a sensitive one-time credential)
  - ``SyncLinkStatusPayload``         → Tier.MEDIUM (error = DESCRIPTION)
  - ``SyncDisconnectPayload``         → Tier.MEDIUM (account labels = USER_NOTE)
  - ``SyncSchedulePlaceholderPayload``→ Tier.LOW (stub; not-implemented payloads)
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field

from moneybin.privacy.payloads.system import RefreshStageRow
from moneybin.privacy.taxonomy import DataClass
from moneybin.protocol.row_set import NO_ROW_SET, row_set


@dataclass(frozen=True, slots=True)
class SyncCoverageInterval:
    """One half-open acknowledged UTC interval."""

    start: Annotated[str, DataClass.TIMESTAMP_OBSERVABILITY]
    end: Annotated[str, DataClass.TIMESTAMP_OBSERVABILITY]


@row_set(NO_ROW_SET)
@dataclass(frozen=True, slots=True)
class SyncHistoryAccountRow:
    """History associated with a safe canonical reference or unresolved scope."""

    account_id: Annotated[str | None, DataClass.RECORD_ID]
    display_name: Annotated[str, DataClass.USER_NOTE]
    acknowledged_intervals: list[SyncCoverageInterval]
    history_target: Annotated[str | None, DataClass.TIMESTAMP_OBSERVABILITY]


@row_set(NO_ROW_SET)
@dataclass(frozen=True, slots=True)
class SyncHistoryPayload:
    """Observed credit and pending intent, without a global completion claim."""

    observed_at: Annotated[str, DataClass.TIMESTAMP_OBSERVABILITY]
    replay_required: Annotated[bool, DataClass.TXN_TYPE]
    discovery_history_target: Annotated[str | None, DataClass.TIMESTAMP_OBSERVABILITY]
    accounts: list[SyncHistoryAccountRow]


@dataclass(frozen=True, slots=True)
class SyncScopeRow:
    """Safe retrieval outcome at its original scope."""

    account_id: Annotated[str | None, DataClass.RECORD_ID]
    display_name: Annotated[str, DataClass.USER_NOTE]
    scope_kind: Annotated[str, DataClass.TXN_TYPE]
    status: Annotated[str, DataClass.TXN_TYPE]
    error_code: Annotated[str | None, DataClass.TXN_TYPE]
    window: SyncCoverageInterval | None
    retry_at: Annotated[str | None, DataClass.TIMESTAMP_OBSERVABILITY] = None


@row_set(NO_ROW_SET)
@dataclass(frozen=True, slots=True)
class SyncSourceRow:
    """One grant's retrieval result and independently observed history."""

    provider: Annotated[str, DataClass.INSTITUTION]
    connection_id: Annotated[str, DataClass.RECORD_ID]
    credential_generation: Annotated[int, DataClass.AGGREGATE]
    protocol_version: Annotated[str | None, DataClass.TXN_TYPE]
    history_phase: Annotated[str, DataClass.TXN_TYPE]
    history: SyncHistoryPayload | None
    outcomes: list[SyncScopeRow]


# ---------------------------------------------------------------------------
# sync_pull — per-institution result row
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class SyncPullInstitutionRow:
    """Per-institution outcome inside SyncPullPayload.institutions."""

    provider_item_id: Annotated[str | None, DataClass.RECORD_ID]
    institution_name: Annotated[str | None, DataClass.INSTITUTION]
    status: Annotated[str, DataClass.TXN_TYPE]
    transaction_count: Annotated[int | None, DataClass.AGGREGATE]
    error: Annotated[str | None, DataClass.DESCRIPTION]
    error_code: Annotated[str | None, DataClass.TXN_TYPE]


# ---------------------------------------------------------------------------
# sync_pull — top-level payload
# ---------------------------------------------------------------------------


@row_set("institutions")
@dataclass(frozen=True, slots=True)
class SyncPullPayload:
    """Payload for ``sync_pull`` — pull result envelope.

    The investment fields mirror ``PullResult`` one-for-one: the CLI reports
    them (and exits non-zero on ``security_resolution_error``), so MCP must
    report the same outcome or the agent calls a partially-failed pull a clean
    success. A failed security resolution is not cosmetic — there is no
    source-native fallback for ``security_id`` and ``cost_basis.py`` skips
    every NULL-security event, so the pull's buys/sells silently vanish from
    lots and realized gains.
    """

    job_id: Annotated[str, DataClass.RECORD_ID]
    transactions_loaded: Annotated[int, DataClass.AGGREGATE]
    accounts_loaded: Annotated[int, DataClass.AGGREGATE]
    balances_loaded: Annotated[int, DataClass.AGGREGATE]
    transactions_removed: Annotated[int, DataClass.AGGREGATE]
    institutions: list[SyncPullInstitutionRow]
    transforms_applied: Annotated[bool, DataClass.TXN_TYPE]
    transforms_duration_seconds: Annotated[float | None, DataClass.AGGREGATE]
    transforms_error: Annotated[str | None, DataClass.DESCRIPTION]
    job_status: Annotated[str, DataClass.TXN_TYPE] = "completed"
    ingestion_state: Annotated[str, DataClass.TXN_TYPE] = "committed"
    ack_state: Annotated[str, DataClass.TXN_TYPE] = "pending"
    ack_error_code: Annotated[str | None, DataClass.TXN_TYPE] = None
    setup_state: Annotated[str, DataClass.TXN_TYPE] = "pending"
    setup_error_code: Annotated[str | None, DataClass.TXN_TYPE] = None
    inspection_error_code: Annotated[str | None, DataClass.TXN_TYPE] = None
    refresh_state: Annotated[str, DataClass.TXN_TYPE] = "pending"
    complete: Annotated[bool, DataClass.TXN_TYPE] = False
    sources: list[SyncSourceRow] = field(default_factory=list)
    # Accepted transfers this pull's refresh reversed (AGGREGATE, Tier.LOW).
    transfers_retired: Annotated[int, DataClass.AGGREGATE] = 0
    # What the pull's own refresh did, step by step. Named exactly as
    # `RefreshRunPayload` names them, and built by the same flattener, so an
    # agent reading sync_pull and refresh_run learns one vocabulary.
    # `transforms_error` above covers only a blocking failure (SQLMesh apply,
    # or an investment_match crash that kept apply from running); `stages`
    # carries the steps it cannot report — each one's counts, its error, and
    # whether it ran at all. A currency pair is CURRENCY (Tier.LOW): it names
    # no account and discloses no amount.
    stages: list[RefreshStageRow] = field(default_factory=list)
    identity_errors: Annotated[list[str], DataClass.TXN_TYPE] = field(
        default_factory=list
    )
    rate_pairs_failed: Annotated[list[str], DataClass.CURRENCY] = field(
        default_factory=list
    )
    rate_pairs_unsupported: Annotated[list[str], DataClass.CURRENCY] = field(
        default_factory=list
    )
    rate_pairs_discarded: Annotated[list[str], DataClass.CURRENCY] = field(
        default_factory=list
    )
    securities_loaded: Annotated[int, DataClass.AGGREGATE] = 0
    investment_transactions_loaded: Annotated[int, DataClass.AGGREGATE] = 0
    holdings_loaded: Annotated[int, DataClass.AGGREGATE] = 0
    holding_lots_loaded: Annotated[int, DataClass.AGGREGATE] = 0
    # Closes newly appended to raw.security_prices — 0 on a pull that re-reports
    # observations already stored, which is the steady state between close dates.
    security_prices_loaded: Annotated[int, DataClass.AGGREGATE] = 0
    opening_bootstrap_rows: Annotated[int, DataClass.AGGREGATE] = 0
    # Canonical account ids carrying BOTH manual and Plaid investment history —
    # lots and gains double-count until one source is chosen per account.
    investment_source_overlap_accounts: Annotated[list[str], DataClass.RECORD_ID] = (
        field(default_factory=list)
    )
    # Per-outcome counts: adopted / auto_bound / minted / proposed / pending.
    security_resolution: Annotated[dict[str, int], DataClass.AGGREGATE] = field(
        default_factory=dict
    )
    security_resolution_error: Annotated[str | None, DataClass.DESCRIPTION] = None


# ---------------------------------------------------------------------------
# sync_status — per-connection row
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class SyncConnectionRow:
    """One connected institution in SyncStatusPayload.connections."""

    id: Annotated[str, DataClass.RECORD_ID]
    provider_item_id: Annotated[str | None, DataClass.RECORD_ID]
    institution_name: Annotated[str | None, DataClass.INSTITUTION]
    provider: Annotated[str, DataClass.INSTITUTION]
    status: Annotated[str, DataClass.TXN_TYPE]
    last_sync: Annotated[str | None, DataClass.TIMESTAMP_OBSERVABILITY]
    created_at: Annotated[str, DataClass.TIMESTAMP_OBSERVABILITY]
    error_code: Annotated[str | None, DataClass.TXN_TYPE]
    guidance: Annotated[str | None, DataClass.DESCRIPTION]

    credential_generation: Annotated[int, DataClass.AGGREGATE] = 1
    protocol_version: Annotated[str | None, DataClass.TXN_TYPE] = None
    history_phase: Annotated[str, DataClass.TXN_TYPE] = "unknown"
    history: SyncHistoryPayload | None = None
    history_unavailable_reason: Annotated[str | None, DataClass.TXN_TYPE] = None
    provider_balance_at: Annotated[str | None, DataClass.TIMESTAMP_OBSERVABILITY] = None
    setup_state: Annotated[str, DataClass.TXN_TYPE] = "unknown"
    refresh_state: Annotated[str, DataClass.TXN_TYPE] = "unknown"


# ---------------------------------------------------------------------------
# sync_status — top-level payload
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class SyncRecoveryJobRow:
    """Local durable obligations, without current remote-health assertions."""

    job_id: Annotated[str, DataClass.RECORD_ID]
    ingestion_state: Annotated[str, DataClass.TXN_TYPE]
    ack_state: Annotated[str, DataClass.TXN_TYPE]
    setup_state: Annotated[str, DataClass.TXN_TYPE]
    refresh_state: Annotated[str, DataClass.TXN_TYPE]
    inspection_error_code: Annotated[str | None, DataClass.TXN_TYPE]


@row_set("connections")
@dataclass(frozen=True, slots=True)
class SyncStatusPayload:
    """Payload for ``sync_status`` — list of connected institutions."""

    connections: list[SyncConnectionRow]
    recovery_jobs: list[SyncRecoveryJobRow] = field(default_factory=list)


# ---------------------------------------------------------------------------
# sync_link — initiate link payload
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class SyncLinkPayload:
    """Payload for ``sync_link`` — link URL + session ID.

    ``link_type`` says which flow the URL opens, which decides what the caller
    tells the user to do next: a widget completes in the browser, a token paste
    comes back for the caller to hand over. The CLI reported it before this
    payload existed; MCP had been dropping it.
    """

    session_id: Annotated[str, DataClass.RECORD_ID]
    link_url: Annotated[str, DataClass.DESCRIPTION]
    expiration: Annotated[str, DataClass.TIMESTAMP_OBSERVABILITY]
    link_type: Annotated[Literal["widget_flow", "token_paste"], DataClass.TXN_TYPE] = (
        "widget_flow"
    )


class SyncInstitutionLinkView(BaseModel):
    """One mediated institution-link session."""

    model_config = ConfigDict(frozen=True)

    kind: Annotated[Literal["institution"], DataClass.TXN_TYPE] = "institution"
    session_id: Annotated[str, DataClass.RECORD_ID]
    link_url: Annotated[str, DataClass.DESCRIPTION]
    expiration: Annotated[str, DataClass.TIMESTAMP_OBSERVABILITY]

    link_type: Annotated[Literal["widget_flow", "token_paste"], DataClass.TXN_TYPE] = (
        "widget_flow"
    )


class SyncAuthView(BaseModel):
    """One safe nonblocking device-auth session result."""

    model_config = ConfigDict(frozen=True)

    kind: Annotated[Literal["auth"], DataClass.TXN_TYPE] = "auth"
    auth_session_id: Annotated[str, DataClass.RECORD_ID]
    status: Annotated[str, DataClass.TXN_TYPE]
    user_code: Annotated[str | None, DataClass.DESCRIPTION]
    verification_url: Annotated[str | None, DataClass.DESCRIPTION]
    expiration: Annotated[str, DataClass.TIMESTAMP_OBSERVABILITY]
    replayed: Annotated[bool, DataClass.TXN_TYPE] = False
    error_code: Annotated[str | None, DataClass.TXN_TYPE] = None


SyncLinkCoarsePayload = Annotated[
    SyncInstitutionLinkView | SyncAuthView,
    Field(discriminator="kind"),
]


# ---------------------------------------------------------------------------
# sync_link_status — link session status payload
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class SyncLinkStatusPayload:
    """Payload for ``sync_link_status`` — link session check."""

    session_id: Annotated[str, DataClass.RECORD_ID]
    status: Annotated[str, DataClass.TXN_TYPE]
    provider_item_id: Annotated[str | None, DataClass.RECORD_ID]
    institution_name: Annotated[str | None, DataClass.INSTITUTION]
    error: Annotated[str | None, DataClass.DESCRIPTION]
    expiration: Annotated[str, DataClass.TIMESTAMP_OBSERVABILITY]
    provider: Annotated[str, DataClass.INSTITUTION] = "plaid"
    connection_id: Annotated[str | None, DataClass.RECORD_ID] = None
    action: Annotated[str | None, DataClass.TXN_TYPE] = None


@row_set("connections")
class SyncGlobalStatusView(BaseModel):
    """Global consolidated sync status."""

    model_config = ConfigDict(frozen=True)

    kind: Annotated[Literal["global"], DataClass.TXN_TYPE] = "global"
    connections: list[SyncConnectionRow]
    recovery_jobs: list[SyncRecoveryJobRow] = Field(default_factory=list)


class SyncSessionStatusView(BaseModel):
    """One consolidated link-session status."""

    model_config = ConfigDict(frozen=True)

    kind: Annotated[Literal["session"], DataClass.TXN_TYPE] = "session"
    session_id: Annotated[str, DataClass.RECORD_ID]
    status: Annotated[str, DataClass.TXN_TYPE]
    provider_item_id: Annotated[str | None, DataClass.RECORD_ID]
    institution_name: Annotated[str | None, DataClass.INSTITUTION]
    error: Annotated[str | None, DataClass.DESCRIPTION]
    expiration: Annotated[str, DataClass.TIMESTAMP_OBSERVABILITY]
    provider: Annotated[str, DataClass.INSTITUTION] = "plaid"
    connection_id: Annotated[str | None, DataClass.RECORD_ID] = None
    action: Annotated[str | None, DataClass.TXN_TYPE] = None


SyncStatusCoarsePayload = Annotated[
    SyncGlobalStatusView | SyncSessionStatusView | SyncAuthView,
    Field(discriminator="kind"),
]


# ---------------------------------------------------------------------------
# sync_disconnect — disconnect result payload
# ---------------------------------------------------------------------------


@row_set(NO_ROW_SET)
@dataclass(frozen=True, slots=True)
class SyncDisconnectPayload:
    """Payload for ``sync_disconnect`` — confirmation of disconnection.

    ``status`` is a fixed string (TXN_TYPE), ``institution`` is the
    caller-supplied institution name (INSTITUTION), ``None`` when the
    disconnected connection has no name, and ``provider_item_id`` identifies
    exactly which connection was removed (RECORD_ID) — needed because two
    same-named connections (e.g. after a relink) share the same
    ``institution`` value.
    """

    status: Annotated[str, DataClass.TXN_TYPE]
    institution: Annotated[str | None, DataClass.INSTITUTION]
    provider_item_id: Annotated[str | None, DataClass.RECORD_ID]

    connection_id: Annotated[str | None, DataClass.RECORD_ID] = None
    remote_revocation: Annotated[str | None, DataClass.TXN_TYPE] = None
    affected_accounts: Annotated[list[str], DataClass.USER_NOTE] = field(
        default_factory=list
    )


@row_set(NO_ROW_SET)
class SyncInstitutionDisconnectView(BaseModel):
    """Confirmation that one institution was disconnected."""

    model_config = ConfigDict(frozen=True)

    kind: Annotated[Literal["institution"], DataClass.TXN_TYPE] = "institution"
    status: Annotated[Literal["disconnected"], DataClass.TXN_TYPE]
    institution: Annotated[str | None, DataClass.INSTITUTION]
    provider_item_id: Annotated[str | None, DataClass.RECORD_ID]

    connection_id: Annotated[str | None, DataClass.RECORD_ID] = None
    remote_revocation: Annotated[str | None, DataClass.TXN_TYPE] = None
    affected_accounts: Annotated[list[str], DataClass.USER_NOTE] = Field(
        default_factory=list
    )


class SyncLogoutView(BaseModel):
    """Confirmation that scoped broker credentials were cleared."""

    model_config = ConfigDict(frozen=True)

    kind: Annotated[Literal["auth"], DataClass.TXN_TYPE] = "auth"
    status: Annotated[Literal["logged_out"], DataClass.TXN_TYPE]
    cleared_auth_sessions: Annotated[int, DataClass.AGGREGATE]


SyncDisconnectCoarsePayload = Annotated[
    SyncInstitutionDisconnectView | SyncLogoutView,
    Field(discriminator="kind"),
]


# ---------------------------------------------------------------------------
# sync_schedule_* — stub / not-implemented placeholder payload
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class SyncSchedulePlaceholderPayload:
    """Placeholder payload for stub sync_schedule_* tools.

    These tools are not yet implemented (return not_implemented_envelope).
    The payload keeps the annotation footprint minimal and Tier.LOW so the
    middleware applies the least-restrictive gate to the stub response.
    """

    action: Annotated[str, DataClass.TXN_TYPE]
    status: Annotated[str, DataClass.TXN_TYPE]
