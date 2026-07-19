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
  - ``SyncDisconnectPayload``         → Tier.LOW (INSTITUTION + TXN_TYPE only)
  - ``SyncSchedulePlaceholderPayload``→ Tier.LOW (stub; not-implemented payloads)
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field

from moneybin.privacy.taxonomy import DataClass

# ---------------------------------------------------------------------------
# sync_pull — per-institution result row
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class SyncPullInstitutionRow:
    """Per-institution outcome inside SyncPullPayload.institutions."""

    provider_item_id: Annotated[str, DataClass.RECORD_ID]
    institution_name: Annotated[str | None, DataClass.INSTITUTION]
    status: Annotated[str, DataClass.TXN_TYPE]
    transaction_count: Annotated[int | None, DataClass.AGGREGATE]
    error: Annotated[str | None, DataClass.DESCRIPTION]
    error_code: Annotated[str | None, DataClass.TXN_TYPE]


# ---------------------------------------------------------------------------
# sync_pull — top-level payload
# ---------------------------------------------------------------------------


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
    securities_loaded: Annotated[int, DataClass.AGGREGATE] = 0
    investment_transactions_loaded: Annotated[int, DataClass.AGGREGATE] = 0
    holdings_loaded: Annotated[int, DataClass.AGGREGATE] = 0
    holding_lots_loaded: Annotated[int, DataClass.AGGREGATE] = 0
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
    provider_item_id: Annotated[str, DataClass.RECORD_ID]
    institution_name: Annotated[str | None, DataClass.INSTITUTION]
    provider: Annotated[str, DataClass.INSTITUTION]
    status: Annotated[str, DataClass.TXN_TYPE]
    last_sync: Annotated[str | None, DataClass.TIMESTAMP_OBSERVABILITY]
    error_code: Annotated[str | None, DataClass.TXN_TYPE]
    guidance: Annotated[str | None, DataClass.DESCRIPTION]


# ---------------------------------------------------------------------------
# sync_status — top-level payload
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class SyncStatusPayload:
    """Payload for ``sync_status`` — list of connected institutions."""

    connections: list[SyncConnectionRow]


# ---------------------------------------------------------------------------
# sync_link — initiate link payload
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class SyncLinkPayload:
    """Payload for ``sync_link`` — link URL + session ID."""

    session_id: Annotated[str, DataClass.RECORD_ID]
    link_url: Annotated[str, DataClass.DESCRIPTION]
    expiration: Annotated[str, DataClass.TIMESTAMP_OBSERVABILITY]


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


class SyncGlobalStatusView(BaseModel):
    """Global consolidated sync status."""

    model_config = ConfigDict(frozen=True)

    kind: Annotated[Literal["global"], DataClass.TXN_TYPE] = "global"
    connections: list[SyncConnectionRow]


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


SyncStatusCoarsePayload = Annotated[
    SyncGlobalStatusView | SyncSessionStatusView,
    Field(discriminator="kind"),
]


# ---------------------------------------------------------------------------
# sync_disconnect — disconnect result payload
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class SyncDisconnectPayload:
    """Payload for ``sync_disconnect`` — confirmation of disconnection.

    Both fields are Tier.LOW: ``status`` is a fixed string (TXN_TYPE),
    ``institution`` is caller-supplied institution name (INSTITUTION).
    """

    status: Annotated[str, DataClass.TXN_TYPE]
    institution: Annotated[str, DataClass.INSTITUTION]


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
