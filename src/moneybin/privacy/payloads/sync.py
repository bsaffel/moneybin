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

from dataclasses import dataclass
from typing import Annotated

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
    """Payload for ``sync_pull`` — pull result envelope."""

    job_id: Annotated[str, DataClass.RECORD_ID]
    transactions_loaded: Annotated[int, DataClass.AGGREGATE]
    accounts_loaded: Annotated[int, DataClass.AGGREGATE]
    balances_loaded: Annotated[int, DataClass.AGGREGATE]
    transactions_removed: Annotated[int, DataClass.AGGREGATE]
    institutions: list[SyncPullInstitutionRow]
    transforms_applied: Annotated[bool, DataClass.TXN_TYPE]
    transforms_duration_seconds: Annotated[float | None, DataClass.AGGREGATE]
    transforms_error: Annotated[str | None, DataClass.DESCRIPTION]


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
