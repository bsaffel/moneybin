"""Typed payload dataclasses for the gsheet (Google Sheets connector) surface.

Each field carries ``Annotated[T, DataClass.X]`` metadata so the Phase 6
middleware can derive sensitivity via ``derive_tier`` without inspecting
tool source code directly.

Tier derivation summary:
  - ``GsheetConnectionRow``     → Tier.MEDIUM (account_id = RECORD_ID, D6;
                                  highest class is DESCRIPTION via
                                  last_status_reason / column_mapping)
  - ``GsheetConnectionsPayload``→ Tier.MEDIUM (via GsheetConnectionRow) — backs
                                  ``gsheet`` and ``gsheet_status``
  - ``GsheetDetection``         → Tier.MEDIUM (column_mapping, detection_notes = DESCRIPTION)
  - ``GsheetInitialPull``       → Tier.MEDIUM (error = DESCRIPTION)
  - ``GsheetConnectPayload``    → Tier.MEDIUM (via GsheetConnectionRow) — backs
                                  ``gsheet_connect`` and ``gsheet_reconnect``
  - ``GsheetAuthPayload``       → Tier.LOW (status = TXN_TYPE)
  - ``GsheetPullRow``           → Tier.MEDIUM (error_message = DESCRIPTION)
  - ``GsheetPullPayload``       → Tier.MEDIUM (via GsheetPullRow)
  - ``GsheetDisconnectPayload`` → Tier.LOW (RECORD_ID + TXN_TYPE only)

``account_id`` is RECORD_ID (spec D6 — the opaque minted canonical surrogate
is not PII; real account numbers live in ``app.account_links.ref_value``).
Field classifications mirror the ``app.gsheet_connections`` registry block
in ``taxonomy.py`` (the cross-layer source of truth, enforced by
``test_annotated_registry_sync``): ``account_name`` / ``workbook_name`` /
``sheet_name`` are user-chosen source labels (INSTITUTION, Tier.LOW);
``last_status_reason`` and ``column_mapping`` are DESCRIPTION (Tier.MEDIUM).
The sheet's *contents* never appear here, only its metadata.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Field

from moneybin.privacy.taxonomy import DataClass

# ---------------------------------------------------------------------------
# Shared connection row — mirrors GSheetConnection.to_dict()
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class GsheetConnectionRow:
    """One Google Sheets connection (mirrors ``GSheetConnection.to_dict()``)."""

    connection_id: Annotated[str, DataClass.RECORD_ID]
    spreadsheet_id: Annotated[str, DataClass.RECORD_ID]
    sheet_gid: Annotated[int, DataClass.RECORD_ID]
    sheet_name: Annotated[str, DataClass.INSTITUTION]
    workbook_name: Annotated[str, DataClass.INSTITUTION]
    adapter: Annotated[str, DataClass.TXN_TYPE]
    alias: Annotated[str | None, DataClass.RECORD_ID]
    account_id: Annotated[str | None, DataClass.RECORD_ID]
    account_name: Annotated[str | None, DataClass.INSTITUTION]
    status: Annotated[str, DataClass.TXN_TYPE]
    last_pull_at: Annotated[str | None, DataClass.TIMESTAMP_OBSERVABILITY]
    last_success_at: Annotated[str | None, DataClass.TIMESTAMP_OBSERVABILITY]
    last_status_reason: Annotated[str | None, DataClass.DESCRIPTION]
    consecutive_failure_count: Annotated[int, DataClass.AGGREGATE]


# ---------------------------------------------------------------------------
# gsheet / gsheet_status — collection read
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class GsheetConnectionsPayload:
    """Payload for ``gsheet`` and ``gsheet_status`` — list of connections."""

    connections: list[GsheetConnectionRow]


class GsheetConnectionsView(BaseModel):
    """Default Google Sheets connection collection projection."""

    model_config = ConfigDict(frozen=True)

    kind: Annotated[Literal["connections"], DataClass.TXN_TYPE] = "connections"
    connections: list[GsheetConnectionRow]


class GsheetStatusView(BaseModel):
    """Connection-health projection for one or every Google Sheet."""

    model_config = ConfigDict(frozen=True)

    kind: Annotated[Literal["status"], DataClass.TXN_TYPE] = "status"
    connections: list[GsheetConnectionRow]


GsheetCoarsePayload = Annotated[
    GsheetConnectionsView | GsheetStatusView,
    Field(discriminator="kind"),
]


# ---------------------------------------------------------------------------
# gsheet_connect / gsheet_reconnect — bind/re-bind result
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class GsheetDetection:
    """Column-detection sub-object inside a connect/reconnect result.

    ``detection_notes`` is populated on ``gsheet_connect`` (first-time detection)
    and omitted on ``gsheet_reconnect`` (re-detection re-pins silently). The name
    is deliberately not ``notes`` — these are system-generated structural hints,
    not the user-authored ``notes`` column (USER_NOTE) in the registry.
    """

    # "high" | "medium" | "low" — a detection-confidence measure; AGGREGATE to
    # match the registry's `confidence` column (app.transaction_categories).
    confidence: Annotated[str, DataClass.AGGREGATE]
    # Header→canonical-field schema mapping; DESCRIPTION to match the registry's
    # `column_mapping` column (app.gsheet_connections).
    column_mapping: Annotated[dict[str, str], DataClass.DESCRIPTION]
    detection_notes: Annotated[list[str] | None, DataClass.DESCRIPTION] = None


@dataclass(frozen=True, slots=True)
class GsheetInitialPull:
    """Initial-pull outcome sub-object inside a connect/reconnect result.

    On success ``rows_*`` are populated and ``error`` is None; on a failed
    pull (e.g. drift_detected) ``rows_*`` are None and ``error`` carries the
    reason. The whole field is None when no pull ran (``no_initial_pull``).
    """

    status: Annotated[str | None, DataClass.TXN_TYPE]
    rows_inserted: Annotated[int | None, DataClass.AGGREGATE] = None
    rows_upserted: Annotated[int | None, DataClass.AGGREGATE] = None
    rows_soft_deleted: Annotated[int | None, DataClass.AGGREGATE] = None
    error: Annotated[str | None, DataClass.DESCRIPTION] = None


@dataclass(frozen=True, slots=True)
class GsheetConnectPayload:
    """Payload for ``gsheet_connect`` and ``gsheet_reconnect``."""

    connection: GsheetConnectionRow
    detection: GsheetDetection
    initial_pull: GsheetInitialPull | None


# ---------------------------------------------------------------------------
# gsheet_auth — OAuth status
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class GsheetAuthPayload:
    """Payload for ``gsheet_auth`` — OAuth authorization status."""

    status: Annotated[str, DataClass.TXN_TYPE]


class GsheetConnectAuthView(BaseModel):
    """Authentication-only consolidated connect outcome."""

    model_config = ConfigDict(frozen=True)

    kind: Annotated[Literal["auth"], DataClass.TXN_TYPE] = "auth"
    status: Annotated[str, DataClass.TXN_TYPE]


class GsheetConnectBindingView(BaseModel):
    """New or reconnected binding outcome."""

    model_config = ConfigDict(frozen=True)

    kind: Annotated[Literal["new", "reconnect"], DataClass.TXN_TYPE]
    connection: GsheetConnectionRow
    detection: GsheetDetection
    initial_pull: GsheetInitialPull | None


GsheetConnectCoarsePayload = Annotated[
    GsheetConnectAuthView | GsheetConnectBindingView,
    Field(discriminator="kind"),
]


# ---------------------------------------------------------------------------
# gsheet_pull — per-connection pull result
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class GsheetPullRow:
    """One per-connection pull outcome inside ``GsheetPullPayload.pulls``."""

    connection_id: Annotated[str, DataClass.RECORD_ID]
    status: Annotated[str, DataClass.TXN_TYPE]
    rows_inserted: Annotated[int, DataClass.AGGREGATE]
    rows_upserted: Annotated[int, DataClass.AGGREGATE]
    rows_soft_deleted: Annotated[int, DataClass.AGGREGATE]
    drift_reason: Annotated[str | None, DataClass.TXN_TYPE]
    error_message: Annotated[str | None, DataClass.DESCRIPTION]


@dataclass(frozen=True, slots=True)
class GsheetPullPayload:
    """Payload for ``gsheet_pull`` — per-connection pull results."""

    pulls: list[GsheetPullRow]


# ---------------------------------------------------------------------------
# gsheet_disconnect — disconnect/purge result
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class GsheetDisconnectPayload:
    """Payload for ``gsheet_disconnect`` — disconnect/purge confirmation."""

    connection_id: Annotated[str, DataClass.RECORD_ID]
    status: Annotated[str, DataClass.TXN_TYPE]
    purged: Annotated[bool, DataClass.TXN_TYPE]


class GsheetDisconnectCoarsePayload(BaseModel):
    """Soft or purged consolidated disconnect result."""

    model_config = ConfigDict(frozen=True)

    kind: Annotated[Literal["disconnected", "absent"], DataClass.TXN_TYPE]
    connection_id: Annotated[str, DataClass.RECORD_ID]
    status: Annotated[str, DataClass.TXN_TYPE]
    purged: Annotated[bool, DataClass.TXN_TYPE]
    recovery: Annotated[dict[str, Any] | None, DataClass.DESCRIPTION] = None
