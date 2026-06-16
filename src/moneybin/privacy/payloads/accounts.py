# src/moneybin/privacy/payloads/accounts.py
"""Typed payload dataclasses for the accounts surface.

Each field carries ``Annotated[T, DataClass.X]`` metadata so the Phase 6
middleware can derive sensitivity via ``derive_tier`` without inspecting
tool source code directly.

``account_id`` is ``RECORD_ID`` (Tier.LOW) — the opaque minted canonical
surrogate (spec D6) is not PII. CRITICAL propagates from
``INSTITUTION_ACCOUNT_NUMBER`` (last_four) and ``ROUTING_NUMBER``
(routing_number) fields on AccountDetail and AccountSettingsPayload.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Annotated

from moneybin.privacy.taxonomy import DataClass


@dataclass(frozen=True, slots=True)
class AccountSummary:
    """One row in the list view. last_four / credit_limit included; middleware masks."""

    account_id: Annotated[str, DataClass.RECORD_ID]
    display_name: Annotated[str | None, DataClass.USER_NOTE]
    institution_name: Annotated[str | None, DataClass.INSTITUTION]
    account_type: Annotated[str, DataClass.TXN_TYPE]
    account_subtype: Annotated[str | None, DataClass.TXN_TYPE]
    holder_category: Annotated[str | None, DataClass.TXN_TYPE]
    iso_currency_code: Annotated[str, DataClass.CURRENCY]
    archived: Annotated[bool, DataClass.TXN_TYPE]
    include_in_net_worth: Annotated[bool, DataClass.TXN_TYPE]
    last_four: Annotated[str | None, DataClass.INSTITUTION_ACCOUNT_NUMBER]
    credit_limit: Annotated[Decimal | None, DataClass.BALANCE]


@dataclass(frozen=True, slots=True)
class AccountListPayload:
    """Payload for accounts (list)."""

    rows: list[AccountSummary]


@dataclass(frozen=True, slots=True)
class AccountDetail:
    """Full account record for accounts_get. Includes routing_number (CRITICAL)."""

    account_id: Annotated[str, DataClass.RECORD_ID]
    display_name: Annotated[str | None, DataClass.USER_NOTE]
    official_name: Annotated[str | None, DataClass.INSTITUTION]
    institution_name: Annotated[str | None, DataClass.INSTITUTION]
    account_type: Annotated[str, DataClass.TXN_TYPE]
    account_subtype: Annotated[str | None, DataClass.TXN_TYPE]
    holder_category: Annotated[str | None, DataClass.TXN_TYPE]
    iso_currency_code: Annotated[str, DataClass.CURRENCY]
    last_four: Annotated[str | None, DataClass.INSTITUTION_ACCOUNT_NUMBER]
    routing_number: Annotated[str | None, DataClass.ROUTING_NUMBER]
    credit_limit: Annotated[Decimal | None, DataClass.BALANCE]
    archived: Annotated[bool, DataClass.TXN_TYPE]
    include_in_net_worth: Annotated[bool, DataClass.TXN_TYPE]
    source_type: Annotated[str | None, DataClass.TXN_TYPE]


@dataclass(frozen=True, slots=True)
class AccountSummaryStats:
    """Aggregates-only snapshot for accounts_summary."""

    total_accounts: Annotated[int, DataClass.AGGREGATE]
    count_by_type: Annotated[dict[str, int], DataClass.AGGREGATE]
    count_by_subtype: Annotated[dict[str, int], DataClass.AGGREGATE]
    count_archived: Annotated[int, DataClass.AGGREGATE]
    count_excluded_from_net_worth: Annotated[int, DataClass.AGGREGATE]
    count_with_recent_activity: Annotated[int, DataClass.AGGREGATE]


@dataclass(frozen=True, slots=True)
class AccountResolutionItem:
    """One candidate in the accounts_resolve result."""

    account_id: Annotated[str, DataClass.RECORD_ID]
    display_name: Annotated[str | None, DataClass.USER_NOTE]
    account_subtype: Annotated[str | None, DataClass.TXN_TYPE]
    institution_name: Annotated[str | None, DataClass.INSTITUTION]
    confidence: Annotated[float, DataClass.AGGREGATE]


@dataclass(frozen=True, slots=True)
class AccountResolvePayload:
    """Payload for accounts_resolve."""

    matches: list[AccountResolutionItem]


@dataclass(frozen=True, slots=True)
class AccountSettingsPayload:
    """Result of accounts_set. Mirrors AccountSettings.to_dict() plus optional extras.

    NOTE: the existing AccountSettings dataclass in account_service.py is a
    persistence-layer record (used by the repo). Don't add Annotated to it; build
    this payload from settings.to_dict() at the tool boundary instead.
    """

    account_id: Annotated[str, DataClass.RECORD_ID]
    display_name: Annotated[str | None, DataClass.USER_NOTE]
    official_name: Annotated[str | None, DataClass.INSTITUTION]
    last_four: Annotated[str | None, DataClass.INSTITUTION_ACCOUNT_NUMBER]
    account_subtype: Annotated[str | None, DataClass.TXN_TYPE]
    holder_category: Annotated[str | None, DataClass.TXN_TYPE]
    iso_currency_code: Annotated[str | None, DataClass.CURRENCY]
    credit_limit: Annotated[Decimal | None, DataClass.BALANCE]
    include_in_net_worth: Annotated[bool, DataClass.TXN_TYPE]
    archived: Annotated[bool, DataClass.TXN_TYPE]
    warnings: Annotated[list[str], DataClass.DESCRIPTION] = field(default_factory=list)
    cascaded_include_in_net_worth: Annotated[bool | None, DataClass.TXN_TYPE] = None


# ---------------------------------------------------------------------------
# accounts_links_pending / accounts_links_set / accounts_links_history
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class LinkCandidateRow:
    """One candidate merge proposal in an account-links pending review group.

    Carries only opaque ids + account labels + match signal/confidence.
    ref_value (which can be a full account number) is never surfaced here.
    """

    decision_id: Annotated[str, DataClass.RECORD_ID]
    candidate_account_id: Annotated[str, DataClass.RECORD_ID]
    # INSTITUTION (LOW) — account display label, not sensitive free text.
    candidate_display_name: Annotated[str, DataClass.INSTITUTION]
    confidence: Annotated[float | None, DataClass.AGGREGATE]
    signal: Annotated[str, DataClass.TXN_TYPE]  # "institution_last4" or "name"


@dataclass(frozen=True, slots=True)
class LinkPendingGroup:
    """One provisional account with its candidate merge proposals."""

    provisional_account_id: Annotated[str, DataClass.RECORD_ID]
    # INSTITUTION (LOW) — account display label, not sensitive free text.
    provisional_display_name: Annotated[str, DataClass.INSTITUTION]
    candidates: list[LinkCandidateRow]


@dataclass(frozen=True, slots=True)
class AccountLinksPendingPayload:
    """Payload for accounts_links_pending — pending review queue grouped by provisional account."""

    groups: list[LinkPendingGroup]
    n_pending: Annotated[int, DataClass.AGGREGATE]


@dataclass(frozen=True, slots=True)
class AccountLinksSetPayload:
    """Payload for accounts_links_set — confirmation of the decision applied."""

    decision_id: Annotated[str, DataClass.RECORD_ID]
    status: Annotated[str, DataClass.TXN_TYPE]  # "accepted" or "rejected"


@dataclass(frozen=True, slots=True)
class LinkHistoryRow:
    """One past account-link decision (accounts_links_history result)."""

    decision_id: Annotated[str, DataClass.RECORD_ID]
    provisional_account_id: Annotated[str, DataClass.RECORD_ID]
    candidate_account_id: Annotated[str, DataClass.RECORD_ID]
    status: Annotated[str, DataClass.TXN_TYPE]
    decided_by: Annotated[str, DataClass.TXN_TYPE]
    decided_at: Annotated[str | None, DataClass.TIMESTAMP_OBSERVABILITY]
    confidence: Annotated[float | None, DataClass.AGGREGATE]
    signal: Annotated[str, DataClass.TXN_TYPE]


@dataclass(frozen=True, slots=True)
class AccountLinksHistoryPayload:
    """Payload for accounts_links_history — decision log, newest first."""

    decisions: list[LinkHistoryRow]


@dataclass(frozen=True, slots=True)
class AccountLinksRunPayload:
    """Payload for accounts_links_run — count of new pending proposals written."""

    new_proposals: Annotated[int, DataClass.AGGREGATE]
