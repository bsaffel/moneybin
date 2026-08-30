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
from datetime import date
from decimal import Decimal
from typing import TYPE_CHECKING, Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Field

from moneybin.privacy.payloads.balances import (
    BalanceAssertionRow,
    BalanceObservationRow,
)
from moneybin.privacy.taxonomy import DataClass
from moneybin.utils.parsing import signal_from_match_signals

if TYPE_CHECKING:
    from collections.abc import Iterable

    from moneybin.services.account_resolution_types import (
        PendingLinkCandidate,
        PendingLinkGroup,
    )


@dataclass(frozen=True, slots=True)
class AccountSummary:
    """One row in the list view. last_four / credit_limit included; middleware masks."""

    account_id: Annotated[str, DataClass.RECORD_ID]
    display_name: Annotated[str | None, DataClass.USER_NOTE]
    institution_name: Annotated[str | None, DataClass.INSTITUTION]
    account_type: Annotated[str | None, DataClass.TXN_TYPE]
    account_subtype: Annotated[str | None, DataClass.TXN_TYPE]
    holder_category: Annotated[str | None, DataClass.TXN_TYPE]
    currency_code: Annotated[str | None, DataClass.CURRENCY]
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
    account_type: Annotated[str | None, DataClass.TXN_TYPE]
    account_subtype: Annotated[str | None, DataClass.TXN_TYPE]
    holder_category: Annotated[str | None, DataClass.TXN_TYPE]
    currency_code: Annotated[str | None, DataClass.CURRENCY]
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


# ---------------------------------------------------------------------------
# Dormant coarse account and balance reads
# ---------------------------------------------------------------------------


class AccountsListView(BaseModel):
    """Paginated account collection."""

    model_config = ConfigDict(frozen=True)

    kind: Literal["list"] = "list"
    rows: list[AccountSummary]


class AccountsDetailView(BaseModel):
    """One deterministically resolved account."""

    model_config = ConfigDict(frozen=True)

    kind: Literal["detail"] = "detail"
    account: AccountDetail


class AccountsSummaryView(BaseModel):
    """Aggregate account-count snapshot."""

    model_config = ConfigDict(frozen=True)

    kind: Literal["summary"] = "summary"
    summary: AccountSummaryStats


class AccountsResolveView(BaseModel):
    """Ranked fuzzy account-reference candidates."""

    model_config = ConfigDict(frozen=True)

    kind: Literal["resolve"] = "resolve"
    matches: list[AccountResolutionItem]


AccountsCoarsePayload = Annotated[
    AccountsListView | AccountsDetailView | AccountsSummaryView | AccountsResolveView,
    Field(discriminator="kind"),
]


class AccountsBalancesLatestView(BaseModel):
    """Most recent balance observations."""

    model_config = ConfigDict(frozen=True)

    kind: Literal["latest"] = "latest"
    observations: list[BalanceObservationRow]


class AccountsBalancesHistoryView(BaseModel):
    """Daily balance history for one resolved account."""

    model_config = ConfigDict(frozen=True)

    kind: Literal["history"] = "history"
    observations: list[BalanceObservationRow]


class AccountsBalancesAssertionsView(BaseModel):
    """Manual balance assertions, optionally filtered to one account."""

    model_config = ConfigDict(frozen=True)

    kind: Literal["assertions"] = "assertions"
    assertions: list[BalanceAssertionRow]


class AccountsBalancesReconcileView(BaseModel):
    """Balance observations whose reconciliation delta exceeds a threshold."""

    model_config = ConfigDict(frozen=True)

    kind: Literal["reconcile"] = "reconcile"
    observations: list[BalanceObservationRow]


AccountsBalancesCoarsePayload = Annotated[
    AccountsBalancesLatestView
    | AccountsBalancesHistoryView
    | AccountsBalancesAssertionsView
    | AccountsBalancesReconcileView,
    Field(discriminator="kind"),
]


@dataclass(frozen=True, slots=True)
class BalanceAssertionStatePayload:
    """Result of declaring one balance assertion's target state."""

    account_id: Annotated[str, DataClass.RECORD_ID]
    as_of: Annotated[date, DataClass.TXN_DATE]
    prior_state: Annotated[Literal["present", "absent"], DataClass.TXN_TYPE]
    state: Annotated[Literal["present", "absent"], DataClass.TXN_TYPE]
    operation_id: Annotated[str, DataClass.RECORD_ID]


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
    currency_code: Annotated[str | None, DataClass.CURRENCY]
    credit_limit: Annotated[Decimal | None, DataClass.BALANCE]
    default_cost_basis_method: Annotated[str | None, DataClass.TXN_TYPE]
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

    Carries only opaque ids + account labels + the match signal and the measured
    ledger overlap. ref_value (which can be a full account number) is never
    surfaced here.

    No confidence field: the resolver's number was a per-signal constant no input
    could move, and a reviewer reading it as a score was reading a label.
    ``overlap_matched`` / ``overlap_comparable`` replace it with a measurement.
    """

    decision_id: Annotated[str, DataClass.RECORD_ID]
    candidate_account_id: Annotated[str, DataClass.RECORD_ID]
    # USER_NOTE (MEDIUM) — matches the canonical display_name class everywhere
    # else (taxonomy.py / AccountSummary / AccountDetail); a user/auto label can
    # embed identifying text, so it must not be under-classified to LOW here.
    candidate_display_name: Annotated[str, DataClass.USER_NOTE]
    # "institution_last4", "name", or "institution_reissue"
    signal: Annotated[str, DataClass.TXN_TYPE]
    #: Transactions of the provisional account that also appear in this
    #: candidate's ledger, out of those in a period both ledgers cover. Both are
    #: 0 when the two share no comparable period, which is absence of evidence
    #: rather than evidence of absence — ``overlap_comparable == 0`` is the only
    #: way to tell the two apart, so neither may be dropped from the surface.
    overlap_matched: Annotated[int, DataClass.AGGREGATE]
    overlap_comparable: Annotated[int, DataClass.AGGREGATE]
    #: Posting-lag tolerance, in days, that ``overlap_matched`` was counted at.
    #: Two sources date the same purchase differently, so the count is agreement
    #: on amount and currency within this window rather than on an exact date.
    #: Without it the ratio reads as a stronger claim than it makes.
    overlap_window_days: Annotated[int, DataClass.AGGREGATE]

    @classmethod
    def from_candidate(cls, c: PendingLinkCandidate) -> LinkCandidateRow:
        """Map a service ``PendingLinkCandidate`` into the surfaced payload row."""
        return cls(
            decision_id=c.decision_id,
            candidate_account_id=c.candidate_account_id,
            candidate_display_name=c.candidate_display_name,
            signal=c.signal,
            overlap_matched=c.overlap.matched,
            overlap_comparable=c.overlap.comparable,
            overlap_window_days=c.overlap.window_days,
        )


@dataclass(frozen=True, slots=True)
class LinkPendingGroup:
    """One provisional account with its candidate merge proposals."""

    provisional_account_id: Annotated[str, DataClass.RECORD_ID]
    # USER_NOTE (MEDIUM) — see LinkCandidateRow.candidate_display_name.
    provisional_display_name: Annotated[str, DataClass.USER_NOTE]
    candidates: list[LinkCandidateRow]
    #: How much history an accept moves. Browse-time magnitude: the reviewer
    #: chooses which proposals are worth opening before any confirm gate runs.
    transactions: Annotated[int, DataClass.AGGREGATE] = 0

    @classmethod
    def from_domain(cls, g: PendingLinkGroup) -> LinkPendingGroup:
        """Map a service ``PendingLinkGroup`` into the surfaced payload group."""
        return cls(
            provisional_account_id=g.provisional_account_id,
            provisional_display_name=g.provisional_display_name,
            candidates=[LinkCandidateRow.from_candidate(c) for c in g.candidates],
            transactions=g.transactions,
        )


@dataclass(frozen=True, slots=True)
class AccountLinksPendingPayload:
    """Payload for accounts_links_pending — pending review queue grouped by provisional account."""

    groups: list[LinkPendingGroup]
    n_pending: Annotated[int, DataClass.AGGREGATE]

    @classmethod
    def from_service(
        cls, groups: Iterable[PendingLinkGroup], n_pending: int
    ) -> AccountLinksPendingPayload:
        """Build the pending payload from ``AccountLinksService.pending()`` output.

        Single mapper shared by the MCP tool and CLI command so the two surfaces
        cannot drift in shape.
        """
        return cls(
            groups=[LinkPendingGroup.from_domain(g) for g in groups],
            n_pending=n_pending,
        )


@dataclass(frozen=True, slots=True)
class AccountLinksSetPayload:
    """Payload for accounts_links_set — confirmation of the decision applied."""

    decision_id: Annotated[str, DataClass.RECORD_ID]
    status: Annotated[str, DataClass.TXN_TYPE]  # "accepted" or "rejected"
    # What the merge's re-match found. None on a reject, which runs no match
    # pass at all — distinct from a pass that ran and found nothing (0), which
    # the caller may report as "checked, clean".
    rematch_auto_merged: Annotated[int | None, DataClass.AGGREGATE] = None
    rematch_pending_review: Annotated[int | None, DataClass.AGGREGATE] = None
    # The pass is a full match run, so it also raises Tier 4 transfer
    # candidates. Omitting them would let an accept queue transfer proposals
    # the response never mentions.
    rematch_pending_transfers: Annotated[int | None, DataClass.AGGREGATE] = None
    # Transfers the user had already accepted that the pass reversed, because
    # dedup made both sides the same physical transaction. In `data`, not only
    # in `actions[]`: a caller reading the counts alone would otherwise never
    # learn a decision of theirs was undone.
    rematch_transfers_retired: Annotated[int | None, DataClass.AGGREGATE] = None


@dataclass(frozen=True, slots=True)
class LinkHistoryRow:
    """One past account-link decision (accounts_links_history result).

    No confidence field, for the same reason as ``LinkCandidateRow``:
    ``app.account_link_decisions.confidence_score`` still records the constant
    the resolver stamped, but replaying a number nothing could move tells a
    reader the decision was scored when it was only labelled.
    """

    decision_id: Annotated[str, DataClass.RECORD_ID]
    provisional_account_id: Annotated[str, DataClass.RECORD_ID]
    candidate_account_id: Annotated[str, DataClass.RECORD_ID]
    # USER_NOTE (MEDIUM) — see LinkCandidateRow.candidate_display_name. A
    # history row that carried only ids made the record of an irreversible
    # merge unreadable to the person who approved it.
    provisional_display_name: Annotated[str, DataClass.USER_NOTE]
    candidate_display_name: Annotated[str, DataClass.USER_NOTE]
    status: Annotated[str, DataClass.TXN_TYPE]
    decided_by: Annotated[str, DataClass.TXN_TYPE]
    decided_at: Annotated[str | None, DataClass.TIMESTAMP_OBSERVABILITY]
    signal: Annotated[str, DataClass.TXN_TYPE]

    @classmethod
    def from_decision_row(cls, r: dict[str, Any]) -> LinkHistoryRow:
        """Map a decoded ``account_link_decisions`` row into the history payload."""
        return cls(
            decision_id=r["decision_id"],
            provisional_account_id=r["provisional_account_id"],
            candidate_account_id=r["candidate_account_id"],
            # .get, not [] — a caller holding rows from somewhere other than
            # AccountLinksService.history() still builds a valid row.
            provisional_display_name=r.get("provisional_display_name", ""),
            candidate_display_name=r.get("candidate_display_name", ""),
            status=r["status"],
            decided_by=r["decided_by"],
            decided_at=(
                str(r["decided_at"]) if r.get("decided_at") is not None else None
            ),
            signal=signal_from_match_signals(r.get("match_signals")),
        )


@dataclass(frozen=True, slots=True)
class AccountLinksHistoryPayload:
    """Payload for accounts_links_history — decision log, newest first."""

    decisions: list[LinkHistoryRow]

    @classmethod
    def from_rows(cls, rows: Iterable[dict[str, Any]]) -> AccountLinksHistoryPayload:
        """Build the history payload from ``AccountLinksService.history()`` rows."""
        return cls(decisions=[LinkHistoryRow.from_decision_row(r) for r in rows])


@dataclass(frozen=True, slots=True)
class AccountLinksRunPayload:
    """Payload for accounts_links_run — count of new pending proposals written."""

    new_proposals: Annotated[int, DataClass.AGGREGATE]
