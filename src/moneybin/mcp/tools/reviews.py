"""Normalized boundaries across MoneyBin's six review queues."""

from __future__ import annotations

import asyncio
from dataclasses import replace
from decimal import Decimal
from functools import cmp_to_key
from typing import Annotated, Any, Literal, cast

from fastmcp import FastMCP
from pydantic import Field, JsonValue

from moneybin import error_codes
from moneybin.config import get_settings
from moneybin.database import Database, get_database
from moneybin.errors import UserError
from moneybin.mcp._registration import register
from moneybin.mcp.confirmation import (
    ConfirmationBinding,
    ConfirmationGrant,
    grant_confirmation_or_raise,
)
from moneybin.mcp.decorator import mcp_tool
from moneybin.mcp.pagination import (
    KeysetPosition,
    KeysetScalar,
    SortDirection,
    compare_keyset,
    decode_keyset_cursor,
    encode_keyset_cursor,
)
from moneybin.mcp.privacy import Sensitivity, tier_to_sensitivity
from moneybin.mcp.write_contracts import (
    AutoRuleDecisionRequest,
    IdentityDecisionRequest,
    OrdinaryReviewDecisionRequest,
    ReviewDecisionRequest,
)
from moneybin.privacy.introspection import extract_data_classes
from moneybin.privacy.payloads.accounts import LinkHistoryRow, LinkPendingGroup
from moneybin.privacy.payloads.categorize import (
    AutoAcceptPayload,
    AutoReviewProposalRow,
    PendingTxnRow,
)
from moneybin.privacy.payloads.investments import (
    SecurityLinkHistoryRow,
    SecurityLinkPendingGroup,
)
from moneybin.privacy.payloads.merchants import (
    MerchantLinkHistoryRow,
    MerchantLinkPendingGroup,
)
from moneybin.privacy.payloads.reviews import (
    AccountLinkHistoryDetails,
    AccountLinkPendingDetails,
    AccountLinkReviewRow,
    AutoRuleHistoryDetails,
    AutoRulePendingDetails,
    AutoRuleReviewRow,
    CategorizationHistoryDetails,
    CategorizationPendingDetails,
    CategorizationReviewRow,
    IdentityDecisionOutcome,
    IdentityLinksDecidePayload,
    MatchHistoryDetails,
    MatchPendingDetails,
    MatchReviewRow,
    MerchantLinkHistoryDetails,
    MerchantLinkPendingDetails,
    MerchantLinkReviewRow,
    ReviewCount,
    ReviewDecisionOutcome,
    ReviewQueueKind,
    ReviewsAccountLinksView,
    ReviewsAutoRulesView,
    ReviewsCategorizationView,
    ReviewsCoarsePayload,
    ReviewsDecidePayload,
    ReviewsMatchesView,
    ReviewsMerchantLinksView,
    ReviewsSecurityLinksView,
    ReviewsSummaryView,
    ReviewStatus,
    SecurityLinkHistoryDetails,
    SecurityLinkPendingDetails,
    SecurityLinkReviewRow,
)
from moneybin.privacy.payloads.transactions import MatchHistoryRow, MatchPendingRow
from moneybin.privacy.redaction import redact_typed
from moneybin.protocol.envelope import ResponseEnvelope, build_envelope
from moneybin.services.account_links_service import AccountLinksService
from moneybin.services.auto_rule_service import AutoRuleService
from moneybin.services.categorization import CategorizationService
from moneybin.services.matching_service import MatchingService
from moneybin.services.merchant_links_service import MerchantLinksService
from moneybin.services.mutation_context import current_operation_id
from moneybin.services.review_decisions_service import (
    IdentityDecisionPlan,
    ReviewDecisionsService,
)
from moneybin.services.security_links_service import SecurityLinksService

_QUEUE_KINDS: tuple[ReviewQueueKind, ...] = (
    "categorization",
    "auto_rules",
    "matches",
    "account_links",
    "merchant_links",
    "security_links",
)


def _text(value: object | None) -> str | None:
    """Return a stable textual timestamp/date without inventing one."""
    return str(value) if value is not None else None


def _review_position(
    cursor: str | None,
    *,
    kind: ReviewQueueKind,
    status: ReviewStatus,
) -> KeysetPosition | None:
    """Decode a keyset cursor and reject cross-queue or cross-status reuse."""
    if cursor is None:
        return None
    try:
        position = decode_keyset_cursor(
            cursor,
            namespace="reviews",
            scope={"kind": kind, "status": status},
        )
    except ValueError as exc:
        raise UserError(
            "Invalid review pagination cursor.",
            code="REVIEW_CURSOR_INVALID",
        ) from exc
    types, _ = _review_key_contract(kind, status)
    for key in (position.snapshot, position.after):
        if len(key) != len(types) or any(
            type(value) is not expected
            for value, expected in zip(key, types, strict=True)
        ):
            raise UserError(
                "Invalid review pagination cursor.",
                code="REVIEW_CURSOR_INVALID",
            )
    return position


def _review_envelope[T](
    data: T,
    *,
    contract_type: type[Any],
    total_count: int,
    returned_count: int,
    next_cursor: str | None = None,
    actions: list[str] | None = None,
) -> ResponseEnvelope[T]:
    """Build and redact a dynamically classified review envelope."""
    classes = extract_data_classes(contract_type)
    tier = max(data_class.tier for data_class in classes)
    redacted = cast(T, redact_typed(data, None))
    envelope = cast(
        ResponseEnvelope[T],
        build_envelope(
            data=redacted,
            sensitivity=cast(Any, tier_to_sensitivity(tier).value),
            total_count=total_count,
            returned_count=returned_count,
            next_cursor=next_cursor,
            actions=actions,
            classes_returned=sorted(data_class.value for data_class in classes),
        ),
    )
    return replace(
        envelope,
        summary=replace(envelope.summary, has_more=next_cursor is not None),
    )


def _pending_categorization_rows(
    service: CategorizationService,
) -> list[CategorizationReviewRow]:
    """Project the existing uncategorized queue into normalized rows."""
    try:
        raw_rows = service.list_uncategorized_transactions(limit=None) or []
    except UserError as exc:
        if exc.code != "schema_out_of_date" or service.count_uncategorized() != 0:
            raise
        raw_rows = []
    ordered = sorted(
        raw_rows,
        key=lambda row: (
            _text(row.get("txn_date")) or "",
            str(row["transaction_id"]),
        ),
        reverse=True,
    )
    attempts = service.project_pending_review_attempts([
        str(row["transaction_id"]) for row in ordered
    ])
    result: list[CategorizationReviewRow] = []
    for row in ordered:
        transaction = PendingTxnRow(
            transaction_id=str(row["transaction_id"]),
            transaction_date=_text(row.get("txn_date")),
            amount=(
                float(cast(Decimal, row["amount"]))
                if row.get("amount") is not None
                else None
            ),
            description=cast(str | None, row.get("description")),
            memo=cast(str | None, row.get("memo")),
            account_id=cast(str | None, row.get("account_id")),
            age_days=(
                int(cast(int, row["age_days"]))
                if row.get("age_days") is not None
                else None
            ),
            pending_transfer_match=bool(row.get("pending_transfer_match", False)),
        )
        summary = transaction.description or f"Transaction {transaction.transaction_id}"
        attempt = attempts.get(transaction.transaction_id)
        if attempt is None:
            continue
        result.append(
            CategorizationReviewRow(
                decision_id=str(attempt["decision_id"]),
                status="pending",
                created_at=transaction.transaction_date,
                summary=summary,
                details=CategorizationPendingDetails(transaction=transaction),
            )
        )
    return result


def _categorization_history_rows(
    service: CategorizationService,
) -> list[CategorizationReviewRow]:
    """Project canonical categorization decisions into history rows."""
    result: list[CategorizationReviewRow] = []
    for decision in service.list_review_decision_history():
        transaction_id = str(decision["transaction_id"])
        category = cast(str | None, decision.get("category"))
        subcategory = cast(str | None, decision.get("subcategory"))
        summary = (
            f"{category} / {subcategory}"
            if category is not None and subcategory
            else category
            or ("Superseded" if decision["status"] == "superseded" else "Rejected")
        )
        result.append(
            CategorizationReviewRow(
                decision_id=str(decision["decision_id"]),
                status=str(decision["status"]),
                created_at=_text(decision.get("decided_at")),
                summary=summary,
                details=CategorizationHistoryDetails(
                    transaction_id=transaction_id,
                    decision_status=cast(
                        Literal["accepted", "rejected", "superseded"],
                        decision["status"],
                    ),
                    category_id=cast(str | None, decision.get("category_id")),
                    category=category,
                    subcategory=subcategory,
                    categorized_by=str(
                        decision.get("categorized_by")
                        or decision.get("decided_by")
                        or "unknown"
                    ),
                    merchant_id=cast(str | None, decision.get("merchant_id")),
                    confidence=(
                        float(cast(Decimal, decision["confidence"]))
                        if decision.get("confidence") is not None
                        else None
                    ),
                    rule_id=cast(str | None, decision.get("rule_id")),
                    source_type=str(decision.get("source_type") or "internal"),
                    reversed_at=_text(decision.get("reversed_at")),
                    reversed_by=cast(str | None, decision.get("reversed_by")),
                ),
            )
        )
    return result


def _pending_auto_rule_rows(service: AutoRuleService) -> list[AutoRuleReviewRow]:
    """Project the complete auto-rule proposal queue with blast-radius fields."""
    result = service.review(limit=service.count_pending_proposals())
    return [
        AutoRuleReviewRow(
            decision_id=str(proposal["proposed_rule_id"]),
            status="pending",
            created_at=None,
            summary=(
                f"{proposal.get('merchant_pattern') or 'Unnamed pattern'} → "
                f"{proposal.get('category') or 'Uncategorized'}"
            ),
            details=AutoRulePendingDetails(
                proposal=AutoReviewProposalRow(
                    proposed_rule_id=str(proposal["proposed_rule_id"]),
                    merchant_pattern=cast(
                        str | None,
                        proposal.get("merchant_pattern"),
                    ),
                    match_type=cast(str | None, proposal.get("match_type")),
                    category=cast(str | None, proposal.get("category")),
                    subcategory=cast(str | None, proposal.get("subcategory")),
                    trigger_count=int(cast(int, proposal.get("trigger_count") or 0)),
                    sample_txn_ids=[
                        str(value)
                        for value in cast(
                            list[object],
                            proposal.get("sample_txn_ids") or [],
                        )
                    ],
                    estimated_match_count=int(
                        proposal.get("estimated_match_count") or 0
                    ),
                    is_broad=bool(proposal.get("is_broad", False)),
                )
            ),
        )
        for proposal in result.proposals
    ]


def _auto_rule_history_rows(service: AutoRuleService) -> list[AutoRuleReviewRow]:
    """Project terminal auto-rule proposal decisions."""
    rows: list[AutoRuleReviewRow] = []
    for proposal in service.list_proposal_history():
        status = cast(
            Literal["approved", "rejected", "superseded"],
            proposal["status"],
        )
        rows.append(
            AutoRuleReviewRow(
                decision_id=str(proposal["proposed_rule_id"]),
                status=status,
                created_at=_text(
                    proposal.get("decided_at") or proposal.get("proposed_at")
                ),
                summary=(
                    f"{proposal.get('merchant_pattern') or 'Unnamed pattern'} → "
                    f"{proposal.get('category') or 'Uncategorized'}"
                ),
                details=AutoRuleHistoryDetails(
                    merchant_pattern=str(proposal["merchant_pattern"]),
                    match_type=str(proposal["match_type"]),
                    category=str(proposal["category"]),
                    subcategory=cast(str | None, proposal.get("subcategory")),
                    trigger_count=int(cast(int, proposal.get("trigger_count") or 0)),
                    sample_txn_ids=[
                        str(value)
                        for value in cast(
                            list[object],
                            proposal.get("sample_txn_ids") or [],
                        )
                    ],
                    decision_status=status,
                    rule_id=cast(str | None, proposal.get("rule_id")),
                    decided_by=cast(str | None, proposal.get("decided_by")),
                ),
            )
        )
    return rows


def _pending_match_rows(service: MatchingService) -> list[MatchReviewRow]:
    """Project the complete pending match decision queue."""
    raw_rows = service.get_pending(limit=None)
    ordered = sorted(
        raw_rows,
        key=lambda row: (
            -float(row.get("confidence_score") or 0.0),
            str(row["match_id"]),
        ),
    )
    result: list[MatchReviewRow] = []
    for row in ordered:
        match = MatchPendingRow(
            match_id=str(row["match_id"]),
            match_type=str(row.get("match_type") or "dedup"),
            match_tier=cast(str | None, row.get("match_tier")),
            confidence_score=float(row.get("confidence_score") or 0.0),
            source_type_a=str(row["source_type_a"]),
            source_transaction_id_a=str(row["source_transaction_id_a"]),
            source_type_b=str(row["source_type_b"]),
            source_transaction_id_b=str(row["source_transaction_id_b"]),
            match_status=str(row["match_status"]),
            component_key=str(row["component_key"]),
        )
        result.append(
            MatchReviewRow(
                decision_id=match.match_id,
                status=match.match_status,
                created_at=_text(row.get("decided_at")),
                summary=(
                    f"{match.match_type} match at "
                    f"{match.confidence_score:.2f} confidence"
                ),
                details=MatchPendingDetails(match=match),
            )
        )
    return result


def _match_history_rows(service: MatchingService) -> list[MatchReviewRow]:
    """Project the actual match history path."""
    result: list[MatchReviewRow] = []
    for row in service.get_log(limit=None):
        match = MatchHistoryRow(
            match_id=str(row["match_id"]),
            match_type=str(row.get("match_type") or "dedup"),
            match_status=str(row["match_status"]),
            confidence_score=float(row.get("confidence_score") or 0.0),
            decided_by=str(row.get("decided_by") or "unknown"),
            decided_at=_text(row.get("decided_at")),
        )
        result.append(
            MatchReviewRow(
                decision_id=match.match_id,
                status=match.match_status,
                created_at=match.decided_at,
                summary=f"{match.match_type} match {match.match_status}",
                details=MatchHistoryDetails(match=match),
            )
        )
    return result


def _pending_account_link_rows(
    service: AccountLinksService,
) -> list[AccountLinkReviewRow]:
    """Project grouped pending account-link review units."""
    timestamp_by_id = {
        str(row["decision_id"]): _text(row.get("decided_at"))
        for row in service.history(limit=None)
    }
    result: list[AccountLinkReviewRow] = []
    for group in service.pending():
        payload = LinkPendingGroup.from_domain(group)
        if not payload.candidates:
            continue
        decision_id = payload.candidates[0].decision_id
        label = payload.provisional_display_name or payload.provisional_account_id
        result.append(
            AccountLinkReviewRow(
                decision_id=decision_id,
                status="pending",
                created_at=timestamp_by_id.get(decision_id),
                summary=f"{label}: {len(payload.candidates)} account candidate(s)",
                details=AccountLinkPendingDetails(group=payload),
            )
        )
    return result


def _account_link_history_rows(
    service: AccountLinksService,
) -> list[AccountLinkReviewRow]:
    """Project the actual account-link history path."""
    result: list[AccountLinkReviewRow] = []
    for raw in service.history(limit=None):
        decision = LinkHistoryRow.from_decision_row(raw)
        result.append(
            AccountLinkReviewRow(
                decision_id=decision.decision_id,
                status=decision.status,
                created_at=decision.decided_at,
                summary=(
                    f"Account {decision.provisional_account_id} to "
                    f"{decision.candidate_account_id}: {decision.status}"
                ),
                details=AccountLinkHistoryDetails(decision=decision),
            )
        )
    return result


def _pending_merchant_link_rows(
    service: MerchantLinksService,
) -> list[MerchantLinkReviewRow]:
    """Project grouped pending merchant-link review units."""
    timestamp_by_id = {
        str(row["decision_id"]): _text(row.get("decided_at"))
        for row in service.history(limit=None)
    }
    result: list[MerchantLinkReviewRow] = []
    for group in service.pending():
        payload = MerchantLinkPendingGroup.from_domain(group)
        if not payload.candidates:
            continue
        decision_id = payload.candidates[0].decision_id
        label = payload.provider_merchant_name or payload.ref_value
        result.append(
            MerchantLinkReviewRow(
                decision_id=decision_id,
                status="pending",
                created_at=timestamp_by_id.get(decision_id),
                summary=f"{label}: {len(payload.candidates)} merchant candidate(s)",
                details=MerchantLinkPendingDetails(group=payload),
            )
        )
    return result


def _merchant_link_history_rows(
    service: MerchantLinksService,
) -> list[MerchantLinkReviewRow]:
    """Project the actual merchant-link history path."""
    result: list[MerchantLinkReviewRow] = []
    for raw in service.history(limit=None):
        decision = MerchantLinkHistoryRow.from_decision_row(raw)
        label = decision.provider_merchant_name or decision.ref_value
        result.append(
            MerchantLinkReviewRow(
                decision_id=decision.decision_id,
                status=decision.status,
                created_at=decision.decided_at,
                summary=f"{label}: {decision.status}",
                details=MerchantLinkHistoryDetails(decision=decision),
            )
        )
    return result


def _pending_security_link_rows(
    service: SecurityLinksService,
) -> list[SecurityLinkReviewRow]:
    """Project grouped pending security-link review units."""
    timestamp_by_id = {
        str(row["decision_id"]): _text(row.get("decided_at"))
        for row in service.history(limit=None)
    }
    result: list[SecurityLinkReviewRow] = []
    for group in service.pending():
        payload = SecurityLinkPendingGroup.from_domain(group)
        if not payload.candidates:
            continue
        decision_id = payload.candidates[0].decision_id
        label = payload.provider_ticker or payload.provider_name or payload.ref_value
        result.append(
            SecurityLinkReviewRow(
                decision_id=decision_id,
                status="pending",
                created_at=timestamp_by_id.get(decision_id),
                summary=f"{label}: {len(payload.candidates)} security candidate(s)",
                details=SecurityLinkPendingDetails(group=payload),
            )
        )
    return result


def _security_link_history_rows(
    service: SecurityLinksService,
) -> list[SecurityLinkReviewRow]:
    """Project the actual security-link history path."""
    result: list[SecurityLinkReviewRow] = []
    for raw in service.history(limit=None):
        decision = SecurityLinkHistoryRow.from_decision_row(raw)
        label = decision.provider_ticker or decision.provider_name or decision.ref_value
        result.append(
            SecurityLinkReviewRow(
                decision_id=decision.decision_id,
                status=decision.status,
                created_at=decision.decided_at,
                summary=f"{label}: {decision.status}",
                details=SecurityLinkHistoryDetails(decision=decision),
            )
        )
    return result


def _load_review_view(
    db: Database,
    *,
    kind: ReviewQueueKind,
    status: ReviewStatus,
) -> ReviewsCoarsePayload:
    """Load one complete normalized collection through its existing service."""
    if kind == "categorization":
        service = CategorizationService(db)
        rows = (
            _pending_categorization_rows(service)
            if status == "pending"
            else _categorization_history_rows(service)
        )
        return ReviewsCategorizationView(status=status, rows=rows)
    if kind == "auto_rules":
        auto_rule_service = AutoRuleService(db)
        rows = (
            _pending_auto_rule_rows(auto_rule_service)
            if status == "pending"
            else _auto_rule_history_rows(auto_rule_service)
        )
        return ReviewsAutoRulesView(status=status, rows=rows)
    if kind == "matches":
        match_service = MatchingService(db)
        rows = (
            _pending_match_rows(match_service)
            if status == "pending"
            else _match_history_rows(match_service)
        )
        return ReviewsMatchesView(status=status, rows=rows)
    if kind == "account_links":
        account_service = AccountLinksService(db, actor="mcp")
        rows = (
            _pending_account_link_rows(account_service)
            if status == "pending"
            else _account_link_history_rows(account_service)
        )
        return ReviewsAccountLinksView(status=status, rows=rows)
    if kind == "merchant_links":
        merchant_service = MerchantLinksService(db, actor="mcp")
        rows = (
            _pending_merchant_link_rows(merchant_service)
            if status == "pending"
            else _merchant_link_history_rows(merchant_service)
        )
        return ReviewsMerchantLinksView(status=status, rows=rows)
    security_service = SecurityLinksService(db, actor="mcp")
    rows = (
        _pending_security_link_rows(security_service)
        if status == "pending"
        else _security_link_history_rows(security_service)
    )
    return ReviewsSecurityLinksView(status=status, rows=rows)


def _view_rows(
    view: ReviewsCoarsePayload,
) -> list[Any]:
    """Return rows from a non-summary review view."""
    if isinstance(view, ReviewsSummaryView):
        raise TypeError("Summary view has counts, not review rows")
    return list(view.rows)


def _review_key_contract(
    kind: ReviewQueueKind,
    status: ReviewStatus,
) -> tuple[tuple[type[object], ...], tuple[SortDirection, ...]]:
    """Return the typed immutable ordering contract for one review queue."""
    if status == "history" or kind == "categorization":
        return ((str, str), ("desc", "asc" if status == "history" else "desc"))
    if kind == "auto_rules":
        return ((str,), ("asc",))
    if kind == "matches":
        return ((float, str), ("desc", "asc"))
    if kind == "account_links":
        return ((str, str), ("asc", "asc"))
    return ((str, str, str), ("asc", "asc", "asc"))


def _review_ordering(
    kind: ReviewQueueKind,
    status: ReviewStatus,
    row: Any,
) -> tuple[tuple[KeysetScalar, ...], tuple[SortDirection, ...]]:
    """Return one immutable queue key whose directions match display order."""
    _, directions = _review_key_contract(kind, status)
    if status == "history":
        return (
            (_text(row.created_at) or "", str(row.decision_id)),
            directions,
        )
    if kind == "categorization":
        return (
            (_text(row.created_at) or "", str(row.decision_id)),
            directions,
        )
    if kind == "auto_rules":
        # trigger_count can change while a proposal is pending; the stable id
        # is the only immutable ordering key currently projected by this queue.
        return ((str(row.decision_id),), directions)
    if kind == "matches":
        return (
            (
                float(row.details.match.confidence_score),
                str(row.decision_id),
            ),
            directions,
        )
    if kind == "account_links":
        return (
            (
                str(row.details.group.provisional_account_id),
                str(row.decision_id),
            ),
            directions,
        )
    if kind == "merchant_links":
        return (
            (
                str(row.details.group.source_type),
                str(row.details.group.ref_value),
                str(row.decision_id),
            ),
            directions,
        )
    return (
        (
            str(row.details.group.ref_kind),
            str(row.details.group.ref_value),
            str(row.decision_id),
        ),
        directions,
    )


def _review_page(
    rows: list[Any],
    *,
    kind: ReviewQueueKind,
    status: ReviewStatus,
    limit: int,
    position: KeysetPosition | None,
) -> tuple[list[Any], str | None]:
    """Page one evolving queue without depending on live-list offsets."""
    if not rows:
        return [], None
    _, directions = _review_key_contract(kind, status)

    def compare_rows(left: Any, right: Any) -> int:
        left_key, _ = _review_ordering(kind, status, left)
        right_key, _ = _review_ordering(kind, status, right)
        return compare_keyset(left_key, right_key, directions)

    ordered = sorted(rows, key=cmp_to_key(compare_rows))
    if position is None:
        snapshot, _ = _review_ordering(kind, status, ordered[0])
        eligible = ordered
    else:
        snapshot = position.snapshot
        try:
            eligible = [
                row
                for row in ordered
                if compare_keyset(
                    _review_ordering(kind, status, row)[0],
                    snapshot,
                    directions,
                )
                >= 0
                and compare_keyset(
                    _review_ordering(kind, status, row)[0],
                    position.after,
                    directions,
                )
                > 0
            ]
        except ValueError as exc:
            raise UserError(
                "Invalid review pagination cursor.",
                code="REVIEW_CURSOR_INVALID",
            ) from exc
    page = eligible[:limit]
    if len(eligible) <= limit or not page:
        return page, None
    after, _ = _review_ordering(kind, status, page[-1])
    return page, encode_keyset_cursor(
        namespace="reviews",
        scope={"kind": kind, "status": status},
        snapshot=snapshot,
        after=after,
        total=position.total if position is not None else len(ordered),
    )


def _review_count(
    db: Database,
    *,
    kind: ReviewQueueKind,
    status: ReviewStatus,
) -> int:
    """Count one queue without materializing expensive row enrichments."""
    if kind == "auto_rules":
        service = AutoRuleService(db)
        return (
            service.count_pending_proposals()
            if status == "pending"
            else service.count_proposal_history()
        )
    return len(_view_rows(_load_review_view(db, kind=kind, status=status)))


def _review_actions(
    *,
    kind: ReviewQueueKind,
    status: ReviewStatus,
    limit: int,
    next_cursor: str | None,
) -> list[str]:
    """Return queue-native decision and continuation actions."""
    if status == "history":
        actions = [
            f"Open the active queue with reviews(kind={kind!r}, status='pending')"
        ]
    else:
        decision_tool = (
            "reviews_decide"
            if kind in {"categorization", "auto_rules", "matches"}
            else "identity_links_decide"
        )
        actions = [f"Use {decision_tool} to decide a row from this queue"]
    if next_cursor is not None:
        actions.append(
            f"Continue with reviews(kind={kind!r}, status={status!r}, "
            f"limit={limit}, cursor='{next_cursor}')"
        )
    return actions


def _summary_actions() -> list[str]:
    """Return executable drill-down calls for every normalized collection."""
    return [
        f"Open reviews(kind={kind!r}, status={status!r}, limit=100)"
        for kind in _QUEUE_KINDS
        for status in ("pending", "history")
    ]


@mcp_tool(dynamic_classification=True, maximum_sensitivity=Sensitivity.HIGH)
def reviews_coarse(
    kind: Literal[
        "summary",
        "categorization",
        "auto_rules",
        "matches",
        "account_links",
        "merchant_links",
        "security_links",
    ] = "summary",
    status: ReviewStatus = "pending",
    limit: Annotated[int, Field(strict=True, ge=1)] = 100,
    cursor: str | None = None,
) -> ResponseEnvelope[ReviewsCoarsePayload]:
    """Summarize or read one normalized review collection."""
    if kind == "summary":
        if limit != 100 or cursor is not None:
            raise UserError(
                "Review summary does not accept pagination overrides.",
                code="REVIEW_PAGINATION_NOT_ALLOWED",
            )
        if status != "pending":
            raise UserError(
                "status is not valid for review summary.",
                code="REVIEW_STATUS_NOT_ALLOWED",
            )
        with get_database(read_only=True) as db:
            counts = [
                ReviewCount(
                    kind=queue_kind,
                    status=queue_status,
                    count=_review_count(
                        db,
                        kind=queue_kind,
                        status=queue_status,
                    ),
                )
                for queue_kind in _QUEUE_KINDS
                for queue_status in cast(
                    tuple[ReviewStatus, ...], ("pending", "history")
                )
            ]
        payload = ReviewsSummaryView(
            counts=counts,
            total=sum(count.count for count in counts),
        )
        return _review_envelope(
            payload,
            contract_type=ReviewsSummaryView,
            total_count=len(payload.counts),
            returned_count=len(payload.counts),
            actions=_summary_actions(),
        )

    queue_kind = kind
    position = _review_position(cursor, kind=queue_kind, status=status)
    with get_database(read_only=True) as db:
        complete = _load_review_view(db, kind=queue_kind, status=status)
    rows = _view_rows(complete)
    page, next_cursor = _review_page(
        rows,
        kind=queue_kind,
        status=status,
        limit=limit,
        position=position,
    )
    payload = complete.model_copy(update={"rows": page})
    return _review_envelope(
        payload,
        contract_type=type(complete),
        total_count=position.total if position is not None else len(rows),
        returned_count=len(page),
        next_cursor=next_cursor,
        actions=_review_actions(
            kind=queue_kind,
            status=status,
            limit=limit,
            next_cursor=next_cursor,
        ),
    )


def register_review_coarse_reads(mcp: FastMCP) -> None:
    """Register the standard normalized review read."""
    register(
        mcp,
        reviews_coarse,
        "reviews",
        "Return exact review counts or one normalized pending/history queue "
        "with deterministic cursor pagination.",
        privacy_actor="reviews",
    )


@mcp_tool(read_only=False, idempotent=True)
def reviews_decide_coarse(
    decisions: list[ReviewDecisionRequest],
) -> ResponseEnvelope[ReviewsDecidePayload]:
    """Accept or reject one atomic ordinary or auto-rule decision batch."""
    operation_id = current_operation_id()
    auto_rule_impact: AutoAcceptPayload | None = None
    with get_database(read_only=False) as db:
        auto_rule_decisions = [
            decision
            for decision in decisions
            if isinstance(decision, AutoRuleDecisionRequest)
        ]
        if auto_rule_decisions:
            if len(auto_rule_decisions) != len(decisions):
                raise UserError(
                    "Auto-rule and ordinary decisions require separate atomic batches.",
                    code=error_codes.MUTATION_INVALID_INPUT,
                )
            service = AutoRuleService(db)
            ids = [decision.decision_id for decision in auto_rule_decisions]
            result = service.decide(
                expected_pending_ids=ids,
                accept=[
                    decision.decision_id
                    for decision in auto_rule_decisions
                    if decision.decision == "accept"
                ],
                reject=[
                    decision.decision_id
                    for decision in auto_rule_decisions
                    if decision.decision == "reject"
                ],
                actor="mcp",
                allow_broad_ids={
                    decision.decision_id
                    for decision in auto_rule_decisions
                    if decision.decision == "accept" and decision.allow_broad
                },
            )
            after = result.statuses
            auto_rule_impact = AutoAcceptPayload(
                approved=result.impact.approved,
                rejected=result.impact.rejected,
                skipped=result.impact.skipped,
                newly_categorized=result.impact.newly_categorized,
                rule_ids=result.impact.rule_ids,
            )
            outcomes = [
                ReviewDecisionOutcome(
                    kind=decision.kind,
                    decision_id=decision.decision_id,
                    decision=decision.decision,
                    status=after.get(decision.decision_id, "pending"),
                    changed=after.get(decision.decision_id) in {"approved", "rejected"},
                    operation_id=operation_id,
                )
                for decision in auto_rule_decisions
            ]
        else:
            ordinary_decisions = cast(
                list[OrdinaryReviewDecisionRequest],
                decisions,
            )
            results = ReviewDecisionsService(db, actor="mcp").apply_ordinary(
                ordinary_decisions
            )
            outcomes = [
                ReviewDecisionOutcome(
                    kind=item.request.kind,
                    decision_id=item.request.decision_id,
                    decision=item.request.decision,
                    status=item.status,
                    changed=item.changed,
                    operation_id=operation_id,
                )
                for item in results
            ]
    return build_envelope(
        data=ReviewsDecidePayload(
            results=outcomes,
            applied_count=sum(item.changed for item in outcomes),
            operation_id=operation_id,
            auto_rule_impact=auto_rule_impact,
        ),
        actions=[
            "Use reviews(status='pending') to continue review",
            "Use system_audit_undo(operation_id=...) to reverse this batch",
        ],
    )


def _identity_binding(
    decisions: list[IdentityDecisionRequest],
    plan: IdentityDecisionPlan,
) -> ConfirmationBinding:
    """Bind one approval to the exact ordered batch and complete live state."""
    arguments: dict[str, JsonValue] = {
        "decisions": [
            cast(JsonValue, decision.model_dump(mode="json")) for decision in decisions
        ],
        "before_state": [item.before_state for item in plan.items],
    }
    resolved_ids: list[str] = []
    for item in plan.items:
        for value in (item.request.decision_id, item.source_id, item.target_id):
            if value not in resolved_ids:
                resolved_ids.append(value)
    blast_radius = {
        key: len({
            entity_id for item in plan.items for entity_id in item.affected_ids[key]
        })
        for key in ("accounts", "merchants", "securities", "transactions", "lots")
    }
    return ConfirmationBinding(
        arguments=arguments,
        resolved_ids=tuple(resolved_ids),
        actor="mcp",
        profile=get_settings().profile,
        authorization_context="local-profile",
        operation_kind="identity_links_decide",
        blast_radius=blast_radius,
    )


def _preview_identity_decisions(
    decisions: list[IdentityDecisionRequest],
) -> IdentityDecisionPlan:
    """Resolve one identity batch on a read-only connection."""
    with get_database(read_only=True) as db:
        return ReviewDecisionsService(db, actor="mcp").plan_identity(decisions)


def _apply_identity_decisions(
    decisions: list[IdentityDecisionRequest],
    *,
    grant: ConfirmationGrant | None,
    expected_binding: ConfirmationBinding,
) -> IdentityDecisionPlan:
    """Apply a revalidated identity batch through the shared decision service."""
    with get_database(read_only=False) as db:
        service = ReviewDecisionsService(db, actor="mcp")

        def verify(plan: IdentityDecisionPlan) -> None:
            binding = _identity_binding(decisions, plan)
            if grant is not None:
                grant.verify(binding)
            elif binding.canonical_bytes() != expected_binding.canonical_bytes():
                raise UserError(
                    "Identity-link state changed after preflight.",
                    code=error_codes.MUTATION_CONFIRMATION_MISMATCH,
                )

        return service.apply_identity(decisions, verify=verify)


@mcp_tool(read_only=False, destructive=True, idempotent=True, timeout_seconds=180.0)
async def identity_links_decide_coarse(
    decisions: list[IdentityDecisionRequest],
    confirmation_token: str | None = None,
) -> ResponseEnvelope[IdentityLinksDecidePayload]:
    """Atomically accept or reject account, merchant, and security identity links."""
    plan = await asyncio.to_thread(_preview_identity_decisions, decisions)
    binding = _identity_binding(decisions, plan)
    if confirmation_token is not None and not plan.destructive:
        raise UserError(
            "confirmation_token is only valid for a batch with a pending accept.",
            code=error_codes.MUTATION_INVALID_INPUT,
        )
    if plan.changed_count == 0:
        raise UserError(
            "Every identity decision is already satisfied.",
            code=error_codes.MUTATION_NOTHING_TO_DO,
        )
    grant: ConfirmationGrant | None = None
    if plan.destructive:
        grant = await grant_confirmation_or_raise(
            binding=binding if confirmation_token is None else None,
            message=(
                "Confirm this complete identity-decision batch. Accepted links "
                "can merge account histories, merchant attribution, or security "
                "lots; every decision in the ordered batch will commit together."
            ),
            confirmation_token=confirmation_token,
        )
    live = await asyncio.to_thread(
        _apply_identity_decisions,
        decisions,
        grant=grant,
        expected_binding=binding,
    )
    operation_id = current_operation_id()
    return build_envelope(
        data=IdentityLinksDecidePayload(
            results=[
                IdentityDecisionOutcome(
                    kind=item.request.kind,
                    decision_id=item.request.decision_id,
                    decision=item.request.decision,
                    status=item.status,
                    changed=item.changed,
                    operation_id=operation_id,
                )
                for item in live.items
            ],
            applied_count=live.changed_count,
            operation_id=operation_id,
        ),
        actions=[
            "Use reviews(status='pending') to continue identity review",
            "Use system_audit_undo(operation_id=...) to reverse this batch",
        ],
    )


def register_review_coarse_writes(mcp: FastMCP) -> None:
    """Register the standard ordinary and identity decision batches."""
    register(
        mcp,
        reviews_decide_coarse,
        "reviews_decide",
        "Accept or reject an atomic batch of transaction, match, or auto-rule "
        "review decisions. Auto-rule decisions use kind='auto_rule' and may set "
        "allow_broad after inspecting estimated_match_count; keep auto-rule and "
        "ordinary decisions in separate calls.",
        privacy_actor="reviews_decide",
    )
    register(
        mcp,
        identity_links_decide_coarse,
        "identity_links_decide",
        "Atomically accept or reject account, merchant, and security identity "
        "link decisions. Any accepted merge confirms the exact normalized full "
        "batch and complete live before-state; reject-only batches do not prompt.",
        privacy_actor="identity_links_decide",
    )


def register_review_tools(mcp: FastMCP) -> None:
    """Register the standard normalized review boundaries."""
    register_review_coarse_reads(mcp)
    register_review_coarse_writes(mcp)
