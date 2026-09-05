"""Normalized boundaries across MoneyBin's seven review queues."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Mapping
from dataclasses import dataclass
from decimal import Decimal
from typing import Annotated, Any, Literal, cast

from fastmcp import FastMCP
from pydantic import Field, JsonValue

from moneybin import error_codes
from moneybin.adapters.matching_adapters import (
    match_history_row,
    match_pending_row,
)
from moneybin.adapters.rematch_report import rematch_actions
from moneybin.config import get_settings
from moneybin.database import Database, get_database
from moneybin.errors import (
    UserError,
    classify_user_error,
    exception_origin,
)
from moneybin.mcp._registration import register
from moneybin.mcp.confirmation import (
    ConfirmationBinding,
    ConfirmationGrant,
    grant_confirmation_or_raise,
)
from moneybin.mcp.decorator import mcp_tool
from moneybin.privacy.classified_envelope import build_classified_envelope
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
    QueueUnavailable,
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
    ReviewsRuleConflictsView,
    ReviewsSecurityLinksView,
    ReviewsSummaryView,
    ReviewStatus,
    RuleConflictHistoryDetails,
    RuleConflictImpact,
    RuleConflictMatcher,
    RuleConflictPendingDetails,
    RuleConflictReviewRow,
    SecurityLinkHistoryDetails,
    SecurityLinkPendingDetails,
    SecurityLinkReviewRow,
)
from moneybin.privacy.sensitivity import Sensitivity
from moneybin.privacy.taxonomy import Tier
from moneybin.protocol.envelope import ResponseEnvelope, build_envelope
from moneybin.protocol.pagination import (
    InvalidKeysetCursorError,
    KeysetPosition,
    KeysetScalar,
    SortDirection,
    canonical_iso_date,
    canonical_iso_timestamp,
    canonicalize_keyset_element,
    decode_keyset_cursor,
    paginate_keyset,
    reject_inverted_keyset,
    validate_keyset_shape,
)
from moneybin.protocol.write_contracts import (
    AutoRuleDecisionRequest,
    IdentityDecisionRequest,
    OrdinaryReviewDecisionRequest,
    ReviewDecisionRequest,
    RuleConflictDecisionRequest,
)
from moneybin.services.account_links_service import AccountLinksService
from moneybin.services.account_resolution_types import UNNAMED_ACCOUNT_LABEL
from moneybin.services.auto_rule_service import AutoRuleService
from moneybin.services.categorization import (
    CategorizationService,
    ConflictDecision,
)
from moneybin.services.identity_confirmation import (
    AccountMergeFacts,
    identity_confirm_message,
)
from moneybin.services.matching_service import MatchingService
from moneybin.services.merchant_links_service import MerchantLinksService
from moneybin.services.mutation_context import current_operation_id
from moneybin.services.review_decisions_service import (
    IdentityDecisionPlan,
    ReviewDecisionsService,
)
from moneybin.services.security_links_service import SecurityLinksService

logger = logging.getLogger(__name__)

_QUEUE_KINDS: tuple[ReviewQueueKind, ...] = (
    "categorization",
    "auto_rules",
    "matches",
    "rule_conflicts",
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
    types, directions = _review_key_contract(kind, status)
    try:
        position = decode_keyset_cursor(
            cursor,
            namespace="reviews",
            scope={"kind": kind, "status": status},
        )
        validate_keyset_shape(position, key_types=types)
        canonical = _canonical_review_position(position, kind=kind, status=status)
        reject_inverted_keyset(canonical, directions)
    except ValueError as exc:
        raise UserError(
            "Invalid review pagination cursor.",
            code=error_codes.REVIEW_CURSOR_INVALID,
        ) from exc
    return canonical


def _canonical_review_position(
    position: KeysetPosition,
    *,
    kind: ReviewQueueKind,
    status: ReviewStatus,
) -> KeysetPosition:
    """Normalize this queue's leading temporal key, if it has one.

    A history queue keys on the instant a decision was made; the pending
    categorization queue keys on a transaction *day*. Canonicalizing one as the
    other would rewrite the key into a spelling no row produces, so the page
    would match nothing at all rather than mis-order — which is why the shape
    is declared per queue here rather than guessed from the value.
    """
    if status == "history" or kind == "rule_conflicts":
        canonicalize = canonical_iso_timestamp
    elif kind == "categorization":
        canonicalize = canonical_iso_date
    else:
        return position

    def normalize(value: str) -> str:
        # `_review_ordering` substitutes "" for a row carrying no timestamp, so
        # the empty key is one a page can mint and must survive normalization.
        return value if value == "" else canonicalize(value)

    return canonicalize_keyset_element(position, index=0, canonicalize=normalize)


def _review_envelope[T](
    data: T,
    *,
    contract_type: type[Any],
    total_count: int,
    returned_count: int,
    next_cursor: str | None = None,
    actions: list[str] | None = None,
    degraded: bool = False,
    degraded_reason: str | None = None,
) -> ResponseEnvelope[T]:
    """Build and redact a dynamically classified review envelope."""
    return build_classified_envelope(
        data,
        contract_type=contract_type,
        total_count=total_count,
        returned_count=returned_count,
        next_cursor=next_cursor,
        actions=actions,
        degraded=degraded,
        degraded_reason=degraded_reason,
        has_more=next_cursor is not None,
    )


def _unavailable_queue(kind: ReviewQueueKind, exc: Exception) -> QueueUnavailable:
    """Mark one review queue uncountable instead of failing the whole summary."""
    classified = classify_user_error(exc)
    if classified is not None:
        return QueueUnavailable(
            kind=kind,
            code=classified.code,
            reason=classified.message,
            hint=classified.hint,
        )
    logger.error(
        f"reviews summary queue {kind} raised {type(exc).__name__} "
        f"at {exception_origin(exc)}"
    )
    return QueueUnavailable(
        kind=kind,
        code=error_codes.INFRA_UNCLASSIFIED_ERROR,
        reason=f"Queue count failed with an unhandled {type(exc).__name__}",
    )


def _pending_categorization_rows(
    service: CategorizationService,
) -> list[CategorizationReviewRow]:
    """Project the existing uncategorized queue into normalized rows.

    A missing ``core.uncategorized_queue`` propagates as ``INFRA_SCHEMA_DRIFT``
    rather than degrading to an empty queue. It is the single definition of an
    uncategorized transaction, so without it the queue's contents are unknown,
    not zero — and the summary already reports one broken queue as
    ``unavailable`` instead of failing the aggregate.
    """
    raw_rows = service.list_uncategorized_transactions(limit=None) or []
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
            currency_code=cast(str | None, row.get("currency_code")),
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


def _match_summary(match_type: str, score: float | None) -> str:
    """One-line match summary, saying "unscored" rather than "0.00 confidence".

    An exact-id match records no score; printing 0.00 would tell the agent the
    engine compared the pair and found nothing in common.
    """
    if score is None:
        return f"{match_type} match, no confidence score recorded"
    return f"{match_type} match at {score:.2f} confidence"


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
        match = match_pending_row(row)
        result.append(
            MatchReviewRow(
                decision_id=match.match_id,
                status=match.match_status,
                created_at=_text(row.get("decided_at")),
                summary=_match_summary(match.match_type, match.confidence_score),
                details=MatchPendingDetails(match=match),
            )
        )
    return result


def _match_history_rows(service: MatchingService) -> list[MatchReviewRow]:
    """Project the actual match history path."""
    result: list[MatchReviewRow] = []
    for row in service.get_log(limit=None):
        match = match_history_row(row)
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


def _conflict_matcher(row: Mapping[str, Any]) -> RuleConflictMatcher:
    """Project the matcher both rules in a conflict share."""
    return RuleConflictMatcher(
        merchant_pattern=str(row["proposed_merchant_pattern"]),
        match_type=str(row["proposed_match_type"]),
        min_amount=(
            float(row["proposed_min_amount"])
            if row["proposed_min_amount"] is not None
            else None
        ),
        max_amount=(
            float(row["proposed_max_amount"])
            if row["proposed_max_amount"] is not None
            else None
        ),
        account_id=(
            str(row["proposed_account_id"])
            if row["proposed_account_id"] is not None
            else None
        ),
    )


def _conflict_label(category: str, subcategory: object) -> str:
    """Render a category pair the way both surfaces show it."""
    return f"{category} / {subcategory}" if subcategory else category


def _pending_rule_conflict_rows(
    service: CategorizationService,
) -> list[RuleConflictReviewRow]:
    """Project rule conflicts still describing live rule state."""
    rows: list[RuleConflictReviewRow] = []
    for row in service.list_rule_conflicts():
        existing = _conflict_label(
            str(row["existing_category"]), row["existing_subcategory"]
        )
        proposed = _conflict_label(
            str(row["proposed_category"]), row["proposed_subcategory"]
        )
        rows.append(
            RuleConflictReviewRow(
                decision_id=str(row["conflict_id"]),
                status="pending",
                created_at=_text(row["detected_at"]),
                summary=(
                    f"{row['proposed_merchant_pattern']}: {existing} vs {proposed}"
                ),
                details=RuleConflictPendingDetails(
                    matcher=_conflict_matcher(row),
                    existing_rule_id=str(row["existing_rule_id"]),
                    existing_name=str(row["existing_name"]),
                    existing_category=str(row["existing_category"]),
                    existing_subcategory=row["existing_subcategory"],
                    existing_priority=int(row["existing_priority"]),
                    proposed_name=str(row["proposed_name"]),
                    proposed_category=str(row["proposed_category"]),
                    proposed_subcategory=row["proposed_subcategory"],
                    proposed_priority=int(row["proposed_priority"]),
                    winner_rule_id=str(row["existing_rule_id"]),
                    reason=(
                        f"Rule {row['existing_rule_id']} already matches this "
                        f"pattern and assigns {existing}; the proposal assigns "
                        f"{proposed}."
                    ),
                ),
            )
        )
    return rows


def _rule_conflict_history_rows(
    service: CategorizationService,
) -> list[RuleConflictReviewRow]:
    """Project settled rule conflicts."""
    rows: list[RuleConflictReviewRow] = []
    for row in service.list_rule_conflict_history():
        resolution = cast(
            Literal["replace", "reprioritize", "cancel"], row["resolution"]
        )
        rows.append(
            RuleConflictReviewRow(
                decision_id=str(row["conflict_id"]),
                status="resolved",
                created_at=_text(row["resolved_at"]),
                summary=(f"{row['proposed_merchant_pattern']}: {resolution}"),
                details=RuleConflictHistoryDetails(
                    matcher=_conflict_matcher(row),
                    existing_rule_id=str(row["existing_rule_id"]),
                    existing_category=str(row["existing_category"]),
                    existing_subcategory=row["existing_subcategory"],
                    proposed_name=str(row["proposed_name"]),
                    proposed_category=str(row["proposed_category"]),
                    proposed_subcategory=row["proposed_subcategory"],
                    resolution=resolution,
                    resolved_rule_id=(
                        str(row["resolved_rule_id"])
                        if row["resolved_rule_id"] is not None
                        else None
                    ),
                ),
            )
        )
    return rows


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
        # Both, not either. The name is what an agent chooses by and the id is
        # what it acts on, so "name or id" left every row missing one of them.
        label = payload.provisional_display_name or UNNAMED_ACCOUNT_LABEL
        result.append(
            AccountLinkReviewRow(
                decision_id=decision_id,
                status="pending",
                created_at=timestamp_by_id.get(decision_id),
                summary=(
                    f"{label} [{payload.provisional_account_id}]: "
                    f"{len(payload.candidates)} account candidate(s)"
                ),
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
                    f"{decision.provisional_display_name or UNNAMED_ACCOUNT_LABEL} "
                    f"[{decision.provisional_account_id}] to "
                    f"{decision.candidate_display_name or UNNAMED_ACCOUNT_LABEL} "
                    f"[{decision.candidate_account_id}]: {decision.status}"
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
    if kind == "rule_conflicts":
        conflict_service = CategorizationService(db)
        rows = (
            _pending_rule_conflict_rows(conflict_service)
            if status == "pending"
            else _rule_conflict_history_rows(conflict_service)
        )
        return ReviewsRuleConflictsView(status=status, rows=rows)
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
    if kind == "rule_conflicts":
        return ((str, str), ("asc", "asc"))
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
                # 0.0 for an unscored (exact-id) match: the keyset contract
                # above declares this key `float`, and a cursor key has to be a
                # total order. It sorts, it is not reported — `data` carries the
                # null.
                float(row.details.match.confidence_score or 0.0),
                str(row.decision_id),
            ),
            directions,
        )
    if kind == "rule_conflicts":
        return (
            (_text(row.created_at) or "", str(row.decision_id)),
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
    _, directions = _review_key_contract(kind, status)
    try:
        page, next_cursor, _ = paginate_keyset(
            rows,
            limit=limit,
            key_of=lambda row: _review_ordering(kind, status, row)[0],
            directions=directions,
            namespace="reviews",
            scope={"kind": kind, "status": status},
            position=position,
        )
    except InvalidKeysetCursorError as exc:
        raise UserError(
            "Invalid review pagination cursor.",
            code=error_codes.REVIEW_CURSOR_INVALID,
        ) from exc
    return page, next_cursor


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
    if kind == "rule_conflicts":
        categorization = CategorizationService(db)
        return (
            categorization.count_rule_conflicts()
            if status == "pending"
            else categorization.count_rule_conflict_history()
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
            if kind in {"categorization", "auto_rules", "matches", "rule_conflicts"}
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
        "rule_conflicts",
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
                code=error_codes.REVIEW_PAGINATION_NOT_ALLOWED,
            )
        if status != "pending":
            raise UserError(
                "status is not valid for review summary.",
                code=error_codes.REVIEW_STATUS_NOT_ALLOWED,
            )
        counts: list[ReviewCount] = []
        unavailable: list[QueueUnavailable] = []
        with get_database(read_only=True) as db:
            for queue_kind in _QUEUE_KINDS:
                # Count each queue independently: one missing view (e.g.
                # core.uncategorized_queue) must not fail the whole summary.
                try:
                    queue_counts = [
                        ReviewCount(
                            kind=queue_kind,
                            status=queue_status,
                            count=_review_count(
                                db,
                                kind=queue_kind,
                                status=queue_status,
                            ),
                        )
                        for queue_status in cast(
                            tuple[ReviewStatus, ...], ("pending", "history")
                        )
                    ]
                except Exception as exc:  # noqa: BLE001 — degrade this queue only
                    unavailable.append(_unavailable_queue(queue_kind, exc))
                    continue
                counts.extend(queue_counts)
        payload = ReviewsSummaryView(
            counts=counts,
            total=sum(count.count for count in counts),
            unavailable=unavailable,
        )
        degraded_reason = (
            " ".join(f"{entry.kind}: {entry.reason}" for entry in unavailable) or None
        )
        return _review_envelope(
            payload,
            contract_type=ReviewsSummaryView,
            total_count=len(payload.counts),
            returned_count=len(payload.counts),
            actions=_summary_actions(),
            degraded=bool(unavailable),
            degraded_reason=degraded_reason,
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
        "with deterministic cursor pagination. kind='rule_conflicts' holds "
        "categorization rules refused because an active rule already matches "
        "the same transactions under a different category; each row names the "
        "rule deciding today and the category the refused rule wanted.",
        privacy_actor="reviews",
    )


@mcp_tool(read_only=False, idempotent=True)
def reviews_decide_coarse(
    decisions: list[ReviewDecisionRequest],
) -> ResponseEnvelope[ReviewsDecidePayload]:
    """Accept or reject one atomic ordinary, auto-rule, or conflict batch."""
    operation_id = current_operation_id()
    auto_rule_impact: AutoAcceptPayload | None = None
    rule_conflict_impact: RuleConflictImpact | None = None
    # Stays None on the auto-rule branch, which folds no match edge and so
    # runs no reconciliation — distinct from a pass that reversed nothing.
    transfers_retired: int | None = None
    with get_database(read_only=False) as db:
        auto_rule_decisions = [
            decision
            for decision in decisions
            if isinstance(decision, AutoRuleDecisionRequest)
        ]
        conflict_decisions = [
            decision
            for decision in decisions
            if isinstance(decision, RuleConflictDecisionRequest)
        ]
        if conflict_decisions:
            if len(conflict_decisions) != len(decisions):
                raise UserError(
                    "Rule-conflict and other decisions require separate "
                    "atomic batches.",
                    code=error_codes.MUTATION_INVALID_INPUT,
                )
            resolutions = CategorizationService(db).resolve_rule_conflicts(
                [
                    ConflictDecision(
                        conflict_id=decision.decision_id,
                        resolution=decision.decision,
                        priority=decision.priority,
                    )
                    for decision in conflict_decisions
                ],
                actor="mcp",
            )
            rule_conflict_impact = RuleConflictImpact(
                resolved=len(resolutions),
                activated_rule_ids=[
                    item.rule_id for item in resolutions if item.rule_id is not None
                ],
                superseded_rule_ids=[
                    rule_id
                    for item in resolutions
                    for rule_id in item.superseded_rule_ids
                ],
            )
            outcomes = [
                ReviewDecisionOutcome(
                    kind="rule_conflict",
                    decision_id=item.conflict_id,
                    decision=item.resolution,
                    status="resolved",
                    changed=True,
                    operation_id=operation_id,
                )
                for item in resolutions
            ]
        elif auto_rule_decisions:
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
            applied = ReviewDecisionsService(db, actor="mcp").apply_ordinary(
                ordinary_decisions
            )
            transfers_retired = applied.transfers_retired
            outcomes = [
                ReviewDecisionOutcome(
                    kind=item.request.kind,
                    decision_id=item.request.decision_id,
                    decision=item.request.decision,
                    status=item.status,
                    changed=item.changed,
                    operation_id=operation_id,
                )
                for item in applied.items
            ]
    actions = [
        "Use reviews(status='pending') to continue review",
        "Use system_audit_undo(operation_id=...) to reverse this batch",
    ]
    if transfers_retired:
        # First, and ahead of the batch undo: that one reverses the decisions
        # in this call, which does not restore the *other* transfer the fold
        # retired — the one the user is about to find missing.
        actions.insert(
            0,
            f"This batch retired {transfers_retired} previously accepted "
            "transfer(s) — inspect with system_audit(), restore with "
            "system_audit_undo() if that was wrong, then "
            "refresh_run(steps=['match']) to re-propose over the legs the "
            "reversal freed",
        )
    return build_envelope(
        data=ReviewsDecidePayload(
            results=outcomes,
            applied_count=sum(item.changed for item in outcomes),
            operation_id=operation_id,
            auto_rule_impact=auto_rule_impact,
            rule_conflict_impact=rule_conflict_impact,
            transfers_retired=transfers_retired,
        ),
        actions=actions,
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
    return ConfirmationBinding(
        arguments=arguments,
        resolved_ids=tuple(resolved_ids),
        actor="mcp",
        profile=get_settings().profile,
        authorization_context="local-profile",
        operation_kind="identity_links_decide",
        blast_radius=plan.blast_radius,
    )


@dataclass(frozen=True)
class _IdentityPreview:
    """A planned identity batch plus everything its prompt needs to describe it.

    The facts are gathered here, on the preview's own read-only connection,
    rather than on the plan: ``plan_identity`` also runs inside the write
    transaction to re-verify the grant, and the prompt is long gone by then.
    """

    plan: IdentityDecisionPlan
    merges: tuple[AccountMergeFacts, ...]
    kinds: tuple[str, ...]


def _preview_identity_decisions(
    decisions: list[IdentityDecisionRequest],
) -> _IdentityPreview:
    """Resolve one identity batch on a read-only connection."""
    with get_database(read_only=True) as db:
        plan = ReviewDecisionsService(db, actor="mcp").plan_identity(decisions)
        accepts = [
            item
            for item in plan.items
            if item.changed and item.request.decision == "accept"
        ]
        links = AccountLinksService(db, actor="mcp")
        merges = tuple(
            links.merge_facts(
                absorbed_account_id=item.source_id,
                survivor_account_id=item.target_id,
            )
            for item in accepts
            if item.request.kind == "account_link"
        )
        kinds = tuple(sorted({item.request.kind for item in accepts}))
    return _IdentityPreview(plan=plan, merges=merges, kinds=kinds)


def _identity_remainder_note(plan: IdentityDecisionPlan) -> str | None:
    """Name what the CLI merge command will not have finished for this batch.

    Counted over the planned items rather than the batch's kinds, because the
    kind set answers a different question. It is deduplicated and accept-only,
    so a second merge collapses into the first and a reject never appears at
    all — both then read as nothing left over, when in fact the batch was
    refused whole and every one of them still has to be sent again.
    """
    pending = [item for item in plan.items if item.changed]
    handled = next(
        (
            item
            for item in pending
            if item.request.kind == "account_link" and item.request.decision == "accept"
        ),
        None,
    )
    remainder = [item for item in pending if item is not handled]
    if not remainder:
        return None
    kinds = ", ".join(sorted({item.request.kind for item in remainder}))
    noun = "decision" if len(remainder) == 1 else "decisions"
    return (
        "The whole batch was refused and nothing was written. That command "
        f"decides one account link; the other {len(remainder)} {noun} "
        f"({kinds}) must be sent here again once the merge is confirmed."
    )


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


@mcp_tool(
    read_only=False,
    destructive=True,
    idempotent=True,
    timeout_seconds=180.0,
    # The response carries record ids and counts; the elicitation carries each
    # ledger's first and last transaction dates and the user's own account
    # labels. Only the payload is walked, so without this the audit event would
    # record `low` for a call that put MEDIUM data in front of the caller.
    #
    # MEDIUM, not CRITICAL, and reviewers have asked twice, so: the one
    # genuinely CRITICAL value this prompt could once render was
    # `raw.tabular_accounts.account_name` -- a file-supplied free-text label
    # that can be a bare account number. `fetch_display_names` no longer
    # returns it on any path, so every label the prompt can now show is either
    # `core.dim_accounts.display_name` or the resolver's constructed
    # institution + masked last four.
    #
    # What remains is the last-four evidence line, from
    # `dim_accounts.last_four` (INSTITUTION_ACCOUNT_NUMBER, CRITICAL). It stays,
    # and MEDIUM stays with it. `identifiers.md` forbids narrowing a mask by
    # arguing a *particular* value is safe; this is the per-field, worst-case
    # claim it asks for instead. That class masks PARTIAL (`redaction.py`:
    # `"****" + value[-4:]`), so for every value the column can hold, masking
    # leaves the same four digits the prompt prints as `…4521`. Tier and mask
    # strength are independent here by design, and disclosing a field's own
    # masked form does not raise the disclosure floor.
    discloses=Tier.MEDIUM,
)
async def identity_links_decide_coarse(
    decisions: list[IdentityDecisionRequest],
    confirmation_token: str | None = None,
) -> ResponseEnvelope[IdentityLinksDecidePayload]:
    """Atomically accept or reject account, merchant, and security identity links.

    An accepted account link also re-runs matching, because the merge is what
    makes the two sources' rows comparable at all. That pass auto-merges
    duplicates it is confident about and queues the rest; `rematch_auto_merged`,
    `rematch_pending_review`, and `rematch_pending_transfers` report it, and are
    null when the batch held no accept (no pass ran).
    `rematch_transfers_retired` counts transfers the user had already accepted
    that the merge reversed — their two sides turned out to be one transaction,
    or their two accounts one account — check it before reporting the batch as
    clean.

    Mutation surface: writes app.account_link_decisions + app.account_links,
    app.merchant_links, app.security_links, and on an account accept also
    app.match_decisions (the re-key onto the surviving account, the re-match
    pass, and reversing any transfer it invalidates) plus a rebuild of core.*
    via SQLMesh. Reverse with system_audit_undo(operation_id).
    """
    preview = await asyncio.to_thread(_preview_identity_decisions, decisions)
    plan = preview.plan
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
            message=identity_confirm_message(
                binding.blast_radius,
                surface="mcp",
                merges=preview.merges,
                kinds=preview.kinds,
            ),
            confirmation_token=confirmation_token,
            # An account merge moves a whole ledger history onto another
            # account and is the case `design-principles.md` names at the top
            # of the confirmation bar. The opaque-token fallback hands that
            # confirmation to the calling agent, so this batch forgoes it and
            # takes the prompt or nothing. Merchant- and security-only batches
            # keep the fallback: neither re-keys a transaction.
            elicitation_only="account_link" in preview.kinds,
            cli_equivalent="moneybin accounts links set",
            cli_note=_identity_remainder_note(preview.plan),
        )
    live = await asyncio.to_thread(
        _apply_identity_decisions,
        decisions,
        grant=grant,
        expected_binding=binding,
    )
    operation_id = current_operation_id()
    # An accepted merge re-runs matching, and that pass can auto-merge rows
    # without asking. Same disclosure as accounts_links_set, from one source.
    rematch = live.rematch
    actions = [
        *rematch_actions(rematch),
        "Use reviews(status='pending') to continue identity review",
        "Use system_audit_undo(operation_id=...) to reverse this batch",
    ]
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
            rematch_auto_merged=None
            if rematch is None
            else rematch.matches_auto_merged,
            rematch_pending_review=(
                None if rematch is None else rematch.matches_pending_review
            ),
            rematch_pending_transfers=(
                None if rematch is None else rematch.matches_pending_transfers
            ),
            rematch_transfers_retired=(
                None if rematch is None else rematch.transfers_retired
            ),
        ),
        actions=actions,
    )


def register_review_coarse_writes(mcp: FastMCP) -> None:
    """Register the standard ordinary and identity decision batches."""
    register(
        mcp,
        reviews_decide_coarse,
        "reviews_decide",
        "Accept or reject an atomic batch of transaction, match, auto-rule, or "
        "rule-conflict review decisions. Auto-rule decisions use "
        "kind='auto_rule' and may set allow_broad after inspecting "
        "estimated_match_count; kind='rule_conflict' takes replace (supersede "
        "the existing rule), reprioritize (activate beside it at an explicit "
        "priority), or cancel, writing app.categorization_rules and "
        "app.rule_conflicts — reverse either with system_audit_undo. Keep each "
        "kind in its own call. Accepting a match can reverse a transfer the "
        "user already accepted, once one component holds both its legs: "
        "`transfers_retired` counts those, and each result's `status` is what "
        "committed — an accept that loses that tiebreak reads 'reversed'.",
        privacy_actor="reviews_decide",
    )
    register(
        mcp,
        identity_links_decide_coarse,
        "identity_links_decide",
        "Atomically accept or reject account, merchant, and security identity "
        "link decisions. Accepting a security decision either merges two "
        "instruments' tax lots, manual events, and price marks, or only binds a "
        "price-feed symbol to a security, which deletes nothing and moves no "
        "row; the confirmation prompt names which one and counts every category "
        "it moves. Accepting an account decision re-runs matching over the "
        "merged account, which can reverse a transfer the user already "
        "accepted: `rematch_transfers_retired` counts those, and "
        "system_audit_undo(operation_id=...) restores them. Any accepted merge or "
        "bind confirms "
        "the exact normalized full batch and complete live before-state; "
        "reject-only batches do not prompt. An accepted account link takes the "
        "prompt only — it refuses confirmation_token, and refuses a client that "
        "cannot prompt rather than issue one.",
        privacy_actor="identity_links_decide",
    )


def register_review_tools(mcp: FastMCP) -> None:
    """Register the standard normalized review boundaries."""
    register_review_coarse_reads(mcp)
    register_review_coarse_writes(mcp)
