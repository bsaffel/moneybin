"""Typed payloads for the normalized review read and decision boundaries."""

from __future__ import annotations

from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field

from moneybin.privacy.payloads.accounts import (
    LinkCandidateRow,
    LinkHistoryRow,
    LinkPendingGroup,
)
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
from moneybin.privacy.payloads.transactions import MatchHistoryRow, MatchPendingRow
from moneybin.privacy.taxonomy import DataClass

ReviewQueueKind = Literal[
    "categorization",
    "auto_rules",
    "matches",
    "rule_conflicts",
    "account_links",
    "merchant_links",
    "security_links",
]
ReviewStatus = Literal["pending", "history"]
ReviewDecisionKind = Literal["categorization", "auto_rule", "match", "rule_conflict"]
#: Every verb a review decision can carry. The first two are the accept/reject
#: axis every other queue uses; the last three are the rule-conflict
#: resolutions, which are not accept/reject — a conflict has three outcomes and
#: two of them activate a rule.
ReviewDecisionVerb = Literal["accept", "reject", "replace", "reprioritize", "cancel"]
IdentityDecisionKind = Literal["account_link", "merchant_link", "security_link"]


class ReviewCount(BaseModel):
    """Exact count for one queue and one collection state."""

    model_config = ConfigDict(frozen=True)

    kind: Annotated[ReviewQueueKind, DataClass.TXN_TYPE]
    status: Annotated[ReviewStatus, DataClass.TXN_TYPE]
    count: Annotated[int, DataClass.AGGREGATE]


class QueueUnavailable(BaseModel):
    """One review queue that could not be counted, and why.

    A queue whose backing view is missing must not fail the whole summary:
    the healthy queues still report exact counts and this names the one that
    did not. ``reason`` carries whatever ``classify_user_error`` produced —
    see ``SectionUnavailable`` for what that does and does not guarantee.
    """

    model_config = ConfigDict(frozen=True)

    kind: Annotated[ReviewQueueKind, DataClass.TXN_TYPE]
    code: Annotated[str, DataClass.TXN_TYPE]
    # Free text carrying exception-derived content — classified as such, not as
    # the low-tier label `code` is. See ``SectionUnavailable``.
    reason: Annotated[str, DataClass.DESCRIPTION]
    hint: Annotated[str | None, DataClass.DESCRIPTION] = None


class ReviewsSummaryView(BaseModel):
    """Exact counts for every normalized review collection.

    ``total`` sums only the queues that reported. When ``unavailable`` is
    non-empty the envelope is marked degraded, so a caller never reads the
    total as complete.
    """

    model_config = ConfigDict(frozen=True)

    kind: Annotated[Literal["summary"], DataClass.TXN_TYPE] = "summary"
    counts: list[ReviewCount]
    total: Annotated[int, DataClass.AGGREGATE]
    unavailable: list[QueueUnavailable] = []


class CategorizationPendingDetails(BaseModel):
    """One uncategorized transaction awaiting a decision."""

    model_config = ConfigDict(frozen=True)

    state: Annotated[Literal["pending"], DataClass.TXN_TYPE] = "pending"
    transaction: PendingTxnRow


class CategorizationHistoryDetails(BaseModel):
    """One terminal transaction categorization decision."""

    model_config = ConfigDict(frozen=True)

    state: Annotated[Literal["history"], DataClass.TXN_TYPE] = "history"
    transaction_id: Annotated[str, DataClass.RECORD_ID]
    decision_status: Annotated[
        Literal["accepted", "rejected", "superseded"],
        DataClass.TXN_TYPE,
    ]
    category_id: Annotated[str | None, DataClass.CATEGORY]
    category: Annotated[str | None, DataClass.CATEGORY]
    subcategory: Annotated[str | None, DataClass.CATEGORY]
    categorized_by: Annotated[str, DataClass.TXN_TYPE]
    merchant_id: Annotated[str | None, DataClass.RECORD_ID]
    confidence: Annotated[float | None, DataClass.AGGREGATE]
    rule_id: Annotated[str | None, DataClass.RECORD_ID]
    source_type: Annotated[str, DataClass.TXN_TYPE]
    reversed_at: Annotated[str | None, DataClass.TIMESTAMP_OBSERVABILITY]
    reversed_by: Annotated[str | None, DataClass.TXN_TYPE]


CategorizationDetails = Annotated[
    CategorizationPendingDetails | CategorizationHistoryDetails,
    Field(discriminator="state"),
]


class CategorizationReviewRow(BaseModel):
    """Normalized categorization queue row."""

    model_config = ConfigDict(frozen=True)

    decision_id: Annotated[str, DataClass.RECORD_ID]
    kind: Annotated[Literal["categorization"], DataClass.TXN_TYPE] = "categorization"
    status: Annotated[str, DataClass.TXN_TYPE]
    created_at: Annotated[str | None, DataClass.TIMESTAMP_OBSERVABILITY]
    summary: Annotated[str, DataClass.DESCRIPTION]
    details: CategorizationDetails


class ReviewsCategorizationView(BaseModel):
    """Categorization pending or history collection."""

    model_config = ConfigDict(frozen=True)

    kind: Annotated[Literal["categorization"], DataClass.TXN_TYPE] = "categorization"
    status: Annotated[ReviewStatus, DataClass.TXN_TYPE]
    rows: list[CategorizationReviewRow]


class AutoRulePendingDetails(BaseModel):
    """One auto-generated categorization rule awaiting review."""

    model_config = ConfigDict(frozen=True)

    state: Annotated[Literal["pending"], DataClass.TXN_TYPE] = "pending"
    proposal: AutoReviewProposalRow


class AutoRuleHistoryDetails(BaseModel):
    """One terminal auto-rule proposal decision."""

    model_config = ConfigDict(frozen=True)

    state: Annotated[Literal["history"], DataClass.TXN_TYPE] = "history"
    merchant_pattern: Annotated[str, DataClass.MERCHANT_NAME]
    match_type: Annotated[str, DataClass.TXN_TYPE]
    category: Annotated[str, DataClass.CATEGORY]
    subcategory: Annotated[str | None, DataClass.CATEGORY]
    trigger_count: Annotated[int, DataClass.AGGREGATE]
    sample_txn_ids: Annotated[list[str], DataClass.RECORD_ID]
    decision_status: Annotated[
        Literal["approved", "rejected", "superseded"],
        DataClass.TXN_TYPE,
    ]
    rule_id: Annotated[str | None, DataClass.RECORD_ID]
    decided_by: Annotated[str | None, DataClass.TXN_TYPE]


AutoRuleDetails = Annotated[
    AutoRulePendingDetails | AutoRuleHistoryDetails,
    Field(discriminator="state"),
]


class AutoRuleReviewRow(BaseModel):
    """Normalized auto-rule proposal row."""

    model_config = ConfigDict(frozen=True)

    decision_id: Annotated[str, DataClass.RECORD_ID]
    kind: Annotated[Literal["auto_rules"], DataClass.TXN_TYPE] = "auto_rules"
    status: Annotated[str, DataClass.TXN_TYPE]
    created_at: Annotated[str | None, DataClass.TIMESTAMP_OBSERVABILITY]
    summary: Annotated[str, DataClass.MERCHANT_NAME]
    details: AutoRuleDetails


class ReviewsAutoRulesView(BaseModel):
    """Auto-rule pending or history collection."""

    model_config = ConfigDict(frozen=True)

    kind: Annotated[Literal["auto_rules"], DataClass.TXN_TYPE] = "auto_rules"
    status: Annotated[ReviewStatus, DataClass.TXN_TYPE]
    rows: list[AutoRuleReviewRow]


class MatchPendingDetails(BaseModel):
    """Pending transaction-match details."""

    model_config = ConfigDict(frozen=True)

    state: Annotated[Literal["pending"], DataClass.TXN_TYPE] = "pending"
    match: MatchPendingRow


class MatchHistoryDetails(BaseModel):
    """Past transaction-match decision details."""

    model_config = ConfigDict(frozen=True)

    state: Annotated[Literal["history"], DataClass.TXN_TYPE] = "history"
    match: MatchHistoryRow


MatchDetails = Annotated[
    MatchPendingDetails | MatchHistoryDetails,
    Field(discriminator="state"),
]


class MatchReviewRow(BaseModel):
    """Normalized match queue row."""

    model_config = ConfigDict(frozen=True)

    decision_id: Annotated[str, DataClass.RECORD_ID]
    kind: Annotated[Literal["matches"], DataClass.TXN_TYPE] = "matches"
    status: Annotated[str, DataClass.TXN_TYPE]
    created_at: Annotated[str | None, DataClass.TIMESTAMP_OBSERVABILITY]
    summary: Annotated[str, DataClass.TXN_TYPE]
    details: MatchDetails


class ReviewsMatchesView(BaseModel):
    """Match pending or history collection."""

    model_config = ConfigDict(frozen=True)

    kind: Annotated[Literal["matches"], DataClass.TXN_TYPE] = "matches"
    status: Annotated[ReviewStatus, DataClass.TXN_TYPE]
    rows: list[MatchReviewRow]


class AccountLinkPendingDetails(BaseModel):
    """One grouped pending account-link review unit."""

    model_config = ConfigDict(frozen=True)

    state: Annotated[Literal["pending"], DataClass.TXN_TYPE] = "pending"
    group: LinkPendingGroup

    @property
    def candidates(self) -> list[LinkCandidateRow]:
        """Expose candidates directly for ergonomic typed access."""
        return self.group.candidates


class AccountLinkHistoryDetails(BaseModel):
    """One past account-link decision."""

    model_config = ConfigDict(frozen=True)

    state: Annotated[Literal["history"], DataClass.TXN_TYPE] = "history"
    decision: LinkHistoryRow


AccountLinkDetails = Annotated[
    AccountLinkPendingDetails | AccountLinkHistoryDetails,
    Field(discriminator="state"),
]


class AccountLinkReviewRow(BaseModel):
    """Normalized account-link queue row."""

    model_config = ConfigDict(frozen=True)

    decision_id: Annotated[str, DataClass.RECORD_ID]
    kind: Annotated[Literal["account_links"], DataClass.TXN_TYPE] = "account_links"
    status: Annotated[str, DataClass.TXN_TYPE]
    created_at: Annotated[str | None, DataClass.TIMESTAMP_OBSERVABILITY]
    summary: Annotated[str, DataClass.USER_NOTE]
    details: AccountLinkDetails


class ReviewsAccountLinksView(BaseModel):
    """Account-link pending or history collection."""

    model_config = ConfigDict(frozen=True)

    kind: Annotated[Literal["account_links"], DataClass.TXN_TYPE] = "account_links"
    status: Annotated[ReviewStatus, DataClass.TXN_TYPE]
    rows: list[AccountLinkReviewRow]


class MerchantLinkPendingDetails(BaseModel):
    """One grouped pending merchant-link review unit."""

    model_config = ConfigDict(frozen=True)

    state: Annotated[Literal["pending"], DataClass.TXN_TYPE] = "pending"
    group: MerchantLinkPendingGroup


class MerchantLinkHistoryDetails(BaseModel):
    """One past merchant-link decision."""

    model_config = ConfigDict(frozen=True)

    state: Annotated[Literal["history"], DataClass.TXN_TYPE] = "history"
    decision: MerchantLinkHistoryRow


MerchantLinkDetails = Annotated[
    MerchantLinkPendingDetails | MerchantLinkHistoryDetails,
    Field(discriminator="state"),
]


class MerchantLinkReviewRow(BaseModel):
    """Normalized merchant-link queue row."""

    model_config = ConfigDict(frozen=True)

    decision_id: Annotated[str, DataClass.RECORD_ID]
    kind: Annotated[Literal["merchant_links"], DataClass.TXN_TYPE] = "merchant_links"
    status: Annotated[str, DataClass.TXN_TYPE]
    created_at: Annotated[str | None, DataClass.TIMESTAMP_OBSERVABILITY]
    summary: Annotated[str, DataClass.MERCHANT_NAME]
    details: MerchantLinkDetails


class ReviewsMerchantLinksView(BaseModel):
    """Merchant-link pending or history collection."""

    model_config = ConfigDict(frozen=True)

    kind: Annotated[Literal["merchant_links"], DataClass.TXN_TYPE] = "merchant_links"
    status: Annotated[ReviewStatus, DataClass.TXN_TYPE]
    rows: list[MerchantLinkReviewRow]


class SecurityLinkPendingDetails(BaseModel):
    """One grouped pending security-link review unit."""

    model_config = ConfigDict(frozen=True)

    state: Annotated[Literal["pending"], DataClass.TXN_TYPE] = "pending"
    group: SecurityLinkPendingGroup


class SecurityLinkHistoryDetails(BaseModel):
    """One past security-link decision."""

    model_config = ConfigDict(frozen=True)

    state: Annotated[Literal["history"], DataClass.TXN_TYPE] = "history"
    decision: SecurityLinkHistoryRow


SecurityLinkDetails = Annotated[
    SecurityLinkPendingDetails | SecurityLinkHistoryDetails,
    Field(discriminator="state"),
]


class SecurityLinkReviewRow(BaseModel):
    """Normalized security-link queue row."""

    model_config = ConfigDict(frozen=True)

    decision_id: Annotated[str, DataClass.RECORD_ID]
    kind: Annotated[Literal["security_links"], DataClass.TXN_TYPE] = "security_links"
    status: Annotated[str, DataClass.TXN_TYPE]
    created_at: Annotated[str | None, DataClass.TIMESTAMP_OBSERVABILITY]
    summary: Annotated[str, DataClass.USER_NOTE]
    details: SecurityLinkDetails


class ReviewsSecurityLinksView(BaseModel):
    """Security-link pending or history collection."""

    model_config = ConfigDict(frozen=True)

    kind: Annotated[Literal["security_links"], DataClass.TXN_TYPE] = "security_links"
    status: Annotated[ReviewStatus, DataClass.TXN_TYPE]
    rows: list[SecurityLinkReviewRow]


class RuleConflictMatcher(BaseModel):
    """The matcher both rules in a conflict share."""

    model_config = ConfigDict(frozen=True)

    merchant_pattern: Annotated[str, DataClass.MERCHANT_NAME]
    match_type: Annotated[str, DataClass.TXN_TYPE]
    min_amount: Annotated[float | None, DataClass.TXN_AMOUNT]
    max_amount: Annotated[float | None, DataClass.TXN_AMOUNT]
    # RECORD_ID (spec D6): opaque canonical surrogate, not PII.
    account_id: Annotated[str | None, DataClass.RECORD_ID]


class RuleConflictPendingDetails(BaseModel):
    """One rule conflict awaiting a decision."""

    model_config = ConfigDict(frozen=True)

    state: Annotated[Literal["pending"], DataClass.TXN_TYPE] = "pending"
    matcher: RuleConflictMatcher
    existing_rule_id: Annotated[str, DataClass.RECORD_ID]
    existing_name: Annotated[str, DataClass.USER_NOTE]
    existing_category: Annotated[str, DataClass.CATEGORY]
    existing_subcategory: Annotated[str | None, DataClass.CATEGORY]
    existing_priority: Annotated[int, DataClass.AGGREGATE]
    proposed_name: Annotated[str, DataClass.USER_NOTE]
    proposed_category: Annotated[str, DataClass.CATEGORY]
    proposed_subcategory: Annotated[str | None, DataClass.CATEGORY]
    proposed_priority: Annotated[int, DataClass.AGGREGATE]
    # The rule deciding the category today, and why it wins — without it a
    # caller cannot tell which of the two identical matchers is in effect.
    winner_rule_id: Annotated[str, DataClass.RECORD_ID]
    reason: Annotated[str, DataClass.CATEGORY]


class RuleConflictHistoryDetails(BaseModel):
    """One settled rule conflict."""

    model_config = ConfigDict(frozen=True)

    state: Annotated[Literal["history"], DataClass.TXN_TYPE] = "history"
    matcher: RuleConflictMatcher
    existing_rule_id: Annotated[str, DataClass.RECORD_ID]
    existing_category: Annotated[str, DataClass.CATEGORY]
    existing_subcategory: Annotated[str | None, DataClass.CATEGORY]
    proposed_name: Annotated[str, DataClass.USER_NOTE]
    proposed_category: Annotated[str, DataClass.CATEGORY]
    proposed_subcategory: Annotated[str | None, DataClass.CATEGORY]
    resolution: Annotated[
        Literal["replace", "reprioritize", "cancel"], DataClass.TXN_TYPE
    ]
    resolved_rule_id: Annotated[str | None, DataClass.RECORD_ID]


RuleConflictDetails = Annotated[
    RuleConflictPendingDetails | RuleConflictHistoryDetails,
    Field(discriminator="state"),
]


class RuleConflictReviewRow(BaseModel):
    """Normalized rule-conflict row."""

    model_config = ConfigDict(frozen=True)

    decision_id: Annotated[str, DataClass.RECORD_ID]
    kind: Annotated[Literal["rule_conflicts"], DataClass.TXN_TYPE] = "rule_conflicts"
    status: Annotated[str, DataClass.TXN_TYPE]
    created_at: Annotated[str | None, DataClass.TIMESTAMP_OBSERVABILITY]
    summary: Annotated[str, DataClass.MERCHANT_NAME]
    details: RuleConflictDetails


class ReviewsRuleConflictsView(BaseModel):
    """Rule-conflict pending or history collection."""

    model_config = ConfigDict(frozen=True)

    kind: Annotated[Literal["rule_conflicts"], DataClass.TXN_TYPE] = "rule_conflicts"
    status: Annotated[ReviewStatus, DataClass.TXN_TYPE]
    rows: list[RuleConflictReviewRow]


ReviewsCoarsePayload = (
    ReviewsSummaryView
    | ReviewsCategorizationView
    | ReviewsAutoRulesView
    | ReviewsMatchesView
    | ReviewsRuleConflictsView
    | ReviewsAccountLinksView
    | ReviewsMerchantLinksView
    | ReviewsSecurityLinksView
)


class ReviewDecisionOutcome(BaseModel):
    """Outcome for one ordinary review decision."""

    model_config = ConfigDict(frozen=True)

    kind: Annotated[ReviewDecisionKind, DataClass.TXN_TYPE]
    decision_id: Annotated[str, DataClass.RECORD_ID]
    decision: Annotated[ReviewDecisionVerb, DataClass.TXN_TYPE]
    status: Annotated[str, DataClass.TXN_TYPE]
    changed: Annotated[bool, DataClass.AGGREGATE]
    operation_id: Annotated[str, DataClass.RECORD_ID]


class RuleConflictImpact(BaseModel):
    """What one rule-conflict resolution batch did to the rule table."""

    model_config = ConfigDict(frozen=True)

    resolved: Annotated[int, DataClass.AGGREGATE]
    activated_rule_ids: Annotated[list[str], DataClass.RECORD_ID]
    superseded_rule_ids: Annotated[list[str], DataClass.RECORD_ID]


class ReviewsDecidePayload(BaseModel):
    """Ordered outcomes for one atomic ordinary-decision batch."""

    model_config = ConfigDict(frozen=True)

    results: list[ReviewDecisionOutcome]
    applied_count: Annotated[int, DataClass.AGGREGATE]
    operation_id: Annotated[str, DataClass.RECORD_ID]
    auto_rule_impact: AutoAcceptPayload | None = None
    # None when the batch held no rule-conflict decision — a batch that ran and
    # activated nothing is a different fact from one that never touched rules.
    rule_conflict_impact: RuleConflictImpact | None = None
    # Standing transfers the batch's accepts reversed, because dedup made both
    # of their sides the same physical transaction. In `data`, not only in
    # `actions[]`, for the reason the identity payload carries its own: a
    # caller reading the outcomes alone would never learn a decision of theirs
    # was undone. None when the batch accepted no match — no reconciliation
    # ran, which is not the same as one that ran and reversed nothing.
    transfers_retired: Annotated[int | None, DataClass.AGGREGATE] = None


class IdentityDecisionOutcome(BaseModel):
    """Outcome for one identity-link decision."""

    model_config = ConfigDict(frozen=True)

    kind: Annotated[IdentityDecisionKind, DataClass.TXN_TYPE]
    decision_id: Annotated[str, DataClass.RECORD_ID]
    decision: Annotated[Literal["accept", "reject"], DataClass.TXN_TYPE]
    status: Annotated[str, DataClass.TXN_TYPE]
    changed: Annotated[bool, DataClass.AGGREGATE]
    operation_id: Annotated[str, DataClass.RECORD_ID]


class IdentityLinksDecidePayload(BaseModel):
    """Ordered outcomes for one atomic identity-decision batch."""

    model_config = ConfigDict(frozen=True)

    results: list[IdentityDecisionOutcome]
    applied_count: Annotated[int, DataClass.AGGREGATE]
    operation_id: Annotated[str, DataClass.RECORD_ID]
    # What the merge's re-match found, mirroring AccountLinksSetPayload so the
    # batched and direct accept paths disclose the same thing. None when the
    # batch held no accept, which runs no match pass at all — distinct from a
    # pass that ran and found nothing (0).
    rematch_auto_merged: Annotated[int | None, DataClass.AGGREGATE] = None
    rematch_pending_review: Annotated[int | None, DataClass.AGGREGATE] = None
    rematch_pending_transfers: Annotated[int | None, DataClass.AGGREGATE] = None
    # Transfers the user had already accepted that the pass reversed, because
    # dedup made both sides the same physical transaction. In `data`, not only
    # in `actions[]`: a caller reading the counts alone would otherwise never
    # learn a decision of theirs was undone.
    rematch_transfers_retired: Annotated[int | None, DataClass.AGGREGATE] = None
