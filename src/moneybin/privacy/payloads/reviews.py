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
    "account_links",
    "merchant_links",
    "security_links",
]
ReviewStatus = Literal["pending", "history"]
ReviewDecisionKind = Literal["categorization", "auto_rule", "match"]
IdentityDecisionKind = Literal["account_link", "merchant_link", "security_link"]


class ReviewCount(BaseModel):
    """Exact count for one queue and one collection state."""

    model_config = ConfigDict(frozen=True)

    kind: Annotated[ReviewQueueKind, DataClass.TXN_TYPE]
    status: Annotated[ReviewStatus, DataClass.TXN_TYPE]
    count: Annotated[int, DataClass.AGGREGATE]


class ReviewsSummaryView(BaseModel):
    """Exact counts for every normalized review collection."""

    model_config = ConfigDict(frozen=True)

    kind: Annotated[Literal["summary"], DataClass.TXN_TYPE] = "summary"
    counts: list[ReviewCount]
    total: Annotated[int, DataClass.AGGREGATE]


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


ReviewsCoarsePayload = (
    ReviewsSummaryView
    | ReviewsCategorizationView
    | ReviewsAutoRulesView
    | ReviewsMatchesView
    | ReviewsAccountLinksView
    | ReviewsMerchantLinksView
    | ReviewsSecurityLinksView
)


class ReviewDecisionOutcome(BaseModel):
    """Outcome for one ordinary review decision."""

    model_config = ConfigDict(frozen=True)

    kind: Annotated[ReviewDecisionKind, DataClass.TXN_TYPE]
    decision_id: Annotated[str, DataClass.RECORD_ID]
    decision: Annotated[Literal["accept", "reject"], DataClass.TXN_TYPE]
    status: Annotated[str, DataClass.TXN_TYPE]
    changed: Annotated[bool, DataClass.AGGREGATE]
    operation_id: Annotated[str, DataClass.RECORD_ID]


class ReviewsDecidePayload(BaseModel):
    """Ordered outcomes for one atomic ordinary-decision batch."""

    model_config = ConfigDict(frozen=True)

    results: list[ReviewDecisionOutcome]
    applied_count: Annotated[int, DataClass.AGGREGATE]
    operation_id: Annotated[str, DataClass.RECORD_ID]
    auto_rule_impact: AutoAcceptPayload | None = None


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
