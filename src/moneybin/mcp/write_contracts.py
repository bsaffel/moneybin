"""Strict request contracts for dormant coarse MCP writes."""

from __future__ import annotations

from decimal import Decimal
from typing import Annotated, Literal, Self

from pydantic import (
    AfterValidator,
    BaseModel,
    ConfigDict,
    Field,
    StrictBool,
    model_validator,
)


def _reject_whitespace_only(value: str) -> str:
    if not value.strip():
        raise ValueError("String must not be whitespace-only")
    return value


def _reject_zero(value: Decimal) -> Decimal:
    if value == 0:
        raise ValueError("Amount must be non-zero")
    return value


NonBlankString = Annotated[
    str,
    Field(min_length=1),
    AfterValidator(_reject_whitespace_only),
]
FiniteDecimal = Annotated[Decimal, Field(allow_inf_nan=False)]
NonZeroFiniteDecimal = Annotated[
    Decimal,
    Field(allow_inf_nan=False),
    AfterValidator(_reject_zero),
]
MatchType = Literal["exact", "contains", "regex"]
FeatureCategory = Literal[
    "mcp-data-sharing",
    "smart-import-parsing",
    "ml-categorization",
    "matching-overview",
]


class _StrictRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)


class SplitTarget(_StrictRequest):
    """One complete transaction split target."""

    amount: NonZeroFiniteDecimal
    category: NonBlankString | None = None
    subcategory: NonBlankString | None = None
    note: str | None = None


class NoteSet(_StrictRequest):
    """Set or clear the note on one transaction."""

    kind: Literal["note_set"]
    transaction_id: NonBlankString
    note: str | None


class TagsSet(_StrictRequest):
    """Replace the complete tag collection on one transaction."""

    kind: Literal["tags_set"]
    transaction_id: NonBlankString
    tags: list[NonBlankString]


class SplitsSet(_StrictRequest):
    """Replace the complete split collection on one transaction."""

    kind: Literal["splits_set"]
    transaction_id: NonBlankString
    splits: list[SplitTarget]


class TagRename(_StrictRequest):
    """Rename one tag everywhere."""

    kind: Literal["tag_rename"]
    old_name: NonBlankString
    new_name: NonBlankString


AnnotationRequest = Annotated[
    NoteSet | TagsSet | SplitsSet | TagRename,
    Field(discriminator="kind"),
]


class CategorizationDecisionRequest(_StrictRequest):
    """Accept or reject one categorization proposal."""

    kind: Literal["categorization"]
    decision_id: NonBlankString
    decision: Literal["accept", "reject"]
    category: NonBlankString | None = None
    subcategory: NonBlankString | None = None
    canonical_merchant_name: NonBlankString | None = None

    @model_validator(mode="after")
    def _validate_decision(self) -> Self:
        result_fields = {
            "category",
            "subcategory",
            "canonical_merchant_name",
        }
        if self.decision == "accept" and self.category is None:
            raise ValueError("Accept requires category")
        if self.decision == "reject" and self.model_fields_set & result_fields:
            raise ValueError("Reject forbids categorization result fields")
        return self


class MatchDecisionRequest(_StrictRequest):
    """Accept or reject one transaction-match proposal."""

    kind: Literal["match"]
    decision_id: NonBlankString
    decision: Literal["accept", "reject"]


ReviewDecisionRequest = Annotated[
    CategorizationDecisionRequest | MatchDecisionRequest,
    Field(discriminator="kind"),
]


class _IdentityDecisionRequest(_StrictRequest):
    decision_id: NonBlankString
    decision: Literal["accept", "reject"]
    target_id: NonBlankString | None = None

    @model_validator(mode="after")
    def _validate_decision(self) -> Self:
        if self.decision == "accept" and self.target_id is None:
            raise ValueError("Accept requires target_id")
        if self.decision == "reject" and "target_id" in self.model_fields_set:
            raise ValueError("Reject forbids target_id")
        return self


class AccountLinkDecisionRequest(_IdentityDecisionRequest):
    """Accept or reject one account-link proposal."""

    kind: Literal["account_link"]


class MerchantLinkDecisionRequest(_IdentityDecisionRequest):
    """Accept or reject one merchant-link proposal."""

    kind: Literal["merchant_link"]


class SecurityLinkDecisionRequest(_IdentityDecisionRequest):
    """Accept or reject one security-link proposal."""

    kind: Literal["security_link"]


IdentityDecisionRequest = Annotated[
    AccountLinkDecisionRequest
    | MerchantLinkDecisionRequest
    | SecurityLinkDecisionRequest,
    Field(discriminator="kind"),
]


class CategoryStateRequest(_StrictRequest):
    """Declare one category's target state."""

    kind: Literal["category"]
    state: Literal["present", "inactive", "absent"]
    category_id: NonBlankString | None = None
    category: NonBlankString | None = None
    subcategory: NonBlankString | None = None
    description: str | None = None
    force: StrictBool = False

    @model_validator(mode="after")
    def _validate_state(self) -> Self:
        replacement_fields = {"category", "subcategory", "description"}
        if self.state == "present":
            if self.category is None:
                raise ValueError("Present requires category")
            if self.force:
                raise ValueError("Only absent accepts force=True")
            return self
        if self.category_id is None:
            raise ValueError(f"{self.state.capitalize()} requires category_id")
        if self.model_fields_set & replacement_fields:
            raise ValueError(f"{self.state.capitalize()} forbids replacement fields")
        if self.state == "inactive" and self.force:
            raise ValueError("Only absent accepts force=True")
        return self


class MerchantStateRequest(_StrictRequest):
    """Declare one merchant mapping's target state."""

    kind: Literal["merchant"]
    state: Literal["present", "absent"]
    merchant_id: NonBlankString | None = None
    raw_pattern: NonBlankString | None = None
    canonical_name: NonBlankString | None = None
    match_type: MatchType | None = None
    category: NonBlankString | None = None
    subcategory: NonBlankString | None = None

    @model_validator(mode="after")
    def _validate_state(self) -> Self:
        replacement_fields = {
            "raw_pattern",
            "canonical_name",
            "match_type",
            "category",
            "subcategory",
        }
        if self.state == "present":
            if self.raw_pattern is None or self.canonical_name is None:
                raise ValueError("Present requires raw_pattern and canonical_name")
            return self
        if self.merchant_id is None:
            raise ValueError("Absent requires merchant_id")
        if self.model_fields_set & replacement_fields:
            raise ValueError("Absent forbids replacement fields")
        return self


TaxonomyStateRequest = Annotated[
    CategoryStateRequest | MerchantStateRequest,
    Field(discriminator="kind"),
]


class ConsentStateRequest(_StrictRequest):
    """Declare consent for one feature category."""

    kind: Literal["consent"]
    category: FeatureCategory
    state: Literal["granted", "revoked"]
    backend: NonBlankString | None = None
    mode: Literal["persistent", "one-time"] | None = None

    @model_validator(mode="after")
    def _validate_state(self) -> Self:
        if self.state == "revoked" and "mode" in self.model_fields_set:
            raise ValueError("Revoked consent forbids mode")
        return self


class CategorizationRuleMatch(_StrictRequest):
    """Strict matching target for one categorization rule."""

    type: MatchType
    value: NonBlankString
    min_amount: FiniteDecimal | None = None
    max_amount: FiniteDecimal | None = None
    account_id: NonBlankString | None = None


class CategorizationRuleTarget(_StrictRequest):
    """Declare one categorization rule's target state."""

    kind: Literal["rule"]
    rule_id: NonBlankString | None = None
    state: Literal["present", "inactive", "absent"]
    match: CategorizationRuleMatch | None = None
    category: NonBlankString | None = None
    priority: int | None = Field(default=None, ge=0, le=10_000)

    @model_validator(mode="after")
    def _validate_state(self) -> Self:
        replacement_fields = {"match", "category", "priority"}
        if self.state == "present":
            if self.match is None or self.category is None or self.priority is None:
                raise ValueError("Present requires match, category, and priority")
            return self
        if self.rule_id is None:
            raise ValueError(f"{self.state.capitalize()} requires rule_id")
        if self.model_fields_set & replacement_fields:
            raise ValueError(f"{self.state.capitalize()} forbids replacement fields")
        return self
