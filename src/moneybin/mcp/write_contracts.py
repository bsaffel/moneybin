"""Strict request contracts for standard coarse MCP writes."""

from __future__ import annotations

from decimal import Decimal
from typing import Annotated, Literal, Self

from pydantic import (
    AfterValidator,
    BaseModel,
    BeforeValidator,
    ConfigDict,
    Field,
    JsonValue,
    StrictBool,
    model_validator,
)

from moneybin.services._validators import (
    CATEGORY_NAME_MAX_LEN,
    DESCRIPTION_MAX_LEN,
    IDENTIFIER_MAX_LEN,
    MERCHANT_NAME_MAX_LEN,
    MERCHANT_PATTERN_MAX_LEN,
    NOTE_MAX_LEN,
    SLUG_MAX_LEN,
)
from moneybin.vocabulary import CategorizationMatchType, ConsentFeatureCategory


def _reject_whitespace_only(value: str) -> str:
    if not value.strip():
        raise ValueError("String must not be whitespace-only")
    return value


def _coerce_finite_json_number(value: object) -> Decimal:
    if isinstance(value, Decimal):
        number = value
    elif isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError("Value must be a finite JSON number or Decimal")
    else:
        number = Decimal(str(value))
    if not number.is_finite():
        raise ValueError("Value must be a finite JSON number or Decimal")
    return number


def _reject_zero(value: Decimal) -> Decimal:
    if value == 0:
        raise ValueError("Amount must be non-zero")
    return value


def _conditional_schema_branch(
    field: str,
    value: str,
    *,
    required: tuple[str, ...] = (),
    forbidden: tuple[str, ...] = (),
    properties: dict[str, JsonValue] | None = None,
) -> dict[str, JsonValue]:
    then: dict[str, JsonValue] = {}
    conditional_properties = dict(properties or {})
    if required:
        then["required"] = list(required)
        conditional_properties.update({
            name: {"not": {"type": "null"}} for name in required
        })
    if forbidden:
        then["not"] = {
            "anyOf": [{"required": [name]} for name in forbidden],
        }
    if conditional_properties:
        then["properties"] = conditional_properties
    return {
        "if": {
            "properties": {field: {"const": value}},
            "required": [field],
        },
        "then": then,
    }


def _conditional_schema_extra(
    *branches: dict[str, JsonValue],
) -> dict[str, JsonValue]:
    return {"allOf": list(branches)}


IdentifierString = Annotated[
    str,
    Field(min_length=1, max_length=IDENTIFIER_MAX_LEN),
    AfterValidator(_reject_whitespace_only),
]
CategoryName = Annotated[
    str,
    Field(min_length=1, max_length=CATEGORY_NAME_MAX_LEN),
    AfterValidator(_reject_whitespace_only),
]
MerchantName = Annotated[
    str,
    Field(min_length=1, max_length=MERCHANT_NAME_MAX_LEN),
    AfterValidator(_reject_whitespace_only),
]
MerchantPattern = Annotated[
    str,
    Field(min_length=1, max_length=MERCHANT_PATTERN_MAX_LEN),
    AfterValidator(_reject_whitespace_only),
]
SlugString = Annotated[
    str,
    Field(min_length=1, max_length=SLUG_MAX_LEN),
    AfterValidator(_reject_whitespace_only),
]
DescriptionText = Annotated[str, Field(max_length=DESCRIPTION_MAX_LEN)]
NoteText = Annotated[str, Field(max_length=NOTE_MAX_LEN)]
FiniteDecimal = Annotated[
    Decimal,
    BeforeValidator(
        _coerce_finite_json_number,
        json_schema_input_type=int | float,
    ),
    Field(allow_inf_nan=False),
]
NonZeroFiniteDecimal = Annotated[
    FiniteDecimal,
    AfterValidator(_reject_zero),
]
SplitAmount = Annotated[
    NonZeroFiniteDecimal,
    Field(
        ge=Decimal("-9999999999999999.99"),
        le=Decimal("9999999999999999.99"),
        max_digits=18,
        decimal_places=2,
    ),
]
MatchType = CategorizationMatchType
FeatureCategory = ConsentFeatureCategory


class _StrictRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)


class SplitTarget(_StrictRequest):
    """One complete transaction split target."""

    model_config = ConfigDict(
        extra="forbid",
        strict=True,
        json_schema_extra={
            "allOf": [
                {
                    "if": {
                        "properties": {
                            "subcategory": {"not": {"type": "null"}},
                        },
                        "required": ["subcategory"],
                    },
                    "then": {
                        "properties": {
                            "category": {"not": {"type": "null"}},
                        },
                        "required": ["category"],
                    },
                }
            ]
        },
    )

    amount: SplitAmount
    category: CategoryName | None = None
    subcategory: CategoryName | None = None
    note: NoteText | None = None

    @model_validator(mode="after")
    def _validate_category_hierarchy(self) -> Self:
        if self.subcategory is not None and self.category is None:
            raise ValueError("Split subcategory requires category")
        return self


class NoteSet(_StrictRequest):
    """Set or clear the note on one transaction."""

    kind: Literal["note_set"]
    transaction_id: IdentifierString
    note: NoteText | None


class TagsSet(_StrictRequest):
    """Replace the complete tag collection on one transaction."""

    kind: Literal["tags_set"]
    transaction_id: IdentifierString
    tags: list[SlugString]


class SplitsSet(_StrictRequest):
    """Replace the complete split collection on one transaction."""

    kind: Literal["splits_set"]
    transaction_id: IdentifierString
    splits: list[SplitTarget]


class TagRename(_StrictRequest):
    """Rename one tag everywhere."""

    kind: Literal["tag_rename"]
    old_name: SlugString
    new_name: SlugString


AnnotationRequest = Annotated[
    NoteSet | TagsSet | SplitsSet | TagRename,
    Field(discriminator="kind"),
]


class CategorizationDecisionRequest(_StrictRequest):
    """Accept or reject one categorization proposal."""

    model_config = ConfigDict(
        json_schema_extra=_conditional_schema_extra(
            _conditional_schema_branch(
                "decision",
                "accept",
                required=("category",),
            ),
            _conditional_schema_branch(
                "decision",
                "reject",
                forbidden=(
                    "category",
                    "subcategory",
                    "canonical_merchant_name",
                ),
            ),
        )
    )

    kind: Literal["categorization"]
    decision_id: IdentifierString
    decision: Literal["accept", "reject"]
    category: CategoryName | None = None
    subcategory: CategoryName | None = None
    canonical_merchant_name: MerchantName | None = None

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
    decision_id: IdentifierString
    decision: Literal["accept", "reject"]


class AutoRuleDecisionRequest(_StrictRequest):
    """Accept or reject one auto-generated categorization-rule proposal."""

    model_config = ConfigDict(
        extra="forbid",
        strict=True,
        json_schema_extra=_conditional_schema_extra(
            _conditional_schema_branch(
                "decision",
                "reject",
                properties={"allow_broad": {"const": False}},
            )
        ),
    )

    kind: Literal["auto_rule"]
    decision_id: IdentifierString
    decision: Literal["accept", "reject"]
    allow_broad: StrictBool = False

    @model_validator(mode="after")
    def _validate_decision(self) -> Self:
        if self.decision == "reject" and self.allow_broad:
            raise ValueError("Reject forbids allow_broad")
        return self


OrdinaryReviewDecisionRequest = CategorizationDecisionRequest | MatchDecisionRequest


ReviewDecisionRequest = Annotated[
    OrdinaryReviewDecisionRequest | AutoRuleDecisionRequest,
    Field(discriminator="kind"),
]


class _IdentityDecisionRequest(_StrictRequest):
    model_config = ConfigDict(
        json_schema_extra=_conditional_schema_extra(
            _conditional_schema_branch(
                "decision",
                "accept",
                required=("target_id",),
            ),
            _conditional_schema_branch(
                "decision",
                "reject",
                forbidden=("target_id",),
            ),
        )
    )

    decision_id: IdentifierString
    decision: Literal["accept", "reject"]
    target_id: IdentifierString | None = None

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

    model_config = ConfigDict(
        json_schema_extra=_conditional_schema_extra(
            _conditional_schema_branch(
                "state",
                "present",
                required=("category",),
                properties={"force": {"const": False}},
            ),
            _conditional_schema_branch(
                "state",
                "inactive",
                required=("category_id",),
                forbidden=("category", "subcategory", "description"),
                properties={"force": {"const": False}},
            ),
            _conditional_schema_branch(
                "state",
                "absent",
                required=("category_id",),
                forbidden=("category", "subcategory", "description"),
            ),
        )
    )

    kind: Literal["category"]
    state: Literal["present", "inactive", "absent"]
    category_id: IdentifierString | None = None
    category: CategoryName | None = None
    subcategory: CategoryName | None = None
    description: DescriptionText | None = None
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

    model_config = ConfigDict(
        json_schema_extra=_conditional_schema_extra(
            _conditional_schema_branch(
                "state",
                "present",
                required=("raw_pattern", "canonical_name"),
            ),
            _conditional_schema_branch(
                "state",
                "absent",
                required=("merchant_id",),
                forbidden=(
                    "raw_pattern",
                    "canonical_name",
                    "match_type",
                    "category",
                    "subcategory",
                ),
            ),
        )
    )

    kind: Literal["merchant"]
    state: Literal["present", "absent"]
    merchant_id: IdentifierString | None = None
    raw_pattern: MerchantPattern | None = None
    canonical_name: MerchantName | None = None
    match_type: MatchType | None = None
    category: CategoryName | None = None
    subcategory: CategoryName | None = None

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

    model_config = ConfigDict(
        json_schema_extra=_conditional_schema_extra(
            _conditional_schema_branch("state", "granted"),
            _conditional_schema_branch(
                "state",
                "revoked",
                forbidden=("mode",),
            ),
        )
    )

    kind: Literal["consent"]
    category: FeatureCategory
    state: Literal["granted", "revoked"]
    backend: IdentifierString | None = None
    mode: Literal["persistent", "one-time"] | None = None

    @model_validator(mode="after")
    def _validate_state(self) -> Self:
        if self.state == "revoked" and "mode" in self.model_fields_set:
            raise ValueError("Revoked consent forbids mode")
        return self


class CategorizationRuleMatch(_StrictRequest):
    """Strict matching target for one categorization rule."""

    type: MatchType
    value: MerchantPattern
    min_amount: FiniteDecimal | None = None
    max_amount: FiniteDecimal | None = None
    account_id: IdentifierString | None = None

    @model_validator(mode="after")
    def _validate_amount_bounds(self) -> Self:
        if (
            self.min_amount is not None
            and self.max_amount is not None
            and self.min_amount > self.max_amount
        ):
            raise ValueError("min_amount must be less than or equal to max_amount")
        return self


class CategorizationRuleTarget(_StrictRequest):
    """Declare one categorization rule's target state.

    The Task 4 adapter derives a deterministic name from the target fields;
    callers cannot provide a separate service-layer rule name.
    """

    model_config = ConfigDict(
        json_schema_extra=_conditional_schema_extra(
            _conditional_schema_branch(
                "state",
                "present",
                required=("matcher", "category", "priority"),
            ),
            _conditional_schema_branch(
                "state",
                "inactive",
                required=("rule_id",),
                forbidden=(
                    "matcher",
                    "category",
                    "subcategory",
                    "priority",
                ),
            ),
            _conditional_schema_branch(
                "state",
                "absent",
                required=("rule_id",),
                forbidden=(
                    "matcher",
                    "category",
                    "subcategory",
                    "priority",
                ),
            ),
        )
    )

    kind: Literal["rule"]
    rule_id: IdentifierString | None = None
    state: Literal["present", "inactive", "absent"]
    matcher: CategorizationRuleMatch | None = None
    category: CategoryName | None = None
    subcategory: CategoryName | None = None
    priority: int | None = Field(default=None, ge=0, le=10_000)

    @model_validator(mode="after")
    def _validate_state(self) -> Self:
        replacement_fields = {"matcher", "category", "subcategory", "priority"}
        if self.state == "present":
            if self.matcher is None or self.category is None or self.priority is None:
                raise ValueError("Present requires matcher, category, and priority")
            return self
        if self.rule_id is None:
            raise ValueError(f"{self.state.capitalize()} requires rule_id")
        if self.model_fields_set & replacement_fields:
            raise ValueError(f"{self.state.capitalize()} forbids replacement fields")
        return self
