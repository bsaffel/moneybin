"""Strict contracts for dormant coarse MCP writes."""

from decimal import Decimal
from typing import Any

import pytest
from pydantic import TypeAdapter, ValidationError

from moneybin.mcp.write_contracts import (
    AccountLinkDecisionRequest,
    AnnotationRequest,
    CategorizationDecisionRequest,
    CategorizationRuleMatch,
    CategorizationRuleTarget,
    CategoryStateRequest,
    ConsentStateRequest,
    IdentityDecisionRequest,
    MatchDecisionRequest,
    MerchantLinkDecisionRequest,
    MerchantStateRequest,
    ReviewDecisionRequest,
    SecurityLinkDecisionRequest,
    SplitTarget,
    TaxonomyStateRequest,
)


def _variant_schema(schema: dict[str, Any], tag: str) -> dict[str, Any]:
    reference = schema["discriminator"]["mapping"][tag]
    assert reference.startswith("#/$defs/")
    return schema["$defs"][reference.removeprefix("#/$defs/")]


def test_annotation_schema_requires_variant_fields() -> None:
    schema = TypeAdapter(AnnotationRequest).json_schema()
    assert schema["discriminator"]["propertyName"] == "kind"
    assert set(schema["discriminator"]["mapping"]) == {
        "note_set",
        "tags_set",
        "splits_set",
        "tag_rename",
    }
    assert set(_variant_schema(schema, "note_set")["required"]) == {
        "kind",
        "transaction_id",
        "note",
    }
    assert set(_variant_schema(schema, "tags_set")["required"]) == {
        "kind",
        "transaction_id",
        "tags",
    }
    assert set(_variant_schema(schema, "splits_set")["required"]) == {
        "kind",
        "transaction_id",
        "splits",
    }
    assert set(_variant_schema(schema, "tag_rename")["required"]) == {
        "kind",
        "old_name",
        "new_name",
    }


@pytest.mark.parametrize(
    ("contract", "tags"),
    [
        (ReviewDecisionRequest, {"categorization", "match"}),
        (
            IdentityDecisionRequest,
            {"account_link", "merchant_link", "security_link"},
        ),
        (TaxonomyStateRequest, {"category", "merchant"}),
    ],
)
def test_coarse_union_schemas_expose_kind_discriminators(
    contract: Any,
    tags: set[str],
) -> None:
    schema = TypeAdapter(contract).json_schema()
    assert schema["discriminator"]["propertyName"] == "kind"
    assert set(schema["discriminator"]["mapping"]) == tags
    for tag in tags:
        assert {"kind"}.issubset(_variant_schema(schema, tag)["required"])
    assert "outputSchema" not in schema


@pytest.mark.parametrize(
    ("payload", "message"),
    [
        ({"kind": "tags_set", "transaction_id": "tx_1", "tags": "food"}, "list"),
        ({"kind": "splits_set", "transaction_id": "tx_1", "splits": "[]"}, "list"),
        ({"kind": "tag_rename", "old_name": "a", "new_name": ""}, "at least 1"),
    ],
)
def test_annotation_requests_reject_stringified_or_empty_values(
    payload: dict[str, object],
    message: str,
) -> None:
    with pytest.raises(ValidationError, match=message):
        TypeAdapter(AnnotationRequest).validate_python(payload)


@pytest.mark.parametrize(
    "payload",
    [
        {"kind": "note_set", "transaction_id": " ", "note": None},
        {"kind": "tags_set", "transaction_id": "tx_1", "tags": [" "]},
        {
            "kind": "splits_set",
            "transaction_id": "tx_1",
            "splits": [{"amount": Decimal("1"), "category": " "}],
        },
        {"kind": "tag_rename", "old_name": "old", "new_name": "\t"},
    ],
)
def test_annotation_requests_reject_whitespace_only_identifiers_and_names(
    payload: dict[str, object],
) -> None:
    with pytest.raises(ValidationError, match="whitespace"):
        TypeAdapter(AnnotationRequest).validate_python(payload)


def test_annotation_models_forbid_extra_fields() -> None:
    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        TypeAdapter(AnnotationRequest).validate_python({
            "kind": "note_set",
            "transaction_id": "tx_1",
            "note": None,
            "unexpected": True,
        })


@pytest.mark.parametrize(
    "amount",
    [Decimal("0"), Decimal("NaN"), Decimal("Infinity"), Decimal("-Infinity")],
)
def test_split_amount_is_finite_and_non_zero(amount: Decimal) -> None:
    with pytest.raises(ValidationError):
        SplitTarget(amount=amount)


@pytest.mark.parametrize("amount", ["1.25", 1, 1.25])
def test_split_amount_rejects_python_coercion(amount: object) -> None:
    with pytest.raises(ValidationError, match="Decimal"):
        TypeAdapter(SplitTarget).validate_python({"amount": amount})


def test_split_target_preserves_native_decimal() -> None:
    split = SplitTarget(
        amount=Decimal("-12.50"),
        category="Food",
        subcategory="Dining",
        note=None,
    )
    assert split.amount == Decimal("-12.50")


@pytest.mark.parametrize(
    "payload",
    [
        {
            "kind": "categorization",
            "decision_id": "cat_1",
            "decision": "accept",
        },
        {
            "kind": "categorization",
            "decision_id": "cat_1",
            "decision": "reject",
            "category": "Food",
        },
        {
            "kind": "categorization",
            "decision_id": "cat_1",
            "decision": "reject",
            "subcategory": "Dining",
        },
        {
            "kind": "categorization",
            "decision_id": "cat_1",
            "decision": "reject",
            "canonical_merchant_name": "Cafe",
        },
    ],
)
def test_categorization_decision_conditional_fields(payload: dict[str, object]) -> None:
    with pytest.raises(ValidationError):
        TypeAdapter(ReviewDecisionRequest).validate_python(payload)


def test_review_decisions_accept_complete_variants() -> None:
    adapter = TypeAdapter(ReviewDecisionRequest)
    accepted = adapter.validate_python({
        "kind": "categorization",
        "decision_id": "cat_1",
        "decision": "accept",
        "category": "Food",
        "subcategory": "Dining",
    })
    rejected = adapter.validate_python({
        "kind": "match",
        "decision_id": "match_1",
        "decision": "reject",
    })
    assert isinstance(accepted, CategorizationDecisionRequest)
    assert isinstance(rejected, MatchDecisionRequest)


@pytest.mark.parametrize(
    "payload",
    [
        {
            "kind": "account_link",
            "decision_id": "decision_1",
            "decision": "accept",
        },
        {
            "kind": "merchant_link",
            "decision_id": "decision_1",
            "decision": "reject",
            "target_id": "merchant_1",
        },
        {
            "kind": "security_link",
            "decision_id": "decision_1",
            "decision": "accept",
            "target_id": " ",
        },
    ],
)
def test_identity_decision_conditional_fields(payload: dict[str, object]) -> None:
    with pytest.raises(ValidationError):
        TypeAdapter(IdentityDecisionRequest).validate_python(payload)


@pytest.mark.parametrize("kind", ["account_link", "merchant_link", "security_link"])
def test_identity_decisions_accept_complete_variants(kind: str) -> None:
    adapter = TypeAdapter(IdentityDecisionRequest)
    accepted = adapter.validate_python({
        "kind": kind,
        "decision_id": "decision_1",
        "decision": "accept",
        "target_id": "target_1",
    })
    rejected = adapter.validate_python({
        "kind": kind,
        "decision_id": "decision_2",
        "decision": "reject",
    })
    assert isinstance(
        accepted,
        (
            AccountLinkDecisionRequest,
            MerchantLinkDecisionRequest,
            SecurityLinkDecisionRequest,
        ),
    )
    assert isinstance(
        rejected,
        (
            AccountLinkDecisionRequest,
            MerchantLinkDecisionRequest,
            SecurityLinkDecisionRequest,
        ),
    )
    assert accepted.target_id == "target_1"
    assert rejected.target_id is None


@pytest.mark.parametrize(
    "payload",
    [
        {"kind": "category", "state": "present"},
        {
            "kind": "category",
            "state": "inactive",
            "category_id": "cat_1",
            "category": "Food",
        },
        {"kind": "category", "state": "inactive"},
        {
            "kind": "category",
            "state": "inactive",
            "category_id": "cat_1",
            "force": True,
        },
        {"kind": "category", "state": "present", "category": "Food", "force": True},
        {
            "kind": "category",
            "state": "absent",
            "category_id": "cat_1",
            "description": "replacement",
        },
        {"kind": "category", "state": "present", "category": " "},
    ],
)
def test_category_state_conditional_fields(payload: dict[str, object]) -> None:
    with pytest.raises(ValidationError):
        TypeAdapter(TaxonomyStateRequest).validate_python(payload)


def test_category_states_accept_complete_targets_and_strict_force() -> None:
    adapter = TypeAdapter(TaxonomyStateRequest)
    present = adapter.validate_python({
        "kind": "category",
        "state": "present",
        "category": "Food",
        "subcategory": "Dining",
        "description": "Restaurants",
    })
    inactive = adapter.validate_python({
        "kind": "category",
        "state": "inactive",
        "category_id": "cat_1",
    })
    absent = adapter.validate_python({
        "kind": "category",
        "state": "absent",
        "category_id": "cat_1",
        "force": True,
    })
    assert isinstance(present, CategoryStateRequest)
    assert isinstance(inactive, CategoryStateRequest)
    assert isinstance(absent, CategoryStateRequest)
    assert present.state == "present"
    assert inactive.state == "inactive"
    assert absent.force is True
    with pytest.raises(ValidationError, match="boolean"):
        adapter.validate_python({
            "kind": "category",
            "state": "absent",
            "category_id": "cat_1",
            "force": 1,
        })


@pytest.mark.parametrize(
    "payload",
    [
        {"kind": "merchant", "state": "present", "raw_pattern": "Cafe"},
        {
            "kind": "merchant",
            "state": "present",
            "raw_pattern": "Cafe",
            "canonical_name": " ",
        },
        {"kind": "merchant", "state": "absent"},
        {
            "kind": "merchant",
            "state": "absent",
            "merchant_id": "merchant_1",
            "canonical_name": "Cafe",
        },
    ],
)
def test_merchant_state_conditional_fields(payload: dict[str, object]) -> None:
    with pytest.raises(ValidationError):
        TypeAdapter(TaxonomyStateRequest).validate_python(payload)


def test_merchant_states_accept_complete_targets() -> None:
    adapter = TypeAdapter(TaxonomyStateRequest)
    present = adapter.validate_python({
        "kind": "merchant",
        "state": "present",
        "raw_pattern": "CAFE",
        "canonical_name": "Cafe",
        "match_type": "exact",
        "category": "Food",
    })
    absent = adapter.validate_python({
        "kind": "merchant",
        "state": "absent",
        "merchant_id": "merchant_1",
    })
    assert isinstance(present, MerchantStateRequest)
    assert isinstance(absent, MerchantStateRequest)
    assert present.state == "present"
    assert absent.state == "absent"


@pytest.mark.parametrize(
    "payload",
    [
        {"kind": "consent", "category": "unknown", "state": "granted"},
        {
            "kind": "consent",
            "category": "mcp-data-sharing",
            "state": "revoked",
            "mode": "persistent",
        },
        {
            "kind": "consent",
            "category": "mcp-data-sharing",
            "state": "granted",
            "backend": " ",
        },
    ],
)
def test_consent_state_rejects_invalid_combinations(payload: dict[str, object]) -> None:
    with pytest.raises(ValidationError):
        ConsentStateRequest.model_validate(payload)


def test_consent_state_keeps_omitted_grant_mode_unset() -> None:
    request = ConsentStateRequest(
        kind="consent",
        category="mcp-data-sharing",
        state="granted",
    )
    assert request.mode is None


@pytest.mark.parametrize(
    "payload",
    [
        {"kind": "rule", "state": "present", "category": "Food", "priority": 10},
        {
            "kind": "rule",
            "state": "present",
            "match": {"type": "contains", "value": "Cafe"},
            "priority": 10,
        },
        {
            "kind": "rule",
            "state": "present",
            "match": {"type": "contains", "value": "Cafe"},
            "category": "Food",
        },
        {"kind": "rule", "state": "inactive"},
        {
            "kind": "rule",
            "state": "inactive",
            "rule_id": "rule_1",
            "category": "Food",
        },
        {
            "kind": "rule",
            "state": "absent",
            "rule_id": "rule_1",
            "match": {"type": "contains", "value": "Cafe"},
        },
        {
            "kind": "rule",
            "state": "present",
            "match": {"type": "contains", "value": " "},
            "category": "Food",
            "priority": 10,
        },
    ],
)
def test_rule_target_conditional_fields(payload: dict[str, object]) -> None:
    with pytest.raises(ValidationError):
        CategorizationRuleTarget.model_validate(payload)


def test_rule_target_accepts_complete_present_and_id_only_terminal_states() -> None:
    present = CategorizationRuleTarget(
        kind="rule",
        rule_id="rule_1",
        state="present",
        match=CategorizationRuleMatch(
            type="contains",
            value="Cafe",
            min_amount=Decimal("-100"),
            max_amount=Decimal("-1"),
            account_id="account_1",
        ),
        category="Food",
        priority=10,
    )
    inactive = CategorizationRuleTarget(
        kind="rule",
        rule_id="rule_1",
        state="inactive",
    )
    absent = CategorizationRuleTarget(
        kind="rule",
        rule_id="rule_1",
        state="absent",
    )
    assert present.match is not None
    assert present.match.min_amount == Decimal("-100")
    assert inactive.state == "inactive"
    assert absent.state == "absent"


@pytest.mark.parametrize(
    "payload",
    [
        {
            "kind": "rule",
            "state": "present",
            "match": {"type": "contains", "value": "Cafe", "unexpected": "x"},
            "category": "Food",
            "priority": 10,
        },
        {
            "kind": "rule",
            "state": "present",
            "match": {"type": "contains", "value": "Cafe"},
            "category": "Food",
            "priority": 10,
            "unexpected": "x",
        },
    ],
)
def test_rule_target_and_nested_match_forbid_extra_fields(
    payload: dict[str, object],
) -> None:
    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        CategorizationRuleTarget.model_validate(payload)
