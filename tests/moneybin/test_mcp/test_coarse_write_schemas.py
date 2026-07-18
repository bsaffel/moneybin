"""Strict contracts for dormant coarse MCP writes."""

import json
from decimal import Decimal
from pathlib import Path
from typing import Any, get_args

import pytest
from jsonschema import Draft202012Validator
from jsonschema import validate as validate_json_schema
from jsonschema.exceptions import ValidationError as JSONSchemaValidationError
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
from moneybin.privacy.consent import FEATURE_CATEGORIES
from moneybin.services.categorization._shared import MatchType as ServiceMatchType
from moneybin.vocabulary import CategorizationMatchType, ConsentFeatureCategory

EVAL_CASES_PATH = Path(__file__).parents[2] / "fixtures/mcp_eval/cases.json"


def _variant_schema(schema: dict[str, Any], tag: str) -> dict[str, Any]:
    reference = schema["discriminator"]["mapping"][tag]
    assert reference.startswith("#/$defs/")
    return schema["$defs"][reference.removeprefix("#/$defs/")]


def _conditional_then(
    schema: dict[str, Any],
    field: str,
    value: str,
) -> dict[str, Any]:
    for condition in schema["allOf"]:
        if_schema = condition["if"]
        if if_schema["properties"].get(field, {}).get("const") == value:
            assert field in if_schema["required"]
            return condition["then"]
    raise AssertionError(f"No conditional schema for {field}={value}")


def _forbidden_fields(then_schema: dict[str, Any]) -> set[str]:
    return {
        required
        for branch in then_schema["not"]["anyOf"]
        for required in branch["required"]
    }


@pytest.mark.parametrize(
    ("contract", "valid_payload", "required_field"),
    [
        (
            CategorizationDecisionRequest,
            {
                "kind": "categorization",
                "decision_id": "cat_1",
                "decision": "accept",
                "category": "Food",
            },
            "category",
        ),
        (
            AccountLinkDecisionRequest,
            {
                "kind": "account_link",
                "decision_id": "decision_1",
                "decision": "accept",
                "target_id": "account_1",
            },
            "target_id",
        ),
        (
            CategoryStateRequest,
            {"kind": "category", "state": "present", "category": "Food"},
            "category",
        ),
        (
            CategoryStateRequest,
            {"kind": "category", "state": "inactive", "category_id": "cat_1"},
            "category_id",
        ),
        (
            CategoryStateRequest,
            {"kind": "category", "state": "absent", "category_id": "cat_1"},
            "category_id",
        ),
        (
            MerchantStateRequest,
            {
                "kind": "merchant",
                "state": "present",
                "raw_pattern": "CAFE",
                "canonical_name": "Cafe",
            },
            "raw_pattern",
        ),
        (
            MerchantStateRequest,
            {
                "kind": "merchant",
                "state": "present",
                "raw_pattern": "CAFE",
                "canonical_name": "Cafe",
            },
            "canonical_name",
        ),
        (
            MerchantStateRequest,
            {
                "kind": "merchant",
                "state": "absent",
                "merchant_id": "merchant_1",
            },
            "merchant_id",
        ),
        (
            CategorizationRuleTarget,
            {
                "kind": "rule",
                "state": "present",
                "matcher": {"type": "contains", "value": "Cafe"},
                "category": "Food",
                "priority": 10,
            },
            "matcher",
        ),
        (
            CategorizationRuleTarget,
            {
                "kind": "rule",
                "state": "present",
                "matcher": {"type": "contains", "value": "Cafe"},
                "category": "Food",
                "priority": 10,
            },
            "category",
        ),
        (
            CategorizationRuleTarget,
            {
                "kind": "rule",
                "state": "present",
                "matcher": {"type": "contains", "value": "Cafe"},
                "category": "Food",
                "priority": 10,
            },
            "priority",
        ),
        (
            CategorizationRuleTarget,
            {"kind": "rule", "state": "inactive", "rule_id": "rule_1"},
            "rule_id",
        ),
        (
            CategorizationRuleTarget,
            {"kind": "rule", "state": "absent", "rule_id": "rule_1"},
            "rule_id",
        ),
    ],
)
def test_conditional_schemas_accept_valid_and_reject_explicit_null(
    contract: Any,
    valid_payload: dict[str, object],
    required_field: str,
) -> None:
    schema = contract.model_json_schema()
    Draft202012Validator.check_schema(schema)

    validate_json_schema(valid_payload, schema, cls=Draft202012Validator)
    contract.model_validate(valid_payload)
    null_payload = valid_payload | {required_field: None}
    with pytest.raises(JSONSchemaValidationError):
        validate_json_schema(null_payload, schema, cls=Draft202012Validator)
    with pytest.raises(ValidationError):
        contract.model_validate(null_payload)
    wrong_type_payload: dict[str, object] = valid_payload | {required_field: [None]}
    with pytest.raises(JSONSchemaValidationError):
        validate_json_schema(wrong_type_payload, schema, cls=Draft202012Validator)
    with pytest.raises(ValidationError):
        contract.model_validate(wrong_type_payload)


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


@pytest.mark.parametrize(
    ("amount", "expected"),
    [(1, Decimal("1")), (-12.5, Decimal("-12.5"))],
)
def test_split_amount_accepts_decoded_json_numbers(
    amount: int | float,
    expected: Decimal,
) -> None:
    split = TypeAdapter(SplitTarget).validate_python({"amount": amount})
    assert split.amount == expected


@pytest.mark.parametrize(
    "amount",
    [
        "1.25",
        True,
        False,
        float("nan"),
        float("inf"),
        float("-inf"),
        object(),
    ],
)
def test_split_amount_rejects_non_json_or_non_finite_values(amount: object) -> None:
    with pytest.raises(ValidationError, match="finite JSON number"):
        TypeAdapter(SplitTarget).validate_python({"amount": amount})


def test_split_target_preserves_native_decimal() -> None:
    split = SplitTarget(
        amount=Decimal("-12.50"),
        category="Food",
        subcategory="Dining",
        note=None,
    )
    assert split.amount == Decimal("-12.50")


def test_decimal_schema_advertises_json_numbers_not_strings() -> None:
    amount_schema = SplitTarget.model_json_schema()["properties"]["amount"]
    advertised_types = {
        branch["type"] for branch in amount_schema.get("anyOf", [amount_schema])
    }
    assert advertised_types <= {"integer", "number"}
    assert "number" in advertised_types


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


def test_categorization_schema_advertises_decision_requirements() -> None:
    schema = _variant_schema(
        TypeAdapter(ReviewDecisionRequest).json_schema(),
        "categorization",
    )
    accept = _conditional_then(schema, "decision", "accept")
    reject = _conditional_then(schema, "decision", "reject")
    assert set(accept["required"]) == {"category"}
    assert _forbidden_fields(reject) == {
        "category",
        "subcategory",
        "canonical_merchant_name",
    }


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


def test_identity_schemas_advertise_decision_requirements() -> None:
    schema = TypeAdapter(IdentityDecisionRequest).json_schema()
    for kind in ("account_link", "merchant_link", "security_link"):
        variant = _variant_schema(schema, kind)
        accept = _conditional_then(variant, "decision", "accept")
        reject = _conditional_then(variant, "decision", "reject")
        assert set(accept["required"]) == {"target_id"}
        assert _forbidden_fields(reject) == {"target_id"}


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


def test_category_schema_advertises_state_requirements() -> None:
    schema = _variant_schema(
        TypeAdapter(TaxonomyStateRequest).json_schema(),
        "category",
    )
    present = _conditional_then(schema, "state", "present")
    inactive = _conditional_then(schema, "state", "inactive")
    absent = _conditional_then(schema, "state", "absent")
    assert set(present["required"]) == {"category"}
    assert present["properties"]["force"] == {"const": False}
    assert set(inactive["required"]) == {"category_id"}
    assert inactive["properties"]["force"] == {"const": False}
    assert _forbidden_fields(inactive) == {
        "category",
        "subcategory",
        "description",
    }
    assert set(absent["required"]) == {"category_id"}
    assert _forbidden_fields(absent) == {
        "category",
        "subcategory",
        "description",
    }


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


def test_merchant_schema_advertises_state_requirements() -> None:
    schema = _variant_schema(
        TypeAdapter(TaxonomyStateRequest).json_schema(),
        "merchant",
    )
    present = _conditional_then(schema, "state", "present")
    absent = _conditional_then(schema, "state", "absent")
    assert set(present["required"]) == {"raw_pattern", "canonical_name"}
    assert set(absent["required"]) == {"merchant_id"}
    assert _forbidden_fields(absent) == {
        "raw_pattern",
        "canonical_name",
        "match_type",
        "category",
        "subcategory",
    }


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


def test_consent_schema_advertises_state_requirements() -> None:
    schema = ConsentStateRequest.model_json_schema()
    granted = _conditional_then(schema, "state", "granted")
    revoked = _conditional_then(schema, "state", "revoked")
    assert granted == {}
    assert _forbidden_fields(revoked) == {"mode"}


def test_contract_enums_match_canonical_service_vocabularies() -> None:
    consent_schema = ConsentStateRequest.model_json_schema()
    rule_schema = CategorizationRuleTarget.model_json_schema()
    matcher_ref = rule_schema["properties"]["matcher"]["anyOf"][0]["$ref"]
    matcher_schema = rule_schema["$defs"][matcher_ref.removeprefix("#/$defs/")]

    assert set(consent_schema["properties"]["category"]["enum"]) == set(
        FEATURE_CATEGORIES
    )
    assert set(matcher_schema["properties"]["type"]["enum"]) == set(
        get_args(ServiceMatchType)
    )
    assert ServiceMatchType is CategorizationMatchType
    assert FEATURE_CATEGORIES == frozenset(get_args(ConsentFeatureCategory))


@pytest.mark.parametrize(
    "payload",
    [
        {"kind": "rule", "state": "present", "category": "Food", "priority": 10},
        {
            "kind": "rule",
            "state": "present",
            "matcher": {"type": "contains", "value": "Cafe"},
            "priority": 10,
        },
        {
            "kind": "rule",
            "state": "present",
            "matcher": {"type": "contains", "value": "Cafe"},
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
            "matcher": {"type": "contains", "value": "Cafe"},
        },
        {
            "kind": "rule",
            "state": "present",
            "matcher": {"type": "contains", "value": " "},
            "category": "Food",
            "priority": 10,
        },
        {
            "kind": "rule",
            "state": "present",
            "matcher": {
                "type": "contains",
                "value": "Cafe",
                "min_amount": -1,
                "max_amount": -100,
            },
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
        matcher=CategorizationRuleMatch.model_validate({
            "type": "contains",
            "value": "Cafe",
            "min_amount": -100,
            "max_amount": -1.25,
            "account_id": "account_1",
        }),
        category="Food",
        subcategory="Dining",
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
    assert present.matcher is not None
    assert present.matcher.min_amount == Decimal("-100")
    assert present.matcher.max_amount == Decimal("-1.25")
    assert present.subcategory == "Dining"
    assert inactive.state == "inactive"
    assert absent.state == "absent"


@pytest.mark.parametrize(
    "value",
    ["1", True, float("nan"), float("inf"), Decimal("NaN")],
)
def test_rule_matcher_amount_bounds_reject_non_json_or_non_finite_values(
    value: object,
) -> None:
    with pytest.raises(ValidationError, match="finite JSON number"):
        CategorizationRuleMatch(
            type="contains",
            value="Cafe",
            min_amount=value,  # type: ignore[arg-type]  # invalid boundary input
        )


def test_rule_schema_advertises_state_requirements_and_derived_name() -> None:
    schema = CategorizationRuleTarget.model_json_schema()
    present = _conditional_then(schema, "state", "present")
    inactive = _conditional_then(schema, "state", "inactive")
    absent = _conditional_then(schema, "state", "absent")
    replacement_fields = {"matcher", "category", "subcategory", "priority"}

    assert set(present["required"]) == {"matcher", "category", "priority"}
    assert set(inactive["required"]) == {"rule_id"}
    assert _forbidden_fields(inactive) == replacement_fields
    assert set(absent["required"]) == {"rule_id"}
    assert _forbidden_fields(absent) == replacement_fields
    assert "name" not in schema["properties"]
    assert "deterministic name" in schema["description"]


def test_proposed_rule_fixture_uses_the_contract_target_state() -> None:
    cases = json.loads(EVAL_CASES_PATH.read_text())
    case = next(row for row in cases if row["id"] == "categorization-rule-and-review")
    arguments = case["expectations"]["standard-45"]["calls"][0]["arguments"]
    target = arguments["rules"][0]

    assert target["state"] == "present"
    assert "matcher" in target
    assert "match" not in target
    validated = CategorizationRuleTarget.model_validate(target)
    assert validated.kind == "rule"
    assert validated.priority == 100


@pytest.mark.parametrize(
    "payload",
    [
        {
            "kind": "rule",
            "state": "present",
            "matcher": {
                "type": "contains",
                "value": "Cafe",
                "unexpected": "x",
            },
            "category": "Food",
            "priority": 10,
        },
        {
            "kind": "rule",
            "state": "present",
            "matcher": {"type": "contains", "value": "Cafe"},
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
