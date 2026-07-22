"""Strict contracts for standard coarse MCP writes."""

import json
from decimal import Decimal
from pathlib import Path
from typing import Any, get_args
from unittest.mock import patch

import pytest
from jsonschema import Draft202012Validator
from jsonschema import validate as validate_json_schema
from jsonschema.exceptions import ValidationError as JSONSchemaValidationError
from mcp.types import TextContent
from pydantic import TypeAdapter, ValidationError

from moneybin.mcp.tools.accounts import BalanceAmount, register_accounts_coarse_writes
from moneybin.mcp.tools.exports import register_export_tools
from moneybin.mcp.tools.privacy import register_privacy_coarse_writes
from moneybin.mcp.tools.taxonomy import register_taxonomy_coarse_writes
from moneybin.mcp.tools.transactions import register_transaction_coarse_writes
from moneybin.mcp.tools.transactions_categorize import (
    register_categorization_coarse_writes,
)
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
from moneybin.services._validators import (
    CATEGORY_NAME_MAX_LEN,
    DESCRIPTION_MAX_LEN,
    IDENTIFIER_MAX_LEN,
    MERCHANT_NAME_MAX_LEN,
    MERCHANT_PATTERN_MAX_LEN,
    NOTE_MAX_LEN,
    SLUG_MAX_LEN,
)
from moneybin.services.categorization._shared import MatchType as ServiceMatchType
from moneybin.vocabulary import CategorizationMatchType, ConsentFeatureCategory

from .schema_assertions import call_tool_raw, isolated_server, listed_tool

EVAL_CASES_PATH = Path(__file__).parents[2] / "fixtures/mcp_eval/cases.json"


async def test_export_write_schemas_keep_event_and_target_state_separate() -> None:
    mcp = isolated_server(register_export_tools)

    export = await listed_tool(mcp, "export_run")
    destinations = await listed_tool(mcp, "exports_set")

    assert set(export.inputSchema["properties"]) == {
        "subject",
        "destination",
        "redaction_mode",
    }
    assert set(destinations.inputSchema["properties"]) == {"target"}
    assert "operation" not in json.dumps(export.inputSchema)
    assert "action" not in json.dumps(destinations.inputSchema)
    assert export.annotations is not None
    assert export.annotations.idempotentHint is False
    assert destinations.annotations is not None
    assert destinations.annotations.idempotentHint is True


@pytest.mark.parametrize(
    ("name", "arguments"),
    [
        (
            "export_run",
            {
                "subject": {"kind": "bundle", "report_id": "core:networth"},
                "destination": {"kind": "local", "name": "exports"},
                "redaction_mode": "redacted",
            },
        ),
        (
            "export_run",
            {
                "subject": {"kind": "bundle"},
                "destination": {
                    "kind": "sheets",
                    "name": "dashboard",
                    "format": "csv",
                },
                "redaction_mode": "redacted",
            },
        ),
        (
            "exports_set",
            {
                "target": {
                    "kind": "local",
                    "state": "absent",
                    "name": "archive",
                    "local_path": "/Users/test/archive",
                }
            },
        ),
    ],
)
async def test_export_write_schemas_reject_cross_variant_fields(
    name: str,
    arguments: dict[str, Any],
) -> None:
    response = await call_tool_raw(
        isolated_server(register_export_tools),
        name,
        arguments,
    )

    assert response.isError is True


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


def _conditional_else(
    schema: dict[str, Any],
    field: str,
    value: str,
) -> dict[str, Any]:
    for condition in schema["allOf"]:
        if_schema = condition["if"]
        if if_schema["properties"].get(field, {}).get("const") == value:
            assert field in if_schema["required"]
            return condition["else"]
    raise AssertionError(f"No conditional schema for {field}={value}")


def _forbidden_fields(then_schema: dict[str, Any]) -> set[str]:
    return {
        required
        for branch in then_schema["not"]["anyOf"]
        for required in branch["required"]
    }


def _assert_required_non_null(
    schema: dict[str, Any],
    fields: set[str],
) -> None:
    assert set(schema["required"]) == fields
    for field in fields:
        assert schema["properties"][field] == {"not": {"type": "null"}}


def _rendered_variants(
    schema: dict[str, Any],
    *,
    collection: str,
) -> dict[str, set[str]]:
    variants = schema["properties"][collection]["items"]["oneOf"]
    return {
        branch["properties"]["kind"]["const"]: set(branch["required"])
        for branch in variants
    }


def _rendered_variant(
    schema: dict[str, Any],
    *,
    collection: str,
    kind: str,
) -> dict[str, Any]:
    return next(
        branch
        for branch in schema["properties"][collection]["items"]["oneOf"]
        if branch["properties"]["kind"]["const"] == kind
    )


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
        "note_add",
        "note_edit",
        "note_delete",
        "tags_set",
        "splits_set",
        "tag_rename",
    }
    assert set(_variant_schema(schema, "note_add")["required"]) == {
        "kind",
        "transaction_id",
        "text",
    }
    assert set(_variant_schema(schema, "note_edit")["required"]) == {
        "kind",
        "note_id",
        "text",
    }
    assert set(_variant_schema(schema, "note_delete")["required"]) == {
        "kind",
        "note_id",
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
        (ReviewDecisionRequest, {"auto_rule", "categorization", "match"}),
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
    ("contract", "payload"),
    [
        (
            AnnotationRequest,
            {
                "kind": "note_add",
                "transaction_id": "x" * (IDENTIFIER_MAX_LEN + 1),
                "text": "note",
            },
        ),
        (
            AnnotationRequest,
            {
                "kind": "note_add",
                "transaction_id": "tx_1",
                "text": "x" * (NOTE_MAX_LEN + 1),
            },
        ),
        (
            AnnotationRequest,
            {
                "kind": "tags_set",
                "transaction_id": "tx_1",
                "tags": ["x" * (SLUG_MAX_LEN + 1)],
            },
        ),
        (
            CategorizationDecisionRequest,
            {
                "kind": "categorization",
                "decision_id": "decision_1",
                "decision": "accept",
                "category": "x" * (CATEGORY_NAME_MAX_LEN + 1),
            },
        ),
        (
            CategorizationDecisionRequest,
            {
                "kind": "categorization",
                "decision_id": "decision_1",
                "decision": "accept",
                "category": "Food",
                "canonical_merchant_name": "x" * (MERCHANT_NAME_MAX_LEN + 1),
            },
        ),
        (
            CategoryStateRequest,
            {
                "kind": "category",
                "state": "present",
                "category": "Food",
                "description": "x" * (DESCRIPTION_MAX_LEN + 1),
            },
        ),
        (
            MerchantStateRequest,
            {
                "kind": "merchant",
                "state": "present",
                "raw_pattern": "x" * (MERCHANT_PATTERN_MAX_LEN + 1),
                "canonical_name": "Merchant",
            },
        ),
        (
            CategorizationRuleMatch,
            {
                "type": "contains",
                "value": "x" * (MERCHANT_PATTERN_MAX_LEN + 1),
            },
        ),
    ],
)
def test_write_contracts_bound_user_supplied_strings(
    contract: Any,
    payload: dict[str, object],
) -> None:
    with pytest.raises(ValidationError, match="at most"):
        TypeAdapter(contract).validate_python(payload)


@pytest.mark.parametrize(
    "payload",
    [
        {"kind": "note_add", "transaction_id": " ", "text": "note"},
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
            "kind": "note_add",
            "transaction_id": "tx_1",
            "text": "note",
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


@pytest.mark.parametrize(
    "amount",
    [
        Decimal("0.001"),
        Decimal("-0.001"),
        Decimal("10000000000000000.00"),
        Decimal("-10000000000000000.00"),
    ],
)
def test_split_amount_enforces_decimal_18_2(amount: Decimal) -> None:
    with pytest.raises(ValidationError):
        SplitTarget(amount=amount)


def test_split_subcategory_requires_category_in_model_and_schema() -> None:
    payload = {"amount": -10, "subcategory": "Dining"}
    with pytest.raises(ValidationError, match="category"):
        SplitTarget.model_validate(payload)
    with pytest.raises(JSONSchemaValidationError):
        validate_json_schema(
            payload,
            SplitTarget.model_json_schema(),
            cls=Draft202012Validator,
        )


def test_decimal_schema_advertises_json_numbers_not_strings() -> None:
    amount_schema = SplitTarget.model_json_schema()["properties"]["amount"]
    advertised_types = {
        branch["type"] for branch in amount_schema.get("anyOf", [amount_schema])
    }
    assert advertised_types <= {"integer", "number"}
    assert "number" in advertised_types


async def test_balance_assertion_coarse_schema_and_annotations() -> None:
    mcp = isolated_server(register_accounts_coarse_writes)

    tool = await listed_tool(mcp, "accounts_balance_assert")
    amount_schema = tool.inputSchema["properties"]["amount"]
    numeric_schema = amount_schema["anyOf"][0]
    advertised_types = {branch["type"] for branch in numeric_schema["anyOf"]}

    assert tool.outputSchema is None
    assert tool.annotations is not None
    assert tool.annotations.readOnlyHint is False
    assert tool.annotations.destructiveHint is True
    assert tool.annotations.idempotentHint is True
    assert tool.inputSchema["properties"]["as_of"] == {
        "format": "date",
        "type": "string",
    }
    assert set(tool.inputSchema["properties"]["state"]["enum"]) == {
        "present",
        "absent",
    }
    assert tool.inputSchema["properties"]["state"]["default"] == "present"
    assert amount_schema["anyOf"][1] == {"type": "null"}
    assert numeric_schema["decimal_places"] == 2
    assert numeric_schema["max_digits"] == 18
    assert advertised_types <= {"integer", "number"}
    assert "number" in advertised_types
    Draft202012Validator.check_schema(tool.inputSchema)

    valid_payloads = [
        {"account": "ACC001", "as_of": "2026-07-01", "amount": 1250},
        {
            "account": "ACC001",
            "as_of": "2026-07-01",
            "state": "present",
            "amount": 1250,
        },
        {"account": "ACC001", "as_of": "2026-07-01", "state": "absent"},
        {
            "account": "ACC001",
            "as_of": "2026-07-01",
            "state": "absent",
            "confirmation_token": "token",
        },
        {
            "account": "ACC001",
            "as_of": "2026-07-01",
            "state": "absent",
            "confirmation_token": None,
        },
    ]
    invalid_payloads = [
        {"account": "ACC001", "as_of": "2026-07-01"},
        {
            "account": "ACC001",
            "as_of": "2026-07-01",
            "state": "present",
        },
        {
            "account": "ACC001",
            "as_of": "2026-07-01",
            "state": "present",
            "amount": None,
        },
        {
            "account": "ACC001",
            "as_of": "2026-07-01",
            "state": "present",
            "amount": 1250,
            "confirmation_token": "token",
        },
        {
            "account": "ACC001",
            "as_of": "2026-07-01",
            "state": "present",
            "amount": 1250,
            "confirmation_token": None,
        },
        {
            "account": "ACC001",
            "as_of": "2026-07-01",
            "state": "absent",
            "amount": 1250,
        },
        {
            "account": "ACC001",
            "as_of": "2026-07-01",
            "state": "absent",
            "amount": None,
        },
    ]
    for payload in valid_payloads:
        validate_json_schema(payload, tool.inputSchema, cls=Draft202012Validator)
    for payload in invalid_payloads:
        with pytest.raises(JSONSchemaValidationError):
            validate_json_schema(payload, tool.inputSchema, cls=Draft202012Validator)


async def test_transaction_annotation_coarse_schema_and_annotations() -> None:
    mcp = isolated_server(register_transaction_coarse_writes)

    tool = await listed_tool(mcp, "transactions_annotate")

    assert tool.outputSchema is None
    assert tool.annotations is not None
    assert tool.annotations.readOnlyHint is False
    assert tool.annotations.destructiveHint is True
    assert tool.annotations.idempotentHint is False
    assert "confirmation_token" in tool.inputSchema["properties"]
    variants = {
        branch["properties"]["kind"]["const"]: set(branch["required"])
        for branch in tool.inputSchema["properties"]["requests"]["items"]["oneOf"]
    }
    assert variants == {
        "note_add": {"kind", "transaction_id", "text"},
        "note_edit": {"kind", "note_id", "text"},
        "note_delete": {"kind", "note_id"},
        "tags_set": {"kind", "transaction_id", "tags"},
        "splits_set": {"kind", "transaction_id", "splits"},
        "tag_rename": {"kind", "old_name", "new_name"},
    }
    Draft202012Validator.check_schema(tool.inputSchema)


async def test_categorization_rules_set_coarse_schema_and_annotations() -> None:
    mcp = isolated_server(register_categorization_coarse_writes)

    tool = await listed_tool(mcp, "transactions_categorize_rules_set")

    assert tool.outputSchema is None
    assert tool.annotations is not None
    assert tool.annotations.readOnlyHint is False
    assert tool.annotations.destructiveHint is True
    assert tool.annotations.idempotentHint is True
    assert "confirmation_token" in tool.inputSchema["properties"]
    Draft202012Validator.check_schema(tool.inputSchema)

    present = {
        "rules": [
            {
                "kind": "rule",
                "state": "present",
                "matcher": {"type": "contains", "value": "Cafe"},
                "category": "Food",
                "priority": 10,
            }
        ]
    }
    inactive = {"rules": [{"kind": "rule", "state": "inactive", "rule_id": "rule_1"}]}
    absent = {
        "rules": [{"kind": "rule", "state": "absent", "rule_id": "rule_1"}],
        "confirmation_token": "token",
    }
    invalid = {
        "rules": [
            {
                "kind": "rule",
                "state": "inactive",
                "rule_id": "rule_1",
                "category": "Food",
            }
        ]
    }
    for payload in (present, inactive, absent):
        validate_json_schema(payload, tool.inputSchema, cls=Draft202012Validator)
    with pytest.raises(JSONSchemaValidationError):
        validate_json_schema(invalid, tool.inputSchema, cls=Draft202012Validator)


async def test_taxonomy_set_coarse_schema_and_annotations() -> None:
    mcp = isolated_server(register_taxonomy_coarse_writes)

    tool = await listed_tool(mcp, "taxonomy_set")

    assert tool.outputSchema is None
    assert tool.annotations is not None
    assert tool.annotations.readOnlyHint is False
    assert tool.annotations.destructiveHint is True
    assert tool.annotations.idempotentHint is True
    assert "confirmation_token" in tool.inputSchema["properties"]
    variants = {
        branch["properties"]["kind"]["const"]: set(branch["required"])
        for branch in tool.inputSchema["properties"]["items"]["items"]["oneOf"]
    }
    assert variants == {
        "category": {"kind", "state"},
        "merchant": {"kind", "state"},
    }
    Draft202012Validator.check_schema(tool.inputSchema)

    for payload in (
        {
            "items": [
                {
                    "kind": "category",
                    "state": "present",
                    "category": "Food",
                }
            ]
        },
        {
            "items": [
                {
                    "kind": "merchant",
                    "state": "present",
                    "raw_pattern": "CAFE",
                    "canonical_name": "Cafe",
                }
            ]
        },
        {
            "items": [
                {
                    "kind": "category",
                    "state": "absent",
                    "category_id": "category_1",
                }
            ],
            "confirmation_token": "token",
        },
    ):
        validate_json_schema(payload, tool.inputSchema, cls=Draft202012Validator)


async def test_privacy_consent_set_coarse_schema_and_annotations() -> None:
    mcp = isolated_server(register_privacy_coarse_writes)

    tool = await listed_tool(mcp, "privacy_consent_set")

    assert tool.outputSchema is None
    assert tool.annotations is not None
    assert tool.annotations.readOnlyHint is False
    assert tool.annotations.destructiveHint is True
    assert tool.annotations.idempotentHint is True
    assert set(tool.inputSchema["properties"]["state"]["enum"]) == {
        "granted",
        "revoked",
    }
    assert set(tool.inputSchema["properties"]["mode"]["enum"]) == {
        "persistent",
        "one-time",
    }
    Draft202012Validator.check_schema(tool.inputSchema)

    for payload in (
        {
            "categories": ["mcp-data-sharing"],
            "state": "granted",
        },
        {
            "categories": ["mcp-data-sharing", "matching-overview"],
            "state": "revoked",
            "backend": "openai",
            "confirmation_token": "token",
        },
    ):
        validate_json_schema(payload, tool.inputSchema, cls=Draft202012Validator)


@pytest.mark.integration
async def test_live_standard_write_discriminators_render_exactly() -> None:
    from moneybin.mcp.server import init_db, mcp

    init_db()
    annotations = await listed_tool(mcp, "transactions_annotate")
    reviews = await listed_tool(mcp, "reviews_decide")
    identities = await listed_tool(mcp, "identity_links_decide")
    taxonomy = await listed_tool(mcp, "taxonomy_set")
    rules = await listed_tool(mcp, "transactions_categorize_rules_set")

    assert _rendered_variants(
        annotations.inputSchema,
        collection="requests",
    ) == {
        "note_add": {"kind", "transaction_id", "text"},
        "note_edit": {"kind", "note_id", "text"},
        "note_delete": {"kind", "note_id"},
        "tags_set": {"kind", "transaction_id", "tags"},
        "splits_set": {"kind", "transaction_id", "splits"},
        "tag_rename": {"kind", "old_name", "new_name"},
    }
    assert _rendered_variants(reviews.inputSchema, collection="decisions") == {
        "auto_rule": {"kind", "decision_id", "decision"},
        "categorization": {"kind", "decision_id", "decision"},
        "match": {"kind", "decision_id", "decision"},
    }
    assert _rendered_variants(identities.inputSchema, collection="decisions") == {
        "account_link": {"kind", "decision_id", "decision"},
        "merchant_link": {"kind", "decision_id", "decision"},
        "security_link": {"kind", "decision_id", "decision"},
    }
    assert _rendered_variants(taxonomy.inputSchema, collection="items") == {
        "category": {"kind", "state"},
        "merchant": {"kind", "state"},
    }

    rule = rules.inputSchema["properties"]["rules"]["items"]
    assert rule["properties"]["kind"]["const"] == "rule"
    assert set(rule["properties"]["state"]["enum"]) == {
        "present",
        "inactive",
        "absent",
    }
    assert set(rule["required"]) == {"kind", "state"}

    for tool in (annotations, reviews, identities, taxonomy, rules):
        assert tool.outputSchema is None
        Draft202012Validator.check_schema(tool.inputSchema)


@pytest.mark.integration
async def test_live_standard_write_conditions_render_exactly() -> None:
    from moneybin.mcp.server import init_db, mcp

    init_db()
    reviews = await listed_tool(mcp, "reviews_decide")
    categorization = _rendered_variant(
        reviews.inputSchema,
        collection="decisions",
        kind="categorization",
    )
    categorization_accept = _conditional_then(
        categorization,
        "decision",
        "accept",
    )
    categorization_reject = _conditional_then(
        categorization,
        "decision",
        "reject",
    )
    _assert_required_non_null(categorization_accept, {"category"})
    assert _forbidden_fields(categorization_reject) == {
        "category",
        "subcategory",
        "canonical_merchant_name",
    }

    identities = await listed_tool(mcp, "identity_links_decide")
    for kind in ("account_link", "merchant_link", "security_link"):
        identity = _rendered_variant(
            identities.inputSchema,
            collection="decisions",
            kind=kind,
        )
        _assert_required_non_null(
            _conditional_then(identity, "decision", "accept"),
            {"target_id"},
        )
        assert _forbidden_fields(_conditional_then(identity, "decision", "reject")) == {
            "target_id"
        }

    taxonomy = await listed_tool(mcp, "taxonomy_set")
    category = _rendered_variant(
        taxonomy.inputSchema,
        collection="items",
        kind="category",
    )
    category_present = _conditional_then(category, "state", "present")
    category_inactive = _conditional_then(category, "state", "inactive")
    category_absent = _conditional_then(category, "state", "absent")
    _assert_required_non_null(category_present, {"category"})
    assert category_present["properties"]["force"] == {"const": False}
    _assert_required_non_null(category_inactive, {"category_id"})
    assert category_inactive["properties"]["force"] == {"const": False}
    assert _forbidden_fields(category_inactive) == {
        "category",
        "subcategory",
        "description",
    }
    _assert_required_non_null(category_absent, {"category_id"})
    assert "force" not in category_absent["properties"]
    assert _forbidden_fields(category_absent) == {
        "category",
        "subcategory",
        "description",
    }

    merchant = _rendered_variant(
        taxonomy.inputSchema,
        collection="items",
        kind="merchant",
    )
    _assert_required_non_null(
        _conditional_then(merchant, "state", "present"),
        {"raw_pattern", "canonical_name"},
    )
    _assert_required_non_null(
        _conditional_then(merchant, "state", "absent"),
        {"merchant_id"},
    )
    assert _forbidden_fields(_conditional_then(merchant, "state", "absent")) == {
        "raw_pattern",
        "canonical_name",
        "match_type",
        "category",
        "subcategory",
    }

    rules = await listed_tool(mcp, "transactions_categorize_rules_set")
    rule = rules.inputSchema["properties"]["rules"]["items"]
    rule_present = _conditional_then(rule, "state", "present")
    rule_inactive = _conditional_then(rule, "state", "inactive")
    rule_absent = _conditional_then(rule, "state", "absent")
    _assert_required_non_null(
        rule_present,
        {"matcher", "category", "priority"},
    )
    _assert_required_non_null(rule_inactive, {"rule_id"})
    _assert_required_non_null(rule_absent, {"rule_id"})
    replacement_fields = {"matcher", "category", "subcategory", "priority"}
    assert _forbidden_fields(rule_inactive) == replacement_fields
    assert _forbidden_fields(rule_absent) == replacement_fields

    consent = await listed_tool(mcp, "privacy_consent_set")
    assert _forbidden_fields(
        _conditional_then(consent.inputSchema, "state", "revoked")
    ) == {"mode"}
    assert _forbidden_fields(
        _conditional_then(consent.inputSchema, "state", "granted")
    ) == {"confirmation_token"}

    balance = await listed_tool(mcp, "accounts_balance_assert")
    absent = _conditional_then(balance.inputSchema, "state", "absent")
    present = _conditional_else(balance.inputSchema, "state", "absent")
    assert _forbidden_fields(absent) == {"amount"}
    assert set(present["required"]) == {"amount"}
    assert present["properties"]["amount"] == {"not": {"type": "null"}}
    assert _forbidden_fields(present) == {"confirmation_token"}


def test_balance_assertion_amount_matches_decimal_18_2_extrema() -> None:
    adapter = TypeAdapter(BalanceAmount)
    maximum = Decimal("9999999999999999.99")

    assert adapter.validate_python(maximum) == maximum
    assert adapter.validate_python(-maximum) == -maximum
    with pytest.raises(ValidationError):
        adapter.validate_python(Decimal("10000000000000000.00"))
    with pytest.raises(ValidationError):
        adapter.validate_python(Decimal("-10000000000000000.00"))


@pytest.mark.parametrize("amount", ["1250.00", True, 1250.001])
async def test_balance_assertion_coarse_rejects_non_json_decimal_boundaries(
    amount: object,
) -> None:
    mcp = isolated_server(register_accounts_coarse_writes)

    response = await call_tool_raw(
        mcp,
        "accounts_balance_assert",
        {
            "account": "ACC001",
            "as_of": "2026-07-01",
            "amount": amount,
        },
    )

    assert response.isError is True


async def test_balance_assertion_coarse_transport_and_public_actor(
    mcp_db: object,
) -> None:
    captured: list[dict[str, Any]] = []
    mcp = isolated_server(register_accounts_coarse_writes)

    with patch("moneybin.mcp.decorator.write_privacy_event", captured.append):
        response = await call_tool_raw(
            mcp,
            "accounts_balance_assert",
            {
                "account": "ACC001",
                "as_of": "2026-07-01",
                "amount": 1250.0,
            },
        )

    text = next(
        block.text for block in response.content if isinstance(block, TextContent)
    )
    assert response.isError is False
    assert response.structuredContent is not None
    assert json.loads(text) == response.structuredContent
    assert response.structuredContent["data"] == {
        "account_id": "ACC001",
        "as_of": "2026-07-01",
        "operation_id": response.structuredContent["data"]["operation_id"],
        "prior_state": "absent",
        "state": "present",
    }
    assert response.structuredContent["summary"]["sensitivity"] == "medium"
    assert len(captured) == 1
    assert captured[0]["actor"] == "mcp.accounts_balance_assert"
    assert captured[0]["sensitivity"] == "medium"
    assert set(captured[0]["classes_returned"]) == {
        "record_id",
        "txn_date",
        "txn_type",
    }


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
    arguments = case["expectations"]["standard-47"]["calls"][0]["arguments"]
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
