"""Tests for the MCP response-envelope output schema."""

from __future__ import annotations

import json

import pytest
from jsonschema import Draft202012Validator
from jsonschema.exceptions import ValidationError

from moneybin import error_codes
from moneybin.errors import RecoveryAction, UserError
from moneybin.mcp.output_schema import output_schema_for
from moneybin.privacy.introspection import PrivacyContractError
from moneybin.privacy.payloads.accounts import AccountListPayload
from moneybin.protocol.envelope import ResponseEnvelope, SummaryMeta

SUCCESS_ENVELOPE = ResponseEnvelope(
    summary=SummaryMeta(
        total_count=0,
        returned_count=0,
        period="2026-07",
        sensitivity="high",
        degraded=True,
        degraded_reason="Sensitive account fields were masked",
    ),
    data=AccountListPayload(rows=[]),
    actions=["Use accounts_get for one account"],
    next_cursor="next-page",
    recovery_actions=[
        RecoveryAction(
            tool="accounts",
            arguments={},
            rationale="Retry the account listing.",
            confidence="suggested",
            idempotent=True,
        )
    ],
)

ERROR_ENVELOPE = ResponseEnvelope(
    summary=SummaryMeta(total_count=0, returned_count=0),
    data=[],
    actions=["Run system_status to inspect MoneyBin"],
    error=UserError(
        "Database is locked",
        code=error_codes.INFRA_DATABASE_LOCKED,
        hint="Wait for the current writer to finish",
        details={"retryable": True},
        recovery_actions=[
            RecoveryAction(
                tool="system_status",
                arguments={},
                rationale="Identify the process holding the database.",
                confidence="suggested",
                idempotent=True,
            )
        ],
    ),
)


def test_output_schema_matches_envelope_wire_keys() -> None:
    schema = output_schema_for(ResponseEnvelope[AccountListPayload])

    assert schema["type"] == "object"
    rendered = json.dumps(schema, sort_keys=True)
    assert '"ok"' in rendered
    assert '"error"' in rendered
    assert all(key in rendered for key in ("summary", "data", "actions"))
    assert "classes_returned" not in json.dumps(schema)


def test_success_and_error_envelopes_validate_against_schema() -> None:
    schema = output_schema_for(ResponseEnvelope[AccountListPayload])
    validator = Draft202012Validator(schema)

    validator.validate(  # pyright: ignore[reportUnknownMemberType]  # untyped dependency
        SUCCESS_ENVELOPE.to_dict()
    )
    validator.validate(  # pyright: ignore[reportUnknownMemberType]  # untyped dependency
        ERROR_ENVELOPE.to_dict()
    )


def test_error_envelope_requires_empty_data() -> None:
    schema = output_schema_for(ResponseEnvelope[AccountListPayload])
    invalid_error = ERROR_ENVELOPE.to_dict() | {"data": [{"unexpected": True}]}

    with pytest.raises(ValidationError):
        Draft202012Validator(schema).validate(  # pyright: ignore[reportUnknownMemberType]  # untyped dependency
            invalid_error
        )


def test_output_schema_rejects_non_envelope_return_hint() -> None:
    with pytest.raises(
        PrivacyContractError, match=r"output schema requires ResponseEnvelope\[T\]"
    ):
        output_schema_for(AccountListPayload)
