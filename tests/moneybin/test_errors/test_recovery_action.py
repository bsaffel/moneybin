"""Tests for the RecoveryAction model."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from moneybin.errors import RecoveryAction


class TestRecoveryActionConstruction:
    """Test construction and field validation of RecoveryAction."""

    def test_minimal_construction(self):
        ra = RecoveryAction(
            tool="system_audit_undo",
            arguments={"operation_id": "op_abc123"},
            rationale="Restore pre-mutation state",
            confidence="certain",
            idempotent=True,
        )
        assert ra.tool == "system_audit_undo"
        assert ra.arguments == {"operation_id": "op_abc123"}
        assert ra.rationale == "Restore pre-mutation state"
        assert ra.confidence == "certain"
        assert ra.idempotent is True

    def test_confidence_accepts_certain(self):
        ra = RecoveryAction(
            tool="t",
            arguments={},
            rationale="r",
            confidence="certain",
            idempotent=True,
        )
        assert ra.confidence == "certain"

    def test_confidence_accepts_suggested(self):
        ra = RecoveryAction(
            tool="t",
            arguments={},
            rationale="r",
            confidence="suggested",
            idempotent=False,
        )
        assert ra.confidence == "suggested"

    def test_confidence_rejects_other_values(self):
        with pytest.raises(ValidationError, match="confidence"):
            RecoveryAction(
                tool="t",
                arguments={},
                rationale="r",
                confidence="maybe",
                idempotent=True,  # type: ignore[arg-type]
            )

    def test_tool_required(self):
        with pytest.raises(ValidationError, match="tool"):
            RecoveryAction(
                arguments={},
                rationale="r",
                confidence="certain",
                idempotent=True,  # type: ignore[call-arg]
            )

    def test_rationale_required(self):
        with pytest.raises(ValidationError, match="rationale"):
            RecoveryAction(
                tool="t",
                arguments={},
                confidence="certain",
                idempotent=True,  # type: ignore[call-arg]
            )

    def test_arguments_can_be_empty(self):
        ra = RecoveryAction(
            tool="system_doctor",
            arguments={},
            rationale="Re-run diagnostic",
            confidence="suggested",
            idempotent=True,
        )
        assert ra.arguments == {}

    def test_arguments_accepts_nested(self):
        ra = RecoveryAction(
            tool="transactions_categorize_run",
            arguments={"methods": ["rules", "merchants"], "scope": {"since": "1h"}},
            rationale="Apply deterministic rules to fill gap",
            confidence="certain",
            idempotent=True,
        )
        assert ra.arguments["methods"] == ["rules", "merchants"]
        assert ra.arguments["scope"]["since"] == "1h"


class TestRecoveryActionSerialization:
    """Test serialization and deserialization of RecoveryAction."""

    def test_round_trips_through_dict(self):
        ra = RecoveryAction(
            tool="import_preview",
            arguments={"paths": ["/x/y.csv"]},
            rationale="Inspect before retry",
            confidence="certain",
            idempotent=True,
        )
        d = ra.model_dump()
        ra2 = RecoveryAction.model_validate(d)
        assert ra == ra2

    def test_round_trips_through_json(self):
        ra = RecoveryAction(
            tool="import_preview",
            arguments={"paths": ["/x/y.csv"]},
            rationale="Inspect before retry",
            confidence="certain",
            idempotent=True,
        )
        s = ra.model_dump_json()
        ra2 = RecoveryAction.model_validate_json(s)
        assert ra == ra2
