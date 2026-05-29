"""Tests for import-confirmation primitive types."""

import pytest

from moneybin.extractors.confidence import Confidence
from moneybin.services.import_confirmation import (
    Accept,
    BridgePayload,
    ConfirmationRequired,
    Override,
    ProposedMapping,
    Resolved,
)


class TestProposedMapping:
    """Validate ProposedMapping shape and immutability."""

    def test_is_frozen(self) -> None:
        p = ProposedMapping(
            field_mapping={"transaction_date": "Date", "amount": "Amount"},
            sample_values={"transaction_date": ["2026-01-01"], "amount": ["10.00"]},
            unmapped_columns=("Notes",),
        )
        with pytest.raises(AttributeError):
            p.field_mapping = {}  # type: ignore[misc]

    def test_carries_unmapped_columns(self) -> None:
        p = ProposedMapping(
            field_mapping={"transaction_date": "Date"},
            sample_values={},
            unmapped_columns=("Memo", "Balance"),
        )
        assert p.unmapped_columns == ("Memo", "Balance")


class TestOverride:
    """Validate Override payload shape."""

    def test_partial_merge_shape(self) -> None:
        o = Override(mapping={"description": "Memo"})
        assert o.mapping == {"description": "Memo"}


class TestAccept:
    """Validate Accept marker type."""

    def test_marker_type(self) -> None:
        a = Accept()
        assert isinstance(a, Accept)


class TestConfirmationRequired:
    """Validate ConfirmationRequired payload and outcomes."""

    def test_carries_proposed_payload_and_confidence(self) -> None:
        c = Confidence(
            score=0.75, tier="medium", flagged=("description",), missing_required=()
        )
        p = ProposedMapping(
            field_mapping={"transaction_date": "Date", "amount": "Amt"},
            sample_values={},
            unmapped_columns=(),
        )
        outcome = ConfirmationRequired(
            channel="tabular",
            confidence=c,
            proposed=p,
            reason="unknown_layout",
        )
        assert outcome.channel == "tabular"
        assert outcome.reason == "unknown_layout"
        assert outcome.confidence.tier == "medium"

    def test_reason_drives_payload_kind(self) -> None:
        c = Confidence(score=0.85, tier="medium", flagged=(), missing_required=())
        p = ProposedMapping(field_mapping={}, sample_values={}, unmapped_columns=())
        out = ConfirmationRequired(
            channel="tabular",
            confidence=c,
            proposed=p,
            reason="validation_failure",
        )
        assert out.reason == "validation_failure"


class TestResolved:
    """Validate Resolved terminal outcome shape."""

    def test_carries_final_mapping_and_format_ref(self) -> None:
        r = Resolved(
            field_mapping={"transaction_date": "Date", "amount": "Amount"},
            format_ref="chase_credit",
            self_accepted=False,
        )
        assert r.format_ref == "chase_credit"
        assert r.self_accepted is False

    def test_self_accepted_records_path(self) -> None:
        r = Resolved(
            field_mapping={"transaction_date": "Date"},
            format_ref=None,
            self_accepted=True,
        )
        assert r.self_accepted is True


class TestBridgePayload:
    """Validate BridgePayload opaque dict shape."""

    def test_carries_channel_specific_blob(self) -> None:
        bp = BridgePayload(payload={"ir": {"pages": []}, "extraction_request": "rows"})
        assert "ir" in bp.payload
