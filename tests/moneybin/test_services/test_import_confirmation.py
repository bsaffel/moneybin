"""Tests for import-confirmation primitive types."""

import pytest

from moneybin.extractors.confidence import Confidence
from moneybin.services.account_resolution_types import (
    AccountCandidate,
    AccountProposal,
    AccountProposalDict,
)
from moneybin.services.import_confirmation import (
    Accept,
    BridgePayload,
    ConfirmationRequired,
    ImportConfirmationRequiredError,
    MappingValidationError,
    Override,
    ProposedMapping,
    Resolved,
    SignConventionProposal,
    resolve_or_confirm,
    validate_partial_mapping,
)


def _account_proposal_dict() -> AccountProposalDict:
    """One account proposal dict via the real serializer (guarantees the shape)."""
    return AccountProposal(
        source_account_key="wf-checking",
        proposed_account_id="prov12345678",
        is_new=True,
        candidates=(
            AccountCandidate(
                account_id="cand87654321",
                display_name="WF Checking",
                confidence=0.5,
                signal="institution_last4",
            ),
        ),
    ).to_dict()


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

    def test_account_proposals_default_empty(self) -> None:
        c = Confidence(score=0.85, tier="high", flagged=(), missing_required=())
        p = ProposedMapping(field_mapping={}, sample_values={}, unmapped_columns=())
        out = ConfirmationRequired(
            channel="tabular", confidence=c, proposed=p, reason="unknown_layout"
        )
        # A mapping-only confirmation carries no account facet.
        assert out.account_proposals == []

    def test_account_confirmation_carries_proposals(self) -> None:
        c = Confidence(score=1.0, tier="high", flagged=(), missing_required=())
        p = ProposedMapping(
            field_mapping={"transaction_date": "Date", "amount": "Amt"},
            sample_values={},
            unmapped_columns=(),
        )
        out = ConfirmationRequired(
            channel="tabular",
            confidence=c,
            proposed=p,
            reason="account_confirmation",
            account_proposals=[_account_proposal_dict()],
        )
        assert out.reason == "account_confirmation"
        assert out.account_proposals[0]["source_account_key"] == "wf-checking"


class TestConfirmationPayloadDict:
    """confirmation_payload_dict serialization includes the account facet."""

    def test_carries_account_proposals(self) -> None:
        from moneybin.services.import_confirmation import confirmation_payload_dict

        c = Confidence(score=1.0, tier="high", flagged=(), missing_required=())
        p = ProposedMapping(
            field_mapping={"transaction_date": "Date"},
            sample_values={},
            unmapped_columns=(),
        )
        proposals = [_account_proposal_dict()]
        out = ConfirmationRequired(
            channel="tabular",
            confidence=c,
            proposed=p,
            reason="account_confirmation",
            account_proposals=proposals,
        )
        d = confirmation_payload_dict(out)
        assert d["reason"] == "account_confirmation"
        assert d["account_proposals"] == proposals
        # The resolved mapping still rides along so the caller sees the full picture.
        assert d["proposed_mapping"] == {"transaction_date": "Date"}

    def test_mapping_only_confirmation_has_empty_account_proposals(self) -> None:
        from moneybin.services.import_confirmation import confirmation_payload_dict

        c = Confidence(score=0.5, tier="low", flagged=(), missing_required=())
        p = ProposedMapping(field_mapping={}, sample_values={}, unmapped_columns=())
        out = ConfirmationRequired(
            channel="tabular", confidence=c, proposed=p, reason="unknown_layout"
        )
        d = confirmation_payload_dict(out)
        assert d["account_proposals"] == []

    def test_confirmation_payload_dict_serializes_a_sign_proposal(self) -> None:
        from moneybin.services.import_confirmation import confirmation_payload_dict

        outcome = ConfirmationRequired(
            channel="pdf",
            confidence=Confidence(
                score=0.75,
                tier="medium",
                flagged=("sign_convention",),
                missing_required=(),
            ),
            proposed=SignConventionProposal(
                sign_convention="negative_is_income",
                evidence=("Transaction Credit",),
                sample_rows=[
                    {
                        "description": "COFFEE",
                        "as_printed": "150.00",
                        "as_recorded": "-150.00",
                    }
                ],
            ),
            reason="sign_convention",
        )
        payload = confirmation_payload_dict(outcome)
        assert payload["reason"] == "sign_convention"
        assert payload["sign_convention"] == "negative_is_income"
        assert payload["sign_evidence"] == ["Transaction Credit"]
        assert payload["sign_sample_rows"] == [
            {
                "description": "COFFEE",
                "as_printed": "150.00",
                "as_recorded": "-150.00",
            }
        ]
        assert payload["bridge_payload"] is None
        assert payload["proposed_mapping"] == {}


class TestSignConventionProposal:
    """Validate SignConventionProposal shape and immutability."""

    def test_is_frozen(self) -> None:
        p = SignConventionProposal(
            sign_convention="negative_is_income",
            evidence=("minimum payment",),
            sample_rows=[],
        )
        with pytest.raises(AttributeError):
            p.sign_convention = "positive_is_income"  # type: ignore[misc]

    def test_carries_evidence_and_sample_rows(self) -> None:
        p = SignConventionProposal(
            sign_convention="negative_is_income",
            evidence=("minimum payment", "credit limit"),
            sample_rows=[
                {
                    "description": "COFFEE",
                    "as_printed": "150.00",
                    "as_recorded": "-150.00",
                }
            ],
        )
        assert p.evidence == ("minimum payment", "credit limit")
        assert p.sample_rows[0]["description"] == "COFFEE"


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


class TestValidatePartialMapping:
    """Validate validate_partial_mapping merging and validation logic."""

    def test_accepts_override_filling_required(self) -> None:
        proposed = {"transaction_date": "Date"}
        override = {"amount": "Amt"}
        validate_partial_mapping(
            proposed=proposed,
            override=override,
            available_columns=("Date", "Amt"),
            required_fields=("transaction_date", "amount"),
        )

    def test_rejects_missing_required_after_merge(self) -> None:
        proposed = {"transaction_date": "Date"}
        override: dict[str, str] = {}
        with pytest.raises(MappingValidationError, match="missing required"):
            validate_partial_mapping(
                proposed=proposed,
                override=override,
                available_columns=("Date", "Amt"),
                required_fields=("transaction_date", "amount"),
            )

    def test_rejects_unknown_source_column(self) -> None:
        proposed = {"transaction_date": "Date", "amount": "Amt"}
        override = {"description": "Notes"}
        with pytest.raises(MappingValidationError, match="not in the source"):
            validate_partial_mapping(
                proposed=proposed,
                override=override,
                available_columns=("Date", "Amt"),
                required_fields=("transaction_date", "amount"),
            )

    def test_rejects_unknown_destination_field_when_allowlisted(self) -> None:
        """Override with a destination not in valid_destinations is rejected."""
        with pytest.raises(MappingValidationError, match="unknown destination"):
            validate_partial_mapping(
                proposed={"transaction_date": "Date", "amount": "Amt"},
                override={"flugulhorn": "Memo"},
                available_columns=("Date", "Amt", "Memo"),
                required_fields=("transaction_date", "amount"),
                valid_destinations=(
                    "transaction_date",
                    "amount",
                    "description",
                ),
            )

    def test_rejects_override_with_amount_and_split_together(self) -> None:
        """Override naming both amount AND the split pair is contradictory.

        amount and (debit_amount, credit_amount) are mutually exclusive
        amount-shapes — accepting both would let transform_dataframe
        silently pick one (via sign_convention) and drop the other. The
        validator surfaces the contradiction up front instead.
        """
        with pytest.raises(MappingValidationError, match="contradictory"):
            validate_partial_mapping(
                proposed={"transaction_date": "Date", "amount": "Amt"},
                override={
                    "amount": "Amount",
                    "debit_amount": "Debit",
                    "credit_amount": "Credit",
                },
                available_columns=("Date", "Amt", "Amount", "Debit", "Credit"),
                required_fields=("transaction_date", "amount"),
            )

    def test_valid_destinations_none_skips_destination_check(self) -> None:
        """Back-compat: valid_destinations=None accepts any override key."""
        merged = validate_partial_mapping(
            proposed={"transaction_date": "Date", "amount": "Amt"},
            override={"custom_field": "Memo"},
            available_columns=("Date", "Amt", "Memo"),
            required_fields=("transaction_date", "amount"),
            valid_destinations=None,
        )
        assert merged["custom_field"] == "Memo"

    def test_override_replaces_proposed_for_named_field(self) -> None:
        proposed = {"transaction_date": "Date", "amount": "Amt", "description": "Memo"}
        override = {"description": "Notes"}
        validate_partial_mapping(
            proposed=proposed,
            override=override,
            available_columns=("Date", "Amt", "Memo", "Notes"),
            required_fields=("transaction_date", "amount", "description"),
        )

    def test_per_channel_required_fields(self) -> None:
        proposed = {"transaction_date": "Date", "amount": "Amt"}
        override: dict[str, str] = {}
        validate_partial_mapping(
            proposed=proposed,
            override=override,
            available_columns=("Date", "Amt"),
            required_fields=("transaction_date", "amount"),
        )
        with pytest.raises(MappingValidationError, match="missing required"):
            validate_partial_mapping(
                proposed=proposed,
                override=override,
                available_columns=("Date", "Amt"),
                required_fields=("transaction_date", "amount", "description"),
            )

    def test_returns_merged_mapping(self) -> None:
        proposed = {"transaction_date": "Date", "amount": "Amt", "description": "Memo"}
        override = {"description": "Notes"}
        merged = validate_partial_mapping(
            proposed=proposed,
            override=override,
            available_columns=("Date", "Amt", "Memo", "Notes"),
            required_fields=("transaction_date", "amount", "description"),
        )
        assert merged == {
            "transaction_date": "Date",
            "amount": "Amt",
            "description": "Notes",
        }


def _make_proposed(*, missing: tuple[str, ...] = ()) -> ProposedMapping:
    fm = {"transaction_date": "Date", "amount": "Amt", "description": "Memo"}
    for m in missing:
        fm.pop(m, None)
    return ProposedMapping(field_mapping=fm, sample_values={}, unmapped_columns=())


class TestResolveOrConfirm:
    """Verify the channel-agnostic confirm/resolve decision tree."""

    def test_no_signal_returns_confirmation_required(self) -> None:
        confidence = Confidence(
            score=0.85, tier="medium", flagged=(), missing_required=()
        )
        out = resolve_or_confirm(
            channel="tabular",
            confidence=confidence,
            proposed=_make_proposed(),
            available_columns=("Date", "Amt", "Memo"),
            required_fields=("transaction_date", "amount", "description"),
            signal=None,
            self_accept_enabled=False,
            actor_kind="human",
        )
        assert isinstance(out, ConfirmationRequired)
        assert out.reason == "unknown_layout"

    def test_accept_returns_resolved(self) -> None:
        confidence = Confidence(
            score=0.95, tier="high", flagged=(), missing_required=()
        )
        out = resolve_or_confirm(
            channel="tabular",
            confidence=confidence,
            proposed=_make_proposed(),
            available_columns=("Date", "Amt", "Memo"),
            required_fields=("transaction_date", "amount", "description"),
            signal=Accept(),
            self_accept_enabled=False,
            actor_kind="human",
        )
        assert isinstance(out, Resolved)
        assert out.self_accepted is False
        assert out.field_mapping["amount"] == "Amt"

    def test_low_can_never_auto_accept(self) -> None:
        confidence = Confidence(
            score=0.3, tier="low", flagged=(), missing_required=("description",)
        )
        out = resolve_or_confirm(
            channel="tabular",
            confidence=confidence,
            proposed=_make_proposed(missing=("description",)),
            available_columns=("Date", "Amt"),
            required_fields=("transaction_date", "amount", "description"),
            signal=Accept(),  # even an explicit Accept on low must surface
            self_accept_enabled=True,
            actor_kind="agent",
        )
        assert isinstance(out, ConfirmationRequired)

    def test_agent_self_accept_high_only_when_enabled(self) -> None:
        confidence = Confidence(
            score=0.95, tier="high", flagged=(), missing_required=()
        )
        out_gated = resolve_or_confirm(
            channel="tabular",
            confidence=confidence,
            proposed=_make_proposed(),
            available_columns=("Date", "Amt", "Memo"),
            required_fields=("transaction_date", "amount", "description"),
            signal=None,
            self_accept_enabled=False,
            actor_kind="agent",
        )
        assert isinstance(out_gated, ConfirmationRequired)
        out_open = resolve_or_confirm(
            channel="tabular",
            confidence=confidence,
            proposed=_make_proposed(),
            available_columns=("Date", "Amt", "Memo"),
            required_fields=("transaction_date", "amount", "description"),
            signal=None,
            self_accept_enabled=True,
            actor_kind="agent",
        )
        assert isinstance(out_open, Resolved)
        assert out_open.self_accepted is True

    def test_human_always_surfaces_first_encounter(self) -> None:
        confidence = Confidence(
            score=0.99, tier="high", flagged=(), missing_required=()
        )
        out = resolve_or_confirm(
            channel="tabular",
            confidence=confidence,
            proposed=_make_proposed(),
            available_columns=("Date", "Amt", "Memo"),
            required_fields=("transaction_date", "amount", "description"),
            signal=None,
            self_accept_enabled=True,  # even with calibration on
            actor_kind="human",
        )
        assert isinstance(out, ConfirmationRequired)

    def test_override_partial_merge_resolves(self) -> None:
        confidence = Confidence(
            score=0.80, tier="medium", flagged=("description",), missing_required=()
        )
        proposed = _make_proposed()
        out = resolve_or_confirm(
            channel="tabular",
            confidence=confidence,
            proposed=proposed,
            available_columns=("Date", "Amt", "Memo", "Notes"),
            required_fields=("transaction_date", "amount", "description"),
            signal=Override(mapping={"description": "Notes"}),
            self_accept_enabled=False,
            actor_kind="human",
        )
        assert isinstance(out, Resolved)
        assert out.field_mapping["description"] == "Notes"
        assert out.field_mapping["amount"] == "Amt"  # fell back to proposed

    def test_override_invalid_resurfaces(self) -> None:
        confidence = Confidence(
            score=0.80, tier="medium", flagged=(), missing_required=()
        )
        proposed = _make_proposed()
        out = resolve_or_confirm(
            channel="tabular",
            confidence=confidence,
            proposed=proposed,
            available_columns=("Date", "Amt", "Memo"),
            required_fields=("transaction_date", "amount", "description"),
            signal=Override(mapping={"description": "Nonexistent"}),
            self_accept_enabled=False,
            actor_kind="human",
        )
        assert isinstance(out, ConfirmationRequired)
        assert out.reason == "validation_failure"
        assert "Nonexistent" in out.error_message

    def test_override_resolves_low_tier(self) -> None:
        """Explicit override on a low-tier proposal is the documented recovery path.

        Req 11 lists "re-call import_confirm with a corrected mapping" as a
        first-class recovery action. The resolver must honor an Override
        even when the detector returned `low`; only no-signal-and-Accept on
        low surfaces.
        """
        confidence = Confidence(
            score=0.4, tier="low", flagged=(), missing_required=("description",)
        )
        proposed = _make_proposed(missing=("description",))
        out = resolve_or_confirm(
            channel="tabular",
            confidence=confidence,
            proposed=proposed,
            available_columns=("Date", "Amt", "Notes"),
            required_fields=("transaction_date", "amount", "description"),
            signal=Override(mapping={"description": "Notes"}),
            self_accept_enabled=False,
            actor_kind="human",
        )
        assert isinstance(out, Resolved)
        assert out.field_mapping["description"] == "Notes"

    def test_override_validation_failure_carries_error_message(self) -> None:
        """ConfirmationRequired.error_message preserves the validator detail."""
        confidence = Confidence(
            score=0.80, tier="medium", flagged=(), missing_required=()
        )
        out = resolve_or_confirm(
            channel="tabular",
            confidence=confidence,
            proposed=_make_proposed(),
            available_columns=("Date", "Amt", "Memo"),
            required_fields=("transaction_date", "amount", "description"),
            signal=Override(mapping={"description": "NotInFile"}),
            self_accept_enabled=False,
            actor_kind="human",
        )
        assert isinstance(out, ConfirmationRequired)
        assert "NotInFile" in out.error_message
        assert "not in the source" in out.error_message


def test_import_confirmation_required_error_carries_outcome() -> None:
    c = Confidence(score=0.5, tier="low", flagged=(), missing_required=("amount",))
    p = ProposedMapping(field_mapping={}, sample_values={}, unmapped_columns=())
    out = ConfirmationRequired(
        channel="tabular", confidence=c, proposed=p, reason="unknown_layout"
    )
    err = ImportConfirmationRequiredError(out)
    assert err.outcome.channel == "tabular"
    assert "requires confirmation" in str(err)
