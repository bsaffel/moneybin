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
    TabularReadOptions,
    disputed_row_fields,
    header_position_ambiguous_recovery,
    header_row_consumed_recovery,
    header_row_consumed_recovery_mcp,
    resolve_or_confirm,
    unreadable_date_recovery,
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
    ).to_dict(proposal_ref="@0")


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

    def test_projects_disputed_rows_through_the_proposed_mapping(self) -> None:
        """confirmation_payload_dict must allowlist via disputed_row_fields.

        Wiring test for the F fix: the raw positional
        ``header_position_ambiguous_rows``/``_header_cells`` on the outcome
        must be projected through ``disputed_row_fields`` using the
        PROPOSED mapping before reaching the payload dict — an unmapped,
        account-shaped cell (``ACCT-XY9Z``) must never appear.
        """
        from moneybin.services.import_confirmation import confirmation_payload_dict

        c = Confidence(score=0.0, tier="low", flagged=(), missing_required=())
        p = ProposedMapping(
            field_mapping={
                "transaction_date": "Date",
                "amount": "Amount",
                "description": "Description",
            },
            sample_values={},
            unmapped_columns=(),
        )
        out = ConfirmationRequired(
            channel="tabular",
            confidence=c,
            proposed=p,
            reason="header_position_ambiguous",
            header_position_ambiguous_rows=(
                ("2026-01-01", "42.50", "Coffee", "ACCT-XY9Z"),
            ),
            header_position_ambiguous_header_cells=(
                "Date",
                "Amount",
                "Description",
                "AccountNumber",
            ),
        )
        d = confirmation_payload_dict(out)
        disputed_rows = d["header_position_ambiguous_rows"]
        assert disputed_rows == [
            {
                "transaction_date": "2026-01-01",
                "amount": "42.50",
                "description": "Coffee",
            }
        ]
        assert isinstance(disputed_rows, list)
        for row in disputed_rows:
            assert isinstance(row, dict)
            assert "ACCT-XY9Z" not in row.values()


class TestDisputedRowFields:
    """disputed_row_fields: the allowlist-and-omit projection (round 17, F).

    Any shape-based masker (mask_pii_shaped included) has an irreducible
    short/alphanumeric-key hole, and identifiers.md's "Account identifiers"
    section closes the list of surfaces allowed to leak such a key. So the
    fix here is narrowing WHAT is shown (an allowlist of destination
    fields resolved by position), never a stronger masker on what leaks
    through.
    """

    _HEADER = ("Date", "Amount", "Description", "AccountNumber")
    _MAPPING = {
        "transaction_date": "Date",
        "amount": "Amount",
        "description": "Description",
    }

    def test_allowed_fields_shown_unmapped_column_omitted(self) -> None:
        rows = [("2026-01-01", "42.50", "Coffee", "ACCT-XY9Z")]
        result = disputed_row_fields(rows, self._HEADER, self._MAPPING)
        assert result == [
            {
                "transaction_date": "2026-01-01",
                "amount": "42.50",
                "description": "Coffee",
            }
        ]

    def test_short_numeric_key_in_unmapped_column_also_omitted(self) -> None:
        """A 4-digit key defeats mask_pii_shaped's digit-run backstop.

        The allowlist design doesn't care about the cell's shape at all --
        it never even inspects the value -- so a masker-defeating short key
        is omitted for the same reason ACCT-XY9Z is: unmapped column.
        """
        rows = [("2026-01-02", "10.00", "Tea", "1234")]
        result = disputed_row_fields(rows, self._HEADER, self._MAPPING)
        assert result == [
            {"transaction_date": "2026-01-02", "amount": "10.00", "description": "Tea"}
        ]
        assert "1234" not in result[0].values()

    def test_blank_header_cell_is_omitted(self) -> None:
        header = ("Date", "", "Description")
        mapping = {
            "transaction_date": "Date",
            "amount": "",
            "description": "Description",
        }
        rows = [("2026-01-01", "42.50", "Coffee")]
        result = disputed_row_fields(rows, header, mapping)
        # Position 1's header cell is blank -- identity unknown -- omitted
        # even though the mapping technically points amount at "".
        assert result == [{"transaction_date": "2026-01-01", "description": "Coffee"}]

    def test_duplicated_header_name_is_omitted(self) -> None:
        # Two columns both named "Amount" -- identity ambiguous at both
        # positions, so neither is shown even though one maps to "amount".
        header = ("Date", "Amount", "Amount")
        mapping = {"transaction_date": "Date", "amount": "Amount"}
        rows = [("2026-01-01", "42.50", "-4.75")]
        result = disputed_row_fields(rows, header, mapping)
        assert result == [{"transaction_date": "2026-01-01"}]

    def test_longer_row_shows_its_aligned_prefix_extra_cell_absent(self) -> None:
        """Round 20: a row longer than the header is not omitted either.

        Row length plays no part in the rule at all -- a position past the
        header's own width simply has no header cell to name it, so it
        never enters ``dest_by_position`` and drops out on its own, same
        as any other unresolvable position. This also matches the real
        read: ``pl.read_csv(..., truncate_ragged_lines=True)`` keeps a
        longer row's leading cells in the header's own columns too.
        """
        rows = [("2026-01-01", "42.50", "Coffee", "ACCT-XY9Z")]
        result = disputed_row_fields(rows, self._HEADER[:3], self._MAPPING)
        assert result == [
            {
                "transaction_date": "2026-01-01",
                "amount": "42.50",
                "description": "Coffee",
            }
        ]
        assert "ACCT-XY9Z" not in result[0].values()

    def test_shorter_row_projects_only_its_own_existing_positions(self) -> None:
        """Round 19: a row shorter than the header is not fully omitted.

        A CSV transaction that omits a trailing optional column (a common
        ragged shape -- ``2026-01-01,42.50`` ahead of a ``Date,Amount,
        Description`` header) must not lose its aligned date/amount
        evidence to a length check that has nothing to do with column
        identity. Only the position this row doesn't have (Description,
        AccountNumber) is naturally absent -- the row is not omitted whole.
        """
        rows = [("2026-01-01", "42.50")]  # missing Description, AccountNumber
        result = disputed_row_fields(rows, self._HEADER, self._MAPPING)
        assert result == [{"transaction_date": "2026-01-01", "amount": "42.50"}]

    def test_shorter_row_still_respects_blank_and_duplicate_header_cells(
        self,
    ) -> None:
        """The shorter-row projection still applies the identity rule per cell."""
        header = ("Date", "", "Amount", "Amount")
        mapping = {"transaction_date": "Date", "amount": "Amount"}
        # Row has only 2 cells: Date and the blank-headed column. Amount's
        # two (duplicated) positions are entirely absent from this row.
        rows = [("2026-01-01", "42.50")]
        result = disputed_row_fields(rows, header, mapping)
        assert result == [{"transaction_date": "2026-01-01"}]

    def test_empty_header_cells_omits_every_row(self) -> None:
        rows = [("2026-01-01", "42.50", "Coffee")]
        result = disputed_row_fields(rows, (), self._MAPPING)
        assert result == [{}]

    def test_no_rows_returns_empty_list(self) -> None:
        assert disputed_row_fields([], self._HEADER, self._MAPPING) == []

    def test_mutation_unmapped_cells_passing_through_would_be_caught(self) -> None:
        """Prove the allowlist filter, not just the happy path, is load-bearing.

        Simulates the mutation "drop the dest_by_column allowlist filter and
        map every header cell verbatim" by hand-deriving what THAT buggy
        behavior would produce, and asserting the real function does NOT
        produce it -- i.e. this test would go red under that mutation.
        """
        rows = [("2026-01-01", "42.50", "Coffee", "ACCT-XY9Z")]
        result = disputed_row_fields(rows, self._HEADER, self._MAPPING)
        buggy_pass_through = {
            "Date": "2026-01-01",
            "Amount": "42.50",
            "Description": "Coffee",
            "AccountNumber": "ACCT-XY9Z",
        }
        assert result[0] != buggy_pass_through
        assert "ACCT-XY9Z" not in result[0].values()

    def test_mutation_restoring_the_longer_row_guard_would_be_caught(self) -> None:
        """Round 20: prove the deleted longer-row guard stays deleted.

        Simulates the mutation "restore ``{} if len(row) > len(header_
        cells) else ...``" by hand-deriving what that buggy guard would
        produce for a longer row (the whole row omitted, as it did before
        round 20) and asserting the real function instead shows the
        aligned prefix.
        """
        rows = [("2026-01-01", "42.50", "Coffee", "ACCT-XY9Z")]
        result = disputed_row_fields(rows, self._HEADER[:3], self._MAPPING)
        buggy_longer_row_result: dict[str, str] = {}
        assert result[0] != buggy_longer_row_result
        assert result == [
            {
                "transaction_date": "2026-01-01",
                "amount": "42.50",
                "description": "Coffee",
            }
        ]

    def test_mutation_restoring_the_exact_length_guard_would_be_caught(self) -> None:
        """Round 19: prove the SHORTER-row projection is load-bearing too.

        Simulates the mutation "restore the old exact-length guard
        (``len(row) != len(header_cells)``)" by hand-deriving what THAT
        buggy guard would produce for a shorter row (the whole row
        omitted, as it did before round 19) and asserting the real
        function instead projects the row's own existing positions.
        """
        rows = [("2026-01-01", "42.50")]
        result = disputed_row_fields(rows, self._HEADER, self._MAPPING)
        buggy_exact_length_result: dict[str, str] = {}
        assert result[0] != buggy_exact_length_result
        assert result == [{"transaction_date": "2026-01-01", "amount": "42.50"}]


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


class TestCoerceSignConvention:
    """The amount-shape coercion its two siblings are modelled on."""

    def test_a_swap_to_split_retires_a_single_amount_rule(self) -> None:
        from moneybin.services.import_confirmation import coerce_sign_convention

        assert (
            coerce_sign_convention(
                field_mapping={"debit_amount": "Soll", "credit_amount": "Haben"},
                detected="negative_is_expense",
            )
            == "split_debit_credit"
        )

    def test_a_swap_to_single_retires_a_split_rule(self) -> None:
        """A split rule against one amount column rejects every row."""
        from moneybin.services.import_confirmation import coerce_sign_convention

        assert (
            coerce_sign_convention(
                field_mapping={"amount": "Amount"},
                detected="split_debit_credit",
            )
            == "negative_is_expense"
        )

    def test_an_unchanged_shape_keeps_the_detected_convention(self) -> None:
        """Including negative_is_income, which no coercion may quietly flip."""
        from moneybin.services.import_confirmation import coerce_sign_convention

        assert (
            coerce_sign_convention(
                field_mapping={"amount": "Amount"},
                detected="negative_is_income",
            )
            == "negative_is_income"
        )


class TestCoerceConfidenceTier:
    """Re-banding a merged mapping; its predecessor was reported wrong twice."""

    _BANDS = {"t_high": 0.90, "t_med": 0.70}

    def test_a_flag_the_override_did_not_name_blocks_high(self) -> None:
        """High is the tier eligible for agent self-accept."""
        from moneybin.services.import_confirmation import coerce_confidence_tier

        assert (
            coerce_confidence_tier(
                field_mapping={
                    "transaction_date": "Date",
                    "amount": "Amount",
                    "description": "Blurb",
                },
                detected_flagged=["description"],
                override_keys={"amount"},
                date_format="%Y-%m-%d",
                structural_red_flag=False,
                **self._BANDS,
            )
            == "medium"
        )

    def test_an_override_covering_every_flag_reaches_high(self) -> None:
        from moneybin.services.import_confirmation import coerce_confidence_tier

        assert (
            coerce_confidence_tier(
                field_mapping={
                    "transaction_date": "Date",
                    "amount": "Amount",
                    "description": "Blurb",
                },
                detected_flagged=["description"],
                override_keys={"description"},
                date_format="%Y-%m-%d",
                structural_red_flag=False,
                **self._BANDS,
            )
            == "high"
        )

    def test_a_flag_on_a_retired_destination_stops_counting(self) -> None:
        """A flag survives `override_keys` but names no column in the result.

        Regression: switching to a split shape retires `amount`, but
        `map_columns` had content-discovered a spare numeric column as `amount`
        and flagged it. The flag is not an override key, so it kept scoring
        0.85 against a mapping that no longer contains the field it names —
        re-confirming a plan the user had already corrected.
        """
        from moneybin.services.import_confirmation import coerce_confidence_tier

        assert (
            coerce_confidence_tier(
                field_mapping={
                    "transaction_date": "Date",
                    "description": "Memo",
                    "debit_amount": "Soll",
                    "credit_amount": "Haben",
                },
                detected_flagged=["amount"],
                override_keys={"debit_amount", "credit_amount"},
                date_format="%Y-%m-%d",
                structural_red_flag=False,
                **self._BANDS,
            )
            == "high"
        )

    def test_a_structural_red_flag_pins_low_whatever_the_score(self) -> None:
        from moneybin.services.import_confirmation import coerce_confidence_tier

        assert (
            coerce_confidence_tier(
                field_mapping={
                    "transaction_date": "Date",
                    "amount": "Amount",
                    "description": "Memo",
                },
                detected_flagged=[],
                override_keys=set(),
                date_format="%Y-%m-%d",
                structural_red_flag=True,
                **self._BANDS,
            )
            == "low"
        )

    def test_an_unread_date_bands_below_high(self) -> None:
        from moneybin.services.import_confirmation import coerce_confidence_tier

        assert (
            coerce_confidence_tier(
                field_mapping={
                    "transaction_date": "Date",
                    "amount": "Amount",
                    "description": "Memo",
                },
                detected_flagged=[],
                override_keys=set(),
                date_format=None,
                structural_red_flag=False,
                **self._BANDS,
            )
            == "medium"
        )


class TestCoerceNumberFormat:
    """Keeping number_format in step with the amount shape actually mapped."""

    def test_a_swap_to_split_columns_re_reads_the_format(self) -> None:
        """map_columns reads the format from `amount`, which an override retires.

        Regression: an override swapping a detected single-amount layout to a
        debit/credit pair left the format derived from the discarded column.
        A US-formatted loser beside European survivors parses `1.234,56` as
        `1.23456` — wrong by three orders of magnitude, silently — and the
        format is then saved for every later import of that layout.
        """
        from moneybin.services.import_confirmation import coerce_number_format

        assert (
            coerce_number_format(
                field_mapping={
                    "transaction_date": "Date",
                    "debit_amount": "Soll",
                    "credit_amount": "Haben",
                },
                sample_values={
                    "amount": ["1,234.56"],  # the retired US column
                    # Each row fills one side only — the real shape of a split
                    # layout, not a column of paired values.
                    "debit_amount": ["1.234,56", "", "2.500,00"],
                    "credit_amount": ["", "3.750,25", ""],
                },
                detected="us",
            )
            == "european"
        )

    def test_an_all_blank_split_column_does_not_shadow_the_other(self) -> None:
        """A blank sample window is not evidence of a format.

        collect_samples takes the first rows unfiltered, so a run of
        credit-only transactions leaves debit_amount a non-empty list of
        blanks. Reading the first *present* destination handed those to
        detect_number_format, which found nothing parseable and returned its
        own "us" default — without ever looking at the column that holds the
        values.
        """
        from moneybin.services.import_confirmation import coerce_number_format

        assert (
            coerce_number_format(
                field_mapping={"debit_amount": "Soll", "credit_amount": "Haben"},
                sample_values={
                    "debit_amount": ["", "", None],
                    "credit_amount": ["1.234,56", "2.500,00", "3.750,25"],
                },
                detected="us",
            )
            == "european"
        )

    def test_an_unchanged_single_amount_keeps_its_format(self) -> None:
        from moneybin.services.import_confirmation import coerce_number_format

        assert (
            coerce_number_format(
                field_mapping={"amount": "Amount"},
                sample_values={"amount": ["1,234.56"]},
                detected="us",
            )
            == "us"
        )

    def test_no_samples_keeps_the_detector_s_answer(self) -> None:
        """Fail soft: a guess from nothing is worse than what the detector read."""
        from moneybin.services.import_confirmation import coerce_number_format

        assert (
            coerce_number_format(
                field_mapping={"debit_amount": "Soll"},
                sample_values={},
                detected="european",
            )
            == "european"
        )
        # Blanks are the same as absent — not evidence for the "us" default.
        assert (
            coerce_number_format(
                field_mapping={"debit_amount": "Soll", "credit_amount": "Haben"},
                sample_values={"debit_amount": ["", None], "credit_amount": [" "]},
                detected="european",
            )
            == "european"
        )


class TestUnreadableDateRecovery:
    """The recovery text four surfaces print verbatim."""

    def test_a_path_with_spaces_stays_runnable(self) -> None:
        """A prescribed command the user cannot paste is not a recovery.

        Bank exports land in directories like "Bank Exports/" often enough
        that an unquoted path breaks the hint exactly when it is needed. The
        CLI already shlex-quotes every other suggested command.
        """
        message = unreadable_date_recovery("/home/me/Bank Exports/jan stmt.csv")
        assert "'/home/me/Bank Exports/jan stmt.csv'" in message
        # The bare, unquoted form must not appear anywhere in the message.
        assert "files /home/me/Bank Exports/jan stmt.csv" not in message

    def test_both_recoveries_are_named(self) -> None:
        """Either cause can be the real one, so neither may be dropped."""
        message = unreadable_date_recovery("/data/plain.csv")
        assert "--mapping transaction_date=" in message
        assert "--date-format" in message

    def test_the_other_five_read_options_ride_along(self) -> None:
        """Every option but the failed one rides along.

        `--format`/`--number-format`/`--sheet`/`--delimiter`/`--encoding`
        appear on both the preview hint and the `import files` retry — but
        never a `--date-format`, since that is the value that just failed
        and the retry already carries its own `<strptime>` placeholder.
        """
        message = unreadable_date_recovery(
            "/data/plain.csv",
            read_options=TabularReadOptions(
                format_name="chase_credit",
                number_format="european",
                sheet="Transactions",
                delimiter=";",
                encoding="latin-1",
            ),
        )
        preview_clause, files_clause = message.split("import files", 1)
        assert "--format chase_credit" in preview_clause
        assert "--sheet Transactions" in preview_clause
        assert "--delimiter ';'" in preview_clause or "--delimiter ;" in preview_clause
        assert "--encoding latin-1" in preview_clause
        assert "--number-format european" in preview_clause
        assert "--format chase_credit" in files_clause
        assert "--number-format european" in files_clause
        assert "--sheet Transactions" in files_clause
        assert "--encoding latin-1" in files_clause
        assert "--date-format <strptime>" in files_clause
        # The failing date-format value itself must never be echoed back as a
        # concrete override — only the placeholder.
        assert message.count("--date-format") == 1

    def test_the_failed_date_format_is_dropped_from_the_retry(self) -> None:
        """A caller's own `date_format` must not reach the printed retry.

        The value object carries every read option, this one included, so
        dropping it is an explicit step rather than a shape the type
        prevents. A retry that repeated it would re-run with the format
        that just failed, and click takes the last `--date-format` wins.
        """
        message = unreadable_date_recovery(
            "/data/plain.csv",
            read_options=TabularReadOptions(
                date_format="%Y%m%d",
                encoding="latin-1",
            ),
        )
        assert "%Y%m%d" not in message
        assert "--date-format <strptime>" in message
        assert message.count("--date-format") == 1
        assert "--encoding latin-1" in message


class TestHeaderPositionAmbiguousRecovery:
    """The recovery text for a row before the detected header."""

    def test_both_recovery_commands_are_named(self) -> None:
        message = header_position_ambiguous_recovery("/data/plain.csv")
        assert "import files" in message
        assert "import confirm" in message
        assert "--confirm" in message
        assert "--accept" in message

    def test_set_read_options_appear_on_both_commands(self) -> None:
        message = header_position_ambiguous_recovery(
            "/data/plain.csv",
            read_options=TabularReadOptions(
                format_name="chase_credit",
                date_format="%Y%m%d",
                number_format="european",
                sheet="Transactions",
                delimiter=";",
                encoding="latin-1",
            ),
        )
        files_clause, confirm_clause = message.split("import confirm", 1)
        for clause in (files_clause, confirm_clause):
            assert "--format chase_credit" in clause
            assert "--date-format %Y%m%d" in clause
            assert "--number-format european" in clause
            assert "--sheet Transactions" in clause
            assert "--encoding latin-1" in clause

    def test_unset_options_add_nothing(self) -> None:
        message = header_position_ambiguous_recovery("/data/plain.csv")
        for flag in (
            "--format",
            "--date-format",
            "--number-format",
            "--sheet",
            "--delimiter",
            "--encoding",
        ):
            assert flag not in message


class TestHeaderRowConsumedRecovery:
    """The consumed-header recovery text, CLI and MCP.

    Both must name the recoveries proven in
    ``test_tabular_import_service.py`` (re-import without naming the stale
    format; delete it) and must no longer send a caller to fix something
    MoneyBin exposes no way to fix — a saved format's ``skip_rows``.
    """

    def test_cli_names_the_proven_recoveries(self) -> None:
        message = header_row_consumed_recovery()
        assert "without --format" in message
        assert "moneybin import formats delete" in message
        assert "correct the saved format" not in message
        assert "Add a header row" not in message

    def test_mcp_names_the_proven_recoveries(self) -> None:
        message = header_row_consumed_recovery_mcp()
        assert "delete_saved_format" in message
        assert "correct the saved format" not in message
        assert "Add a header row" not in message


def test_import_confirmation_required_error_carries_outcome() -> None:
    c = Confidence(score=0.5, tier="low", flagged=(), missing_required=("amount",))
    p = ProposedMapping(field_mapping={}, sample_values={}, unmapped_columns=())
    out = ConfirmationRequired(
        channel="tabular", confidence=c, proposed=p, reason="unknown_layout"
    )
    err = ImportConfirmationRequiredError(out)
    assert err.outcome.channel == "tabular"
    assert "requires confirmation" in str(err)
