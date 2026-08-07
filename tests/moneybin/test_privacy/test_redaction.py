"""Per-class field redaction (PR 2: CRITICAL only)."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import Annotated, Literal, TypedDict

import pytest
from pydantic import BaseModel

from moneybin.privacy.redaction import (
    ConsentSet,
    MaskStrength,
    _scrub_embedded_pii,  # pyright: ignore[reportPrivateUsage]
    mask_strength,
    redact_records,
    redact_typed,
)
from moneybin.privacy.taxonomy import DataClass


@dataclass(frozen=True)
class _AccountRow:
    account_id: Annotated[str, DataClass.ACCOUNT_IDENTIFIER]
    routing_number: Annotated[str | None, DataClass.ROUTING_NUMBER]
    last_four: Annotated[str | None, DataClass.INSTITUTION_ACCOUNT_NUMBER]
    balance: Annotated[Decimal, DataClass.BALANCE]
    category: Annotated[str, DataClass.CATEGORY]


@dataclass(frozen=True)
class _AccountList:
    rows: list[_AccountRow]
    total_balance: Annotated[Decimal, DataClass.AGGREGATE]


class _PublicUnionArm(TypedDict):
    kind: Literal["public"]
    label: Annotated[str, DataClass.DESCRIPTION]


class _SecretUnionArm(TypedDict):
    kind: Literal["secret"]
    account_id: Annotated[str, DataClass.ACCOUNT_IDENTIFIER]


@dataclass(frozen=True)
class _TypedDictUnionPayload:
    details: _PublicUnionArm | _SecretUnionArm


def _sample_row() -> _AccountRow:
    return _AccountRow(
        account_id="acct_1234567890",
        routing_number="011000015",
        last_four="4242",
        balance=Decimal("1234.56"),
        category="checking",
    )


def test_account_identifier_masked_to_last_four() -> None:
    out = redact_typed(_sample_row(), consent=None)
    assert out.account_id == "****7890"


def test_account_identifier_short_value_fully_masked() -> None:
    row = _AccountRow(
        account_id="ab",
        routing_number=None,
        last_four=None,
        balance=Decimal("0"),
        category="checking",
    )
    out = redact_typed(row, consent=None)
    assert out.account_id == "****"


def test_routing_number_masked_to_constant() -> None:
    out = redact_typed(_sample_row(), consent=None)
    assert out.routing_number == "*****"


def test_routing_number_none_passes_through() -> None:
    row = _AccountRow(
        account_id="acct_1234",
        routing_number=None,
        last_four=None,
        balance=Decimal("0"),
        category="checking",
    )
    out = redact_typed(row, consent=None)
    assert out.routing_number is None


def test_institution_account_number_uses_last_four_pattern() -> None:
    out = redact_typed(_sample_row(), consent=None)
    assert out.last_four == "****4242"


@pytest.mark.parametrize(
    "value", [4, Decimal("4"), True, b"4021", ("4", "0"), Decimal("40.21")]
)
def test_a_partial_mask_masks_a_non_string_whole_instead_of_raising(
    value: object,
) -> None:
    """A transform that raises is not a weaker mask; it is no answer at all.

    This is the only transform that measures its input, so the only one that can
    fail on a value's shape — and lineage hands a class down through an expression
    without its type. An ``INSTITUTION_ACCOUNT_NUMBER`` column wrapped in
    ``length()`` arrives here as an ``int``, where ``len()`` raised ``TypeError``
    out of ``redact_records``. Every consumer of the shared table saw it:
    ``sql_query`` answered ``infra_unclassified_error`` ("This is a MoneyBin bug"),
    and a saved report could be created and then never run on any surface.

    The ``Sized`` non-strings are the sharper half — a 2-tuple measured shorter
    than four and returned ``"****"``, a partial mask's output for a value it
    never partially masked.
    """
    (masked,) = redact_records(
        [{"n": value}], {"n": DataClass.INSTITUTION_ACCOUNT_NUMBER}, consent=None
    )

    assert masked["n"] == "*****"


def test_a_partial_mask_still_keeps_the_last_four_of_a_string() -> None:
    """The benign twin: whole-masking a non-string must not widen to strings.

    Without this, replacing the partial mask outright would pass the test above
    and silently destroy the last four digits every consumer relies on.
    """
    (masked,) = redact_records(
        [{"n": "1234567890"}], {"n": DataClass.INSTITUTION_ACCOUNT_NUMBER}, consent=None
    )

    assert masked["n"] == "****7890"


def test_the_non_string_fallback_does_not_move_the_measured_mask_strength() -> None:
    """``mask_strength`` probes with strings, and the ordering it feeds must hold.

    Guards across the privacy surface rank classes by this value; a partial mask
    that measured WHOLE would let one stand in for a genuinely whole-masked class
    at the same tier — the substitution ``MaskStrength``'s own docstring exists to
    prevent.
    """
    assert mask_strength(DataClass.INSTITUTION_ACCOUNT_NUMBER) is MaskStrength.PARTIAL
    assert mask_strength(DataClass.ACCOUNT_IDENTIFIER) is MaskStrength.PARTIAL


def test_high_tier_balance_passes_through_in_pr2() -> None:
    out = redact_typed(_sample_row(), consent=None)
    assert out.balance == Decimal("1234.56")


def test_low_tier_category_passes_through() -> None:
    out = redact_typed(_sample_row(), consent=None)
    assert out.category == "checking"


def test_recurses_into_list_payload() -> None:
    payload = _AccountList(
        rows=[_sample_row(), _sample_row()], total_balance=Decimal("2469.12")
    )
    out = redact_typed(payload, consent=None)
    assert all(r.account_id == "****7890" for r in out.rows)
    assert out.total_balance == Decimal("2469.12")


class _PydAccount(BaseModel):
    account_id: Annotated[str, DataClass.ACCOUNT_IDENTIFIER]
    routing_number: Annotated[str | None, DataClass.ROUTING_NUMBER]
    category: Annotated[str, DataClass.CATEGORY]


class _PydAccountList(BaseModel):
    rows: list[_PydAccount]
    total_balance: Annotated[Decimal, DataClass.AGGREGATE]


def test_redacts_pydantic_model_critical_fields() -> None:
    """Pydantic BaseModel payloads must be traversed + rebuilt, not passed through.

    Regression for the leak where build_envelope/_count_pydantic_payload
    accepted BaseModel payloads but redact_typed returned them unmodified —
    CRITICAL Annotated fields on a Pydantic payload leaked raw values.
    """
    model = _PydAccount(
        account_id="acct_1234567890",
        routing_number="011000015",
        category="checking",
    )
    out = redact_typed(model, consent=None)
    assert out.account_id == "****7890"
    assert out.routing_number == "*****"
    assert out.category == "checking"  # LOW — passes through


def test_redacts_pydantic_nested_list() -> None:
    payload = _PydAccountList(
        rows=[
            _PydAccount(
                account_id="acct_1234567890", routing_number=None, category="checking"
            )
        ],
        total_balance=Decimal("100.00"),
    )
    out = redact_typed(payload, consent=None)
    assert out.rows[0].account_id == "****7890"
    assert out.total_balance == Decimal("100.00")


def test_import_files_preserves_bridge_input_but_masks_explicit_account_keys() -> None:
    """Bridge prose stays usable while semantically typed account keys mask."""
    from moneybin.privacy.introspection import derive_tier  # noqa: PLC0415
    from moneybin.privacy.payloads.imports import (  # noqa: PLC0415
        ImportFilesPayload,
        ImportPerFileRow,
    )
    from moneybin.privacy.taxonomy import Tier  # noqa: PLC0415

    payload = ImportFilesPayload(
        imported_count=0,
        failed_count=0,
        total_count=1,
        transforms_applied=False,
        transforms_duration_seconds=None,
        transforms_error=None,
        files=[
            ImportPerFileRow(
                path="statement.pdf",
                status="confirmation_required",
                source_type=None,
                rows_loaded=0,
                import_id=None,
                error=None,
                confirmation_payload={
                    "channel": "pdf",
                    "bridge_payload": {
                        "transparency_notice": "Review this statement.",
                        "source_file": "statement.pdf",
                        "document_text": (
                            "Account ending 5678\n"
                            "05/01 COFFEE SHOP -12.34\n"
                            "05/02 PAYROLL 2500.00"
                        ),
                        "tables_preview": [
                            {
                                "page": 1,
                                "header": ["Date", "Description", "Amount"],
                                "rows": [
                                    ["05/01", "COFFEE SHOP", "-12.34"],
                                    ["05/02", "PAYROLL", "2500.00"],
                                ],
                            }
                        ],
                        "fingerprint": {"issuer": "example"},
                        "request_kind": "propose_recipe",
                        "saved_recipe_for_re_derive": None,
                    },
                    "account_proposals": [
                        {
                            "source_account_key": "12345678",
                            "requires_confirm": True,
                        }
                    ],
                },
            )
        ],
    )

    out = redact_typed(payload, consent=None)

    assert derive_tier(ImportFilesPayload) is Tier.CRITICAL
    confirmation = out.files[0].confirmation_payload
    assert confirmation is not None
    bridge = confirmation["bridge_payload"]
    assert bridge is not None
    assert bridge["document_text"] == (
        "Account ending 5678\n05/01 COFFEE SHOP -12.34\n05/02 PAYROLL 2500.00"
    )
    assert bridge["tables_preview"][0]["rows"] == [
        ["05/01", "COFFEE SHOP", "-12.34"],
        ["05/02", "PAYROLL", "2500.00"],
    ]
    assert confirmation["account_proposals"][0]["source_account_key"] == "****5678"


def test_account_proposal_ref_survives_the_mask_that_hides_its_key() -> None:
    """The positional referent stays readable in the payload that masks the key.

    Both fields ride the same proposal. If ``proposal_ref`` masked alongside
    ``source_account_key`` the gate would be unanswerable from the envelope —
    which is the whole reason the ref exists. Asserted on the pydantic payload
    rather than the raw TypedDict: validation there drops any key the proposal
    does not declare, so this fails if the field is missing as well as if it
    is classified into a masking class.
    """
    from moneybin.privacy.payloads.imports import (  # noqa: PLC0415
        ImportConfirmRequiredPayload,
    )

    payload = ImportConfirmRequiredPayload(
        preview_id="prev_1",
        channel="ofx",
        tier="high",
        score=0.9,
        reason="account_confirmation",
        account_proposals=[
            {
                "source_account_key": "12345678",
                "proposal_ref": "@0",
                "requires_confirm": True,
            }
        ],
    )

    out = redact_typed(payload, consent=None)

    proposal = out.account_proposals[0]
    assert proposal["source_account_key"] == "****5678"
    assert proposal["proposal_ref"] == "@0"


def test_typed_dict_union_selects_the_matching_discriminator_arm() -> None:
    """A later TypedDict arm cannot leak a CRITICAL-only field."""
    payload = _TypedDictUnionPayload(
        details={"kind": "secret", "account_id": "12345678"}
    )

    out = redact_typed(payload, consent=None)

    assert out.details == {"kind": "secret", "account_id": "****5678"}


@dataclass(frozen=True)
class _SetContainer:
    rows: frozenset[_AccountRow]


@dataclass(frozen=True)
class _MappingContainer:
    by_id: dict[str, _AccountRow]


@dataclass(frozen=True)
class _OptionalListContainer:
    rows: list[_AccountRow] | None


def test_redacts_inside_frozenset() -> None:
    """_redact must traverse set/frozenset, not just list/tuple — mirrors _walk."""
    container = _SetContainer(rows=frozenset({_sample_row()}))
    out = redact_typed(container, consent=None)
    assert all(r.account_id == "****7890" for r in out.rows)


def test_redacts_inside_mapping_values() -> None:
    """_redact must traverse dict/Mapping values — mirrors _walk."""
    container = _MappingContainer(by_id={"a": _sample_row()})
    out = redact_typed(container, consent=None)
    assert out.by_id["a"].account_id == "****7890"


@dataclass(frozen=True)
class _HeteroTupleContainer:
    pair: tuple[
        Annotated[str, DataClass.ACCOUNT_IDENTIFIER],
        Annotated[str, DataClass.CATEGORY],
    ]


def test_redacts_heterogeneous_tuple_per_position() -> None:
    """Fixed-length tuple[A, B] redacts each position with its own type.

    Regression: the sequence branch used to apply the first element's type
    to every position, leaking a CRITICAL second element typed otherwise.
    """
    container = _HeteroTupleContainer(pair=("acct_1234567890", "checking"))
    out = redact_typed(container, consent=None)
    assert out.pair[0] == "****7890"  # ACCOUNT_IDENTIFIER masked
    assert out.pair[1] == "checking"  # CATEGORY passthrough


def test_redacts_optional_list_union_arm() -> None:
    """A `list[X] | None` field must still be redacted.

    The generic-alias union arm used to raise TypeError on isinstance and
    fall through unredacted.
    """
    container = _OptionalListContainer(rows=[_sample_row()])
    out = redact_typed(container, consent=None)
    assert out.rows is not None
    assert out.rows[0].account_id == "****7890"


def test_idempotent_on_already_redacted() -> None:
    once = redact_typed(_sample_row(), consent=None)
    twice = redact_typed(once, consent=None)
    assert once == twice


def test_scrub_embedded_pii_is_identity_in_pr2() -> None:
    text = "Account 1234567890 was charged $42 on 2026-05-17"
    assert _scrub_embedded_pii(text) == text


def test_consent_set_is_placeholder_dataclass() -> None:
    # PR 2: ConsentSet exists for type signatures but has no fields.
    cs = ConsentSet()
    assert cs == ConsentSet()


def test_transforms_covers_every_data_class() -> None:
    """Every ``DataClass`` value must have a ``_TRANSFORMS`` entry.

    Without this guard, adding a new ``DataClass`` to ``taxonomy.py`` would
    silently fall through to ``_TRANSFORMS.get(meta, _passthrough)`` — a
    future CRITICAL class would pass through unredacted with no failure.
    The redaction-module docstring promises "the unit tests will fail
    otherwise"; this test makes the promise enforceable.
    """
    from moneybin.privacy.redaction import (  # noqa: PLC0415
        _TRANSFORMS,  # pyright: ignore[reportPrivateUsage]
    )

    missing = set(DataClass) - set(_TRANSFORMS)
    assert not missing, (
        f"DataClass values missing from _TRANSFORMS: {sorted(m.name for m in missing)}"
    )


def test_every_masking_transform_returns_text_whatever_it_is_given() -> None:
    """The one fact ``apply_export_redaction`` retypes a masked column on.

    A redacted export declares ``VARCHAR`` for every masked column, because every
    masking transform today answers with a string. That is a property of
    ``_TRANSFORMS``, not a second list beside it — the same reason
    ``mask_strength`` measures rather than restates. PR 3's amount bucketing is
    the live case: a bucket returned as a ``Decimal`` or a range tuple would keep
    masking correctly while making ``VARCHAR`` a lie, and the export would fail on
    the typed channel only. This turns that into a red test at the transform.
    """
    from moneybin.privacy.redaction import (  # noqa: PLC0415
        _TRANSFORMS,  # pyright: ignore[reportPrivateUsage]
    )

    probes: tuple[object, ...] = (
        "1234567890",
        4,
        Decimal("40.21"),
        True,
        b"4021",
        date(2026, 7, 28),
        ("4", "0"),
    )
    offenders = {
        data_class.name: type(masked).__name__
        for data_class in _TRANSFORMS
        if mask_strength(data_class) is not MaskStrength.PASSTHROUGH
        for probe in probes
        if not isinstance(masked := _TRANSFORMS[data_class](probe, None), str)
    }

    assert not offenders, (
        "a masked column is exported as VARCHAR, so every masking transform must "
        f"return text; these returned something else: {offenders}"
    )
    # None survives as None — a masked column stays nullable, and VARCHAR is.
    assert all(
        _TRANSFORMS[data_class](None, None) is None
        for data_class in _TRANSFORMS
        if mask_strength(data_class) is not MaskStrength.PASSTHROUGH
    )


def test_unclassified_type_passes_through_with_warning(
    caplog: pytest.LogCaptureFixture,
) -> None:
    # A bare class without Annotated metadata — redact_typed should
    # warn (PrivacyContractError handling happens at the @mcp_tool
    # registration boundary, not inside the per-call redactor) and
    # return the value unchanged.
    @dataclass(frozen=True)
    class _Untyped:
        x: str

    with caplog.at_level("WARNING", logger="moneybin.privacy.redaction"):
        out = redact_typed(_Untyped(x="raw"), consent=None)
    assert out.x == "raw"
