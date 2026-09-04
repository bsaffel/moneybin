"""The shared classified-envelope builder and its classification primitive.

These tests pin the privacy properties the builder replaces at every call
site: the tier it declares, the data classes it reports, and the redaction
it applies. A builder that under-declares a tier, drops a class, or skips a
transform relative to the inline pipeline it supersedes is a regression on a
security-relevant path, so the parity tests below reproduce that pipeline
verbatim and assert equality.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Annotated, Any

import pytest

from moneybin.privacy.classified_envelope import (
    build_classified_envelope,
    classify,
    tier_sensitivity,
)
from moneybin.privacy.introspection import (
    PrivacyContractError,
    extract_data_classes,
)
from moneybin.privacy.redaction import redact_typed
from moneybin.privacy.sensitivity import tier_to_sensitivity
from moneybin.privacy.taxonomy import DataClass, Tier
from moneybin.protocol.envelope import ResponseEnvelope, build_envelope


@dataclass(frozen=True)
class _AccountRow:
    """CRITICAL payload — carries an account identifier that must be masked."""

    account_id: Annotated[str, DataClass.ACCOUNT_IDENTIFIER]
    institution: Annotated[str, DataClass.INSTITUTION]


@dataclass(frozen=True)
class _AccountView:
    rows: list[_AccountRow]
    total: Annotated[int, DataClass.AGGREGATE]


@dataclass(frozen=True)
class _CategoryView:
    """LOW payload — nothing here has an active redaction transform."""

    category: Annotated[str, DataClass.CATEGORY]
    total: Annotated[int, DataClass.AGGREGATE]


@dataclass(frozen=True)
class _MerchantView:
    """MEDIUM payload."""

    merchant: Annotated[str, DataClass.MERCHANT_NAME]


class _Unclassified:
    """Intentionally lacks Annotated metadata — a contract violation."""

    x: str


def _legacy_envelope(
    data: Any,
    contract_types: list[type[Any]],
    **kwargs: Any,
) -> ResponseEnvelope[Any]:
    """The inline pipeline the builder replaces, reproduced verbatim.

    Copied from the call sites this change deletes (``_review_envelope``,
    ``_account_read_envelope``, ``_coarse_envelope``, …) so the parity tests
    compare against what actually shipped, not against a paraphrase.
    """
    classes = {
        data_class
        for contract_type in contract_types
        for data_class in extract_data_classes(contract_type)
    }
    tier = max(data_class.tier for data_class in classes)
    return build_envelope(
        data=redact_typed(data, None),
        sensitivity=tier_to_sensitivity(tier).value,  # pyright: ignore[reportArgumentType]
        classes_returned=sorted(data_class.value for data_class in classes),
        **kwargs,
    )


# --- classify() -----------------------------------------------------------


def test_classify_unions_classes_across_contract_types() -> None:
    result = classify(_CategoryView, _MerchantView)

    assert result.classes == frozenset({
        DataClass.CATEGORY,
        DataClass.AGGREGATE,
        DataClass.MERCHANT_NAME,
    })


def test_classify_tier_is_the_max_across_the_union() -> None:
    assert classify(_CategoryView, _AccountView).tier is Tier.CRITICAL


def test_classify_defaults_to_the_single_contract_type() -> None:
    assert classify(_CategoryView).tier is Tier.LOW


def test_classes_returned_is_sorted_data_class_values() -> None:
    assert classify(_AccountRow).classes_returned == [
        "account_identifier",
        "institution",
    ]


def test_sensitivity_agrees_with_the_mcp_tier_mapping() -> None:
    """The builder derives the sensitivity string without importing mcp.

    ``moneybin.privacy`` cannot import ``moneybin.privacy.sensitivity`` (that is the
    dependency direction MB-49 removes), so the builder spells the mapping
    itself. This pins the two spellings together.
    """
    for tier in Tier:
        assert tier_sensitivity(tier) == tier_to_sensitivity(tier).value


def test_classes_returned_is_empty_for_an_unclassified_type() -> None:
    """Reporting no classes is honest; claiming a tier for them is not."""
    assert classify(_Unclassified).classes_returned == []


def test_tier_raises_for_an_unclassified_type() -> None:
    with pytest.raises(PrivacyContractError):
        _ = classify(_Unclassified).tier


def test_sensitivity_raises_for_an_unclassified_type() -> None:
    with pytest.raises(PrivacyContractError):
        _ = classify(_Unclassified).sensitivity


# --- build_classified_envelope(): privacy properties ----------------------


def test_masks_a_critical_field_in_the_payload() -> None:
    envelope = build_classified_envelope(
        _AccountRow(account_id="123456789012", institution="Example Institution")
    )

    assert envelope.data.account_id == "****9012"


def test_masks_critical_fields_nested_in_a_list() -> None:
    view = _AccountView(
        rows=[_AccountRow(account_id="987654321098", institution="X")], total=1
    )

    envelope = build_classified_envelope(view)

    assert envelope.data.rows[0].account_id == "****1098"


def test_declares_the_max_tier_as_summary_sensitivity() -> None:
    envelope = build_classified_envelope(
        _AccountRow(account_id="123456789012", institution="X")
    )

    assert envelope.summary.sensitivity == "critical"


def test_reports_every_class_reachable_from_the_payload() -> None:
    view = _AccountView(
        rows=[_AccountRow(account_id="123456789012", institution="X")], total=1
    )

    envelope = build_classified_envelope(view)

    assert envelope.classes_returned == [
        "account_identifier",
        "aggregate",
        "institution",
    ]


def test_union_contract_declares_the_critical_arm_the_instance_omits() -> None:
    """A coarse tool whose declared union spans tiers must report the max.

    The runtime instance is the LOW arm; the contract still admits the
    CRITICAL one, and the tool's declared sensitivity is what an agent
    trusts before it sees the payload.
    """
    envelope = build_classified_envelope(
        _CategoryView(category="groceries", total=3),
        contract_type=[_CategoryView, _AccountView],
    )

    assert envelope.summary.sensitivity == "critical"
    assert "account_identifier" in (envelope.classes_returned or [])


def test_refuses_an_unclassified_payload_rather_than_shipping_it_untiered() -> None:
    """Fail closed: no envelope at all beats an envelope claiming ``low``."""
    with pytest.raises(PrivacyContractError):
        build_classified_envelope(_Unclassified())


# --- build_classified_envelope(): parity with the inline pipeline ---------


def test_parity_with_the_inline_pipeline_for_a_single_contract_type() -> None:
    data = _AccountView(
        rows=[_AccountRow(account_id="123456789012", institution="X")], total=1
    )

    assert build_classified_envelope(data).to_dict() == (
        _legacy_envelope(data, [type(data)]).to_dict()
    )


def test_parity_with_the_inline_pipeline_for_an_explicit_contract_type() -> None:
    data = _CategoryView(category="groceries", total=3)

    built = build_classified_envelope(data, contract_type=_CategoryView)

    assert built.to_dict() == _legacy_envelope(data, [_CategoryView]).to_dict()
    assert (
        built.classes_returned
        == _legacy_envelope(data, [_CategoryView]).classes_returned
    )


def test_parity_with_the_inline_pipeline_for_a_multi_type_union() -> None:
    data = _CategoryView(category="groceries", total=3)
    types = [_CategoryView, _MerchantView]

    built = build_classified_envelope(data, contract_type=types)

    assert built.to_dict() == _legacy_envelope(data, types).to_dict()
    assert built.classes_returned == _legacy_envelope(data, types).classes_returned


# --- build_classified_envelope(): metadata passthrough --------------------


def test_forwards_count_and_pagination_metadata() -> None:
    envelope = build_classified_envelope(
        _CategoryView(category="groceries", total=3),
        total_count=50,
        returned_count=1,
        next_cursor="opaque",
        period="2026-01 to 2026-04",
    )

    assert envelope.summary.total_count == 50
    assert envelope.summary.returned_count == 1
    assert envelope.next_cursor == "opaque"
    assert envelope.summary.period == "2026-01 to 2026-04"


def test_forwards_degraded_actions_and_display_currency() -> None:
    envelope = build_classified_envelope(
        _CategoryView(category="groceries", total=3),
        actions=["do the next thing"],
        degraded=True,
        degraded_reason="aggregates only",
        display_currency="USD",
    )

    assert envelope.actions == ["do the next thing"]
    assert envelope.summary.degraded is True
    assert envelope.summary.degraded_reason == "aggregates only"
    assert envelope.summary.display_currency == "USD"


def test_has_more_override_forces_a_last_page_without_a_cursor() -> None:
    """Paginated sites treat "no cursor" as "no more", not "total > returned"."""
    envelope = build_classified_envelope(
        _CategoryView(category="groceries", total=3),
        total_count=50,
        returned_count=1,
        next_cursor=None,
        has_more=False,
    )

    assert envelope.summary.has_more is False


def test_omitting_has_more_keeps_the_count_derived_default() -> None:
    envelope = build_classified_envelope(
        _CategoryView(category="groceries", total=3),
        total_count=50,
        returned_count=1,
    )

    assert envelope.summary.has_more is True
