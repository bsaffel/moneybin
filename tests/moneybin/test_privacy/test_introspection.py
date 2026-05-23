"""Type-hint walking for privacy classification."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Annotated, TypedDict

import pytest

from moneybin.privacy.introspection import (
    PrivacyContractError,
    derive_tier,
    extract_data_classes,
)
from moneybin.privacy.taxonomy import DataClass, Tier


@dataclass(frozen=True)
class _Flat:
    account_id: Annotated[str, DataClass.ACCOUNT_IDENTIFIER]
    amount: Annotated[float, DataClass.TXN_AMOUNT]
    category: Annotated[str, DataClass.CATEGORY]


@dataclass(frozen=True)
class _Nested:
    rows: list[_Flat]
    total: Annotated[float, DataClass.AGGREGATE]


@dataclass(frozen=True)
class _DeepOptional:
    payload: _Nested | None
    extras: dict[str, Annotated[str, DataClass.RECORD_ID]]


class _AggregateTD(TypedDict):
    total: Annotated[float, DataClass.AGGREGATE]
    count: Annotated[int, DataClass.AGGREGATE]


class _Unclassified:
    """Intentionally lacks Annotated metadata."""

    x: str
    y: int


def test_extracts_flat_dataclass_fields() -> None:
    classes = extract_data_classes(_Flat)
    assert classes == {
        DataClass.ACCOUNT_IDENTIFIER,
        DataClass.TXN_AMOUNT,
        DataClass.CATEGORY,
    }


def test_recurses_into_list_of_nested_dataclass() -> None:
    classes = extract_data_classes(_Nested)
    assert DataClass.ACCOUNT_IDENTIFIER in classes
    assert DataClass.TXN_AMOUNT in classes
    assert DataClass.CATEGORY in classes
    assert DataClass.AGGREGATE in classes


def test_handles_optional_and_dict_value_types() -> None:
    classes = extract_data_classes(_DeepOptional)
    # From _Nested → _Flat fields
    assert DataClass.ACCOUNT_IDENTIFIER in classes
    # From dict[str, Annotated[..., RECORD_ID]]
    assert DataClass.RECORD_ID in classes


def test_typeddict_fields_with_annotated_values() -> None:
    classes = extract_data_classes(_AggregateTD)
    assert classes == {DataClass.AGGREGATE}


def test_derive_tier_returns_max() -> None:
    # _Flat has ACCOUNT_IDENTIFIER (CRITICAL), TXN_AMOUNT (HIGH), CATEGORY (LOW)
    assert derive_tier(_Flat) == Tier.CRITICAL
    # _AggregateTD only has AGGREGATE (LOW)
    assert derive_tier(_AggregateTD) == Tier.LOW


def test_unclassified_type_raises_privacy_contract_error() -> None:
    with pytest.raises(PrivacyContractError, match="_Unclassified"):
        derive_tier(_Unclassified)


def test_empty_dataclass_raises_privacy_contract_error() -> None:
    @dataclass(frozen=True)
    class _Empty:
        pass

    with pytest.raises(PrivacyContractError):
        derive_tier(_Empty)


def test_derive_tier_is_cached() -> None:
    # Same type returns the same Tier without re-walking; verify by
    # monkey-patching extract_data_classes after the first call and
    # confirming the second call doesn't invoke it.
    from moneybin.privacy import introspection

    sentinel = object()
    first = derive_tier(_Flat)
    original = introspection.extract_data_classes

    def _boom(_t: type) -> set[DataClass]:
        raise AssertionError("cache should have prevented this call")

    introspection.extract_data_classes = _boom  # type: ignore[assignment]
    try:
        second = derive_tier(_Flat)
    finally:
        introspection.extract_data_classes = original  # type: ignore[assignment]
    assert first == second
    assert sentinel is sentinel  # silence unused-var lint
