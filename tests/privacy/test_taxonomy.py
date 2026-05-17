"""Tests for the DataClass taxonomy and tier mapping."""

from moneybin.privacy.taxonomy import DataClass, Tier


def test_tier_is_ordered_low_to_critical() -> None:
    assert Tier.LOW < Tier.MEDIUM < Tier.HIGH < Tier.CRITICAL
    assert int(Tier.LOW) == 1
    assert int(Tier.CRITICAL) == 4


def test_every_dataclass_has_a_tier() -> None:
    for member in DataClass:
        assert isinstance(member.tier, Tier), f"{member.name} is missing a Tier mapping"


def test_account_identifier_is_critical() -> None:
    assert DataClass.ACCOUNT_IDENTIFIER.tier is Tier.CRITICAL


def test_record_id_is_low() -> None:
    assert DataClass.RECORD_ID.tier is Tier.LOW


def test_dataclass_values_are_lowercase_snake() -> None:
    for member in DataClass:
        assert member.value == member.name.lower(), (
            f"{member.name} value {member.value!r} must be lowercase of name"
        )
