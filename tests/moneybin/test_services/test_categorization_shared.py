"""Tests for CategorizedBy and SOURCE_PRIORITY constants."""

from decimal import Decimal

import pytest

from moneybin.services.categorization._shared import (
    PLAID_MIN_CONFIDENCE,
    SOURCE_PRIORITY,
    canonical_matcher_key,
    matcher_digest,
    plaid_confidence_to_numeric,
    priority_case_sql,
)


def test_provider_native_replaces_plaid_at_priority_6():
    assert "plaid" not in SOURCE_PRIORITY
    assert SOURCE_PRIORITY["provider_native"] == 6
    # ladder order preserved around it
    assert (
        SOURCE_PRIORITY["ml"]
        < SOURCE_PRIORITY["provider_native"]
        < SOURCE_PRIORITY["ai"]
    )


def test_priority_case_sql_uses_provider_native():
    sql = priority_case_sql("EXCLUDED.categorized_by")
    assert "WHEN 'provider_native' THEN 6" in sql
    assert "'plaid'" not in sql


@pytest.mark.parametrize(
    "level,expected",
    [
        ("VERY_HIGH", 0.99),
        ("HIGH", 0.90),
        ("MEDIUM", 0.70),
        ("LOW", 0.40),
        ("UNKNOWN", None),
        (None, None),
        ("garbage", None),
    ],
)
def test_plaid_confidence_to_numeric(level: str | None, expected: float | None) -> None:
    assert plaid_confidence_to_numeric(level) == expected


def test_gate_admits_medium_and_above() -> None:
    for lvl in ("VERY_HIGH", "HIGH", "MEDIUM"):
        val = plaid_confidence_to_numeric(lvl)
        assert val is not None and val >= PLAID_MIN_CONFIDENCE
    for lvl in ("LOW", "UNKNOWN"):
        val = plaid_confidence_to_numeric(lvl)
        assert val is None or val < PLAID_MIN_CONFIDENCE


# --- canonical_matcher_key -------------------------------------------------


class TestCanonicalMatcherKey:
    """One matcher identity, shared by rule creation and target-state resolution."""

    @pytest.mark.unit
    def test_case_and_surrounding_whitespace_normalize_to_one_key(self) -> None:
        assert canonical_matcher_key(
            merchant_pattern="  Coffee Shop ",
            match_type="contains",
        ) == canonical_matcher_key(
            merchant_pattern="COFFEE SHOP",
            match_type="contains",
        )

    @pytest.mark.unit
    def test_inner_whitespace_is_significant(self) -> None:
        """`contains` matches the pattern literally, so two spaces are not one."""
        assert canonical_matcher_key(
            merchant_pattern="COFFEE  SHOP", match_type="contains"
        ) != canonical_matcher_key(
            merchant_pattern="COFFEE SHOP", match_type="contains"
        )

    @pytest.mark.unit
    def test_regex_case_is_preserved(self) -> None:
        """Casefolding a regex would turn the non-digit class into the digit class."""
        assert canonical_matcher_key(
            merchant_pattern=r"\\D+", match_type="regex"
        ) != canonical_matcher_key(merchant_pattern=r"\\d+", match_type="regex")

    @pytest.mark.unit
    def test_match_type_is_part_of_the_key(self) -> None:
        assert canonical_matcher_key(
            merchant_pattern="COFFEE SHOP", match_type="contains"
        ) != canonical_matcher_key(merchant_pattern="COFFEE SHOP", match_type="exact")

    @pytest.mark.unit
    def test_amount_bounds_normalize_across_numeric_types(self) -> None:
        assert canonical_matcher_key(
            merchant_pattern="COFFEE SHOP",
            match_type="contains",
            min_amount=5,
            max_amount=50.0,
        ) == canonical_matcher_key(
            merchant_pattern="COFFEE SHOP",
            match_type="contains",
            min_amount=Decimal("5.00"),
            max_amount=Decimal("50.000"),
        )

    @pytest.mark.unit
    def test_null_bounds_and_account_are_distinct_from_set_ones(self) -> None:
        unbounded = canonical_matcher_key(
            merchant_pattern="COFFEE SHOP", match_type="contains"
        )
        bounded = canonical_matcher_key(
            merchant_pattern="COFFEE SHOP", match_type="contains", min_amount=0
        )
        scoped = canonical_matcher_key(
            merchant_pattern="COFFEE SHOP",
            match_type="contains",
            account_id="acct_11112222",
        )
        assert unbounded != bounded
        assert unbounded != scoped

    @pytest.mark.unit
    def test_account_id_is_matched_exactly(self) -> None:
        assert canonical_matcher_key(
            merchant_pattern="COFFEE SHOP",
            match_type="contains",
            account_id="acct_11112222",
        ) != canonical_matcher_key(
            merchant_pattern="COFFEE SHOP",
            match_type="contains",
            account_id="ACCT_11112222",
        )

    @pytest.mark.unit
    def test_digest_is_stable_and_key_specific(self) -> None:
        key = canonical_matcher_key(
            merchant_pattern="Coffee Shop", match_type="contains"
        )
        other = canonical_matcher_key(
            merchant_pattern="Coffee Shoppe", match_type="contains"
        )
        assert matcher_digest(key) == matcher_digest(key)
        assert matcher_digest(key) != matcher_digest(other)
        assert len(matcher_digest(key)) == 64
