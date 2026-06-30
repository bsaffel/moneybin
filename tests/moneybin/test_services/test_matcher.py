"""Unit tests for match_merchant_with_name — the DRY rung-2 name-matching helper."""

from moneybin.services.categorization._shared import Merchant
from moneybin.services.categorization.matcher import match_merchant_with_name


def _make_merchant(
    merchant_id: str = "mid1",
    raw_pattern: str | None = "Starbucks",
    match_type: str = "exact",
    canonical_name: str = "Starbucks",
    category: str | None = "Food & Drink",
    subcategory: str | None = None,
    exemplars: list[str] | None = None,
) -> Merchant:
    return Merchant(
        merchant_id=merchant_id,
        raw_pattern=raw_pattern,
        match_type=match_type,
        canonical_name=canonical_name,
        category=category,
        subcategory=subcategory,
        exemplars=exemplars or [],
    )


def test_exact_merchant_name_hit_returns_exact_strength() -> None:
    """Blank description + exact merchant_name match → strength="exact"."""
    merchants = [_make_merchant(merchant_id="mStar", match_type="exact")]
    result = match_merchant_with_name(
        merchants,
        description="",
        memo=None,
        merchant_name="Starbucks",
    )
    assert result is not None, "exact merchant_name match must return a hit"
    assert result["merchant_id"] == "mStar"
    assert result["strength"] == "exact"


def test_fuzzy_merchant_name_hit_returns_fuzzy_strength() -> None:
    """Blank description + contains merchant_name match → strength="fuzzy"."""
    merchants = [
        _make_merchant(
            merchant_id="mFuzz", raw_pattern="starbucks", match_type="contains"
        )
    ]
    result = match_merchant_with_name(
        merchants,
        description="",
        memo=None,
        merchant_name="Starbucks",
    )
    assert result is not None, "fuzzy merchant_name match must return a hit"
    assert result["merchant_id"] == "mFuzz"
    assert result["strength"] == "fuzzy"


def test_exact_description_wins_over_merchant_name() -> None:
    """Exact description match is returned even when merchant_name would match a different entry."""
    merchant_a = _make_merchant(
        merchant_id="mA",
        raw_pattern="FANCY_CORP",
        match_type="exact",
        canonical_name="Fancy Corp",
    )
    merchant_b = _make_merchant(
        merchant_id="mB",
        raw_pattern="OTHERCORP",
        match_type="exact",
        canonical_name="Other Corp",
    )
    merchants = [merchant_a, merchant_b]

    result = match_merchant_with_name(
        merchants,
        description="FANCY_CORP",
        memo=None,
        merchant_name="OTHERCORP",
    )
    assert result is not None
    assert result["merchant_id"] == "mA", (
        "exact description match must win over merchant_name; "
        f"expected mA, got {result['merchant_id']!r}"
    )


def test_no_match_when_merchant_name_matches_nothing() -> None:
    """Blank description + merchant_name matching no catalog entry → None."""
    merchants = [_make_merchant(merchant_id="mStar", match_type="exact")]
    result = match_merchant_with_name(
        merchants,
        description="",
        memo=None,
        merchant_name="SomeUnknownBrand",
    )
    assert result is None, "no catalog match must return None"


def test_none_when_both_description_and_merchant_name_are_none() -> None:
    """No description and no merchant_name → None (nothing to match on)."""
    merchants = [_make_merchant()]
    result = match_merchant_with_name(
        merchants,
        description=None,
        memo=None,
        merchant_name=None,
    )
    assert result is None


def test_exact_name_match_upgrades_fuzzy_desc_match() -> None:
    """An exact merchant_name match upgrades a fuzzy description match (prefer the name hit).

    Merchant A fuzzy-matches the description but does NOT match the merchant_name.
    Merchant B does NOT match the description but exactly matches the merchant_name.
    The helper must prefer merchant B (exact name hit) over merchant A (fuzzy desc hit).
    """
    merchant_a = _make_merchant(
        merchant_id="mFuzzyDesc",
        raw_pattern="fancycorp",
        match_type="contains",
        canonical_name="FancyCorp (desc-only)",
    )
    merchant_b = _make_merchant(
        merchant_id="mExactName",
        raw_pattern="Starbucks",
        match_type="exact",
        canonical_name="Starbucks (name-only)",
    )
    merchants = [merchant_a, merchant_b]

    result = match_merchant_with_name(
        merchants,
        description="FANCYCORP STORE",  # fuzzy-matches merchant_a via contains
        memo=None,
        merchant_name="Starbucks",  # exact-matches merchant_b; does not match merchant_a
    )
    assert result is not None
    assert result["merchant_id"] == "mExactName", (
        "an exact merchant_name hit must win over a fuzzy description hit; "
        f"got {result['merchant_id']!r}"
    )
    assert result["strength"] == "exact"
