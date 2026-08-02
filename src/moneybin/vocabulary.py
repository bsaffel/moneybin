"""Dependency-neutral public domain vocabularies."""

from typing import Literal, get_args

CategorizationMatchType = Literal["exact", "contains", "regex"]
ConsentFeatureCategory = Literal[
    "mcp-data-sharing",
    "smart-import-parsing",
    "ml-categorization",
    "matching-overview",
]
# What a provider documented itself as returning. Declared by the adapter, never
# inferred from the data: comparing close ratios across a known split date yields
# a guess that flips silently when a provider changes policy. Only 'raw' is
# eligible to value a holding, because an adjusted series stops being correctly
# adjusted after the next corporate action. Mirrored by the CHECK on
# raw.security_prices.price_basis, which test_protocol.py asserts against.
PriceBasis = Literal["raw", "split_adjusted", "split_and_dividend_adjusted"]

CATEGORIZATION_MATCH_TYPES: frozenset[CategorizationMatchType] = frozenset(
    get_args(CategorizationMatchType)
)
CONSENT_FEATURE_CATEGORIES: frozenset[ConsentFeatureCategory] = frozenset(
    get_args(ConsentFeatureCategory)
)
PRICE_BASES: frozenset[PriceBasis] = frozenset(get_args(PriceBasis))
