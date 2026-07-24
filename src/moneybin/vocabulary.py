"""Dependency-neutral public domain vocabularies."""

from typing import Literal, get_args

CategorizationMatchType = Literal["exact", "contains", "regex"]
ConsentFeatureCategory = Literal[
    "mcp-data-sharing",
    "smart-import-parsing",
    "ml-categorization",
    "matching-overview",
]

CATEGORIZATION_MATCH_TYPES: frozenset[CategorizationMatchType] = frozenset(
    get_args(CategorizationMatchType)
)
CONSENT_FEATURE_CATEGORIES: frozenset[ConsentFeatureCategory] = frozenset(
    get_args(ConsentFeatureCategory)
)
