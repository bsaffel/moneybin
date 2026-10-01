"""Typed payload dataclasses for the categories-mappings curation surface (MB-180 PR2).

Each field carries ``Annotated[T, DataClass.X]`` metadata so the Phase 6
middleware can derive sensitivity via ``derive_tier`` without inspecting tool
source code directly.

``category``/``subcategory`` here are the *imported* vocabulary text — a
source's own category string, not MoneyBin's resolved taxonomy. Per the
owner's ruling (docs/specs/category-source-map.md), that text is never
displayed as a category; it is an input the curator matches against an
existing category or uses to mint a new one. They are nonetheless declared
``DataClass.CATEGORY`` (Tier.LOW), matching every other ``category`` /
``subcategory`` field in the registry
(``test_annotated_registry_sync.py::test_payload_fields_match_registry``
enforces one ``DataClass`` per column name across the whole schema,
regardless of a given table's provenance) — the semantic distinction above
is a service-layer / UX contract, not a privacy-tier one.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Annotated

from moneybin.privacy.taxonomy import DataClass
from moneybin.protocol.row_set import NO_ROW_SET, row_set

if TYPE_CHECKING:
    from collections.abc import Iterable

    from moneybin.services.categorization.queries import UnmappedSourceTerm

# ---------------------------------------------------------------------------
# categories_mappings_pending
# ---------------------------------------------------------------------------


@row_set(NO_ROW_SET)
@dataclass(frozen=True, slots=True)
class UnmappedSourceTermRow:
    """One imported vocabulary term with no ``app.category_source_map`` row."""

    source_origin: Annotated[str, DataClass.TXN_TYPE]
    category: Annotated[str, DataClass.CATEGORY]
    subcategory: Annotated[str | None, DataClass.CATEGORY]
    transaction_count: Annotated[int, DataClass.AGGREGATE]
    suggestions: Annotated[list[str], DataClass.CATEGORY]

    @classmethod
    def from_domain(cls, term: UnmappedSourceTerm) -> UnmappedSourceTermRow:
        """Map a service ``UnmappedSourceTerm`` into the payload row."""
        return cls(
            source_origin=term.source_origin,
            category=term.category,
            subcategory=term.subcategory,
            transaction_count=term.transaction_count,
            suggestions=list(term.suggestions),
        )


@row_set("terms")
@dataclass(frozen=True, slots=True)
class CategoryMappingsPendingPayload:
    """Payload for ``categories_mappings_pending`` — unmapped terms, biggest first."""

    terms: list[UnmappedSourceTermRow]

    @classmethod
    def from_domain(
        cls, terms: Iterable[UnmappedSourceTerm]
    ) -> CategoryMappingsPendingPayload:
        """Build the pending payload from ``CategorizationService.list_unmapped_source_terms``."""
        return cls(terms=[UnmappedSourceTermRow.from_domain(t) for t in terms])


# ---------------------------------------------------------------------------
# categories_mappings_set
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class CategoryMappingSetPayload:
    """Payload for ``categories_mappings_set`` — mapping confirmation."""

    source_origin: Annotated[str, DataClass.TXN_TYPE]
    category: Annotated[str, DataClass.CATEGORY]
    subcategory: Annotated[str | None, DataClass.CATEGORY]
    category_id: Annotated[str, DataClass.CATEGORY]
    action: Annotated[str, DataClass.TXN_TYPE]
