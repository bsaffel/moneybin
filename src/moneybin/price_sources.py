"""The price-source registry, loaded from the seed SQLMesh materializes.

One declaration — ``sqlmesh/models/seeds/price_source_map.csv`` — read two ways.
``prep.stg_security_prices`` and ``core.fct_security_prices`` join the seeded
table; everything Python dispatches on reads this module. The CSV is parsed
here rather than queried from ``seeds.price_source_map`` because adapter
routing has to work before any transform has ever run, and because a registry
that could disagree with itself between the two readers would restore the split
it exists to remove.

Why the CSV is canonical and not a Python literal: the SQL models cannot import
Python, and the failure this registry ends is a source declared on one side and
missing from the other. See the seed model's header for what each column means
and for the two rules — append ranks, never delete a row — that outlive any one
adapter.
"""

from __future__ import annotations

import csv
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Final

_SEEDS_DIR: Final = Path(__file__).parent / "sqlmesh" / "models" / "seeds"

REGISTRY_CSV: Final = _SEEDS_DIR / "price_source_map.csv"
SEED_MODEL_SQL: Final = _SEEDS_DIR / "price_source_map.sql"

_SECURITY_TYPE_SEPARATOR: Final = "|"


FEED_KEY_ROLE: Final = "feed_key"


@dataclass(frozen=True, slots=True)
class PriceSource:
    """One source a resolved close can carry."""

    source_type: str
    source_rank: int
    ref_kind: str | None
    ref_role: str | None
    security_types: frozenset[str]

    @property
    def feed_ref_kind(self) -> str:
        """The ref_kind, for a source that resolves through ``app.security_links``.

        Narrows the optional at the call sites that only ever hold a provider
        source, so a derived source reaching one fails here instead of writing
        a NULL ref_kind into a binding no join can match.
        """
        if self.ref_kind is None:
            raise ValueError(
                f"price source {self.source_type!r} declares no ref_kind; it is "
                "derived at model build and never resolves through app.security_links"
            )
        return self.ref_kind


def _load() -> tuple[PriceSource, ...]:
    with REGISTRY_CSV.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    return tuple(
        PriceSource(
            source_type=row["source_type"],
            source_rank=int(row["source_rank"]),
            ref_kind=row["ref_kind"] or None,
            ref_role=row["ref_role"] or None,
            security_types=frozenset(
                filter(None, row["security_types"].split(_SECURITY_TYPE_SEPARATOR))
            ),
        )
        for row in sorted(rows, key=lambda r: int(r["source_rank"]))
    )


PRICE_SOURCES: Final[tuple[PriceSource, ...]] = _load()

_BY_SOURCE_TYPE: Final = {source.source_type: source for source in PRICE_SOURCES}

# The sources whose rows land in ``raw.security_prices`` and resolve through an
# accepted binding. Mirrors the join ``prep.stg_security_prices`` performs.
REF_KIND_BY_SOURCE_TYPE: Final[dict[str, str]] = {
    source.source_type: source.ref_kind
    for source in PRICE_SOURCES
    if source.ref_kind is not None
}


def feed_key_ref_kinds(sources: Iterable[PriceSource]) -> frozenset[str]:
    """Refs that name a market-data symbol rather than a second catalog row.

    Accepting one BINDS the feed; accepting an identity ref MERGES two securities
    and deletes one — opposite operations behind one reviewer intent.

    Keyed on ref_role, never on security_types. Retiring a provider empties its
    security_types, so deriving this set from that column would silently
    reclassify the retired provider's ref_kind as an identity ref and route its
    still-pending review decisions into the merge path.

    Takes its sources as an argument so that rule can be tested by applying it to
    a retired registry. Reading the module constant instead would only ever see
    the unretired rows, which is how the first attempt at that guard came to pass
    under the very derivation it was written to reject.
    """
    return frozenset(
        source.feed_ref_kind for source in sources if source.ref_role == FEED_KEY_ROLE
    )


FEED_KEY_REF_KINDS: Final[frozenset[str]] = feed_key_ref_kinds(PRICE_SOURCES)

_BY_SECURITY_TYPE: Final = {
    security_type: source
    for source in PRICE_SOURCES
    for security_type in source.security_types
}


def price_source(source_type: str) -> PriceSource:
    """The registry row for *source_type*, or ``KeyError``.

    Deliberately unforgiving. The dispatch this replaced routed every
    non-CoinGecko source to Tiingo, so an unregistered source_type silently
    fetched another provider's series instead of reporting anything.
    """
    try:
        return _BY_SOURCE_TYPE[source_type]
    except KeyError:
        raise KeyError(
            f"{source_type!r} is not a registered price source; known sources are "
            f"{sorted(_BY_SOURCE_TYPE)}. Add a row to {REGISTRY_CSV.name}."
        ) from None


def source_for_security_type(security_type: str) -> PriceSource | None:
    """The source that prices *security_type*, or ``None`` if none does.

    ``None`` is an ordinary answer, not a failure: cash and other carry no
    market quote at all, because a sweep position's unit value is its face
    value rather than a traded price.
    """
    return _BY_SECURITY_TYPE.get(security_type)
