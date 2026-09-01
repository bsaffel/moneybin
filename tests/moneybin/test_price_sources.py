"""The price-source registry — the one declaration every dispatch site reads.

Assertions here are deliberately *literal*, which is the opposite of how the
rest of the price tests are written. Everywhere else, restating the vocabulary
would drift the moment someone edited the model; here the registry IS the
declaration, so a test that re-derives from it asserts nothing. This file is
where the shipped set is pinned — above all the rank order, which
``docs/specs/investments-price-feeds.md`` forbids reordering because it
silently revalues every historical holding.
"""

from __future__ import annotations

import csv
import re
from dataclasses import replace

import pytest

from moneybin.price_sources import (
    FEED_KEY_REF_KINDS,
    FEED_KEY_ROLE,
    PRICE_SOURCES,
    REF_KIND_BY_SOURCE_TYPE,
    REGISTRY_CSV,
    SEED_MODEL_SQL,
    feed_key_ref_kinds,
    price_source,
    source_for_security_type,
)
from tests.moneybin.price_model_helpers import FCT_PRICES_PATH


def test_an_unknown_source_type_fails_loudly_instead_of_falling_through() -> None:
    """The whole point of the registry: no silent default for an unknown source.

    ``_adapter_for`` used to route every non-CoinGecko source to Tiingo, so a
    typo'd or unregistered source_type fetched the wrong provider's series
    rather than reporting anything.
    """
    with pytest.raises(KeyError, match="yahoo"):
        price_source("yahoo")


def test_the_shipped_rank_order_is_pinned() -> None:
    """Append ranks; never reorder them.

    Inserting a provider ahead of an incumbent changes which close wins on
    every historical date where both hold a row, silently revaluing
    ``core.dim_holdings.market_value``. A new source appends a row here; a
    changed row means someone reordered, which is a deliberate announced
    revaluation and not a refactor.
    """
    assert [(s.source_type, s.source_rank) for s in PRICE_SOURCES] == [
        ("override", 1),
        ("plaid", 2),
        ("tiingo", 3),
        ("coingecko", 4),
        ("trade_implied", 5),
    ]


def test_a_ref_kind_marks_the_sources_that_resolve_through_security_links() -> None:
    """Exactly the sources whose rows land in ``raw.security_prices``.

    ``override`` and ``trade_implied`` are derived at model build and never
    pass through ``app.security_links``, so they carry no ref_kind — which is
    what lets ``core.fct_security_prices`` scope its same-pull withhold to the
    provider sources without enumerating them.
    """
    assert REF_KIND_BY_SOURCE_TYPE == {
        "plaid": "plaid_security_id",
        "tiingo": "tiingo_ticker",
        "coingecko": "coingecko_slug",
    }


def test_feed_key_ref_kinds_are_the_sources_price_service_derives_a_key_for() -> None:
    """``plaid`` resolves through links but is written by the extractor.

    ``SecurityLinksService.accept`` routes on this set: a ref_kind inside it
    BINDS a feed, one outside MERGES two securities and deletes one. A
    ``plaid_security_id`` decision is an identity ref and must stay outside.
    """
    assert FEED_KEY_REF_KINDS == frozenset({"tiingo_ticker", "coingecko_slug"})


def test_a_security_type_routes_to_the_source_that_declares_it() -> None:
    crypto = source_for_security_type("crypto")
    equity = source_for_security_type("equity")
    assert crypto is not None
    assert equity is not None
    assert crypto.source_type == "coingecko"
    assert equity.source_type == "tiingo"


def test_a_security_type_no_source_declares_routes_nowhere() -> None:
    """Cash and other carry no market quote; a sweep position's unit value is face."""
    assert source_for_security_type("cash") is None
    assert source_for_security_type("other") is None


def test_no_two_sources_claim_the_same_security_type() -> None:
    """Routing must be a function, not a first-match-wins scan."""
    claimed: dict[str, str] = {}
    for source in PRICE_SOURCES:
        for security_type in source.security_types:
            assert security_type not in claimed, (
                f"{security_type!r} is claimed by both {claimed[security_type]!r} "
                f"and {source.source_type!r}; routing would depend on row order"
            )
            claimed[security_type] = source.source_type


def test_a_source_price_service_fetches_declares_a_ref_kind() -> None:
    """A routed source with no ref_kind writes rows staging discards forever."""
    for source in PRICE_SOURCES:
        if source.security_types:
            assert source.ref_kind is not None, (
                f"{source.source_type!r} is routed by security type but declares "
                "no ref_kind, so every row it writes is dropped by "
                "prep.stg_security_prices' INNER JOIN"
            )


def test_the_shipped_ref_roles_are_pinned() -> None:
    """What accepting a ref DOES is permanent; retirement must not change it.

    ``ref_role`` is a separate column precisely so it cannot be derived from
    ``security_types``, which empties when a provider is retired. Deriving it
    would reclassify a retired provider's ref_kind from a feed key to an
    identity ref, sending its still-pending review decisions into the merge
    path — which re-points every reference and deletes a security.
    """
    assert {s.source_type: s.ref_role for s in PRICE_SOURCES} == {
        "override": None,
        "plaid": "identity",
        "tiingo": FEED_KEY_ROLE,
        "coingecko": FEED_KEY_ROLE,
        "trade_implied": None,
    }


def test_retiring_a_provider_cannot_reclassify_its_ref_kind() -> None:
    """Clearing security_types must leave the feed-key set untouched.

    The regression this pins is a derivation, not a value: FEED_KEY_REF_KINDS
    once read ``if source.security_types``, which made the documented
    retirement procedure — clear security_types, never delete the row — flip
    the ref_kind's routing as a side effect.
    """
    retired = [
        replace(source, security_types=frozenset())
        if source.ref_role == FEED_KEY_ROLE
        else source
        for source in PRICE_SOURCES
    ]
    assert not any(source.security_types for source in retired), (
        "the fixture must actually retire every feed source, or it pins nothing"
    )

    assert feed_key_ref_kinds(retired) == FEED_KEY_REF_KINDS, (
        "retiring every provider changed the feed-key set, so the derivation is "
        "reading security_types again; a retired provider's open review decisions "
        "would route to the merge path, which deletes a security"
    )


def test_a_source_declaring_a_ref_role_declares_the_ref_kind_it_roles() -> None:
    for source in PRICE_SOURCES:
        assert (source.ref_role is None) == (source.ref_kind is None), (
            f"{source.source_type!r} declares ref_role={source.ref_role!r} and "
            f"ref_kind={source.ref_kind!r}; a role with no ref to apply it to "
            "routes nothing, and a ref with no role routes to the merge path"
        )


def test_every_source_the_price_fact_can_emit_is_registered() -> None:
    """The two derived branches name their source_type as a SQL literal.

    ``override`` and ``trade_implied`` are computed in ``core.fct_security_prices``
    rather than read from staging, so nothing forces them through the registry
    the way a provider row is forced. Deleting either registry row would send
    its close to the ELSE-99 bucket and let a provider outrank a user's own
    mark, which is the one ordering the model exists to guarantee.
    """
    emitted = set(re.findall(r"'(\w+)' AS source_type", FCT_PRICES_PATH.read_text()))
    assert emitted, f"no `'<source>' AS source_type` literals in {FCT_PRICES_PATH.name}"
    registered = {source.source_type for source in PRICE_SOURCES}
    assert emitted <= registered, (
        f"{sorted(emitted - registered)} win dates in core.fct_security_prices "
        f"but carry no rank in {REGISTRY_CSV.name}, so they resolve at rank 99 "
        "and lose to every registered source"
    )


def test_the_seed_model_loads_the_file_the_registry_loads() -> None:
    """One file, two readers — SQLMesh materializes it, Python imports it.

    A seed pointed at a second CSV would restore exactly the split this
    registry exists to remove, and nothing else would notice.
    """
    path = re.search(r"path\s+'([^']+)'", SEED_MODEL_SQL.read_text())
    assert path is not None, f"no `path '<file>.csv'` in {SEED_MODEL_SQL.name}"
    assert path.group(1) == REGISTRY_CSV.name


def test_the_seed_declares_every_column_the_registry_csv_carries() -> None:
    """A column absent from the MODEL header is dropped from the seeded table."""
    columns_block = re.search(
        r"columns\s*\((.*?)\)", SEED_MODEL_SQL.read_text(), re.DOTALL
    )
    assert columns_block is not None, f"no `columns (...)` in {SEED_MODEL_SQL.name}"
    declared = re.findall(r"(\w+)\s+\w+", columns_block.group(1))

    with REGISTRY_CSV.open(newline="", encoding="utf-8") as handle:
        header = next(csv.reader(handle))

    assert declared == header
