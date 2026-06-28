"""Tests for MerchantResolver (M1T adopt-or-mint ladder)."""

from __future__ import annotations

import pytest

from moneybin.database import Database
from moneybin.services.audit_service import AuditService
from moneybin.services.categorization.applier import MatchApplier
from moneybin.services.merchant_resolver import MerchantResolution, MerchantResolver


@pytest.fixture()
def applier(db: Database) -> MatchApplier:
    """MatchApplier bound to the test database."""
    return MatchApplier(db, audit=AuditService(db))


def test_rung1_adopts_bound_id(db: Database, applier: MatchApplier) -> None:
    r = MerchantResolver(db)
    r._links.insert(  # pyright: ignore[reportPrivateUsage]
        link_id="lk1",
        merchant_id="mBound",
        ref_kind="merchant_entity_id",
        ref_value="ent1",
        source_type="plaid",
        decided_by="auto",
        actor="system",
    )
    res = r.resolve(
        merchant_entity_id="ent1",
        source_type="plaid",
        provider_merchant_name="Whatever",
        name_match=None,
        bindings=r.load_bindings(),
        applier=applier,
    )
    assert res == MerchantResolution(
        merchant_id="mBound", outcome="adopted", created=False
    )


def test_rung2_exact_name_match_auto_binds(db: Database, applier: MatchApplier) -> None:
    r = MerchantResolver(db)
    name_match = {"merchant_id": "mExact", "strength": "exact"}
    res = r.resolve(
        merchant_entity_id="ent2",
        source_type="plaid",
        provider_merchant_name="Starbucks",
        name_match=name_match,
        bindings={},
        applier=applier,
    )
    assert res.merchant_id == "mExact" and res.outcome == "auto_bound"
    assert r._links.lookup("plaid", "ent2") == "mExact"  # pyright: ignore[reportPrivateUsage]


def test_rung3_fuzzy_proposes_and_does_not_bind(
    db: Database, applier: MatchApplier
) -> None:
    r = MerchantResolver(db)
    name_match = {"merchant_id": "mFuzzy", "strength": "fuzzy"}
    res = r.resolve(
        merchant_entity_id="ent3",
        source_type="plaid",
        provider_merchant_name="Star Bucks",
        name_match=name_match,
        bindings={},
        applier=applier,
    )
    assert res.merchant_id == "mFuzzy" and res.outcome == "proposed"
    assert r._links.lookup("plaid", "ent3") is None  # pyright: ignore[reportPrivateUsage]
    assert len(r._decisions.list_pending()) == 1  # pyright: ignore[reportPrivateUsage]


def test_rung4_mints_plaid_merchant_and_binds(
    db: Database, applier: MatchApplier
) -> None:
    r = MerchantResolver(db)
    res = r.resolve(
        merchant_entity_id="ent4",
        source_type="plaid",
        provider_merchant_name="New Cafe",
        name_match=None,
        bindings={},
        applier=applier,
    )
    assert res.created and res.outcome == "minted"
    assert r._links.lookup("plaid", "ent4") == res.merchant_id  # pyright: ignore[reportPrivateUsage]
