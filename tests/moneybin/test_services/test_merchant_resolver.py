"""Tests for MerchantResolver (M1T adopt-or-mint ladder)."""

from __future__ import annotations

import pytest

from moneybin.database import Database
from moneybin.services.audit_service import AuditService
from moneybin.services.categorization.applier import MatchApplier
from moneybin.services.merchant_resolver import (
    HarvestResult,
    MerchantResolution,
    MerchantResolver,
)


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


def test_harvest_degrades_when_prep_view_absent(db: Database) -> None:
    """harvest() returns HarvestResult(0, 0) on a never-transformed DB.

    The ``db`` fixture has the app tables (merchant_links, transaction_categories)
    but SQLMesh has not run, so ``prep.int_transactions__merged`` does not exist.
    Without the CatalogException guard the harvest SELECT raises raw — and
    ``merchants_links_run`` (MCP, no ``handle_cli_errors`` wrapper) would surface
    it. The guard must degrade gracefully like ``list_pending``/``count_pending``.
    """
    view_present = db.execute(
        "SELECT COUNT(*) FROM duckdb_views() "
        "WHERE schema_name = 'prep' AND view_name = 'int_transactions__merged'"
    ).fetchone()
    assert view_present is not None and view_present[0] == 0, (
        "precondition: prep.int_transactions__merged must be absent for this test "
        "to exercise the CatalogException guard"
    )

    r = MerchantResolver(db)
    assert r.harvest() == HarvestResult(bound=0, conflicts=0)


def test_propose_dedups_pending_decisions(db: Database) -> None:
    """Two proposals for the same (ref_value, candidate) create exactly ONE pending row.

    N uncategorized txns sharing one unbound fuzzy entity must not stack N
    duplicate pending decisions, and a re-run must not re-propose. The dedup
    guard in ``_propose`` skips the insert when a pending, non-reversed decision
    already proposes that binding.
    """
    r = MerchantResolver(db)
    name_match = {"merchant_id": "mFuzzyDup", "strength": "fuzzy"}

    for _ in range(2):
        res = r.resolve(
            merchant_entity_id="entDup",
            source_type="plaid",
            provider_merchant_name="Star Bucks",
            name_match=name_match,
            bindings={},
            applier=MatchApplier(db, audit=AuditService(db)),
        )
        # Categorization still uses the candidate merchant on every call.
        assert res.merchant_id == "mFuzzyDup" and res.outcome == "proposed"

    pending = r._decisions.list_pending()  # pyright: ignore[reportPrivateUsage]
    dup_pending = [d for d in pending if d["ref_value"] == "entDup"]
    assert len(dup_pending) == 1, (
        f"expected exactly one pending decision for entDup; got {len(dup_pending)}"
    )


def test_rejected_decision_not_reproposed(db: Database, applier: MatchApplier) -> None:
    """A user-rejected (non-reversed) decision suppresses re-proposing the same binding.

    Regression for the M1T review finding: ``_pending_decision_exists`` matched
    only ``status='pending'``, so a rejected harvest/resolve conflict was
    re-proposed on every subsequent run and the queue never drained. The dedup
    guard must also block on a ``rejected`` decision (``reversed_at IS NULL``).
    """
    r = MerchantResolver(db)
    name_match = {"merchant_id": "mRej", "strength": "fuzzy"}

    # First contact → one pending proposal.
    r.resolve(
        merchant_entity_id="entRej",
        source_type="plaid",
        provider_merchant_name="Star Bucks",
        name_match=name_match,
        bindings={},
        applier=applier,
    )
    pending = [
        d
        for d in r._decisions.list_pending()  # pyright: ignore[reportPrivateUsage]
        if d["ref_value"] == "entRej"
    ]
    assert len(pending) == 1
    decision_id = pending[0]["decision_id"]

    # User rejects it.
    r._decisions.update_status(  # pyright: ignore[reportPrivateUsage]
        decision_id, status="rejected", decided_by="user", actor="cli"
    )

    # Re-contact the same unbound fuzzy entity → must NOT re-propose.
    r.resolve(
        merchant_entity_id="entRej",
        source_type="plaid",
        provider_merchant_name="Star Bucks",
        name_match=name_match,
        bindings={},
        applier=applier,
    )
    pending_after = [
        d
        for d in r._decisions.list_pending()  # pyright: ignore[reportPrivateUsage]
        if d["ref_value"] == "entRej"
    ]
    assert len(pending_after) == 0, (
        "a rejected candidate must not be re-proposed (queue would never drain)"
    )
