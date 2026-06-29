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
        rejected=set(),
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
        rejected=set(),
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
        rejected=set(),
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
        rejected=set(),
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
            rejected=set(),
            applier=MatchApplier(db, audit=AuditService(db)),
        )
        # Categorization still uses the candidate merchant on every call.
        assert res.merchant_id == "mFuzzyDup" and res.outcome == "proposed"

    pending = r._decisions.list_pending()  # pyright: ignore[reportPrivateUsage]
    dup_pending = [d for d in pending if d["ref_value"] == "entDup"]
    assert len(dup_pending) == 1, (
        f"expected exactly one pending decision for entDup; got {len(dup_pending)}"
    )


def test_decision_blocks_propose_scoped_to_source_type(
    db: Database,
) -> None:
    """_decision_blocks_propose is keyed on (source_type, ref_value, candidate).

    A rejected decision for (source_type='plaid', ref_value='E1', candidate='M1')
    must NOT block _propose for (source_type='simplefin', 'E1', 'M1') — the
    two source types are distinct pairings even when ref_value and candidate
    are identical.
    """
    r = MerchantResolver(db)
    # Seed a rejected decision for the plaid source.
    r._decisions.insert(  # pyright: ignore[reportPrivateUsage]
        decision_id="dbp_plaid",
        ref_kind="merchant_entity_id",
        ref_value="E1",
        source_type="plaid",
        provider_merchant_name=None,
        candidate_merchant_id="M1",
        confidence_score=0.5,
        match_signals={},
        decided_by="auto",
        actor="system",
        status="rejected",
    )

    # simplefin with the same ref_value + candidate must NOT be blocked.
    blocks = r._decision_blocks_propose(  # pyright: ignore[reportPrivateUsage]
        "E1", "simplefin", "M1"
    )
    assert not blocks, (
        "plaid-rejected decision must not block a simplefin proposal for the same "
        "(ref_value, candidate)"
    )

    # plaid itself IS blocked.
    blocks_plaid = r._decision_blocks_propose(  # pyright: ignore[reportPrivateUsage]
        "E1", "plaid", "M1"
    )
    assert blocks_plaid, "plaid decision must still be blocked after rejection"


def test_load_rejected_returns_only_rejected_non_reversed(
    db: Database,
) -> None:
    """load_rejected() returns only (source_type, ref_value, candidate) for rejected, non-reversed decisions.

    Seeds three decisions: one rejected (must be in result), one pending (must
    not be in result), and one rejected-then-reversed (must not be in result).
    """
    r = MerchantResolver(db)
    decisions = r._decisions  # pyright: ignore[reportPrivateUsage]

    # Rejected, non-reversed — should appear.
    decisions.insert(
        decision_id="lr_rejected",
        ref_kind="merchant_entity_id",
        ref_value="E_REJ",
        source_type="plaid",
        provider_merchant_name=None,
        candidate_merchant_id="M_REJ",
        confidence_score=0.5,
        match_signals={},
        decided_by="auto",
        actor="system",
        status="pending",
    )
    decisions.update_status(
        "lr_rejected", status="rejected", decided_by="user", actor="cli"
    )

    # Pending — must NOT appear.
    decisions.insert(
        decision_id="lr_pending",
        ref_kind="merchant_entity_id",
        ref_value="E_PEND",
        source_type="plaid",
        provider_merchant_name=None,
        candidate_merchant_id="M_PEND",
        confidence_score=0.5,
        match_signals={},
        decided_by="auto",
        actor="system",
        status="pending",
    )

    # Rejected then reversed — must NOT appear.
    decisions.insert(
        decision_id="lr_reversed",
        ref_kind="merchant_entity_id",
        ref_value="E_REV",
        source_type="plaid",
        provider_merchant_name=None,
        candidate_merchant_id="M_REV",
        confidence_score=0.5,
        match_signals={},
        decided_by="auto",
        actor="system",
        status="pending",
    )
    decisions.update_status(
        "lr_reversed", status="rejected", decided_by="user", actor="cli"
    )
    # Mark as reversed.
    db.execute(
        "UPDATE app.merchant_link_decisions SET reversed_at = CURRENT_TIMESTAMP, "
        "reversed_by = 'user' WHERE decision_id = 'lr_reversed'"
    )

    result = r.load_rejected()

    assert ("plaid", "E_REJ", "M_REJ") in result, (
        "rejected non-reversed must be in result"
    )
    assert ("plaid", "E_PEND", "M_PEND") not in result, "pending must not be in result"
    assert ("plaid", "E_REV", "M_REV") not in result, "reversed must not be in result"


def test_rejected_fuzzy_candidate_mints_new_merchant(
    db: Database, applier: MatchApplier
) -> None:
    """Rejected fuzzy candidate → resolve() falls through to rung 4 and mints.

    The user rejected (plaid, E1) → M1 (fuzzy). On the next resolve() call,
    the spec says: "reject → resolver mints a new merchant for the id on its
    next pass." The result must have outcome='minted', created=True, and
    merchant_id != 'M1'.
    """
    r = MerchantResolver(db)
    rejected: set[tuple[str, str, str]] = {("plaid", "E1", "M1")}
    name_match = {"merchant_id": "M1", "strength": "contains"}
    bindings: dict[tuple[str, str], str] = {}

    res = r.resolve(
        merchant_entity_id="E1",
        source_type="plaid",
        provider_merchant_name="Coffee Co",
        name_match=name_match,
        bindings=bindings,
        rejected=rejected,
        applier=applier,
    )

    assert res.outcome == "minted", f"expected minted, got {res.outcome!r}"
    assert res.created is True
    assert res.merchant_id != "M1", (
        "must mint a NEW merchant, not return the rejected one"
    )
    # The minted merchant must be bound in the cache so later txns with E1 adopt it.
    assert ("plaid", "E1") in bindings, "binding must be recorded"
    assert bindings[("plaid", "E1")] == res.merchant_id


def test_rejected_exact_candidate_also_mints(
    db: Database, applier: MatchApplier
) -> None:
    """Rejected exact-match candidate → resolve() also falls through to rung 4 (mint).

    A rejection overrides BOTH fuzzy and exact candidates — the user said
    "not this merchant"; minting a distinct entity is the spec-faithful response.
    """
    r = MerchantResolver(db)
    rejected: set[tuple[str, str, str]] = {("plaid", "E2", "M2")}
    name_match = {"merchant_id": "M2", "strength": "exact"}

    res = r.resolve(
        merchant_entity_id="E2",
        source_type="plaid",
        provider_merchant_name="Exact Match Co",
        name_match=name_match,
        bindings={},
        rejected=rejected,
        applier=applier,
    )

    assert res.outcome == "minted", (
        f"expected minted for rejected exact, got {res.outcome!r}"
    )
    assert res.created is True
    assert res.merchant_id != "M2"


def test_non_rejected_fuzzy_still_proposes(db: Database, applier: MatchApplier) -> None:
    """Non-rejected fuzzy match still proposes (regression guard — current behavior preserved)."""
    r = MerchantResolver(db)
    name_match = {"merchant_id": "M3", "strength": "fuzzy"}

    res = r.resolve(
        merchant_entity_id="E3",
        source_type="plaid",
        provider_merchant_name="Fuzzy Co",
        name_match=name_match,
        bindings={},
        rejected=set(),  # nothing rejected
        applier=applier,
    )

    assert res.outcome == "proposed"
    assert res.merchant_id == "M3"


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
        rejected=set(),
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

    # Re-contact with empty rejected set — the _decision_blocks_propose dedup
    # guard (DB-level) must still prevent re-proposing the same candidate.
    r.resolve(
        merchant_entity_id="entRej",
        source_type="plaid",
        provider_merchant_name="Star Bucks",
        name_match=name_match,
        bindings={},
        rejected=set(),
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
