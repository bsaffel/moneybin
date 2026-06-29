"""Tests for MerchantLinksService (M1T review-queue service).

Fixture layout:
- ``seeded_pending_decision``: inserts one pending ``merchant_link_decision``
  row via ``MerchantLinkDecisionsRepo`` with ``ref_value="ent_pending"``,
  ``source_type="plaid"``, ``candidate_merchant_id="mCandidate"``.
- Tests use the function-scoped ``db`` fixture from conftest.
"""

from __future__ import annotations

import pytest

from moneybin.database import Database
from moneybin.repositories.merchant_link_decisions_repo import MerchantLinkDecisionsRepo
from moneybin.repositories.merchant_links_repo import MerchantLinksRepo
from moneybin.services.merchant_links_service import MerchantLinksService

# ---------------------------------------------------------------------------
# Seeded constants
# ---------------------------------------------------------------------------

_DECISION_ID = "d_seeded"
_REF_VALUE = "ent_pending"
_SOURCE_TYPE = "plaid"
_CANDIDATE_MERCHANT_ID = "mCandidate"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def seeded_pending_decision(db: Database) -> None:
    """Insert one pending merchant_link_decision row via the repo (Invariant-10 path)."""
    MerchantLinkDecisionsRepo(db).insert(
        decision_id=_DECISION_ID,
        ref_kind="merchant_entity_id",
        ref_value=_REF_VALUE,
        source_type=_SOURCE_TYPE,
        provider_merchant_name="Starbucks",
        candidate_merchant_id=_CANDIDATE_MERCHANT_ID,
        confidence_score=0.5,
        match_signals={"signal": "fuzzy_name", "value": "Starbucks"},
        decided_by="auto",
        actor="system",
        status="pending",
    )


# ---------------------------------------------------------------------------
# count_pending
# ---------------------------------------------------------------------------


def test_count_pending_empty(db: Database) -> None:
    """Zero pending when no decisions exist."""
    svc = MerchantLinksService(db)
    assert svc.count_pending() == 0


def test_count_pending_one(db: Database, seeded_pending_decision: None) -> None:
    """One pending after seeding a single decision."""
    svc = MerchantLinksService(db)
    assert svc.count_pending() == 1


# ---------------------------------------------------------------------------
# set — accept path
# ---------------------------------------------------------------------------


def test_set_accept_binds_and_clears_pending(
    db: Database, seeded_pending_decision: None
) -> None:
    """Accept creates the binding and clears the pending count to 0."""
    svc = MerchantLinksService(db, actor="cli")
    assert svc.count_pending() == 1

    svc.set(_DECISION_ID, target_merchant_id=_CANDIDATE_MERCHANT_ID)

    assert svc.count_pending() == 0
    # The accepted binding now exists via MerchantLinksRepo.
    bound = MerchantLinksRepo(db).lookup(_SOURCE_TYPE, _REF_VALUE)
    assert bound == _CANDIDATE_MERCHANT_ID


def test_set_accept_auto_rejects_siblings(db: Database) -> None:
    """Accept of one decision auto-rejects sibling pending decisions for same ref_value."""
    decisions = MerchantLinkDecisionsRepo(db)
    # Seed two pending decisions for the same ref_value.
    decisions.insert(
        decision_id="d_primary",
        ref_kind="merchant_entity_id",
        ref_value=_REF_VALUE,
        source_type=_SOURCE_TYPE,
        candidate_merchant_id="mPrimary",
        confidence_score=0.5,
        match_signals={"signal": "fuzzy_name", "value": "primary"},
        decided_by="auto",
        actor="system",
    )
    decisions.insert(
        decision_id="d_sibling",
        ref_kind="merchant_entity_id",
        ref_value=_REF_VALUE,
        source_type=_SOURCE_TYPE,
        candidate_merchant_id="mSibling",
        confidence_score=0.4,
        match_signals={"signal": "fuzzy_name", "value": "sibling"},
        decided_by="auto",
        actor="system",
    )

    svc = MerchantLinksService(db, actor="cli")
    assert svc.count_pending() == 1  # 2 decisions but 1 distinct ref_value

    svc.set("d_primary", target_merchant_id="mPrimary")

    assert svc.count_pending() == 0

    # Sibling must be rejected.
    sibling = decisions.fetch_by_id("d_sibling")
    assert sibling is not None
    assert sibling["status"] == "rejected"


# ---------------------------------------------------------------------------
# set — reject path
# ---------------------------------------------------------------------------


def test_set_reject_clears_pending_without_binding(
    db: Database, seeded_pending_decision: None
) -> None:
    """Reject (target_merchant_id=None) clears pending without creating a binding."""
    svc = MerchantLinksService(db, actor="cli")

    svc.set(_DECISION_ID, target_merchant_id=None)

    assert svc.count_pending() == 0
    # No binding should exist.
    assert MerchantLinksRepo(db).lookup(_SOURCE_TYPE, _REF_VALUE) is None


# ---------------------------------------------------------------------------
# history
# ---------------------------------------------------------------------------


def test_history_empty(db: Database) -> None:
    """Empty list when no decisions exist."""
    svc = MerchantLinksService(db)
    assert svc.history() == []


def test_history_returns_decisions(db: Database, seeded_pending_decision: None) -> None:
    """history() returns the seeded decision row."""
    svc = MerchantLinksService(db)
    rows = svc.history()
    assert len(rows) == 1
    assert rows[0]["decision_id"] == _DECISION_ID
